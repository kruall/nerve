"""Durable physical-host lease records.

The partial unique index installed by v044 is the final arbiter: overlapping
pools may name a host more than once, but only one active exclusive lease can
exist for its physical host.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Mapping
from typing import Any

from nerve.utils.time import utc_now_iso


_HANDLE_ACTIVE = ("active", "releasing", "quarantined")
_WAIT_OUTCOMES = frozenset({
    "LEASE_GRANTED", "HOST_PERMANENTLY_UNAVAILABLE", "REQUEST_CANCELLED",
    "DEADLOCK_REPLAN_REQUIRED",
})


def _row(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    value = dict(row)
    for key in ("labels", "capabilities", "member_ids"):
        if key in value:
            try:
                value[key] = json.loads(value[key])
            except (TypeError, ValueError):
                value[key] = {} if key != "member_ids" else []
    if "requested_hosts_json" in value:
        try:
            value["requested_hosts"] = json.loads(value["requested_hosts_json"])
        except (TypeError, ValueError):
            value["requested_hosts"] = []
    return value


class ResourceStore:
    async def _allocate_resource_queue_ticket(self) -> int:
        async with self.db.execute("UPDATE resource_wait_allocator SET next_ticket=next_ticket+1 WHERE singleton=1 RETURNING next_ticket-1") as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError("resource queue ticket allocator is unavailable")
        return int(row[0])

    # Retained handles deliberately live beside leases rather than changing the
    # lease lifecycle: an Operation terminal transition only removes its ref.
    async def get_session_resource_handle(self, handle_id: str) -> dict[str, Any] | None:
        async with self.db.execute("SELECT * FROM session_resource_handles WHERE id=?", (handle_id,)) as c:
            return _row(await c.fetchone())

    async def list_session_resource_handles(self, session_id: str, *, states: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
        marks = " AND state IN (%s)" % ",".join("?" for _ in states) if states else ""
        async with self.db.execute(f"SELECT * FROM session_resource_handles WHERE session_id=?{marks} ORDER BY created_at, id", (session_id, *(states or ()))) as c:
            return [_row(row) async for row in c]

    async def create_session_resource_handle(self, handle: Mapping[str, Any]) -> dict[str, Any]:
        now = utc_now_iso()
        await self._write("""INSERT INTO session_resource_handles
            (id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (handle["id"], handle["session_id"], handle["pool"], handle["host_id"], handle["lease_id"], handle["fencing_token"], handle.get("state", "active"), now, now))
        row = await self.get_session_resource_handle(str(handle["id"])); assert row is not None
        return row

    async def create_session_resource_handles(self, handles: list[Mapping[str, Any]]) -> None:
        """Atomically retain a newly granted handle bundle.

        Leases are allocated separately by the allocator.  Keeping all handle
        rows in one transaction means a failed retention never makes a partial
        bundle externally usable; the caller then releases every new lease.
        """
        now = utc_now_iso()
        async with self._atomic():
            for handle in handles:
                await self.db.execute("""INSERT INTO session_resource_handles
                    (id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""", (
                    handle["id"], handle["session_id"], handle["pool"], handle["host_id"],
                    handle["lease_id"], handle["fencing_token"], now, now,
                ))

    async def update_session_resource_handle(self, handle_id: str, *, expected_state: str, state: str, release_reason: str | None = None) -> bool:
        if state not in {"active", "releasing", "released", "quarantined"}:
            raise ValueError("invalid handle state")
        now = utc_now_iso()
        result = await self._write("""UPDATE session_resource_handles SET state=?, release_reason=?, updated_at=?,
            released_at=CASE WHEN ? IN ('released', 'quarantined') THEN COALESCE(released_at, ?) ELSE NULL END
            WHERE id=? AND state=?""", (state, release_reason, now, state, now, handle_id, expected_state))
        return bool(result.rowcount)

    async def delete_session_resource_handle(self, handle_id: str) -> bool:
        return bool((await self._write("DELETE FROM session_resource_handles WHERE id=?", (handle_id,))).rowcount)

    async def attach_operation_resource_ref(self, operation_id: str, handle_id: str) -> bool:
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                "SELECT 1 FROM session_resource_handles WHERE id=? AND state='active'",
                (handle_id,),
            ) as c:
                if await c.fetchone() is None:
                    return False
            try:
                await self.db.execute(
                    """INSERT INTO operation_resource_refs(operation_id, handle_id, created_at)
                       VALUES (?, ?, ?)""",
                    (operation_id, handle_id, utc_now_iso()),
                )
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint failed: operation_resource_refs" not in str(exc):
                    raise
                return False
            return True

    async def detach_operation_resource_refs(self, operation_id: str) -> int:
        return (await self._write("DELETE FROM operation_resource_refs WHERE operation_id=?", (operation_id,))).rowcount

    async def list_operation_resource_refs(self, operation_id: str) -> list[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM operation_resource_refs WHERE operation_id=? ORDER BY created_at, handle_id", (operation_id,)) as c:
            return [dict(row) async for row in c]

    async def list_handle_operation_refs(self, handle_id: str) -> list[dict[str, Any]]:
        """Read the durable quiescence proof for a retained handle."""
        async with self.db.execute(
            "SELECT * FROM operation_resource_refs WHERE handle_id=? ORDER BY operation_id",
            (handle_id,),
        ) as c:
            return [dict(row) async for row in c]

    async def begin_release_handle(
        self, session_id: str, handle_id: str, intent: Mapping[str, Any],
    ) -> tuple[str | None, list[str]]:
        """Atomically fence an idle handle and record its release intent.

        The immediate transaction makes the active-state/ref check and the
        state transition one serialization point with operation attachment.
        A non-empty conflict result is read-only; a non-active handle returns
        no intent id and performs no intent write.
        """
        now = utc_now_iso()
        async with self._atomic():
            await self.db.execute("BEGIN IMMEDIATE")
            async with self.db.execute(
                """SELECT state FROM session_resource_handles
                   WHERE id=? AND session_id=?""", (handle_id, session_id),
            ) as c:
                handle = await c.fetchone()
            if handle is None or handle["state"] != "active":
                return None, []

            async with self.db.execute(
                """SELECT operation_id FROM operation_resource_refs
                   WHERE handle_id=? ORDER BY operation_id""", (handle_id,),
            ) as c:
                conflicts = [str(row[0]) async for row in c]
            if conflicts:
                return None, conflicts

            result = await self.db.execute(
                """UPDATE session_resource_handles SET state='releasing',
                       release_reason=NULL, updated_at=?
                   WHERE id=? AND session_id=? AND state='active'""",
                (now, handle_id, session_id),
            )
            if result.rowcount != 1:
                return None, []
            await self.db.execute(
                """INSERT INTO resource_recovery_intents
                   (id, kind, session_id, handle_id, operation_id, payload_json,
                    state, created_at, updated_at)
                   VALUES (?, 'release', ?, ?, ?, ?, 'processing', ?, ?)""",
                (intent["id"], session_id, handle_id, intent.get("operation_id"),
                 json.dumps(intent.get("payload", {})), now, now),
            )
            return str(intent["id"]), []

    async def session_resource_is_live(self, session_id: str) -> bool:
        """Durable portion of the single retained-resource liveness predicate.

        ``pending`` and ``claimed`` are respectively resume-pending and
        resuming.  A pending resource wait is included independently so a
        durable lease acquisition cannot be released merely because its
        execution transition is between states.
        """
        async with self.db.execute(
            """SELECT EXISTS(
                 SELECT 1 FROM sessions
                  WHERE id=? AND status='active'
                 UNION ALL
                 SELECT 1 FROM executions
                  WHERE session_id=? AND (
                    status IN ('queued','starting','running','cancelling')
                    OR continuation_state IN ('pending','claimed')
                  )
                 UNION ALL
                 SELECT 1 FROM resource_wait_operations
                  WHERE session_id=? AND state='pending'
               )""",
            (session_id, session_id, session_id),
        ) as c:
            row = await c.fetchone()
        return bool(row[0])

    async def get_resource_wait_operation(self, wait_id: str) -> dict[str, Any] | None:
        async with self.db.execute("SELECT * FROM resource_wait_operations WHERE id=?", (wait_id,)) as c:
            return _row(await c.fetchone())

    async def list_resource_wait_operations(self, *, state: str | None = None) -> list[dict[str, Any]]:
        where, params = ("WHERE state=?", (state,)) if state else ("", ())
        async with self.db.execute(f"SELECT * FROM resource_wait_operations {where} ORDER BY queue_ticket, id", params) as c:
            return [_row(row) async for row in c]

    async def find_pending_resource_wait_operation(self, operation_id: str) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM resource_wait_operations WHERE operation_id=? AND state='pending' ORDER BY created_at LIMIT 1",
            (operation_id,),
        ) as c:
            return _row(await c.fetchone())

    async def create_resource_wait_operation(self, wait: Mapping[str, Any]) -> dict[str, Any]:
        if wait.get("outcome") is not None and wait["outcome"] not in _WAIT_OUTCOMES:
            raise ValueError("invalid resource wait outcome")
        now = utc_now_iso()
        # Tickets are allocator-owned, not clock-derived.  A replan may pass
        # its old positive ticket, while ordinary subscriptions get the next
        # durable value even across restart.
        async with self._atomic():
            ticket = int(wait.get("queue_ticket") or 0)
            if ticket <= 0:
                ticket = await self._allocate_resource_queue_ticket()
            else:
                await self.db.execute(
                    "UPDATE resource_wait_allocator SET next_ticket=MAX(next_ticket, ?) WHERE singleton=1",
                    (ticket + 1,),
                )
            await self.db.execute("""INSERT INTO resource_wait_operations
                (id, session_id, operation_id, request_kind, requested_hosts_json, pool, queue_ticket, state, outcome, wakeup_generation, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (wait["id"], wait["session_id"], wait["operation_id"], wait["request_kind"], json.dumps(wait.get("requested_hosts", [])), wait["pool"], ticket, wait.get("state", "pending"), wait.get("outcome"), wait.get("wakeup_generation", 0), now, now))
        row = await self.get_resource_wait_operation(str(wait["id"])); assert row is not None
        return row

    async def replan_resource_wait_operation(self, *, wait_id: str, operation_id: str) -> dict[str, Any] | None:
        """Clone a deadlock victim's complete bundle and ticket in one commit."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                "SELECT * FROM resource_wait_operations WHERE id=? AND outcome='DEADLOCK_REPLAN_REQUIRED'",
                (wait_id,),
            ) as c:
                previous = await c.fetchone()
            if previous is None:
                return None
            async with self.db.execute(
                "SELECT session_id, status FROM executions WHERE id=?", (operation_id,),
            ) as c:
                operation = await c.fetchone()
            if (operation is None or operation["session_id"] != previous["session_id"]
                    or operation["status"] not in {"queued", "starting", "running", "cancelling"}):
                return None
            async with self.db.execute(
                "SELECT id FROM resource_wait_operations WHERE operation_id=? AND state='pending'", (operation_id,),
            ) as c:
                existing = await c.fetchone()
            if existing is not None:
                return None
            new_id = f"wait-{uuid.uuid4().hex}"
            await self.db.execute("""INSERT INTO resource_wait_operations
                (id, session_id, operation_id, request_kind, requested_hosts_json, pool, queue_ticket, state, outcome, wakeup_generation, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', NULL, 0, ?, ?)""",
                (new_id, previous["session_id"], operation_id, previous["request_kind"],
                 previous["requested_hosts_json"], previous["pool"], previous["queue_ticket"], now, now),
            )
        return await self.get_resource_wait_operation(new_id)

    async def update_resource_wait_operation(self, wait_id: str, *, expected_state: str, state: str, outcome: str | None = None) -> bool:
        if state not in {"pending", "granted", "cancelled", "failed"}:
            raise ValueError("invalid wait state")
        if outcome is not None and outcome not in _WAIT_OUTCOMES:
            raise ValueError("invalid resource wait outcome")
        now = utc_now_iso()
        result = await self._write("""UPDATE resource_wait_operations SET state=?, outcome=?, updated_at=?,
            settled_at=CASE WHEN ?='pending' THEN NULL ELSE COALESCE(settled_at, ?) END WHERE id=? AND state=?""", (state, outcome, now, state, now, wait_id, expected_state))
        return bool(result.rowcount)

    async def delete_resource_wait_operation(self, wait_id: str) -> bool:
        return bool((await self._write("DELETE FROM resource_wait_operations WHERE id=?", (wait_id,))).rowcount)

    async def get_resource_recovery_intent(self, intent_id: str) -> dict[str, Any] | None:
        async with self.db.execute("SELECT * FROM resource_recovery_intents WHERE id=?", (intent_id,)) as c:
            return _row(await c.fetchone())

    async def list_resource_recovery_intents(self, *, state: str | None = None) -> list[dict[str, Any]]:
        where, params = ("WHERE state=?", (state,)) if state else ("", ())
        async with self.db.execute(f"SELECT * FROM resource_recovery_intents {where} ORDER BY created_at, id", params) as c:
            return [_row(row) async for row in c]

    async def create_resource_recovery_intent(self, intent: Mapping[str, Any]) -> dict[str, Any]:
        now = utc_now_iso()
        await self._write("""INSERT INTO resource_recovery_intents
            (id, kind, session_id, handle_id, operation_id, payload_json, state, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (intent["id"], intent["kind"], intent.get("session_id"), intent.get("handle_id"), intent.get("operation_id"), json.dumps(intent.get("payload", {})), intent.get("state", "prepared"), now, now))
        row = await self.get_resource_recovery_intent(str(intent["id"])); assert row is not None
        return row

    async def update_resource_recovery_intent(self, intent_id: str, *, expected_state: str, state: str) -> bool:
        if state not in {"prepared", "processing", "completed", "failed"}:
            raise ValueError("invalid recovery intent state")
        now = utc_now_iso()
        result = await self._write("""UPDATE resource_recovery_intents SET state=?, updated_at=?,
            completed_at=CASE WHEN ? IN ('completed', 'failed') THEN COALESCE(completed_at, ?) ELSE NULL END
            WHERE id=? AND state=?""", (state, now, state, now, intent_id, expected_state))
        return bool(result.rowcount)

    async def delete_resource_recovery_intent(self, intent_id: str) -> bool:
        return bool((await self._write("DELETE FROM resource_recovery_intents WHERE id=?", (intent_id,))).rowcount)

    async def terminalize_resource_wait_operation(
        self, *, wait_id: str, outcome: str, handles: list[Mapping[str, Any]] | None = None,
    ) -> bool:
        """Win one wait generation and publish its one durable continuation.

        The handle inserts precede the outbox state in this transaction.  Thus a
        claimed continuation can always resolve every newly granted handle.
        """
        if outcome not in _WAIT_OUTCOMES:
            raise ValueError("invalid resource wait outcome")
        handles = handles or []
        if (outcome == "LEASE_GRANTED") != bool(handles):
            raise ValueError("a lease grant must include handles")
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                "SELECT state, operation_id, wakeup_generation FROM resource_wait_operations WHERE id=?",
                (wait_id,),
            ) as c:
                wait = await c.fetchone()
            if wait is None:
                return False
            if wait["state"] != "pending":
                async with self.db.execute("SELECT outcome FROM resource_wait_operations WHERE id=?", (wait_id,)) as c:
                    settled = await c.fetchone()
                return settled is not None and settled["outcome"] == outcome
            lease_ids = [str(handle["lease_id"]) for handle in handles]
            handle_ids = [str(handle["id"]) for handle in handles]
            if len(lease_ids) != len(set(lease_ids)) or len(handle_ids) != len(set(handle_ids)):
                return False
            for handle in handles:
                async with self.db.execute(
                    "SELECT id FROM session_resource_handles WHERE lease_id=? AND state IN (?, ?, ?)",
                    (handle["lease_id"], *_HANDLE_ACTIVE),
                ) as c:
                    active_handle = await c.fetchone()
                if active_handle is not None and str(active_handle["id"]) != str(handle["id"]):
                    return False
            status = "succeeded" if outcome == "LEASE_GRANTED" else (
                "cancelled" if outcome == "REQUEST_CANCELLED" else "failed"
            )
            generation = int(wait["wakeup_generation"]) + 1
            result = json.dumps({"outcome": outcome, "wait_id": wait_id, "generation": generation})
            async with self.db.execute(
                "SELECT status FROM executions WHERE id=? AND status IN ('queued','starting','running','cancelling')",
                (wait["operation_id"],),
            ) as c:
                if await c.fetchone() is None:
                    return False
            for handle in handles:
                await self.db.execute(
                    """INSERT INTO session_resource_handles
                       (id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
                    (handle["id"], handle["session_id"], handle["pool"], handle["host_id"],
                     handle["lease_id"], handle["fencing_token"], now, now),
                )

            await self.db.execute(
                """UPDATE resource_wait_operations
                   SET state=?, outcome=?, wakeup_generation=wakeup_generation+1,
                       settled_at=?, updated_at=?
                   WHERE id=? AND state='pending'""",
                ("granted" if outcome == "LEASE_GRANTED" else "cancelled" if outcome == "REQUEST_CANCELLED" else "failed",
                 outcome, now, now, wait_id),
            )
            await self.db.execute(
                """UPDATE executions
                   SET status=?, result=?, finished_at=?, continuation_state = 'pending',
                       continuation_error = NULL,
                       updated_at = ?,
                       revision = revision + 1
                   WHERE id=? AND status IN ('queued','starting','running','cancelling')""",
                (status, result, now, now, wait["operation_id"]),
            )
            await self.db.execute(
                """UPDATE resource_lease_requests SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""",
                (now, wait["operation_id"]),
            )
            await self.db.execute(
                """UPDATE resource_lease_bundles SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""",
                (now, wait["operation_id"]),
            )
            await self.db.execute(
                "DELETE FROM operation_resource_refs WHERE operation_id=?",
                (wait["operation_id"],),
            )
        return True

    async def commit_handle_grant(self, *, wait_id: str, handle: Mapping[str, Any]) -> bool:
        """Compatibility wrapper for the R5 single-handle caller."""
        return await self.terminalize_resource_wait_operation(
            wait_id=wait_id, outcome="LEASE_GRANTED", handles=[handle],
        )

    async def commit_operation_terminal(self, *, operation_id: str, status: str, result: Mapping[str, Any]) -> bool:
        """Settle one Operation and wake its owner, intentionally retaining all handles."""
        if status not in {"succeeded", "failed", "cancelled", "lost"}:
            raise ValueError("terminal execution status required")
        now = utc_now_iso()
        async with self._atomic():
            won = await self.db.execute("""UPDATE executions SET status=?, result=?, finished_at=?, updated_at=?, revision=revision+1,
                continuation_state=CASE WHEN auto_continue=1 THEN 'pending' ELSE 'suppressed' END WHERE id=? AND status IN ('queued','starting','running','cancelling')""", (status, json.dumps(dict(result)), now, now, operation_id))
            if not won.rowcount:
                async with self.db.execute("SELECT status FROM executions WHERE id=?", (operation_id,)) as c:
                    row = await c.fetchone()
                return row is not None and row["status"] in {"succeeded", "failed", "cancelled", "lost"}
            await self.db.execute("DELETE FROM operation_resource_refs WHERE operation_id=?", (operation_id,))
        return True
    async def get_session_resource_reservation(self, session_id: str) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM session_resource_reservations WHERE session_id=?", (session_id,)
        ) as cursor:
            return _row(await cursor.fetchone())

    async def create_session_resource_reservation(
        self, *, session_id: str, pool: str, worktree_identity: str, lease: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist a reservation only after its normal exclusive lease exists."""
        now = utc_now_iso()
        async with self._atomic():
            insert = await self.db.execute(
                """INSERT INTO session_resource_reservations
                   (session_id, pool, host_id, lease_id, worktree_identity, state, created_at)
                   VALUES (?, ?, ?, ?, ?, 'active', ?)
                   ON CONFLICT(session_id) DO UPDATE SET
                     pool=excluded.pool,
                     host_id=excluded.host_id,
                     lease_id=excluded.lease_id,
                     worktree_identity=excluded.worktree_identity,
                     state='active',
                     created_at=excluded.created_at,
                     released_at=NULL,
                     quarantine_reason=NULL
                   WHERE session_resource_reservations.state='released'""",
                (session_id, pool, lease["host_id"], lease["id"], worktree_identity, now),
            )
            if not insert.rowcount:
                raise ValueError("session reservation already exists and is not replaceable")
            await self.db.execute(
                """INSERT INTO resource_events(event_type, host_id, execution_id, lease_id, detail, created_at)
                   VALUES ('session_reservation_acquired', ?, ?, ?, ?, ?)""",
                (lease["host_id"], lease["execution_id"], lease["id"], session_id, now),
            )
        row = await self.get_session_resource_reservation(session_id)
        assert row is not None
        return row

    async def settle_session_resource_reservation(
        self, *, session_id: str, state: str, reason: str | None = None,
    ) -> bool:
        if state not in {"released", "quarantined"}:
            raise ValueError("invalid reservation state")
        result = await self._write(
            """UPDATE session_resource_reservations
               SET state=?, released_at=?, quarantine_reason=?
               WHERE session_id=? AND state='active'""",
            (state, utc_now_iso(), reason[:500] if reason else None, session_id),
        )
        return bool(result.rowcount)

    async def list_active_session_resource_reservations(self) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT * FROM session_resource_reservations WHERE state='active' ORDER BY created_at"
        ) as cursor:
            return [_row(row) async for row in cursor]

    async def quarantine_active_session_reservation(self, session_id: str, *, reason: str) -> bool:
        """Last-resort DB lifecycle guard used when a session is deleted."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                "SELECT host_id, lease_id FROM session_resource_reservations WHERE session_id=? AND state='active'",
                (session_id,),
            ) as cursor:
                reservation = await cursor.fetchone()
            if reservation is None:
                return False
            updated = await self.db.execute(
                """UPDATE resource_leases SET state='quarantined', revoking_at=COALESCE(revoking_at, ?), quarantine_reason=?
                   WHERE id=? AND state IN ('active','revoking')""",
                (now, reason[:500], reservation["lease_id"]),
            )
            if not updated.rowcount:
                return False
            await self.db.execute(
                "UPDATE resource_hosts SET quarantined=1, quarantine_reason=?, updated_at=? WHERE id=?",
                (reason[:500], now, reservation["host_id"]),
            )
        return True

    async def enqueue_resource_request(self, *, request_id: str, execution_id: str,
                                       session_id: str, slot: str, pool: str,
                                       requested_host: str | None = None) -> dict[str, Any]:
        now = utc_now_iso()
        try:
            async with self._atomic():
                ticket = await self._allocate_resource_queue_ticket()
                await self.db.execute(
                """INSERT INTO resource_lease_requests
                   (id, execution_id, session_id, slot, pool, mode, state, requested_at, requested_host, queue_ticket)
                   VALUES (?, ?, ?, ?, ?, 'exclusive', 'queued', ?, ?, ?)""",
                (request_id, execution_id, session_id, slot, pool, now, requested_host, ticket),
                )
        except sqlite3.IntegrityError:
            pass
        async with self.db.execute(
            """SELECT * FROM resource_lease_requests
               WHERE execution_id=? AND slot=? AND state='queued'""",
            (execution_id, slot),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise ValueError("resource request could not be queued")
        return dict(row)

    async def cancel_resource_requests(self, execution_id: str) -> int:
        now = utc_now_iso()
        async with self._atomic():
            result = await self.db.execute(
                """UPDATE resource_lease_requests SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""",
                (now, execution_id),
            )
            # v052 bundles are stateful so restart/cancellation cannot revive
            # an abandoned multi-slot request.  Old one-slot rows simply have
            # a NULL bundle id and are unaffected.
            await self.db.execute(
                """UPDATE resource_lease_bundles SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""", (now, execution_id)
            )
        return result.rowcount

    async def requeue_resource_requests(self, execution_id: str) -> int:
        """Restore pre-backend acquisitions interrupted by daemon shutdown."""
        async with self._atomic():
            result = await self.db.execute(
                """UPDATE resource_lease_requests
                   SET state='queued', lease_id=NULL, settled_at=NULL
                   WHERE execution_id=? AND state='acquired'""",
                (execution_id,),
            )
            await self.db.execute(
                """UPDATE resource_lease_bundles SET state='queued', settled_at=NULL
                   WHERE execution_id=? AND state='acquired'""", (execution_id,)
            )
        return result.rowcount

    async def settle_resource_bundles(self, execution_id: str) -> None:
        await self._write(
            """UPDATE resource_lease_bundles SET state='released', settled_at=?
               WHERE execution_id=? AND state='acquired'""",
            (utc_now_iso(), execution_id),
        )

    async def enqueue_resource_bundle(self, *, bundle_id: str, execution_id: str,
                                      session_id: str, requests: list[Mapping[str, Any]],
                                      queue_ticket: int | None = None) -> dict[str, Any]:
        """Create a durable bundle, or reuse this execution's queued bundle."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                """SELECT id FROM resource_lease_bundles
                   WHERE execution_id=? AND state='queued'
                   ORDER BY requested_at, id LIMIT 1""",
                (execution_id,),
            ) as cursor:
                existing = await cursor.fetchone()
            if existing is not None:
                existing_id = str(existing[0])
                async with self.db.execute(
                    """SELECT id, slot, pool, requested_host, queue_ticket FROM resource_lease_requests
                       WHERE bundle_id=? AND state='queued' ORDER BY sequence""",
                    (existing_id,),
                ) as cursor:
                    existing_requests = [dict(row) async for row in cursor]
                expected = [(str(item["slot"]), str(item["pool"]), item.get("requested_host")) for item in requests]
                actual = [(str(item["slot"]), str(item["pool"]), item.get("requested_host")) for item in existing_requests]
                if actual != expected:
                    raise ValueError("queued resource bundle does not match execution plan")
                return {"id": existing_id, "requests": existing_requests}
            if queue_ticket is None:
                ticket = await self._allocate_resource_queue_ticket()
            else:
                ticket = int(queue_ticket)
                await self.db.execute("UPDATE resource_wait_allocator SET next_ticket=MAX(next_ticket, ?) WHERE singleton=1", (ticket + 1,))
            stored_requests = [{**request, "queue_ticket": ticket} for request in requests]
            await self.db.execute(
                "INSERT INTO resource_lease_bundles (id, execution_id, session_id, state, requested_at) VALUES (?, ?, ?, 'queued', ?)",
                (bundle_id, execution_id, session_id, now),
            )
            for request in stored_requests:
                await self.db.execute(
                    """INSERT INTO resource_lease_requests
                       (id, execution_id, session_id, slot, pool, mode, state, requested_at, bundle_id, requested_host, queue_ticket)
                       VALUES (?, ?, ?, ?, ?, 'exclusive', 'queued', ?, ?, ?, ?)""",
                    (request["id"], execution_id, session_id, request["slot"], request["pool"], now, bundle_id, request.get("requested_host"), ticket),
                )
        return {"id": bundle_id, "requests": stored_requests}

    async def try_acquire_resource_bundle(self, *, bundle_id: str,
                                          candidates: Mapping[str, list[str]], ttl_seconds: int,
                                          enforce_pool_fifo: bool = True) -> list[dict[str, Any]] | None:
        """Allocate every bundle slot or none, preserving per-pool FIFO.

        The matching is deliberately deterministic: slots are sorted by their
        durable queue sequence and candidate host ids are inventory-sorted.
        """
        from datetime import datetime, timedelta, timezone
        now = utc_now_iso(); expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        async with self._atomic():
            async with self.db.execute("SELECT * FROM resource_lease_bundles WHERE id=? AND state='queued'", (bundle_id,)) as c:
                bundle = await c.fetchone()
            if bundle is None: return None
            async with self.db.execute("SELECT * FROM resource_lease_requests WHERE bundle_id=? AND state='queued' ORDER BY sequence", (bundle_id,)) as c:
                rows = [dict(row) async for row in c]
            if not rows: return None
            ids = {row['id'] for row in rows}
            # A bundle may pass a pool only when every older queued request in
            # that pool is part of this same bundle (needed for two same-pool slots).
            if enforce_pool_fifo:
                for pool in {row['pool'] for row in rows}:
                    async with self.db.execute("SELECT id FROM resource_lease_requests WHERE pool=? AND state='queued' ORDER BY sequence", (pool,)) as c:
                        queued = [row[0] async for row in c]
                    if any(request_id not in ids for request_id in queued[:sum(r['pool'] == pool for r in rows)]):
                        return None
            available: dict[str, int] = {}
            for host_id in sorted({host for row in rows for host in candidates.get(row['id'], [])}):
                async with self.db.execute("""SELECT fencing_token FROM resource_hosts h WHERE id=? AND enabled=1 AND draining=0 AND offline=0 AND quarantined=0 AND NOT EXISTS (SELECT 1 FROM resource_leases l WHERE l.host_id=h.id AND l.state IN ('active','revoking','quarantined'))""", (host_id,)) as c:
                    host = await c.fetchone()
                if host is not None: available[host_id] = int(host[0])
            assignment: dict[str, str] = {}
            def match(index: int, used: set[str]) -> bool:
                if index == len(rows): return True
                row = rows[index]
                for host_id in sorted(candidates.get(row['id'], [])):
                    if host_id in available and host_id not in used:
                        assignment[row['id']] = host_id
                        if match(index + 1, used | {host_id}): return True
                assignment.pop(row['id'], None)
                return False
            if not match(0, set()): return None
            for row in rows:
                host_id = assignment[row['id']]; token = available[host_id] + 1
                changed = await self.db.execute("UPDATE resource_hosts SET fencing_token=?, updated_at=? WHERE id=? AND fencing_token=?", (token, now, host_id, available[host_id]))
                if not changed.rowcount: return None
                lease_id = row['lease_id'] or f"lease-{row['id'].removeprefix('request-')}"
                await self.db.execute("""INSERT INTO resource_leases (id, host_id, execution_id, session_id, pool, fencing_token, state, requested_at, acquired_at, heartbeat_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)""", (lease_id, host_id, row['execution_id'], row['session_id'], row['pool'], token, row['requested_at'], now, now, expires))
                await self.db.execute("UPDATE resource_lease_requests SET state='acquired', lease_id=?, settled_at=? WHERE id=?", (lease_id, now, row['id']))
                await self.db.execute("INSERT INTO resource_events(event_type, host_id, execution_id, lease_id, detail, created_at) VALUES ('lease_acquired', ?, ?, ?, ?, ?)", (host_id, row['execution_id'], lease_id, bundle_id, now))
            await self.db.execute("UPDATE resource_lease_bundles SET state='acquired', settled_at=? WHERE id=?", (now, bundle_id))
        return [await self.get_resource_lease(row['lease_id'] or f"lease-{row['id'].removeprefix('request-')}") for row in rows]

    async def try_acquire_resource_request(
        self, *, request_id: str, lease_id: str, host_ids: list[str],
        ttl_seconds: int,
    ) -> dict[str, Any] | None:
        """Atomically grant only the oldest queued request for its pool."""
        from datetime import datetime, timedelta, timezone

        now = utc_now_iso()
        expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        async with self._atomic():
            async with self.db.execute(
                "SELECT * FROM resource_lease_requests WHERE id=? AND state='queued'",
                (request_id,),
            ) as cursor:
                request = await cursor.fetchone()
            if request is None:
                return None
            async with self.db.execute(
                """SELECT id FROM resource_lease_requests
                   WHERE pool=? AND state='queued' ORDER BY sequence LIMIT 1""",
                (request["pool"],),
            ) as cursor:
                head = await cursor.fetchone()
            if head is None or head[0] != request_id:
                return None
            for host_id in host_ids:
                async with self.db.execute(
                    """SELECT fencing_token FROM resource_hosts h
                       WHERE id=? AND enabled=1 AND draining=0 AND offline=0
                         AND quarantined=0 AND NOT EXISTS (
                           SELECT 1 FROM resource_leases l WHERE l.host_id=h.id
                           AND l.state IN ('active','revoking','quarantined'))""",
                    (host_id,),
                ) as cursor:
                    host = await cursor.fetchone()
                if host is None:
                    continue
                token = int(host[0]) + 1
                await self.db.execute(
                    "UPDATE resource_hosts SET fencing_token=?, updated_at=? WHERE id=? AND fencing_token=?",
                    (token, now, host_id, host[0]),
                )
                try:
                    await self.db.execute(
                        """INSERT INTO resource_leases
                           (id, host_id, execution_id, session_id, pool, fencing_token,
                            state, requested_at, acquired_at, heartbeat_at, expires_at)
                           VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)""",
                        (lease_id, host_id, request["execution_id"], request["session_id"],
                         request["pool"], token, request["requested_at"], now, now, expires),
                    )
                except sqlite3.IntegrityError:
                    continue
                await self.db.execute(
                    """UPDATE resource_lease_requests
                       SET state='acquired', lease_id=?, settled_at=?
                       WHERE id=? AND state='queued'""",
                    (lease_id, now, request_id),
                )
                await self.db.execute(
                    """INSERT INTO resource_events
                       (event_type, host_id, execution_id, lease_id, created_at)
                       VALUES ('lease_acquired', ?, ?, ?, ?)""",
                    (host_id, request["execution_id"], lease_id, now),
                )
                break
            else:
                return None
        return await self.get_resource_lease(lease_id)

    async def heartbeat_resource_lease(self, *, lease_id: str, execution_id: str,
                                       fencing_token: int, ttl_seconds: int) -> bool:
        from datetime import datetime, timedelta, timezone
        expires = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        result = await self._write(
            """UPDATE resource_leases SET heartbeat_at=?, expires_at=?
               WHERE id=? AND execution_id=? AND fencing_token=? AND state='active'""",
            (utc_now_iso(), expires, lease_id, execution_id, fencing_token),
        )
        return bool(result.rowcount)

    async def revoke_expired_resource_leases(self) -> list[dict[str, Any]]:
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                "SELECT id FROM resource_leases WHERE state='active' AND expires_at IS NOT NULL AND expires_at<=?",
                (now,),
            ) as cursor:
                ids = [str(row[0]) async for row in cursor]
            if ids:
                marks = ",".join("?" for _ in ids)
                await self.db.execute(
                    f"UPDATE resource_leases SET state='revoking', revoking_at=? WHERE id IN ({marks}) AND state='active'",
                    (now, *ids),
                )
                for lease_id in ids:
                    await self.db.execute(
                        """INSERT INTO resource_events(event_type, lease_id, created_at)
                           VALUES ('lease_expired_revoking', ?, ?)""",
                        (lease_id, now),
                    )
        rows = []
        for lease_id in ids:
            row = await self.get_resource_lease(lease_id)
            if row is not None:
                rows.append(row)
        return rows

    async def list_resource_requests(self) -> list[dict[str, Any]]:
        async with self.db.execute(
            """SELECT * FROM resource_lease_requests
               WHERE state='queued' ORDER BY queue_ticket, sequence"""
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        positions: dict[str, int] = {}
        for row in rows:
            pool = str(row["pool"])
            positions[pool] = positions.get(pool, 0) + 1
            row["position"] = positions[pool]
        return rows

    async def list_resource_events(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT * FROM resource_events ORDER BY sequence DESC LIMIT ?",
            (max(1, min(limit, 500)),),
        ) as cursor:
            return [dict(row) async for row in cursor]
    async def seed_resource_host(self, host: Mapping[str, Any]) -> None:
        now = utc_now_iso()
        await self._write(
            """INSERT INTO resource_hosts (id, connection_ref, display_name, labels, capabilities, enabled, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET connection_ref=excluded.connection_ref, display_name=excluded.display_name,
                 labels=excluded.labels, capabilities=excluded.capabilities, enabled=excluded.enabled, updated_at=excluded.updated_at""",
            (host["id"], host["connection_ref"], host.get("display_name", host["id"]),
             json.dumps(host.get("labels", {})), json.dumps(host.get("capabilities", {})), int(bool(host.get("enabled", True))), now, now),
        )

    async def list_resource_hosts(self) -> list[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM resource_hosts ORDER BY id") as c:
            return [_row(r) async for r in c]

    async def get_resource_host(self, host_id: str) -> dict[str, Any] | None:
        async with self.db.execute("SELECT * FROM resource_hosts WHERE id=?", (host_id,)) as c:
            return _row(await c.fetchone())

    async def set_resource_host_state(self, host_id: str, *, draining: bool | None = None, quarantined: bool | None = None, reason: str | None = None) -> dict[str, Any]:
        sets, args = ["updated_at=?"], [utc_now_iso()]
        if draining is not None:
            sets.append("draining=?"); args.append(int(draining))
        if quarantined is not None:
            sets.append("quarantined=?"); args.append(int(quarantined))
            sets.append("quarantine_reason=?"); args.append(reason if quarantined else None)
        args.append(host_id)
        result = await self._write(f"UPDATE resource_hosts SET {', '.join(sets)} WHERE id=?", tuple(args))
        if not result.rowcount:
            raise KeyError(host_id)
        row = await self.get_resource_host(host_id)
        assert row is not None
        return row

    async def recover_resource_host(self, host_id: str) -> dict[str, Any]:
        """Only an operator-confirmed recovery may retire quarantined leases."""
        now = utc_now_iso()
        async with self._atomic():
            result = await self.db.execute("UPDATE resource_hosts SET quarantined=0, quarantine_reason=NULL, updated_at=? WHERE id=?", (now, host_id))
            if not result.rowcount:
                raise KeyError(host_id)
            await self.db.execute("UPDATE resource_leases SET state='released', released_at=? WHERE host_id=? AND state='quarantined'", (now, host_id))
            await self.db.execute(
                """UPDATE session_resource_reservations
                   SET state='released', released_at=?, quarantine_reason=NULL
                   WHERE host_id=? AND state='quarantined'""",
                (now, host_id),
            )
        row = await self.get_resource_host(host_id)
        assert row is not None
        return row

    async def acquire_resource_lease(self, *, lease_id: str, execution_id: str, session_id: str, pool: str, host_id: str) -> dict[str, Any] | None:
        """CAS acquire. IntegrityError means another pool already owns host."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute("SELECT fencing_token FROM resource_hosts WHERE id=? AND enabled=1 AND draining=0 AND offline=0 AND quarantined=0", (host_id,)) as c:
                host = await c.fetchone()
            if host is None:
                return None
            token = int(host[0]) + 1
            try:
                await self.db.execute("UPDATE resource_hosts SET fencing_token=?, updated_at=? WHERE id=?", (token, now, host_id))
                await self.db.execute(
                    """INSERT INTO resource_leases (id, host_id, execution_id, session_id, pool, fencing_token, state, requested_at, acquired_at, heartbeat_at)
                       VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, ?)""",
                    (lease_id, host_id, execution_id, session_id, pool, token, now, now, now),
                )
            except sqlite3.IntegrityError:
                return None
        return await self.get_resource_lease(lease_id)

    async def get_resource_lease(self, lease_id: str) -> dict[str, Any] | None:
        async with self.db.execute("SELECT * FROM resource_leases WHERE id=?", (lease_id,)) as c:
            return _row(await c.fetchone())

    async def release_resource_lease(self, *, lease_id: str, execution_id: str, fencing_token: int) -> bool:
        result = await self._write("""UPDATE resource_leases SET state='released', released_at=?
            WHERE id=? AND execution_id=? AND fencing_token=? AND state IN ('active','revoking')""", (utc_now_iso(), lease_id, execution_id, fencing_token))
        return bool(result.rowcount)

    async def quarantine_resource_lease(self, *, lease_id: str, execution_id: str, fencing_token: int, reason: str) -> bool:
        now = utc_now_iso()
        async with self._atomic():
            result = await self.db.execute("""UPDATE resource_leases SET state='quarantined', revoking_at=COALESCE(revoking_at, ?), quarantine_reason=?
                WHERE id=? AND execution_id=? AND fencing_token=? AND state IN ('active','revoking')""", (now, reason[:500], lease_id, execution_id, fencing_token))
            if not result.rowcount:
                return False
            async with self.db.execute("SELECT host_id FROM resource_leases WHERE id=?", (lease_id,)) as c:
                row = await c.fetchone()
            await self.db.execute("UPDATE resource_hosts SET quarantined=1, quarantine_reason=?, updated_at=? WHERE id=?", (reason[:500], now, row[0]))
        return True

    async def list_resource_leases(self) -> list[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM resource_leases ORDER BY requested_at, id") as c:
            return [_row(r) async for r in c]
