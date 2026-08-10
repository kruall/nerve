"""Persistence and compare-and-set transitions for detached executions."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from nerve.utils.time import utc_now_iso
from nerve.resources import ResourceHandleConflictError


ACTIVE_EXECUTION_STATUSES = ("queued", "starting", "running", "cancelling")
TERMINAL_EXECUTION_STATUSES = ("succeeded", "failed", "cancelled", "lost")
CONTINUABLE_EXECUTION_STATUSES = ("succeeded", "failed", "lost")

_JSON_COLUMNS = (
    "profile_snapshot", "plan", "resource_requests", "selected_leases",
    "backend_handle", "result",
)


def _decode_execution(row: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(row)
    for column in _JSON_COLUMNS:
        raw = result.get(column)
        if raw is None:
            continue
        try:
            result[column] = json.loads(raw)
        except (TypeError, ValueError):
            result[column] = None if column in {"backend_handle", "result"} else {}
    result["continuation"] = {
        "state": result.get("continuation_state"),
        "session_id": result.get("session_id"),
        "attempted_at": result.get("continuation_claimed_at"),
        "completed_at": result.get("continuation_completed_at"),
        "error": result.get("continuation_error"),
    }
    return result


class ExecutionStore:
    """Database mixin for the durable execution state machine."""

    async def create_execution(
        self,
        execution_id: str,
        *,
        session_id: str,
        kind: str,
        profile_version: str,
        profile_hash: str,
        profile_snapshot: Mapping[str, Any],
        plan: Mapping[str, Any],
        resource_requests: Sequence[Mapping[str, Any]],
        handle_ids: Sequence[str] = (),
        completion_target_type: str = "session",
        completion_target_id: str | None = None,
        auto_continue: bool = True,
        legacy_compatibility: bool = False,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        if len(handle_ids) != len(set(handle_ids)):
            raise ValueError("resource handle ids must be distinct")
        try:
            async with self._atomic():
                # R9 serialized every session.  Calls that did not opt into
                # explicit handles retain that behaviour, including races,
                # while explicit Operations can run beside disjoint explicit
                # Operations.  The marker is persisted in the plan so this
                # remains true after a restart.
                if legacy_compatibility:
                    async with self.db.execute(
                        """SELECT id FROM executions WHERE session_id=?
                           AND status IN ('queued', 'starting', 'running', 'cancelling')
                           ORDER BY id LIMIT 1""",
                        (session_id,),
                    ) as cursor:
                        conflict = await cursor.fetchone()
                    if conflict is not None:
                        raise ValueError("session already owns an active execution")
                else:
                    async with self.db.execute(
                        """SELECT id FROM executions WHERE session_id=?
                           AND status IN ('queued', 'starting', 'running', 'cancelling')
                             AND json_extract(plan, '$.legacy_resource_handles') = 1
                           ORDER BY id LIMIT 1""",
                        (session_id,),
                    ) as cursor:
                        conflict = await cursor.fetchone()
                    if conflict is not None:
                        raise ValueError("session already owns an active execution")
                for position, handle_id in enumerate(handle_ids):
                    async with self.db.execute(
                        """SELECT 1
                             FROM session_resource_handles AS h
                             JOIN resource_leases AS l ON l.id=h.lease_id
                             JOIN resource_hosts AS host ON host.id=h.host_id
                            WHERE h.id=? AND h.session_id=? AND h.state='active'
                              AND l.session_id=h.session_id AND l.host_id=h.host_id
                              AND l.fencing_token=h.fencing_token AND l.state='active'
                              AND host.enabled=1 AND host.draining=0 AND host.offline=0
                              AND host.quarantined=0 AND host.permanently_unavailable=0""",
                        (handle_id, session_id),
                    ) as cursor:
                        if await cursor.fetchone() is None:
                            raise ValueError("resource handle does not belong to this session")
                if handle_ids:
                    marks = ",".join("?" for _ in handle_ids)
                    async with self.db.execute(
                        f"SELECT DISTINCT operation_id FROM operation_resource_refs "
                        f"WHERE handle_id IN ({marks}) ORDER BY operation_id",
                        tuple(handle_ids),
                    ) as cursor:
                        operation_ids = [str(row[0]) async for row in cursor]
                    if operation_ids:
                        raise ResourceHandleConflictError(operation_ids)
                await self.db.execute(
                """INSERT INTO executions
                   (id, session_id, kind, profile_version, profile_hash,
                    profile_snapshot, plan, resource_requests, selected_leases,
                    completion_target_type, completion_target_id, auto_continue,
                    status, created_at, queued_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?, 'queued', ?, ?, ?)""",
                (
                    execution_id, session_id, kind, profile_version, profile_hash,
                    json.dumps(dict(profile_snapshot)), json.dumps(dict(plan)),
                    json.dumps(list(resource_requests)), completion_target_type,
                    completion_target_id, int(auto_continue), now, now, now,
                ),
                )
                for position, handle_id in enumerate(handle_ids):
                    await self.db.execute(
                        "INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at) VALUES (?, ?, ?, ?)",
                        (execution_id, handle_id, position, now),
                    )
        except sqlite3.IntegrityError as exc:
            # V059's unique handle index is the final arbiter between separate
            # processes.  Return the same stable conflict with its Operation
            # ids rather than leaking SQLite's constraint text.
            if ("uq_operation_resource_refs_one_active_per_handle" in str(exc)
                    or "operation_resource_refs.handle_id" in str(exc)):
                marks = ",".join("?" for _ in handle_ids)
                async with self.db.execute(
                    f"SELECT DISTINCT operation_id FROM operation_resource_refs "
                    f"WHERE handle_id IN ({marks}) ORDER BY operation_id",
                    tuple(handle_ids),
                ) as cursor:
                    operation_ids = [str(row[0]) async for row in cursor]
                raise ResourceHandleConflictError(operation_ids) from exc
            raise
        row = await self.get_execution(execution_id)
        assert row is not None
        return row

    async def get_execution(self, execution_id: str) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM executions WHERE id = ?", (execution_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return _decode_execution(row) if row else None

    async def list_session_executions(
        self,
        session_id: str,
        *,
        include_terminal: bool = True,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        where = "session_id = ? AND dismissed_at IS NULL"
        params: list[Any] = [session_id]
        if not include_terminal:
            placeholders = ",".join("?" for _ in ACTIVE_EXECUTION_STATUSES)
            where += f" AND status IN ({placeholders})"
            params.extend(ACTIVE_EXECUTION_STATUSES)
        params.append(max(1, min(int(limit), 100)))
        async with self.db.execute(
            f"""SELECT * FROM executions WHERE {where}
                ORDER BY created_at DESC, id DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            return [_decode_execution(row) async for row in cursor]

    async def list_active_executions(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in ACTIVE_EXECUTION_STATUSES)
        async with self.db.execute(
            f"""SELECT * FROM executions WHERE status IN ({placeholders})
                ORDER BY created_at ASC, id ASC""",
            ACTIVE_EXECUTION_STATUSES,
        ) as cursor:
            return [_decode_execution(row) async for row in cursor]

    async def list_terminal_executions(self) -> list[dict[str, Any]]:
        async with self.db.execute(
            """SELECT * FROM executions
               WHERE status IN ('succeeded', 'failed', 'cancelled')
               ORDER BY created_at ASC, id ASC""",
        ) as cursor:
            return [_decode_execution(row) async for row in cursor]

    async def session_execution_activity(
        self, session_ids: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        ids = [str(value) for value in session_ids if value]
        if not ids:
            return {}
        id_marks = ",".join("?" for _ in ids)
        state_marks = ",".join("?" for _ in ACTIVE_EXECUTION_STATUSES)
        result = {
            session_id: {"active_execution_count": 0, "execution_statuses": []}
            for session_id in ids
        }
        async with self.db.execute(
            f"""SELECT session_id, status, COUNT(*) AS count
                FROM executions
                WHERE session_id IN ({id_marks}) AND status IN ({state_marks})
                GROUP BY session_id, status""",
            (*ids, *ACTIVE_EXECUTION_STATUSES),
        ) as cursor:
            async for row in cursor:
                activity = result[str(row["session_id"])]
                count = int(row["count"])
                activity["active_execution_count"] += count
                activity["execution_statuses"].extend([str(row["status"])] * count)
        return result

    async def transition_execution(
        self,
        execution_id: str,
        *,
        to_status: str,
        expect: Sequence[str],
        fields: Mapping[str, Any] | None = None,
    ) -> bool:
        if to_status not in (*ACTIVE_EXECUTION_STATUSES, *TERMINAL_EXECUTION_STATUSES):
            raise ValueError("unknown execution status")
        expected = tuple(expect)
        if not expected:
            return False
        now = utc_now_iso()
        values = dict(fields or {})
        allowed = {"backend_name", "backend_handle", "selected_leases", "result"}
        values = {key: value for key, value in values.items() if key in allowed}
        for key in ("backend_handle", "selected_leases", "result"):
            if key in values:
                values[key] = json.dumps(values[key])
        if to_status == "running":
            values.setdefault("started_at", now)
        if to_status in TERMINAL_EXECUTION_STATUSES:
            values.setdefault("finished_at", now)
        assignments = ["status = ?", "revision = revision + 1", "updated_at = ?"]
        params: list[Any] = [to_status, now]
        for key, value in values.items():
            if key in {"started_at", "finished_at"} or key in allowed:
                assignments.append(f"{key} = ?")
                params.append(value)
        placeholders = ",".join("?" for _ in expected)
        params.extend([execution_id, *expected])
        statement = f"""UPDATE executions SET {', '.join(assignments)}
            WHERE id = ? AND status IN ({placeholders})"""
        if to_status in TERMINAL_EXECUTION_STATUSES:
            async with self._atomic():
                result = await self.db.execute(statement, tuple(params))
                if result.rowcount:
                    await self.db.execute("DELETE FROM operation_resource_refs WHERE operation_id=?", (execution_id,))
        else:
            result = await self._write(statement, tuple(params))
        return (result.rowcount or 0) == 1

    async def finish_execution(
        self,
        execution_id: str,
        *,
        status: str,
        result: Mapping[str, Any],
        expect: Sequence[str] = ("starting", "running"),
    ) -> bool:
        """Atomically settle work and create its continuation outbox item.

        A prior cancellation changes the status to ``cancelling`` and stamps
        ``cancel_requested_at`` in the same database transaction, so this CAS
        cannot subsequently publish a completion continuation.
        """
        if status not in CONTINUABLE_EXECUTION_STATUSES:
            raise ValueError("terminal status is not continuable")
        now = utc_now_iso()
        expected = tuple(expect)
        placeholders = ",".join("?" for _ in expected)
        async with self._atomic():
            update = await self.db.execute(
                f"""UPDATE executions
                SET status = ?, result = ?, finished_at = ?, updated_at = ?,
                    revision = revision + 1,
                    continuation_state = CASE WHEN auto_continue = 1 THEN 'pending' ELSE 'suppressed' END,
                    continuation_error = NULL
                WHERE id = ? AND status IN ({placeholders})
                  AND cancel_requested_at IS NULL""",
            (
                status, json.dumps(dict(result)), now, now, execution_id,
                *expected,
            ),
            )
            if update.rowcount:
                await self.db.execute("DELETE FROM operation_resource_refs WHERE operation_id=?", (execution_id,))
        return (update.rowcount or 0) == 1

    async def dismiss_session_execution(
        self, execution_id: str, *, session_id: str,
    ) -> bool:
        """CAS-dismiss a terminal row only after its continuation is settled.

        This intentionally changes no execution, lease, log, result, or audit
        data; ``dismissed_at`` only controls the session-panel listing.
        """
        now = utc_now_iso()
        update = await self._write(
            """UPDATE executions
               SET dismissed_at = ?, updated_at = ?, revision = revision + 1
               WHERE id = ? AND session_id = ? AND dismissed_at IS NULL
                 AND status IN ('succeeded', 'failed', 'cancelled', 'lost')
                 AND (
                     continuation_state IN ('completed', 'failed', 'suppressed')
                     OR (auto_continue = 0 AND continuation_state = 'none')
                 )""",
            (now, now, execution_id, session_id),
        )
        return (update.rowcount or 0) == 1

    async def suppress_execution_continuation(self, execution_id: str) -> bool:
        """Forget automatic delivery without cancelling the execution."""
        now = utc_now_iso()
        update = await self._write(
            """UPDATE executions
               SET auto_continue = 0,
                   continuation_state = CASE
                       WHEN continuation_state IN ('none', 'pending') THEN 'suppressed'
                       ELSE continuation_state END,
                   updated_at = ?, revision = revision + 1
               WHERE id = ? AND continuation_state != 'claimed'""",
            (now, execution_id),
        )
        return (update.rowcount or 0) == 1

    async def request_execution_cancel(
        self, execution_id: str, *, reason: str | None,
    ) -> bool:
        """Accept cancellation and suppress a pending/claimed continuation."""
        now = utc_now_iso()
        active_marks = ",".join("?" for _ in ACTIVE_EXECUTION_STATUSES)
        result = await self._write(
            f"""UPDATE executions
                SET status = CASE
                        WHEN status IN ({active_marks}) THEN 'cancelling'
                        ELSE status
                    END,
                    cancel_reason = ?, cancel_requested_at = ?, updated_at = ?,
                    revision = revision + 1, continuation_state = 'suppressed',
                    continuation_error = NULL
                WHERE id = ?
                  AND (status IN ({active_marks}) OR continuation_state IN ('pending', 'claimed'))""",
            (
                *ACTIVE_EXECUTION_STATUSES, (reason or "user requested cancellation")[:500],
                now, now, execution_id, *ACTIVE_EXECUTION_STATUSES,
            ),
        )
        return (result.rowcount or 0) == 1

    async def finalize_execution_cancelled(
        self, execution_id: str, *, result: Mapping[str, Any] | None = None,
    ) -> bool:
        """Terminally cancel an execution and settle only its unleased queue work.

        A selected lease is durable evidence that remote work might have
        started, and callers must establish quiescence (or quarantine it)
        before reaching this transition.  Conversely, a queued request with
        no selected lease can never have reached a resource backend; settling
        it in the same transaction prevents an orphaned queue entry from
        surviving a crash between the execution and queue updates.
        """
        now = utc_now_iso()
        async with self._atomic():
            update = await self.db.execute(
                """UPDATE executions
                   SET status = 'cancelled', result = ?, finished_at = ?, updated_at = ?,
                       revision = revision + 1, continuation_state = 'suppressed'
                   WHERE id = ? AND status = 'cancelling'
                     AND NOT EXISTS (
                         SELECT 1 FROM resource_leases
                         WHERE execution_id=? AND state IN ('active', 'revoking')
                     )""",
                (json.dumps(dict(result or {"outcome": "cancelled"})), now, now,
                 execution_id, execution_id),
            )
            if not update.rowcount:
                return False
            await self.db.execute(
                """UPDATE resource_lease_requests SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""",
                (now, execution_id),
            )
            await self.db.execute(
                """UPDATE resource_lease_bundles SET state='cancelled', settled_at=?
                   WHERE execution_id=? AND state='queued'""",
                (now, execution_id),
            )
            await self.db.execute("DELETE FROM operation_resource_refs WHERE operation_id=?", (execution_id,))
        return True

    async def suppress_session_executions(
        self, session_id: str, *, reason: str,
    ) -> list[str]:
        """Atomically cancel active rows and suppress pending continuations."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                """SELECT id FROM executions
                   WHERE session_id = ? AND (
                       status IN ('queued', 'starting', 'running', 'cancelling')
                       OR continuation_state IN ('pending', 'claimed')
                   )""",
                (session_id,),
            ) as cursor:
                ids = [str(row[0]) async for row in cursor]
            await self.db.execute(
                """UPDATE executions
                   SET status = CASE
                           WHEN status IN ('queued', 'starting', 'running', 'cancelling')
                           THEN 'cancelling' ELSE status END,
                       cancel_reason = ?, cancel_requested_at = COALESCE(cancel_requested_at, ?),
                       continuation_state = 'suppressed', updated_at = ?,
                       revision = revision + 1
                   WHERE session_id = ? AND (
                       status IN ('queued', 'starting', 'running', 'cancelling')
                       OR continuation_state IN ('pending', 'claimed')
                   )""",
                (reason[:500], now, now, session_id),
            )
        return ids

    async def claim_execution_continuation(
        self, execution_id: str,
    ) -> tuple[bool, dict[str, Any] | None]:
        """Claim once, suppressing rows whose owner is no longer resumable."""
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                """SELECT e.*, s.status AS owner_status, s.source AS owner_source
                   FROM executions e LEFT JOIN sessions s ON s.id = e.session_id
                   WHERE e.id = ?""",
                (execution_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None or row["continuation_state"] != "pending":
                return False, _decode_execution(row) if row else None
            if row["owner_status"] in (None, "stopped", "archived") or row["owner_source"] == "external":
                await self.db.execute(
                    """UPDATE executions SET continuation_state = 'suppressed',
                       updated_at = ?, revision = revision + 1
                       WHERE id = ? AND continuation_state = 'pending'""",
                    (now, execution_id),
                )
                return False, _decode_execution(row)
            cursor = await self.db.execute(
                """UPDATE executions SET continuation_state = 'claimed',
                   continuation_claimed_at = ?, updated_at = ?, revision = revision + 1
                   WHERE id = ? AND continuation_state = 'pending'
                     AND cancel_requested_at IS NULL""",
                (now, now, execution_id),
            )
            claimed = (cursor.rowcount or 0) == 1
            await cursor.close()
        return claimed, await self.get_execution(execution_id)

    async def list_pending_execution_continuations(self) -> list[dict[str, Any]]:
        async with self.db.execute(
            """SELECT * FROM executions WHERE continuation_state = 'pending'
               ORDER BY finished_at ASC, id ASC""",
        ) as cursor:
            return [_decode_execution(row) async for row in cursor]

    async def fail_claimed_execution_continuations_on_restart(self) -> int:
        """Quarantine uncertain pre-restart claims instead of dispatching twice.

        A claimed row may already have reached the model before the daemon
        crashed. Retrying it could create a duplicate turn, so startup records
        the uncertainty as a terminal outbox failure and only replays rows that
        were never claimed.
        """
        now = utc_now_iso()
        update = await self._write(
            """UPDATE executions
               SET continuation_state = 'failed',
                   continuation_completed_at = ?,
                   continuation_error = 'daemon restarted after continuation claim',
                   updated_at = ?, revision = revision + 1
               WHERE continuation_state = 'claimed'""",
            (now, now),
        )
        return int(update.rowcount or 0)

    async def settle_execution_continuation(
        self, execution_id: str, *, success: bool, error: str | None = None,
    ) -> bool:
        now = utc_now_iso()
        update = await self._write(
            """UPDATE executions
               SET continuation_state = ?, continuation_completed_at = ?,
                   continuation_error = ?, updated_at = ?, revision = revision + 1
               WHERE id = ? AND continuation_state = 'claimed'""",
            (
                "completed" if success else "failed", now,
                None if success else (error or "continuation failed")[:1000],
                now, execution_id,
            ),
        )
        return (update.rowcount or 0) == 1

    async def append_execution_log(
        self,
        execution_id: str,
        *,
        stream: str,
        text: str,
        max_lines: int = 5000,
        max_chars: int = 2 * 1024 * 1024,
    ) -> None:
        if stream not in {"stdout", "stderr", "system"}:
            stream = "system"
        value = text[:65_536]
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO execution_logs(execution_id, stream, timestamp, text)
                   VALUES (?, ?, ?, ?)""",
                (execution_id, stream, utc_now_iso(), value),
            )
            async with self.db.execute(
                """SELECT sequence, length(text) AS chars FROM execution_logs
                   WHERE execution_id = ? ORDER BY sequence DESC""",
                (execution_id,),
            ) as cursor:
                rows = await cursor.fetchall()
            used = 0
            keep = 0
            for row in rows:
                chars = int(row["chars"] or 0)
                if keep >= max_lines or (keep and used + chars > max_chars):
                    break
                used += chars
                keep += 1
            if keep < len(rows):
                cutoff = int(rows[keep - 1]["sequence"]) if keep else int(rows[0]["sequence"]) + 1
                await self.db.execute(
                    "DELETE FROM execution_logs WHERE execution_id = ? AND sequence < ?",
                    (execution_id, cutoff),
                )

    async def tail_execution_logs(
        self, execution_id: str, *, limit: int, before: int | None = None,
    ) -> dict[str, Any]:
        bounded = max(1, min(int(limit), 500))
        where = "execution_id = ?"
        params: list[Any] = [execution_id]
        if before is not None:
            where += " AND sequence < ?"
            params.append(int(before))
        params.append(bounded + 1)
        async with self.db.execute(
            f"""SELECT sequence, stream, timestamp, text FROM execution_logs
                WHERE {where} ORDER BY sequence DESC LIMIT ?""",
            tuple(params),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        has_more = len(rows) > bounded
        rows = rows[:bounded]
        rows.reverse()
        return {
            "entries": rows,
            "has_more": has_more,
            "next_before": rows[0]["sequence"] if has_more and rows else None,
            "oldest_sequence": rows[0]["sequence"] if rows else None,
            "newest_sequence": rows[-1]["sequence"] if rows else None,
        }
