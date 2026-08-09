"""Durable physical-host lease records.

The partial unique index installed by v044 is the final arbiter: overlapping
pools may name a host more than once, but only one active exclusive lease can
exist for its physical host.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from nerve.utils.time import utc_now_iso


_ACTIVE = ("active", "revoking")


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
    return value


class ResourceStore:
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
                                       session_id: str, slot: str, pool: str) -> dict[str, Any]:
        now = utc_now_iso()
        try:
            await self._write(
                """INSERT INTO resource_lease_requests
                   (id, execution_id, session_id, slot, pool, mode, state, requested_at)
                   VALUES (?, ?, ?, ?, ?, 'exclusive', 'queued', ?)""",
                (request_id, execution_id, session_id, slot, pool, now),
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
        result = await self._write(
            """UPDATE resource_lease_requests SET state='cancelled', settled_at=?
               WHERE execution_id=? AND state='queued'""",
            (now, execution_id),
        )
        return result.rowcount

    async def requeue_resource_requests(self, execution_id: str) -> int:
        """Restore pre-backend acquisitions interrupted by daemon shutdown."""
        result = await self._write(
            """UPDATE resource_lease_requests
               SET state='queued', lease_id=NULL, settled_at=NULL
               WHERE execution_id=? AND state='acquired'""",
            (execution_id,),
        )
        return result.rowcount

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
               WHERE state='queued' ORDER BY sequence"""
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
