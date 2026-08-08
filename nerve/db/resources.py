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
