"""Durable exclusive-tool leases and FIFO wait subscriptions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


class ToolLeaseStore:
    """Mixin for leases that serialize access to a named agent tool.

    The state is database-backed, so ownership and the waiting queue survive a
    restart. Expired leases are reclaimed by every contended operation and by
    the cron handoff sweep; a forgotten release cannot block a tool forever.
    """

    @staticmethod
    def _tool_lease_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _tool_lease_iso(value: datetime) -> str:
        return value.isoformat()

    async def get_tool_lease(self, tool_name: str, *, now: datetime | None = None) -> dict | None:
        """Return the live lease, treating an expired row as absent."""
        now_iso = self._tool_lease_iso(now or self._tool_lease_now())
        async with self.db.execute(
            "SELECT * FROM tool_leases WHERE tool_name = ? AND expires_at > ?",
            (tool_name, now_iso),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def can_acquire_tool_lease(
        self, tool_name: str, session_id: str, *, now: datetime | None = None,
    ) -> bool:
        lease = await self.get_tool_lease(tool_name, now=now)
        return lease is None or lease["session_id"] == session_id

    async def acquire_tool_lease(
        self, tool_name: str, session_id: str, lease_seconds: int, *, now: datetime | None = None,
    ) -> tuple[str, dict]:
        """Atomically acquire a free/expired lease.

        Returns ``("acquired" | "already_owned" | "busy", lease)``.
        Repeated acquire by the same session is idempotent; use renew to extend.
        """
        started = now or self._tool_lease_now()
        now_iso = self._tool_lease_iso(started)
        expires_at = self._tool_lease_iso(started + timedelta(seconds=lease_seconds))
        async with self._atomic():
            await self.db.execute(
                "DELETE FROM tool_leases WHERE tool_name = ? AND expires_at <= ?",
                (tool_name, now_iso),
            )
            async with self.db.execute(
                "SELECT * FROM tool_leases WHERE tool_name = ?", (tool_name,),
            ) as cursor:
                row = await cursor.fetchone()
            if row:
                lease = dict(row)
                return ("already_owned" if lease["session_id"] == session_id else "busy", lease)
            await self.db.execute(
                """INSERT INTO tool_leases
                   (tool_name, session_id, acquired_at, expires_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (tool_name, session_id, now_iso, expires_at, now_iso),
            )
        return "acquired", {
            "tool_name": tool_name, "session_id": session_id,
            "acquired_at": now_iso, "expires_at": expires_at, "updated_at": now_iso,
        }

    async def renew_tool_lease(
        self, tool_name: str, session_id: str, lease_seconds: int, *, now: datetime | None = None,
    ) -> dict | None:
        """Renew a live lease owned by ``session_id``; otherwise return None."""
        started = now or self._tool_lease_now()
        now_iso = self._tool_lease_iso(started)
        expires_at = self._tool_lease_iso(started + timedelta(seconds=lease_seconds))
        async with self._atomic():
            cursor = await self.db.execute(
                """UPDATE tool_leases SET expires_at = ?, updated_at = ?
                   WHERE tool_name = ? AND session_id = ? AND expires_at > ?""",
                (expires_at, now_iso, tool_name, session_id, now_iso),
            )
            changed = cursor.rowcount
            await cursor.close()
            if not changed:
                return None
            async with self.db.execute(
                "SELECT * FROM tool_leases WHERE tool_name = ?", (tool_name,),
            ) as result:
                row = await result.fetchone()
        return dict(row) if row else None

    async def release_tool_lease(self, tool_name: str, session_id: str) -> bool:
        """Release only the caller's lease. Never releases another session."""
        result = await self._write(
            "DELETE FROM tool_leases WHERE tool_name = ? AND session_id = ?",
            (tool_name, session_id),
        )
        return bool(result.rowcount)

    async def subscribe_tool_lease(
        self,
        tool_name: str,
        session_id: str,
        prompt: str,
        lease_seconds: int,
        wait_seconds: int,
        *,
        now: datetime | None = None,
    ) -> dict:
        """Put a session in the FIFO handoff queue, replacing its old wait."""
        started = now or self._tool_lease_now()
        now_iso = self._tool_lease_iso(started)
        expires_at = self._tool_lease_iso(started + timedelta(seconds=wait_seconds))
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO tool_lease_subscriptions
                   (tool_name, session_id, prompt, lease_seconds, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tool_name, session_id) DO UPDATE SET
                       prompt = excluded.prompt, lease_seconds = excluded.lease_seconds,
                       created_at = excluded.created_at, expires_at = excluded.expires_at""",
                (tool_name, session_id, prompt, lease_seconds, now_iso, expires_at),
            )
            async with self.db.execute(
                "SELECT * FROM tool_lease_subscriptions WHERE tool_name = ? AND session_id = ?",
                (tool_name, session_id),
            ) as cursor:
                row = await cursor.fetchone()
        assert row is not None
        return dict(row)

    async def unsubscribe_tool_lease(self, tool_name: str, session_id: str) -> bool:
        result = await self._write(
            "DELETE FROM tool_lease_subscriptions WHERE tool_name = ? AND session_id = ?",
            (tool_name, session_id),
        )
        return bool(result.rowcount)

    async def list_ready_tool_lease_subscriptions(
        self, *, now: datetime | None = None, limit: int = 50,
    ) -> list[dict]:
        """Return the FIFO waiter for every currently unleased tool."""
        now_iso = self._tool_lease_iso(now or self._tool_lease_now())
        async with self.db.execute(
            """
            SELECT s.* FROM tool_lease_subscriptions AS s
            JOIN (
                SELECT subscription.tool_name, MIN(subscription.id) AS id
                FROM tool_lease_subscriptions AS subscription
                JOIN sessions AS candidate ON candidate.id = subscription.session_id
                WHERE subscription.expires_at > ?
                  AND candidate.status != 'archived'
                  AND candidate.source != 'external'
                GROUP BY subscription.tool_name
            ) AS first_waiter ON first_waiter.id = s.id
            JOIN sessions AS session ON session.id = s.session_id
            WHERE session.status != 'archived' AND session.source != 'external'
              AND NOT EXISTS (
                  SELECT 1 FROM tool_leases AS lease
                  WHERE lease.tool_name = s.tool_name AND lease.expires_at > ?
              )
            ORDER BY s.id ASC LIMIT ?
            """,
            (now_iso, now_iso, limit),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def claim_tool_lease_subscription(
        self, subscription_id: int, *, now: datetime | None = None,
    ) -> dict | None:
        """Atomically reserve a released tool for its selected waiter."""
        started = now or self._tool_lease_now()
        now_iso = self._tool_lease_iso(started)
        async with self._atomic():
            await self.db.execute("DELETE FROM tool_leases WHERE expires_at <= ?", (now_iso,))
            await self.db.execute(
                "DELETE FROM tool_lease_subscriptions WHERE expires_at <= ?", (now_iso,),
            )
            async with self.db.execute(
                """SELECT s.* FROM tool_lease_subscriptions AS s
                   JOIN sessions AS session ON session.id = s.session_id
                   WHERE s.id = ? AND session.status != 'archived'
                     AND session.source != 'external'""",
                (subscription_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return None
            subscription = dict(row)
            async with self.db.execute(
                """SELECT s.id FROM tool_lease_subscriptions AS s
                   JOIN sessions AS session ON session.id = s.session_id
                   WHERE s.tool_name = ? AND s.expires_at > ?
                     AND session.status != 'archived'
                     AND session.source != 'external'
                   ORDER BY s.id ASC LIMIT 1""",
                (subscription["tool_name"], now_iso),
            ) as cursor:
                first_waiter = await cursor.fetchone()
            if not first_waiter or first_waiter["id"] != subscription_id:
                return None
            async with self.db.execute(
                "SELECT 1 FROM tool_leases WHERE tool_name = ? AND expires_at > ?",
                (subscription["tool_name"], now_iso),
            ) as cursor:
                if await cursor.fetchone():
                    return None
            expires_at = self._tool_lease_iso(
                started + timedelta(seconds=int(subscription["lease_seconds"])),
            )
            await self.db.execute(
                """INSERT INTO tool_leases
                   (tool_name, session_id, acquired_at, expires_at, updated_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (subscription["tool_name"], subscription["session_id"], now_iso, expires_at, now_iso),
            )
            await self.db.execute("DELETE FROM tool_lease_subscriptions WHERE id = ?", (subscription_id,))
        subscription["acquired_at"] = now_iso
        subscription["lease_expires_at"] = expires_at
        return subscription
