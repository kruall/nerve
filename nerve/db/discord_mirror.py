"""Persistence for the Discord audit-forum session mirror."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


class DiscordMirrorStore:
    """Mixin providing restart-safe Discord mirror checkpoints."""

    async def get_discord_session_mirror(
        self, session_id: str,
    ) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM discord_session_mirrors WHERE session_id = ?",
            (session_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        try:
            result["live_message_ids"] = json.loads(
                result.get("live_message_ids") or "[]"
            )
        except (TypeError, json.JSONDecodeError):
            result["live_message_ids"] = []
        return result

    async def upsert_discord_session_mirror(
        self,
        session_id: str,
        *,
        guild_id: int,
        forum_id: int,
        thread_id: int,
        starter_message_id: int,
        header_hash: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO discord_session_mirrors
                       (session_id, guild_id, forum_id, thread_id,
                        starter_message_id, header_hash, live_message_ids,
                        created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, '[]', ?, ?)
                   ON CONFLICT(session_id) DO UPDATE SET
                       guild_id = excluded.guild_id,
                       forum_id = excluded.forum_id,
                       thread_id = excluded.thread_id,
                       starter_message_id = excluded.starter_message_id,
                       header_hash = excluded.header_hash,
                       live_message_ids = '[]',
                       updated_at = excluded.updated_at""",
                (
                    session_id,
                    str(guild_id),
                    str(forum_id),
                    str(thread_id),
                    str(starter_message_id),
                    header_hash,
                    now,
                    now,
                ),
            )
            # A newly created/recreated thread has no projected items yet.
            # Clearing checkpoints in the same transaction prevents a crash
            # between remapping the thread and invalidating the old IDs.
            await self.db.execute(
                "DELETE FROM discord_session_mirror_items WHERE session_id = ?",
                (session_id,),
            )

    async def update_discord_mirror_header(
        self, session_id: str, header_hash: str,
    ) -> None:
        await self._write(
            """UPDATE discord_session_mirrors
               SET header_hash = ?, updated_at = ?
               WHERE session_id = ?""",
            (header_hash, datetime.now(timezone.utc).isoformat(), session_id),
        )

    async def set_discord_mirror_live_messages(
        self, session_id: str, message_ids: list[int],
    ) -> None:
        await self._write(
            """UPDATE discord_session_mirrors
               SET live_message_ids = ?, updated_at = ?
               WHERE session_id = ?""",
            (
                json.dumps([str(value) for value in message_ids]),
                datetime.now(timezone.utc).isoformat(),
                session_id,
            ),
        )

    async def get_discord_mirror_items(
        self, session_id: str,
    ) -> dict[tuple[str, int], dict[str, Any]]:
        async with self.db.execute(
            """SELECT * FROM discord_session_mirror_items
               WHERE session_id = ?""",
            (session_id,),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        result: dict[tuple[str, int], dict[str, Any]] = {}
        for row in rows:
            try:
                row["discord_message_ids"] = json.loads(
                    row.get("discord_message_ids") or "[]"
                )
            except (TypeError, json.JSONDecodeError):
                row["discord_message_ids"] = []
            result[(str(row["item_kind"]), int(row["item_id"]))] = row
        return result

    async def upsert_discord_mirror_item(
        self,
        session_id: str,
        *,
        item_kind: str,
        item_id: int,
        discord_message_ids: list[int],
        content_hash: str,
    ) -> None:
        await self._write(
            """INSERT INTO discord_session_mirror_items
                   (session_id, item_kind, item_id, discord_message_ids,
                    content_hash, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(session_id, item_kind, item_id) DO UPDATE SET
                   discord_message_ids = excluded.discord_message_ids,
                   content_hash = excluded.content_hash,
                   updated_at = excluded.updated_at""",
            (
                session_id,
                item_kind,
                item_id,
                json.dumps([str(value) for value in discord_message_ids]),
                content_hash,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    async def delete_discord_mirror_item(
        self,
        session_id: str,
        *,
        item_kind: str,
        item_id: int,
    ) -> None:
        await self._write(
            """DELETE FROM discord_session_mirror_items
               WHERE session_id = ? AND item_kind = ? AND item_id = ?""",
            (session_id, item_kind, item_id),
        )

    async def clear_discord_mirror_items(self, session_id: str) -> None:
        await self._write(
            "DELETE FROM discord_session_mirror_items WHERE session_id = ?",
            (session_id,),
        )

    async def list_discord_mirror_sessions(
        self,
        *,
        active_after: str | None = None,
        offset: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return sessions eligible for reconciliation, oldest first.

        Existing mirror rows are always returned so restarts can repair and
        continue them. Unmirrored historical sessions are skipped unless they
        were created or updated after this mirror process started; a first
        enablement therefore does not flood Discord with the entire old
        session archive, while an old session is picked up when it becomes
        active again.
        """
        if active_after is None:
            where = "m.session_id IS NOT NULL"
            params: tuple[Any, ...] = (limit, offset)
        else:
            where = (
                "(m.session_id IS NOT NULL "
                "OR datetime(s.updated_at) >= datetime(?))"
            )
            params = (active_after, limit, offset)
        async with self.db.execute(
            f"""SELECT s.*
                FROM sessions s
                LEFT JOIN discord_session_mirrors m ON m.session_id = s.id
                WHERE ({where})
                  AND s.id NOT LIKE 'cron:%'
                  AND lower(COALESCE(s.source, '')) NOT IN ('cron', 'system')
                ORDER BY s.created_at ASC, s.id ASC
                LIMIT ? OFFSET ?""",
            params,
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_discord_mirror_content_items(
        self, session_id: str,
    ) -> list[dict[str, Any]]:
        """Return messages and non-duplicated lifecycle events in one timeline.

        Tool calls already live in assistant message blocks. Their separate
        MCP audit events include the same calls and are intentionally omitted.
        """
        async with self.db.execute(
            """SELECT *
               FROM (
                   SELECT 'message' AS item_kind, id AS item_id,
                          role AS item_type, content, thinking,
                          blocks AS details, created_at
                   FROM messages WHERE session_id = ?
                   UNION ALL
                   SELECT 'event' AS item_kind, id AS item_id,
                          event_type AS item_type, NULL AS content,
                          NULL AS thinking, details, created_at
                   FROM session_events
                   WHERE session_id = ?
                     AND event_type NOT IN (
                         'codex_rate_limits',
                         'external_tool_call'
                     )
               )
               ORDER BY julianday(created_at) ASC,
                        item_kind ASC,
                        item_id ASC""",
            (session_id, session_id),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (TypeError, json.JSONDecodeError):
                    pass
        return rows
