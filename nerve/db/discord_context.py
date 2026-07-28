"""Persistence for bounded Discord project-thread context."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


class DiscordContextStore:
    """Mixin providing restart-safe rolling context for Discord threads."""

    async def get_discord_thread_context(
        self, thread_id: int,
    ) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM discord_thread_contexts WHERE thread_id = ?",
            (str(thread_id),),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None

        result = dict(row)
        try:
            messages = json.loads(result.get("recent_messages") or "[]")
            result["recent_messages"] = (
                messages if isinstance(messages, list) else []
            )
        except (TypeError, json.JSONDecodeError):
            result["recent_messages"] = []
        return result

    async def upsert_discord_thread_context(
        self,
        *,
        guild_id: int,
        forum_id: int,
        thread_id: int,
        summary: str,
        summary_through_message_id: int,
        recent_messages: list[dict[str, Any]],
        last_message_id: int,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO discord_thread_contexts
                   (thread_id, guild_id, forum_id, summary,
                    summary_through_message_id, recent_messages,
                    last_message_id, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(thread_id) DO UPDATE SET
                   guild_id = excluded.guild_id,
                   forum_id = excluded.forum_id,
                   summary = excluded.summary,
                   summary_through_message_id =
                       excluded.summary_through_message_id,
                   recent_messages = excluded.recent_messages,
                   last_message_id = excluded.last_message_id,
                   updated_at = excluded.updated_at""",
            (
                str(thread_id),
                str(guild_id),
                str(forum_id),
                summary,
                str(summary_through_message_id),
                json.dumps(
                    recent_messages,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                str(last_message_id),
                now,
                now,
            ),
        )

    async def mark_discord_thread_context_delivered(
        self, thread_id: int, message_id: int,
    ) -> None:
        """Advance the successful agent-handoff checkpoint monotonically."""
        await self._write(
            """UPDATE discord_thread_contexts
               SET last_delivered_message_id = CASE
                       WHEN CAST(last_delivered_message_id AS INTEGER) < ?
                       THEN ?
                       ELSE last_delivered_message_id
                   END,
                   updated_at = ?
               WHERE thread_id = ?""",
            (
                message_id,
                str(message_id),
                datetime.now(timezone.utc).isoformat(),
                str(thread_id),
            ),
        )
