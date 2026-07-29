"""Persistence for editable Discord project-forum prompts."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class DiscordProjectPromptStore:
    """Mixin storing one managed prompt thread for each project forum."""

    async def get_discord_project_prompt(
        self,
        forum_id: int,
    ) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM discord_project_prompts WHERE forum_id = ?",
            (str(forum_id),),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def upsert_discord_project_prompt(
        self,
        *,
        guild_id: int,
        forum_id: int,
        project: str,
        thread_id: int,
        message_id: int | None,
        content: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO discord_project_prompts
                   (forum_id, guild_id, project, thread_id, message_id,
                    content, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(forum_id) DO UPDATE SET
                   guild_id = excluded.guild_id,
                   project = excluded.project,
                   thread_id = excluded.thread_id,
                   message_id = excluded.message_id,
                   content = excluded.content,
                   updated_at = excluded.updated_at""",
            (
                str(forum_id),
                str(guild_id),
                project,
                str(thread_id),
                str(message_id or ""),
                content,
                now,
                now,
            ),
        )

    async def delete_discord_project_prompt(self, forum_id: int) -> None:
        await self._write(
            "DELETE FROM discord_project_prompts WHERE forum_id = ?",
            (str(forum_id),),
        )
