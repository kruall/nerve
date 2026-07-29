"""V46: Persist managed editable prompts for Discord project forums."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS discord_project_prompts (
            forum_id TEXT PRIMARY KEY,
            guild_id TEXT NOT NULL,
            project TEXT NOT NULL,
            thread_id TEXT NOT NULL UNIQUE,
            message_id TEXT NOT NULL DEFAULT '',
            content TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    logger.info("v046: added Discord project-forum prompts")
