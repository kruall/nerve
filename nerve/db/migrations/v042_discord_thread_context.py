"""V42: Persist bounded context for Discord project-forum threads."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS discord_thread_contexts (
            thread_id TEXT PRIMARY KEY,
            guild_id TEXT NOT NULL,
            forum_id TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            summary_through_message_id TEXT NOT NULL DEFAULT '0',
            recent_messages JSON NOT NULL DEFAULT '[]',
            last_message_id TEXT NOT NULL DEFAULT '0',
            last_delivered_message_id TEXT NOT NULL DEFAULT '0',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_discord_thread_contexts_forum
            ON discord_thread_contexts(forum_id, updated_at);
        """
    )
    logger.info("v042: added persistent Discord project-thread context")
