"""V41: Persist Discord audit-forum session mirrors."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS discord_session_mirrors (
            session_id TEXT PRIMARY KEY REFERENCES sessions(id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            guild_id TEXT NOT NULL,
            forum_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            starter_message_id TEXT NOT NULL,
            header_hash TEXT NOT NULL DEFAULT '',
            live_message_ids JSON NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_discord_mirrors_forum
            ON discord_session_mirrors(forum_id, updated_at);

        CREATE TABLE IF NOT EXISTS discord_session_mirror_items (
            session_id TEXT NOT NULL REFERENCES sessions(id)
                ON DELETE CASCADE ON UPDATE CASCADE,
            item_kind TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            discord_message_ids JSON NOT NULL,
            content_hash TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (session_id, item_kind, item_id)
        );
        CREATE INDEX IF NOT EXISTS idx_discord_mirror_items_session
            ON discord_session_mirror_items(session_id, item_kind, item_id);
        """
    )
    logger.info("v041: added persistent Discord session mirror state")
