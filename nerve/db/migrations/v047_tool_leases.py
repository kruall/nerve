"""V47: Durable exclusive-tool leases and handoff subscriptions."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS tool_leases (
            tool_name TEXT PRIMARY KEY,
            session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            acquired_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_tool_leases_expires_at ON tool_leases(expires_at);

        CREATE TABLE IF NOT EXISTS tool_lease_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tool_name TEXT NOT NULL,
            session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            prompt TEXT NOT NULL,
            lease_seconds INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            UNIQUE(tool_name, session_id)
        );
        CREATE INDEX IF NOT EXISTS idx_tool_lease_subscriptions_ready
            ON tool_lease_subscriptions(tool_name, created_at);
        CREATE INDEX IF NOT EXISTS idx_tool_lease_subscriptions_expires_at
            ON tool_lease_subscriptions(expires_at);
        """
    )
    logger.info("v047: added durable tool leases and subscriptions")
