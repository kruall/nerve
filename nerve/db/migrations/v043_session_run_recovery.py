"""V43: durable checkpoints for agent turns interrupted by a restart."""

from __future__ import annotations

import aiosqlite


SQL = """
CREATE TABLE IF NOT EXISTS session_run_recovery (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    channel TEXT,
    user_message TEXT NOT NULL DEFAULT '',
    channel_context JSON,
    started_at TEXT NOT NULL,
    recovery_attempts INTEGER NOT NULL DEFAULT 0,
    last_recovery_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_session_run_recovery_started
    ON session_run_recovery(started_at);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
