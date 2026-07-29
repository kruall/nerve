"""V44: persist immutable Discord delivery targets for sessions."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


SQL = """
CREATE TABLE IF NOT EXISTS discord_session_bindings (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    guild_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_discord_session_bindings_thread
    ON discord_session_bindings(guild_id, thread_id);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)

    # Best-effort backfill for existing Discord sessions. channel_sessions is
    # mutable (a thread can roll over to a fresh session after the sticky
    # period), so keep the oldest mapping for each session: that is the
    # closest durable record of the thread that originally created it.
    async with db.execute(
        """SELECT channel_key, session_id
           FROM channel_sessions
           WHERE channel_key LIKE 'discord:%:%'
           ORDER BY updated_at ASC, channel_key ASC"""
    ) as cursor:
        rows = await cursor.fetchall()

    seen_sessions: set[str] = set()
    backfilled = 0
    for row in rows:
        channel_key = str(row[0])
        session_id = str(row[1])
        if session_id in seen_sessions:
            continue

        parts = channel_key.split(":", 2)
        if (
            len(parts) != 3
            or parts[0] != "discord"
            or not parts[1].isdigit()
            or not parts[2].isdigit()
        ):
            continue

        seen_sessions.add(session_id)
        result = await db.execute(
            """INSERT OR IGNORE INTO discord_session_bindings
                   (session_id, guild_id, thread_id)
               VALUES (?, ?, ?)""",
            (session_id, parts[1], parts[2]),
        )
        if result.rowcount > 0:
            backfilled += 1

    logger.info(
        "v044: added persistent Discord session bindings; backfilled %d",
        backfilled,
    )
