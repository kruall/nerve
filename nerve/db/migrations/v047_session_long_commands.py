"""V47: durable detached build/test commands."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS session_long_commands (
            id           TEXT PRIMARY KEY,
            session_id   TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            command_json TEXT NOT NULL,
            cwd          TEXT NOT NULL,
            output_path  TEXT NOT NULL,
            status_path  TEXT NOT NULL,
            process_pid  INTEGER,
            timeout_at   TEXT NOT NULL,
            prompt       TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'running',
            resume_state TEXT NOT NULL DEFAULT 'pending',
            exit_code    INTEGER,
            details      TEXT NOT NULL DEFAULT '',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at  TIMESTAMP
        )
        """
    )
    await db.execute(
        """CREATE INDEX IF NOT EXISTS idx_long_commands_running
           ON session_long_commands(status, timeout_at)"""
    )
    logger.info("v047: created session_long_commands table")
