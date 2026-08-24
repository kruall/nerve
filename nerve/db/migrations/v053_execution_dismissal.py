"""Persist UI dismissal without deleting detached execution evidence."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    await db.execute("ALTER TABLE executions ADD COLUMN dismissed_at TEXT")
    await db.execute(
        "CREATE INDEX idx_executions_session_visible_created "
        "ON executions(session_id, dismissed_at, created_at DESC)",
    )
    logger.info("v053: execution dismissal state added")
