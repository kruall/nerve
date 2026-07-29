"""V45: Persist the model-routing tier and reasoning effort per session."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import aiosqlite

logger = logging.getLogger(__name__)


async def up(db: aiosqlite.Connection) -> None:
    installed_at = datetime.now(timezone.utc).isoformat()
    await db.execute("ALTER TABLE sessions ADD COLUMN model_tier TEXT")
    await db.execute("ALTER TABLE sessions ADD COLUMN reasoning_effort TEXT")
    await db.execute(
        "ALTER TABLE sessions ADD COLUMN model_pinned INTEGER NOT NULL DEFAULT 0"
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS model_routing_audit_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            cursor_updated_at TEXT NOT NULL DEFAULT '',
            cursor_session_id TEXT NOT NULL DEFAULT '',
            last_run_at TEXT,
            last_summary TEXT
        )
        """
    )
    await db.execute(
        """
        INSERT OR IGNORE INTO model_routing_audit_state
            (id, cursor_updated_at, cursor_session_id)
        VALUES (1, ?, '')
        """,
        (installed_at,),
    )
    logger.info(
        "v045: added session routing fields and model-routing audit state"
    )
