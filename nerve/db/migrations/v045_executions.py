"""Durable session-owned detached executions and bounded logs."""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


SQL = """
CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    profile_snapshot JSON NOT NULL,
    plan JSON NOT NULL,
    resource_requests JSON NOT NULL DEFAULT '[]',
    selected_leases JSON NOT NULL DEFAULT '[]',
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'starting', 'running', 'cancelling',
        'succeeded', 'failed', 'cancelled', 'lost'
    )),
    revision INTEGER NOT NULL DEFAULT 0,
    backend_name TEXT,
    backend_handle JSON,
    result JSON,
    cancel_reason TEXT,
    cancel_requested_at TEXT,
    continuation_state TEXT NOT NULL DEFAULT 'none' CHECK (
        continuation_state IN ('none', 'pending', 'claimed', 'completed', 'failed', 'suppressed')
    ),
    continuation_claimed_at TEXT,
    continuation_completed_at TEXT,
    continuation_error TEXT,
    created_at TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_executions_session_created
    ON executions(session_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_status_created
    ON executions(status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_executions_continuation
    ON executions(continuation_state, finished_at ASC);

-- The first lifecycle implementation deliberately permits one active detached
-- execution per owner session.  The constraint makes concurrent starts safe;
-- a future configurable multi-execution mode can replace this index.
CREATE UNIQUE INDEX IF NOT EXISTS uq_executions_one_active_per_session
    ON executions(session_id)
    WHERE status IN ('queued', 'starting', 'running', 'cancelling');

CREATE TABLE IF NOT EXISTS execution_logs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(id) ON DELETE CASCADE,
    stream TEXT NOT NULL CHECK (stream IN ('stdout', 'stderr', 'system')),
    timestamp TEXT NOT NULL,
    text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_logs_tail
    ON execution_logs(execution_id, sequence DESC);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
    logger.info("v045: executions and execution_logs tables created")
