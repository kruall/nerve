"""Join/detach/forget lifecycle state for executions and workflow runs."""

from __future__ import annotations

import aiosqlite


SQL = """
ALTER TABLE executions ADD COLUMN auto_continue INTEGER NOT NULL DEFAULT 1;

ALTER TABLE workflow_runs ADD COLUMN owner_session_id TEXT;
ALTER TABLE workflow_runs ADD COLUMN auto_continue INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_runs ADD COLUMN continuation_state TEXT NOT NULL DEFAULT 'none'
    CHECK(continuation_state IN ('none','pending','claimed','completed','failed','suppressed'));
ALTER TABLE workflow_runs ADD COLUMN continuation_claimed_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN continuation_completed_at TEXT;
ALTER TABLE workflow_runs ADD COLUMN continuation_error TEXT;

CREATE INDEX idx_workflow_runs_continuation
    ON workflow_runs(continuation_state, finished_at ASC);
"""


async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
