"""Pinned workflow-preset controller and stage-run journal."""
import aiosqlite
SQL = """
CREATE TABLE preset_workflows (
 id TEXT PRIMARY KEY, observer_session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
 plan JSON NOT NULL, preset_hash TEXT NOT NULL, spec_hash TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('queued','running','cancelling','succeeded','failed','cancelled','lost','blocked')),
 revision INTEGER NOT NULL DEFAULT 0, result JSON, created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, updated_at TEXT NOT NULL);
CREATE INDEX idx_preset_workflows_active ON preset_workflows(status, created_at);
CREATE TABLE workflow_stage_runs (
 id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL REFERENCES preset_workflows(id) ON DELETE CASCADE,
 stage_id TEXT NOT NULL, runner TEXT NOT NULL CHECK(runner IN ('agent','execution')), spec JSON NOT NULL, spec_hash TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('queued','starting','running','cancelling','succeeded','failed','cancelled','lost')),
 child_type TEXT, child_id TEXT, revision INTEGER NOT NULL DEFAULT 0, result JSON, artifact JSON,
 created_at TEXT NOT NULL, started_at TEXT, finished_at TEXT, updated_at TEXT NOT NULL,
 UNIQUE(workflow_id, stage_id));
CREATE INDEX idx_workflow_stage_active ON workflow_stage_runs(workflow_id,status);
CREATE TABLE workflow_completion_outbox (
 workflow_id TEXT PRIMARY KEY REFERENCES preset_workflows(id) ON DELETE CASCADE,
 state TEXT NOT NULL CHECK(state IN ('pending','claimed','completed','suppressed','failed')),
 claimed_at TEXT, completed_at TEXT, error TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
ALTER TABLE executions ADD COLUMN completion_target_type TEXT NOT NULL DEFAULT 'session' CHECK(completion_target_type IN ('session','workflow'));
ALTER TABLE executions ADD COLUMN completion_target_id TEXT;
"""
async def up(db: aiosqlite.Connection) -> None: await db.executescript(SQL)
