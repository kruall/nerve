"""Durable operator decisions for stopped preset workflows."""
import aiosqlite

SQL = """
CREATE TABLE workflow_action_audit (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 workflow_id TEXT NOT NULL REFERENCES preset_workflows(id) ON DELETE CASCADE,
 actor TEXT NOT NULL, action TEXT NOT NULL, reason TEXT, idempotency_key TEXT NOT NULL,
 prior_revision INTEGER NOT NULL, resulting_status TEXT NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(workflow_id, idempotency_key)
);
CREATE INDEX idx_workflow_action_audit_workflow ON workflow_action_audit(workflow_id, id);
"""

async def up(db: aiosqlite.Connection) -> None:
    await db.executescript(SQL)
