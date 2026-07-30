"""V48: Persist Discord project-task completion audit state."""

from __future__ import annotations

from datetime import datetime, timezone

import aiosqlite


async def up(db: aiosqlite.Connection) -> None:
    activated_at = datetime.now(timezone.utc).isoformat()
    await db.executescript(
        """
        CREATE TABLE IF NOT EXISTS discord_project_task_audit_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            activated_at TEXT NOT NULL,
            baseline_initialized INTEGER NOT NULL DEFAULT 0,
            baseline_thread_ids JSON NOT NULL DEFAULT '[]',
            last_run_at TEXT,
            last_summary TEXT
        );

        CREATE TABLE IF NOT EXISTS discord_project_task_audits (
            thread_id TEXT PRIMARY KEY,
            guild_id TEXT NOT NULL,
            project TEXT NOT NULL,
            completion_notification_id TEXT,
            completion_record JSON NOT NULL DEFAULT '{}',
            result TEXT NOT NULL CHECK (result IN ('verified', 'follow-up-created')),
            summary TEXT NOT NULL DEFAULT '',
            follow_up_task_ids JSON NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_discord_project_task_audits_project
            ON discord_project_task_audits(project, updated_at);
        """
    )
    await db.execute(
        """INSERT OR IGNORE INTO discord_project_task_audit_state
               (id, activated_at, baseline_initialized, baseline_thread_ids)
           VALUES (1, ?, 0, '[]')""",
        (activated_at,),
    )
