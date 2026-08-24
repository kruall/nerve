"""Reconcile task-board schema for databases upgraded from kruall_main_v2.

The pre-rebase fork used migration versions V43-V62 for execution/resource
state.  Upstream main independently used V43/V44 for ``tasks.position`` and
``task_events``.  The runner translates legacy fork version markers by +2;
this final, idempotent migration adds the upstream pieces those databases
could not have applied under their original version numbers.
"""

from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)

_POSITION_GAP = 1024.0


async def _column_exists(
    db: aiosqlite.Connection, table: str, column: str,
) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return any(row[1] == column for row in await cursor.fetchall())


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


async def _add_and_backfill_task_position(db: aiosqlite.Connection) -> int:
    if await _column_exists(db, "tasks", "position"):
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_tasks_status_position "
            "ON tasks(status, position)"
        )
        return 0

    await db.execute(
        "ALTER TABLE tasks ADD COLUMN position REAL NOT NULL DEFAULT 0"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tasks_status_position "
        "ON tasks(status, position)"
    )
    async with db.execute(
        "SELECT id, status FROM tasks "
        "ORDER BY status ASC, deadline ASC NULLS LAST, created_at DESC, id DESC"
    ) as cursor:
        rows = await cursor.fetchall()

    updates: list[tuple[float, str]] = []
    current_lane: object = object()
    rank = 0.0
    for task_id, status in rows:
        if status != current_lane:
            current_lane = status
            rank = 0.0
        rank += _POSITION_GAP
        updates.append((rank, task_id))
    if updates:
        await db.executemany(
            "UPDATE tasks SET position = ? WHERE id = ?", updates,
        )
    return len(updates)


async def _create_and_seed_task_events(db: aiosqlite.Connection) -> int:
    if await _table_exists(db, "task_events"):
        return 0

    await db.executescript(
        """
        CREATE TABLE task_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            from_status TEXT,
            to_status TEXT NOT NULL,
            actor TEXT NOT NULL DEFAULT 'system',
            created_at TEXT NOT NULL
        );
        CREATE INDEX idx_task_events_task
            ON task_events(task_id, created_at);
        CREATE INDEX idx_task_events_created
            ON task_events(created_at);
        """
    )
    await db.execute(
        """
        INSERT INTO task_events (
            task_id, from_status, to_status, actor, created_at
        )
        SELECT id, NULL, status, 'backfill', COALESCE(created_at, updated_at)
        FROM tasks
        WHERE COALESCE(created_at, updated_at) IS NOT NULL
        """
    )
    async with db.execute("SELECT COUNT(*) FROM task_events") as cursor:
        return (await cursor.fetchone())[0]


async def up(db: aiosqlite.Connection) -> None:
    positioned = await _add_and_backfill_task_position(db)
    events = await _create_and_seed_task_events(db)
    logger.info(
        "v065: reconciled task-board lineage (%d positions, %d events)",
        positioned,
        events,
    )
