"""Tests for database migration discovery invariants."""

import importlib
from types import SimpleNamespace

import aiosqlite
import pytest

from nerve.db.migrations import runner


async def _apply_through(db: aiosqlite.Connection, ceiling: int) -> None:
    for version, module_name in runner.discover_migrations():
        if version > ceiling:
            break
        module = importlib.import_module(
            f"nerve.db.migrations.{module_name}"
        )
        await module.up(db)
        await db.execute(
            "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
            (version,),
        )
        await db.commit()


def test_discover_migrations_rejects_duplicate_versions(monkeypatch):
    modules = [
        SimpleNamespace(name="v049_first"),
        SimpleNamespace(name="v049_second"),
    ]
    monkeypatch.setattr(runner.pkgutil, "iter_modules", lambda _paths: modules)

    with pytest.raises(RuntimeError, match="Duplicate database migration version V49"):
        runner.discover_migrations()


@pytest.mark.asyncio
async def test_v49_database_applies_later_migrations(tmp_path):
    db = await aiosqlite.connect(tmp_path / "nerve.db")
    try:
        await db.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        await db.execute("INSERT INTO schema_version (version) VALUES (49)")
        await db.execute(
            """CREATE TABLE executions (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL, plan JSON NOT NULL,
                status TEXT NOT NULL, created_at TEXT NOT NULL
            )"""
        )
        await db.execute(
            """CREATE TABLE tasks (
                id TEXT PRIMARY KEY, title TEXT NOT NULL,
                status TEXT NOT NULL, deadline TEXT,
                created_at TEXT, updated_at TEXT
            )"""
        )
        # Legacy fork V49 already includes its resource queue (now V47).
        # Keep this synthetic
        # fixture minimal, but structurally valid for later ALTER migrations.
        await db.execute(
            """CREATE TABLE resource_lease_requests (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                execution_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                slot TEXT NOT NULL,
                pool TEXT NOT NULL,
                mode TEXT NOT NULL,
                state TEXT NOT NULL,
                lease_id TEXT,
                requested_at TEXT NOT NULL,
                settled_at TEXT
            )"""
        )
        # Later migrations also extend these V49 tables.  Their remaining
        # historical columns are immaterial to the upgrade path under test.
        await db.execute(
            """CREATE TABLE resource_hosts (
                id TEXT PRIMARY KEY,
                connection_ref TEXT NOT NULL,
                display_name TEXT NOT NULL,
                labels JSON NOT NULL DEFAULT '{}',
                capabilities JSON NOT NULL DEFAULT '{}',
                enabled INTEGER NOT NULL DEFAULT 1,
                draining INTEGER NOT NULL DEFAULT 0,
                offline INTEGER NOT NULL DEFAULT 0,
                quarantined INTEGER NOT NULL DEFAULT 0,
                quarantine_reason TEXT,
                fencing_token INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""",
        )
        await db.execute(
            """CREATE TABLE preset_workflows (
                id TEXT PRIMARY KEY,
                observer_session_id TEXT NOT NULL,
                plan JSON NOT NULL,
                preset_hash TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                result JSON,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                updated_at TEXT NOT NULL,
                completion_mode TEXT NOT NULL DEFAULT 'observer'
            )"""
        )
        await db.commit()

        assert await runner.run_migrations(db) == 65
        async with db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='session_resource_reservations'"
        ) as cursor:
            assert await cursor.fetchone() == ("session_resource_reservations",)
        async with db.execute("PRAGMA table_info(executions)") as cursor:
            execution_columns = {row[1] async for row in cursor}
        async with db.execute("PRAGMA table_info(resource_lease_requests)") as cursor:
            request_columns = {row[1] async for row in cursor}
        async with db.execute("PRAGMA table_info(resource_hosts)") as cursor:
            host_columns = {row[1] async for row in cursor}
        async with db.execute("PRAGMA table_info(session_resource_handles)") as cursor:
            handle_columns = {row[1] async for row in cursor}
        async with db.execute("PRAGMA table_info(operation_resource_refs)") as cursor:
            ref_columns = {row[1] async for row in cursor}
        async with db.execute("PRAGMA table_info(preset_workflows)") as cursor:
            workflow_columns = {row[1] async for row in cursor}

        assert {"dismissed_at", "parent_operation_id", "private_operation"} <= execution_columns
        assert {"bundle_id", "queue_ticket", "requested_host"} <= request_columns
        assert {
            "recovery_generation", "recovery_claimed_generation",
            "permanently_unavailable", "recovery_claim_state",
            "recovery_claimed_at", "recovery_claim_expires_at", "recovery_retry_at",
        } <= host_columns
        assert {"auto_release_when_session_idle", "worktree_identity"} <= handle_columns
        assert "position" in ref_columns
        assert {"parent_operation_id", "allocation_state"} <= workflow_columns

        async with db.execute("PRAGMA index_list(executions)") as cursor:
            execution_indexes = {row[1] async for row in cursor}
        async with db.execute("PRAGMA index_list(resource_hosts)") as cursor:
            host_indexes = {row[1] async for row in cursor}
        async with db.execute("PRAGMA index_list(operation_resource_refs)") as cursor:
            ref_indexes = {row[1] async for row in cursor}
        async with db.execute("PRAGMA index_list(preset_workflows)") as cursor:
            workflow_indexes = {row[1] async for row in cursor}
        assert "idx_executions_parent_operation" in execution_indexes
        assert "uq_executions_one_active_per_session" not in execution_indexes
        assert "idx_resource_hosts_recovery_claim" in host_indexes
        assert "uq_operation_resource_refs_one_active_per_handle" in ref_indexes
        assert {
            "uq_preset_workflows_parent_operation",
            "idx_preset_workflows_allocation_state",
        } <= workflow_indexes
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_upstream_v44_database_applies_shifted_fork_chain(tmp_path):
    db = await aiosqlite.connect(tmp_path / "upstream-v44.db")
    try:
        await _apply_through(db, 44)
        assert await runner.get_current_version(db) == 44
        assert not await runner._table_exists(db, "executions")

        assert await runner.run_migrations(db) == 65
        assert await runner._table_exists(db, "executions")
        assert await runner._table_exists(db, "task_events")
        async with db.execute("PRAGMA table_info(tasks)") as cursor:
            assert "position" in {row[1] async for row in cursor}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_legacy_fork_v62_is_remapped_and_gets_task_board_schema(tmp_path):
    db = await aiosqlite.connect(tmp_path / "legacy-fork-v62.db")
    try:
        await db.execute(
            "CREATE TABLE schema_version (version INTEGER PRIMARY KEY)"
        )
        await db.execute(
            "INSERT INTO schema_version (version) VALUES (62)"
        )
        await db.execute("CREATE TABLE executions (id TEXT PRIMARY KEY)")
        await db.execute(
            """CREATE TABLE tasks (
                id TEXT PRIMARY KEY, title TEXT NOT NULL,
                status TEXT NOT NULL, deadline TEXT,
                created_at TEXT, updated_at TEXT
            )"""
        )
        await db.execute(
            """INSERT INTO tasks (
                id, title, status, created_at, updated_at
            ) VALUES ('task-1', 'Legacy task', 'pending', '2000', '2000')"""
        )
        await db.commit()

        assert await runner.run_migrations(db) == 65
        async with db.execute("SELECT MAX(version) FROM schema_version") as cursor:
            assert (await cursor.fetchone())[0] == 65
        async with db.execute("SELECT position FROM tasks WHERE id='task-1'") as cursor:
            assert (await cursor.fetchone())[0] == 1024.0
        async with db.execute(
            "SELECT task_id, actor FROM task_events"
        ) as cursor:
            assert await cursor.fetchall() == [("task-1", "backfill")]
    finally:
        await db.close()
