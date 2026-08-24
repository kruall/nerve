"""Focused contract tests for V55-V58 retained-resource persistence."""

import importlib

import aiosqlite
import pytest

from nerve.db import Database
from nerve.db.migrations import runner
from nerve.resources import LeaseService, ResourceInventory


TABLES = (
    "session_resource_handles",
    "operation_resource_refs",
    "resource_wait_operations",
    "resource_recovery_intents",
    "resource_wait_allocator",
)


async def _schema(db: aiosqlite.Connection) -> dict[str, str]:
    rows = await (
        await db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') "
            "AND (name IN (?, ?, ?, ?, ?) OR name LIKE 'idx_resource_%' "
            "OR name LIKE 'uq_session_resource_handles_%' "
            "OR name IN ('uq_operation_resource_refs_one_active_per_handle', "
            "'uq_executions_one_active_per_session')) ORDER BY name",
            TABLES,
        )
    ).fetchall()
    return {name: " ".join(sql.split()) for name, sql in rows if sql}


async def _apply_through_v54(db: aiosqlite.Connection) -> None:
    for version, module_name in runner.discover_migrations():
        if version > 54:
            break
        await importlib.import_module(f"nerve.db.migrations.{module_name}").up(db)
        await db.execute("INSERT INTO schema_version(version) VALUES (?)", (version,))
        await db.commit()


async def _apply_through_v59(db: aiosqlite.Connection) -> None:
    for version, module_name in runner.discover_migrations():
        if version > 59:
            break
        await importlib.import_module(f"nerve.db.migrations.{module_name}").up(db)
        await db.execute("INSERT INTO schema_version(version) VALUES (?)", (version,))
        await db.commit()


async def _apply_through_v60(db: aiosqlite.Connection) -> None:
    for version, module_name in runner.discover_migrations():
        if version > 60:
            break
        await importlib.import_module(f"nerve.db.migrations.{module_name}").up(db)
        await db.execute("INSERT INTO schema_version(version) VALUES (?)", (version,))
        await db.commit()


async def _legacy_rows(db: aiosqlite.Connection) -> None:
    await db.execute(
        "INSERT INTO sessions(id, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
        ("session-legacy", "legacy", "2000-01-01", "2000-01-01"),
    )
    await db.execute(
        """INSERT INTO resource_hosts(id, connection_ref, display_name, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        ("host-legacy", "ssh://legacy", "legacy", "2000-01-01", "2000-01-01"),
    )
    await db.execute(
        """INSERT INTO resource_leases(
               id, host_id, execution_id, session_id, pool, fencing_token, state,
               requested_at, acquired_at, heartbeat_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("lease-legacy", "host-legacy", "old-execution", "session-legacy", "pool-a", 4,
         "released", "2000-01-01", "2000-01-01", "2000-01-01"),
    )
    await db.commit()


async def _legacy_snapshot(db: aiosqlite.Connection) -> tuple[tuple, tuple, tuple]:
    return (
        await (await db.execute("SELECT * FROM sessions WHERE id='session-legacy'")).fetchone(),
        await (await db.execute("SELECT * FROM resource_hosts WHERE id='host-legacy'")).fetchone(),
        await (await db.execute("SELECT * FROM resource_leases WHERE id='lease-legacy'")).fetchone(),
    )


@pytest.mark.asyncio
async def test_v065_fresh_and_upgrade_schemas_match_and_preserve_legacy_rows(tmp_path):
    fresh = await aiosqlite.connect(tmp_path / "fresh.db")
    upgraded = await aiosqlite.connect(tmp_path / "upgrade.db")
    try:
        await runner.run_migrations(fresh)
        await _apply_through_v54(upgraded)
        await _legacy_rows(upgraded)
        legacy_before = await _legacy_snapshot(upgraded)

        assert await runner.run_migrations(upgraded) == 65
        fresh_schema = await _schema(fresh)
        assert fresh_schema == await _schema(upgraded)
        assert (await (await fresh.execute("SELECT next_ticket FROM resource_wait_allocator")).fetchone())[0] == 1
        assert "idx_resource_wait_operations_pending_ticket_host" in fresh_schema
        columns = {row[1] for row in await (await fresh.execute("PRAGMA table_info(resource_hosts)")).fetchall()}
        assert {"recovery_generation", "recovery_claimed_generation", "permanently_unavailable", "recovery_claim_state", "recovery_claim_expires_at", "recovery_retry_at", "supervisor_provisioned_at", "supervisor_provision_status", "supervisor_provision_error"} <= columns
        assert "idx_resource_hosts_recovery_claim" in fresh_schema
        # V059/V060 only append defaulted columns; legacy values stay intact.
        after = await _legacy_snapshot(upgraded)
        assert after[0] == legacy_before[0] and after[2] == legacy_before[2]
        assert after[1][:len(legacy_before[1])] == legacy_before[1]
        ref_columns = {row[1] for row in await (await fresh.execute("PRAGMA table_info(operation_resource_refs)")).fetchall()}
        handle_columns = {row[1] for row in await (await fresh.execute("PRAGMA table_info(session_resource_handles)")).fetchall()}
        assert {"position"} <= ref_columns
        assert {"auto_release_when_session_idle", "worktree_identity"} <= handle_columns
        indexes = {row[1] for row in await (await fresh.execute("PRAGMA index_list(operation_resource_refs)")).fetchall()}
        assert "uq_operation_resource_refs_one_active_per_handle" in indexes
        execution_indexes = {row[1] for row in await (await fresh.execute("PRAGMA index_list(executions)")).fetchall()}
        assert "uq_executions_one_active_per_session" not in execution_indexes
        execution_columns = {row[1] for row in await (await fresh.execute("PRAGMA table_info(executions)")).fetchall()}
        workflow_columns = {row[1] for row in await (await fresh.execute("PRAGMA table_info(preset_workflows)")).fetchall()}
        assert {"parent_operation_id", "private_operation"} <= execution_columns
        assert {"parent_operation_id", "allocation_state"} <= workflow_columns
        assert {"idx_executions_parent_operation", "uq_preset_workflows_parent_operation"} <= {
            row[1] for row in await (await fresh.execute("PRAGMA index_list(executions)")).fetchall()
        } | {
            row[1] for row in await (await fresh.execute("PRAGMA index_list(preset_workflows)")).fetchall()
        }
    finally:
        await fresh.close()
        await upgraded.close()


@pytest.mark.asyncio
async def test_v060_backfills_deterministic_ref_positions_and_enforces_order_uniqueness(tmp_path):
    db = await aiosqlite.connect(tmp_path / "v059.db")
    try:
        await _apply_through_v59(db)
        # Foreign-key enforcement is intentionally immaterial to this schema
        # migration; V060 must deterministically order every historical row.
        await db.executemany(
            "INSERT INTO operation_resource_refs(operation_id, handle_id, created_at) VALUES (?, ?, ?)",
            [
                ("operation-a", "handle-b", "2000-01-02"),
                ("operation-a", "handle-c", "2000-01-01"),
                ("operation-a", "handle-a", "2000-01-01"),
            ],
        )
        await db.commit()
        migration = importlib.import_module("nerve.db.migrations.v060_ordered_operation_resource_refs")
        await migration.up(db)
        rows = await (await db.execute(
            "SELECT handle_id, position FROM operation_resource_refs WHERE operation_id='operation-a' ORDER BY position",
        )).fetchall()
        assert rows == [("handle-a", 0), ("handle-c", 1), ("handle-b", 2)]
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                "INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at) VALUES ('operation-a', 'handle-d', 0, 'now')",
            )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_v061_removes_terminal_refs_before_installing_handle_fence(tmp_path):
    db = await aiosqlite.connect(tmp_path / "terminal-refs.db")
    try:
        await _apply_through_v60(db)
        await db.execute(
            "INSERT INTO sessions(id, title, created_at, updated_at) VALUES ('s', 's', 'now', 'now')"
        )
        for operation_id, status in (("terminal", "failed"), ("active", "running")):
            await db.execute(
                """INSERT INTO executions(
                       id, session_id, kind, profile_version, profile_hash,
                       profile_snapshot, plan, status, created_at, queued_at, updated_at
                   ) VALUES (?, 's', 'remote', '1', 'hash', '{}', '{}', ?, 'now', 'now', 'now')""",
                (operation_id, status),
            )
        await db.executemany(
            """INSERT INTO operation_resource_refs(
                   operation_id, handle_id, position, created_at
               ) VALUES (?, 'shared-handle', 0, 'now')""",
            [("terminal",), ("active",)],
        )
        await db.commit()

        migration = importlib.import_module("nerve.db.migrations.v061_concurrent_operation_handles")
        await migration.up(db)

        refs = await (await db.execute(
            "SELECT operation_id, handle_id FROM operation_resource_refs ORDER BY operation_id"
        )).fetchall()
        assert refs == [("active", "shared-handle")]
        indexes = {row[1] for row in await (await db.execute(
            "PRAGMA index_list(operation_resource_refs)"
        )).fetchall()}
        assert "uq_operation_resource_refs_one_active_per_handle" in indexes
        execution_indexes = {row[1] for row in await (await db.execute(
            "PRAGMA index_list(executions)"
        )).fetchall()}
        assert "uq_executions_one_active_per_session" not in execution_indexes
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_v57_backfills_pending_waits_and_queued_bundle_with_unique_tickets(tmp_path):
    db = await aiosqlite.connect(tmp_path / "backfill.db")
    try:
        await _apply_through_v54(db)
        for ident in ("s", "ordinary"):
            await db.execute("INSERT INTO sessions(id, title, created_at, updated_at) VALUES (?, ?, 'now', 'now')", (ident, ident))
        for ident, session in (("wait-op", "s"), ("ordinary-op", "ordinary")):
            await db.execute("""INSERT INTO executions(id, session_id, kind, profile_version, profile_hash, profile_snapshot, plan, status, created_at, queued_at, updated_at)
                              VALUES (?, ?, 'remote', '1', 'h', '{}', '{}', 'queued', 'now', 'now', 'now')""", (ident, session))
        await importlib.import_module("nerve.db.migrations.v055_resource_operation_handles").up(db)
        await importlib.import_module("nerve.db.migrations.v056_resource_wait_outcomes").up(db)
        await db.execute("""INSERT INTO resource_wait_operations(id, session_id, operation_id, request_kind, requested_hosts_json, pool, queue_ticket, state, created_at, updated_at)
                          VALUES ('wait', 's', 'wait-op', 'host', '[{"pool":"p","host":"a"}]', 'p', 99, 'pending', '2000', '2000')""")
        await db.execute("INSERT INTO resource_lease_bundles(id, execution_id, session_id, state, requested_at) VALUES ('bundle', 'ordinary-op', 'ordinary', 'queued', '2001')")
        for ident in ("r1", "r2"):
            await db.execute("""INSERT INTO resource_lease_requests(id, execution_id, session_id, slot, pool, mode, state, requested_at, bundle_id)
                              VALUES (?, 'ordinary-op', 'ordinary', ?, 'p', 'exclusive', 'queued', '2001', 'bundle')""", (ident, ident))
        await importlib.import_module("nerve.db.migrations.v057_resource_wait_fair_queue").up(db)
        tickets = await (await db.execute("SELECT DISTINCT queue_ticket FROM resource_lease_requests WHERE bundle_id='bundle'")).fetchall()
        wait_ticket = (await (await db.execute("SELECT queue_ticket FROM resource_wait_operations WHERE id='wait'")).fetchone())[0]
        next_ticket = (await (await db.execute("SELECT next_ticket FROM resource_wait_allocator")).fetchone())[0]
        assert len(tickets) == 1 and tickets[0][0] != wait_ticket and next_ticket > max(tickets[0][0], wait_ticket)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_v58_backfills_exact_host_and_reattaches_queued_bundle(tmp_path):
    database = Database(tmp_path / "restart.db")
    try:
        await database.connect()
        await database.db.execute("ALTER TABLE resource_lease_requests DROP COLUMN requested_host")
        await database.db.execute("DELETE FROM schema_version WHERE version=58")
        await database.db.commit()

        await database.create_session("session-1")
        await database.create_execution(
            "execution-1", session_id="session-1", kind="remote", profile_version="1",
            profile_hash="hash", profile_snapshot={},
            plan={"resource_hosts": {"exact": "host-a"}}, resource_requests=[],
        )
        await database.db.execute(
            """INSERT INTO resource_lease_bundles(
                   id, execution_id, session_id, state, requested_at
               ) VALUES ('bundle-1', 'execution-1', 'session-1', 'queued', 'now')"""
        )
        await database.db.execute(
            """INSERT INTO resource_lease_requests(
                   id, execution_id, session_id, slot, pool, mode, state,
                   requested_at, bundle_id, queue_ticket
               ) VALUES ('request-exact', 'execution-1', 'session-1', 'exact', 'workers',
                         'exclusive', 'queued', 'now', 'bundle-1', 7)"""
        )
        await database.db.execute(
            """INSERT INTO resource_lease_requests(
                   id, execution_id, session_id, slot, pool, mode, state,
                   requested_at, bundle_id, queue_ticket
               ) VALUES ('request-any', 'execution-1', 'session-1', 'any', 'workers',
                         'exclusive', 'queued', 'now', 'bundle-1', 7)"""
        )
        await database.db.commit()

        migration = importlib.import_module("nerve.db.migrations.v058_resource_request_hosts")
        await migration.up(database.db)
        await database.db.commit()
        rows = await (
            await database.db.execute(
                "SELECT id, requested_host, queue_ticket FROM resource_lease_requests ORDER BY id"
            )
        ).fetchall()
        assert [(row[0], row[1], row[2]) for row in rows] == [
            ("request-any", None, 7), ("request-exact", "host-a", 7),
        ]

        inventory = ResourceInventory(database, {
            "connections": ["lab-ssh"],
            "hosts": [
                {"id": "host-a", "connection_ref": "lab-ssh"},
                {"id": "host-b", "connection_ref": "lab-ssh"},
            ],
            "pools": [{"id": "workers", "members": ["host-a", "host-b"]}],
        })
        await inventory.initialize()
        service = LeaseService(db=database, inventory=inventory)
        leases = await service.acquire(
            execution_id="execution-1", session_id="session-1",
            requests=[
                {"slot": "exact", "pool": "workers", "host": "host-a"},
                {"slot": "any", "pool": "workers"},
            ],
        )
        assert {lease["host_id"] for lease in leases} == {"host-a", "host-b"}
        retained = await (
            await database.db.execute(
                "SELECT queue_ticket FROM resource_lease_requests WHERE bundle_id='bundle-1'"
            )
        ).fetchall()
        assert [row[0] for row in retained] == [7, 7]
    finally:
        await database.close()


@pytest.mark.asyncio
async def test_v55_rejects_invalid_states_and_duplicate_resource_references(tmp_path):
    db = await aiosqlite.connect(tmp_path / "handles.db")
    try:
        await db.execute("PRAGMA foreign_keys = ON")
        await runner.run_migrations(db)
        await db.execute(
            "INSERT INTO sessions(id, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
            ("session-1", "s", "now", "now"),
        )
        await db.execute(
            """INSERT INTO executions(
                   id, session_id, kind, profile_version, profile_hash, profile_snapshot,
                   plan, status, created_at, queued_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("operation-1", "session-1", "remote", "1", "hash", "{}", "{}", "queued",
             "now", "now", "now"),
        )
        await db.execute(
            """INSERT INTO resource_hosts(id, connection_ref, display_name, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            ("host-1", "ssh://one", "one", "now", "now"),
        )
        await db.execute(
            """INSERT INTO resource_leases(
                   id, host_id, execution_id, session_id, pool, fencing_token, state,
                   requested_at, acquired_at, heartbeat_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("lease-1", "host-1", "operation-1", "session-1", "pool-a", 1, "active",
             "now", "now", "now"),
        )
        await db.execute(
            """INSERT INTO session_resource_handles(
                   id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("handle-1", "session-1", "pool-a", "host-1", "lease-1", 1, "active", "now", "now"),
        )
        await db.execute(
            "INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at) VALUES (?, ?, ?, ?)",
            ("operation-1", "handle-1", 0, "now"),
        )
        await db.commit()

        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                "UPDATE session_resource_handles SET state='invalid' WHERE id='handle-1'"
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                "INSERT INTO operation_resource_refs(operation_id, handle_id, created_at) VALUES (?, ?, ?)",
                ("operation-1", "handle-1", "later"),
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO resource_wait_operations(
                       id, session_id, operation_id, request_kind, requested_hosts_json, pool,
                       queue_ticket, state, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("wait-1", "session-1", "operation-1", "unknown", "[]", "pool-a", 1,
                 "pending", "now", "now"),
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO resource_wait_operations(
                       id, session_id, operation_id, request_kind, requested_hosts_json, pool,
                       queue_ticket, state, outcome, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ("wait-invalid-insert", "session-1", "operation-1", "pool", "[]", "pool-a", 2,
                 "pending", "not-an-outcome", "now", "now"),
            )
        await db.execute(
            """INSERT INTO resource_wait_operations(
                   id, session_id, operation_id, request_kind, requested_hosts_json, pool,
                   queue_ticket, state, created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("wait-valid", "session-1", "operation-1", "pool", "[]", "pool-a", 3,
             "pending", "now", "now"),
        )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                "UPDATE resource_wait_operations SET outcome='not-an-outcome' WHERE id='wait-valid'"
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO resource_recovery_intents(
                       id, kind, payload_json, state, created_at, updated_at
                   ) VALUES (?, ?, ?, ?, ?, ?)""",
                ("intent-1", "acquire", "{}", "unknown", "now", "now"),
            )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_v55_enforces_active_lease_and_foreign_keys_and_installs_lookup_indexes(tmp_path):
    # These names are part of the query contract: active handle lookup, reverse
    # operation lookup, FIFO waiter selection, and restart recovery scanning.
    # All must remain partial indexes as declared by V55.
    # No resource lifecycle code is exercised here.
    db = await aiosqlite.connect(tmp_path / "constraints.db")
    try:
        await db.execute("PRAGMA foreign_keys = ON")
        await runner.run_migrations(db)
        await db.execute(
            "INSERT INTO sessions(id, title, created_at, updated_at) VALUES ('s', 's', 'now', 'now')"
        )
        await db.execute(
            """INSERT INTO resource_hosts(id, connection_ref, display_name, created_at, updated_at)
               VALUES ('h', 'ssh://h', 'h', 'now', 'now')"""
        )
        await db.execute(
            """INSERT INTO resource_leases(
                   id, host_id, execution_id, session_id, pool, fencing_token, state,
                   requested_at, acquired_at, heartbeat_at
               ) VALUES ('l', 'h', 'old-op', 's', 'p', 1, 'released', 'now', 'now', 'now')"""
        )
        await db.execute(
            """INSERT INTO session_resource_handles(
                   id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at
               ) VALUES ('first', 's', 'p', 'h', 'l', 1, 'active', 'now', 'now')"""
        )
        await db.commit()

        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO session_resource_handles(
                       id, session_id, pool, host_id, lease_id, fencing_token, state, created_at, updated_at
                   ) VALUES ('second', 's', 'p', 'h', 'l', 1, 'releasing', 'now', 'now')"""
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at)
                   VALUES ('missing-operation', 'first', 1, 'now')"""
            )
        with pytest.raises(aiosqlite.IntegrityError):
            await db.execute(
                """INSERT INTO resource_recovery_intents(
                       id, kind, handle_id, payload_json, state, created_at, updated_at
                   ) VALUES ('missing-handle', 'reconcile', 'nope', '{}', 'prepared', 'now', 'now')"""
            )

        indexes = {
            row[0]
            async for row in await db.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name IN (?, ?, ?, ?)",
                (
                    "idx_session_resource_handles_active_session",
                    "idx_operation_resource_refs_handle",
                    "idx_resource_wait_operations_pending_ticket",
                    "idx_resource_recovery_intents_pending",
                ),
            )
        }
        assert indexes == {
            "idx_session_resource_handles_active_session",
            "idx_operation_resource_refs_handle",
            "idx_resource_wait_operations_pending_ticket",
            "idx_resource_recovery_intents_pending",
        }
    finally:
        await db.close()
