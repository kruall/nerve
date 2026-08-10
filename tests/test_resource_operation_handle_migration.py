"""Focused contract tests for V53/V54 retained-resource persistence."""

import importlib

import aiosqlite
import pytest

from nerve.db.migrations import runner


TABLES = (
    "session_resource_handles",
    "operation_resource_refs",
    "resource_wait_operations",
    "resource_recovery_intents",
)


async def _schema(db: aiosqlite.Connection) -> dict[str, str]:
    rows = await (
        await db.execute(
            "SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') "
            "AND (name IN (?, ?, ?, ?) OR name LIKE 'idx_resource_%' "
            "OR name LIKE 'uq_session_resource_handles_%') ORDER BY name",
            TABLES,
        )
    ).fetchall()
    return {name: " ".join(sql.split()) for name, sql in rows if sql}


async def _apply_through_v52(db: aiosqlite.Connection) -> None:
    for version, module_name in runner.discover_migrations():
        if version > 52:
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
async def test_v53_fresh_and_upgrade_schemas_match_and_preserve_legacy_rows(tmp_path):
    fresh = await aiosqlite.connect(tmp_path / "fresh.db")
    upgraded = await aiosqlite.connect(tmp_path / "upgrade.db")
    try:
        await runner.run_migrations(fresh)
        await _apply_through_v52(upgraded)
        await _legacy_rows(upgraded)
        legacy_before = await _legacy_snapshot(upgraded)

        assert await runner.run_migrations(upgraded) == 54
        assert await _schema(fresh) == await _schema(upgraded)
        assert await _legacy_snapshot(upgraded) == legacy_before
    finally:
        await fresh.close()
        await upgraded.close()


@pytest.mark.asyncio
async def test_v53_rejects_invalid_states_and_duplicate_resource_references(tmp_path):
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
            "INSERT INTO operation_resource_refs(operation_id, handle_id, created_at) VALUES (?, ?, ?)",
            ("operation-1", "handle-1", "now"),
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
async def test_v53_enforces_active_lease_and_foreign_keys_and_installs_lookup_indexes(tmp_path):
    # These names are part of the query contract: active handle lookup, reverse
    # operation lookup, FIFO waiter selection, and restart recovery scanning.
    # All must remain partial indexes as declared by V53.
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
                """INSERT INTO operation_resource_refs(operation_id, handle_id, created_at)
                   VALUES ('missing-operation', 'first', 'now')"""
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
