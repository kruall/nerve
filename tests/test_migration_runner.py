"""Tests for database migration discovery invariants."""

from types import SimpleNamespace

import aiosqlite
import pytest

from nerve.db.migrations import runner


def test_discover_migrations_rejects_duplicate_versions(monkeypatch):
    modules = [
        SimpleNamespace(name="v049_first"),
        SimpleNamespace(name="v049_second"),
    ]
    monkeypatch.setattr(runner.pkgutil, "iter_modules", lambda _paths: modules)

    with pytest.raises(RuntimeError, match="Duplicate database migration version V49"):
        runner.discover_migrations()


@pytest.mark.asyncio
async def test_v49_database_applies_session_reservations_migration(tmp_path):
    db = await aiosqlite.connect(tmp_path / "nerve.db")
    try:
        await db.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        await db.execute("INSERT INTO schema_version (version) VALUES (49)")
        await db.commit()

        assert await runner.run_migrations(db) == 50
        async with db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='session_resource_reservations'"
        ) as cursor:
            assert await cursor.fetchone() == ("session_resource_reservations",)
    finally:
        await db.close()
