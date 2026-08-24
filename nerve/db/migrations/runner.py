"""Migration runner — discovers and applies numbered migration files."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

# Before the task-board migrations landed in upstream main, kruall_main_v2
# independently used V43-V62 for its execution/resource migration chain.  The
# rebased history moves that chain to V45-V64.  Existing fork databases need
# their recorded version translated once so they continue at the equivalent
# point in the shifted chain; upstream databases have no ``executions`` table
# at V43/V44 and therefore must not be translated.
_LEGACY_FORK_MIN_VERSION = 43
_LEGACY_FORK_MAX_VERSION = 62
_LEGACY_FORK_VERSION_SHIFT = 2


def discover_migrations() -> list[tuple[int, str]]:
    """Scan the migrations package for vNNN_*.py files.

    Returns sorted list of (version, module_name) tuples.
    """
    migrations_dir = Path(__file__).parent
    results: list[tuple[int, str]] = []
    modules_by_version: dict[int, str] = {}
    for info in pkgutil.iter_modules([str(migrations_dir)]):
        name = info.name
        if name.startswith("v") and "_" in name:
            try:
                version = int(name.split("_", 1)[0][1:])
                previous = modules_by_version.get(version)
                if previous is not None:
                    raise RuntimeError(
                        f"Duplicate database migration version V{version}: "
                        f"{previous} and {name}"
                    )
                modules_by_version[version] = name
                results.append((version, name))
            except ValueError:
                continue
    results.sort(key=lambda x: x[0])
    return results


async def get_current_version(db: aiosqlite.Connection) -> int:
    """Read the current schema version from the database."""
    try:
        async with db.execute("SELECT MAX(version) FROM schema_version") as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 0
    except Exception:
        return 0


async def _table_exists(db: aiosqlite.Connection, table: str) -> bool:
    async with db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ) as cursor:
        return await cursor.fetchone() is not None


async def _column_exists(
    db: aiosqlite.Connection, table: str, column: str,
) -> bool:
    async with db.execute(f"PRAGMA table_info({table})") as cursor:
        return any(row[1] == column for row in await cursor.fetchall())


async def _remap_legacy_fork_version(
    db: aiosqlite.Connection, current: int,
) -> int:
    """Translate the pre-rebase fork lineage to its shifted version.

    The legacy signature is ``executions`` without the complete upstream
    task-board schema.  Checking both sides matters: a new rebased database
    interrupted between V45 and V64 also has ``executions`` and must resume at
    its recorded version, not be translated again.  Recording the translated
    version is enough: ``schema_version`` is append-only and readers use MAX.
    V65 then idempotently adds the upstream pieces the legacy fork never ran.
    """
    if not (_LEGACY_FORK_MIN_VERSION <= current <= _LEGACY_FORK_MAX_VERSION):
        return current
    if not await _table_exists(db, "executions"):
        return current
    has_task_position = await _column_exists(db, "tasks", "position")
    has_task_events = await _table_exists(db, "task_events")
    if has_task_position and has_task_events:
        return current

    translated = current + _LEGACY_FORK_VERSION_SHIFT
    await db.execute(
        "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
        (translated,),
    )
    await db.commit()
    logger.info(
        "Translated legacy kruall_main_v2 schema version V%d to V%d",
        current,
        translated,
    )
    return translated


async def run_migrations(db: aiosqlite.Connection) -> int:
    """Apply all pending migrations in order.

    Returns the final schema version after applying migrations.
    """
    current = await get_current_version(db)
    current = await _remap_legacy_fork_version(db, current)
    migrations = discover_migrations()

    applied = 0
    for version, module_name in migrations:
        if current >= version:
            continue

        full_module = f"nerve.db.migrations.{module_name}"
        mod = importlib.import_module(full_module)

        if not hasattr(mod, "up"):
            logger.warning("Migration %s has no up() function, skipping", module_name)
            continue

        logger.info("Applying migration V%d (%s)...", version, module_name)
        try:
            await mod.up(db)
            await db.execute(
                "INSERT OR REPLACE INTO schema_version (version) VALUES (?)",
                (version,),
            )
            await db.commit()
            applied += 1
            logger.info("Migration V%d applied successfully", version)
        except Exception:
            logger.exception("Migration V%d failed", version)
            raise

    final_version = await get_current_version(db)
    if applied > 0:
        logger.info(
            "Database migrated to schema version %d (%d migrations applied)",
            final_version, applied,
        )
    return final_version
