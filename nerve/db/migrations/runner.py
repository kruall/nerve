"""Migration runner — discovers and applies numbered migration files."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)


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


async def run_migrations(db: aiosqlite.Connection) -> int:
    """Apply all pending migrations in order.

    Returns the final schema version after applying migrations.
    """
    current = await get_current_version(db)
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
