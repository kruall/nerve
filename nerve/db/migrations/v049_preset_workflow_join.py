"""Persist which path owns delivery of a preset workflow completion."""
import aiosqlite


async def up(db: aiosqlite.Connection) -> None:
    await db.execute(
        """ALTER TABLE preset_workflows ADD COLUMN completion_mode TEXT NOT NULL
           DEFAULT 'observer' CHECK(completion_mode IN ('observer', 'join'))"""
    )
