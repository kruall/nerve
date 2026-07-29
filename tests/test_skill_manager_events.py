"""Skill-manager change notifications used by external projections."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.skills.manager import SkillManager


def _db():
    db = MagicMock()
    db.upsert_skill = AsyncMock()
    db.get_skill_row = AsyncMock(return_value={"id": "demo", "enabled": 1})
    db.list_skills = AsyncMock(return_value=[])
    db.delete_skill_row = AsyncMock()
    db.update_skill_enabled = AsyncMock()
    return db


@pytest.mark.asyncio
async def test_mutations_emit_projection_events(tmp_path):
    db = _db()
    manager = SkillManager(tmp_path, db)
    listener = AsyncMock()
    manager.add_change_listener(listener)

    created = await manager.create_skill(
        "Demo",
        "Demo description",
        "Initial body",
    )
    await manager.update_skill(
        created.id,
        "---\nname: Demo\ndescription: Updated\n---\nChanged\n",
    )
    await manager.toggle_skill(created.id, False)
    await manager.delete_skill(created.id)

    assert [call.args for call in listener.await_args_list] == [
        ("create", "demo"),
        ("update", "demo"),
        ("toggle", "demo"),
        ("delete", "demo"),
    ]


@pytest.mark.asyncio
async def test_projection_failure_does_not_roll_back_skill_creation(tmp_path):
    db = _db()
    manager = SkillManager(tmp_path, db)
    manager.add_change_listener(
        AsyncMock(side_effect=RuntimeError("Discord unavailable")),
    )

    created = await manager.create_skill(
        "Demo",
        "Demo description",
        "Initial body",
    )

    assert created.id == "demo"
    assert (tmp_path / "skills" / "demo" / "SKILL.md").exists()
    db.upsert_skill.assert_awaited_once()
