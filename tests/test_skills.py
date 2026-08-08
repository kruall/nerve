"""Skill amendments and dependency composition."""

from pathlib import Path

import pytest

from nerve.agent.tools.handlers.skills import skill_get_handler
from nerve.agent.tools.registry import ToolContext
from nerve.skills.manager import AMENDMENTS_REFERENCE, SkillManager


def _write_skill(workspace: Path, skill_id: str, raw: str) -> None:
    skill_dir = workspace / "skills" / skill_id
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(raw, encoding="utf-8")


def _raw_skill(name: str, body: str, *, version: str = "1.0.0", extra: str = "") -> str:
    return (
        "---\n"
        f"name: {name}\n"
        f"description: {name} description\n"
        f"version: {version}\n"
        f"{extra}"
        "---\n\n"
        f"{body}\n"
    )


@pytest.mark.asyncio
async def test_skill_get_loads_required_dependencies_and_lists_suggestions(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(workspace, "typescript", _raw_skill("typescript", "Use strict types."))
    _write_skill(workspace, "pinia", _raw_skill("pinia", "Use defineStore."))
    _write_skill(
        workspace,
        "vue",
        _raw_skill(
            "vue",
            "Build Vue components.",
            extra=(
                "dependencies:\n"
                "  requires:\n"
                "    - typescript\n"
                "  suggests:\n"
                "    - skill: pinia\n"
                "      when: the repository uses Pinia\n"
            ),
        ),
    )

    manager = SkillManager(workspace, db)
    await manager.discover()
    result = await skill_get_handler(
        ToolContext(session_id="test", workspace=workspace, db=db, skill_manager=manager),
        {"name": "vue"},
    )
    text = result.content[0]["text"]

    assert "Required dependency: typescript" in text
    assert text.index("Use strict types.") < text.index("Build Vue components.")
    assert "`pinia` — the repository uses Pinia" in text
    assert "Use defineStore." not in text


@pytest.mark.asyncio
async def test_dependency_cycle_and_missing_skill_are_warnings(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(
        workspace, "a",
        _raw_skill("a", "A", extra="dependencies:\n  requires: [b, missing]\n"),
    )
    _write_skill(
        workspace, "b",
        _raw_skill("b", "B", extra="dependencies:\n  requires: [a]\n"),
    )

    manager = SkillManager(workspace, db)
    await manager.discover()
    dependencies, warnings = await manager.resolve_required_dependencies("a")

    assert [skill.id for skill in dependencies] == ["b"]
    assert any("Dependency cycle ignored: a -> b -> a" in warning for warning in warnings)
    assert any("Required skill not found: missing" in warning for warning in warnings)


@pytest.mark.asyncio
async def test_append_amendment_is_loaded_but_not_listed_as_reference(
    tmp_path, db, monkeypatch,
):
    monkeypatch.setattr("nerve.config._config", None)
    workspace = tmp_path / "ws"
    _write_skill(workspace, "demo", _raw_skill("demo", "Stable instructions."))
    refs = workspace / "skills" / "demo" / "references"
    refs.mkdir()
    (refs / "DETAILS.md").write_text("Details", encoding="utf-8")

    manager = SkillManager(workspace, db)
    await manager.discover()
    amendment_id, revision = await manager.append_amendment(
        "demo",
        title="Prefer the repository wrapper",
        observation="The raw command missed required environment variables.",
        change="Run `./dev test` instead of invoking pytest directly.",
        evidence=["scripts/dev:42", "session result: 18 tests passed"],
        session_id="session-1",
    )
    result = await skill_get_handler(
        ToolContext(
            session_id="session-2", workspace=workspace, db=db,
            skill_manager=manager,
        ),
        {"name": "demo"},
    )
    text = result.content[0]["text"]

    assert amendment_id in text
    assert revision in text
    assert "Prefer the repository wrapper" in text
    assert "scripts/dev:42" in text
    assert "`DETAILS.md`" in text
    assert f"`{AMENDMENTS_REFERENCE}`" not in text


@pytest.mark.asyncio
async def test_consolidation_refuses_stale_amendments_revision(tmp_path, db, monkeypatch):
    monkeypatch.setattr("nerve.config._config", None)
    workspace = tmp_path / "ws"
    original = _raw_skill("demo", "Stable instructions.")
    _write_skill(workspace, "demo", original)

    manager = SkillManager(workspace, db)
    await manager.discover()
    await manager.append_amendment(
        "demo", title="First", observation="Observed first.", change="Apply first."
    )
    stale_revision = await manager.amendments_revision("demo")
    await manager.append_amendment(
        "demo", title="Second", observation="Observed second.", change="Apply second."
    )

    replacement = _raw_skill("demo", "Consolidated instructions.", version="1.0.1")
    with pytest.raises(ValueError, match="pending amendments changed"):
        await manager.update_skill(
            "demo", replacement,
            clear_amendments=True,
            amendments_revision=stale_revision,
        )

    skill_path = workspace / "skills" / "demo" / "SKILL.md"
    amendments_path = workspace / "skills" / "demo" / "references" / AMENDMENTS_REFERENCE
    assert skill_path.read_text(encoding="utf-8") == original
    assert amendments_path.exists()

    current_revision = await manager.amendments_revision("demo")
    updated = await manager.update_skill(
        "demo", replacement,
        clear_amendments=True,
        amendments_revision=current_revision,
    )
    assert updated is not None
    assert updated.version == "1.0.1"
    assert skill_path.read_text(encoding="utf-8") == replacement
    assert not amendments_path.exists()
