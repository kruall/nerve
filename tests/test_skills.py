"""Skill amendments and dependency composition."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from nerve.agent.tools.handlers.skills import skill_get_handler
from nerve.agent.tools.registry import ToolContext
from nerve.gateway.routes.skills import get_skill_detail
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
    _write_skill(workspace, "base", _raw_skill("base", "Base instructions."))
    _write_skill(
        workspace,
        "typescript",
        _raw_skill(
            "typescript",
            "Use strict types.",
            extra=(
                "metadata:\n"
                "  nerve:\n"
                "    dependencies:\n"
                "      required: [base]\n"
            ),
        ),
    )
    _write_skill(
        workspace,
        "vue",
        _raw_skill(
            "vue",
            "Build Vue components.",
            extra=(
                "metadata:\n"
                "  nerve:\n"
                "    dependencies:\n"
                "      required:\n"
                "        - typescript\n"
                "      suggested:\n"
                "        - skill: pinia\n"
                "          when: the repository uses Pinia\n"
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

    assert result.is_error is False
    assert result.structured["dependency_resolution"]["order"] == [
        "base", "typescript", "vue",
    ]
    assert "Required dependency: typescript" in text
    assert text.index("Base instructions.") < text.index("Use strict types.")
    assert text.index("Use strict types.") < text.index("Build Vue components.")
    assert "condition (advisory, not evaluated by Nerve): the repository uses Pinia" in text
    assert "Nerve does not load these automatically" in text


@pytest.mark.asyncio
async def test_canonical_and_legacy_declarations_resolve_identically(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(workspace, "common", _raw_skill("common", "Common"))
    _write_skill(
        workspace,
        "canonical",
        _raw_skill(
            "canonical",
            "Canonical",
            extra=(
                "metadata:\n"
                "  nerve:\n"
                "    dependencies:\n"
                "      required: [common]\n"
            ),
        ),
    )
    _write_skill(
        workspace,
        "legacy",
        _raw_skill("legacy", "Legacy", extra="dependencies: common\n"),
    )

    manager = SkillManager(workspace, db)
    await manager.discover()
    canonical = await manager.resolve_required_dependencies("canonical")
    legacy = await manager.resolve_required_dependencies("legacy")

    assert canonical.ok and legacy.ok
    assert [skill.id for skill in canonical.bundle[:-1]] == ["common"]
    assert [skill.id for skill in legacy.bundle[:-1]] == ["common"]
    assert (await manager.get_skill("canonical")).dependency_source == "canonical"
    assert (await manager.get_skill("legacy")).dependency_source == "legacy"


@pytest.mark.asyncio
async def test_dependency_diamond_has_stable_deduplicated_order(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(workspace, "shared", _raw_skill("shared", "Shared"))
    for skill_id in ("left", "right"):
        _write_skill(
            workspace,
            skill_id,
            _raw_skill(skill_id, skill_id, extra="dependencies:\n  required: [shared]\n"),
        )
    _write_skill(
        workspace,
        "root",
        _raw_skill("root", "Root", extra="dependencies:\n  required: [right, left]\n"),
    )

    manager = SkillManager(workspace, db)
    await manager.discover()
    first = await manager.resolve_required_dependencies("root")
    second = await manager.resolve_required_dependencies("root")

    assert first.ok and second.ok
    assert [skill.id for skill in first.bundle] == ["shared", "left", "right", "root"]
    assert [skill.id for skill in second.bundle] == ["shared", "left", "right", "root"]


@pytest.mark.asyncio
async def test_cycle_and_missing_required_dependency_fail_closed(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(
        workspace, "a",
        _raw_skill("a", "A instructions", extra="dependencies:\n  required: [b, missing]\n"),
    )
    _write_skill(
        workspace, "b",
        _raw_skill("b", "B instructions", extra="dependencies:\n  required: [a]\n"),
    )

    manager = SkillManager(workspace, db)
    await manager.discover()
    resolution = await manager.resolve_required_dependencies("a")

    assert not resolution.ok
    assert resolution.bundle == []
    assert {issue.code for issue in resolution.errors} == {
        "dependency_cycle", "missing_required_dependency",
    }

    result = await skill_get_handler(
        ToolContext(session_id="test", workspace=workspace, db=db, skill_manager=manager),
        {"name": "a"},
    )
    text = result.content[0]["text"]
    assert result.is_error is True
    assert "A instructions" not in text
    assert "required dependency cycle: a -> b -> a" in text
    usage = await db.get_skill_usage("a")
    assert usage[0]["success"] == 0
    assert "required skill 'missing' was not found" in usage[0]["error"]


@pytest.mark.asyncio
async def test_required_dependency_must_be_enabled_and_model_invocable(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(workspace, "dependency", _raw_skill("dependency", "Dependency"))
    _write_skill(
        workspace, "root",
        _raw_skill("root", "Root", extra="dependencies:\n  required: [dependency]\n"),
    )
    manager = SkillManager(workspace, db)
    await manager.discover()

    await manager.toggle_skill("dependency", False)
    reloaded = SkillManager(workspace, db)
    await reloaded.discover()
    disabled = await reloaded.resolve_required_dependencies("root")
    assert [issue.code for issue in disabled.errors] == ["required_dependency_disabled"]

    await reloaded.toggle_skill("dependency", True)
    await reloaded.update_skill(
        "dependency",
        _raw_skill(
            "dependency", "Dependency", extra="disable-model-invocation: true\n"
        ),
    )
    not_invocable = await reloaded.resolve_required_dependencies("root")
    assert [issue.code for issue in not_invocable.errors] == [
        "required_dependency_not_model_invocable"
    ]

    await reloaded.update_skill("dependency", _raw_skill("dependency", "Dependency"))
    assert (await reloaded.resolve_required_dependencies("root")).ok


@pytest.mark.asyncio
async def test_invalid_dependency_metadata_is_actionable(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(
        workspace,
        "root",
        _raw_skill(
            "root",
            "Root",
            extra=(
                "metadata:\n"
                "  nerve:\n"
                "    dependencies:\n"
                "      required:\n"
                "        - dep\n"
                "        - dep\n"
                "        - root\n"
                "        - skill: versioned\n"
                "          version: '>=2'\n"
                "      suggested: [dep]\n"
            ),
        ),
    )
    manager = SkillManager(workspace, db)
    await manager.discover()
    resolution = await manager.resolve_required_dependencies("root")
    codes = {issue.code for issue in resolution.errors}

    assert {
        "duplicate_dependency",
        "self_dependency",
        "unsupported_dependency_field",
        "conflicting_dependency_modes",
    } <= codes
    assert any("version constraints are deferred" in issue.message for issue in resolution.errors)


@pytest.mark.asyncio
async def test_dependency_depth_and_count_limits_fail_closed(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(workspace, "leaf", _raw_skill("leaf", "Leaf"))
    _write_skill(
        workspace, "middle",
        _raw_skill("middle", "Middle", extra="dependencies:\n  required: [leaf]\n"),
    )
    _write_skill(
        workspace, "deep-root",
        _raw_skill("deep-root", "Root", extra="dependencies:\n  required: [middle]\n"),
    )
    _write_skill(
        workspace, "z-alternate",
        _raw_skill("z-alternate", "Alternate", extra="dependencies:\n  required: [leaf]\n"),
    )
    _write_skill(
        workspace, "shared-root",
        _raw_skill(
            "shared-root", "Root",
            extra="dependencies:\n  required: [leaf, z-alternate]\n",
        ),
    )
    for skill_id in ("one", "two", "three"):
        _write_skill(workspace, skill_id, _raw_skill(skill_id, skill_id))
    _write_skill(
        workspace, "wide-root",
        _raw_skill("wide-root", "Root", extra="dependencies:\n  required: [three, two, one]\n"),
    )
    manager = SkillManager(workspace, db)
    await manager.discover()

    deep = await manager.resolve_required_dependencies("deep-root", max_depth=1)
    shared = await manager.resolve_required_dependencies("shared-root", max_depth=1)
    wide = await manager.resolve_required_dependencies("wide-root", max_dependencies=2)
    assert [issue.code for issue in deep.errors] == ["dependency_depth_limit"]
    assert [issue.code for issue in shared.errors] == ["dependency_depth_limit"]
    assert [issue.code for issue in wide.errors] == ["dependency_count_limit"]


@pytest.mark.asyncio
async def test_suggested_dependency_is_never_loaded_or_state_checked(tmp_path, db):
    workspace = tmp_path / "ws"
    _write_skill(
        workspace,
        "root",
        _raw_skill(
            "root",
            "Root",
            extra=(
                "metadata:\n"
                "  nerve:\n"
                "    dependencies:\n"
                "      suggested:\n"
                "        - skill: missing\n"
                "          when: Optional integration is enabled\n"
            ),
        ),
    )
    manager = SkillManager(workspace, db)
    await manager.discover()
    resolution = await manager.resolve_required_dependencies("root")

    assert resolution.ok
    assert [skill.id for skill in resolution.bundle] == ["root"]
    assert [suggestion.skill for suggestion in resolution.suggested] == ["missing"]


@pytest.mark.asyncio
async def test_skill_http_detail_exposes_dependency_diagnostics(
    tmp_path, db, monkeypatch,
):
    workspace = tmp_path / "ws"
    _write_skill(
        workspace,
        "root",
        _raw_skill("root", "Root", extra="dependencies:\n  required: [missing]\n"),
    )
    manager = SkillManager(workspace, db)
    await manager.discover()
    monkeypatch.setattr(
        "nerve.gateway.routes.skills.get_deps",
        lambda: SimpleNamespace(
            db=db,
            engine=SimpleNamespace(_skill_manager=manager),
        ),
    )

    detail = await get_skill_detail("root", user={})
    assert detail["dependency_source"] == "legacy"
    assert detail["dependency_resolution"]["ok"] is False
    assert detail["dependency_resolution"]["errors"][0]["code"] == (
        "missing_required_dependency"
    )


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
