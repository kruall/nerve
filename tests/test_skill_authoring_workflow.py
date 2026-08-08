"""Regression coverage for the Nerve skill-authoring contract."""

from pathlib import Path

from nerve.agent.tools.handlers.skills import (
    SKILL_AMEND_SPEC,
    SKILL_CREATE_SPEC,
    SKILL_UPDATE_SPEC,
)
from nerve.bootstrap import PRODUCTIVITY_CRONS
from nerve.skills.manager import validate_skill_package
from nerve.workspace import install_bundled_skills


def _cron_prompt(job_id: str) -> str:
    return next(job["prompt"] for job in PRODUCTIVITY_CRONS if job["id"] == job_id)


def test_bundled_skill_authoring_runbook_is_canonical_and_installed(tmp_path):
    installed = install_bundled_skills(tmp_path)
    assert "nerve-skill-development" in installed
    raw = (tmp_path / "skills" / "nerve-skill-development" / "SKILL.md").read_text()
    package = validate_skill_package(raw, "nerve-skill-development", allow_legacy=False)
    assert package.version == "1.0.0"
    assert "skill_create" in package.body
    assert (tmp_path / "skills" / "nerve-skill-development" / "references" / "validation.md").is_file()


def test_automation_prompts_require_canonical_proposals_and_current_tokens():
    extractor = _cron_prompt("skill-extractor")
    reviser = _cron_prompt("skill-reviser")
    for prompt in (extractor, reviser):
        assert "nerve-skill-development" in prompt
        assert "canonical SKILL.md" in prompt
        assert "metadata.nerve" in prompt
        assert "validation" in prompt
        assert "forward-test" in prompt
        assert "Never propose legacy top-level Nerve fields" in prompt
        assert "requires:" not in prompt
        assert "suggests:" not in prompt
    assert "skill_revision" in reviser
    assert "amendments_revision" in reviser
    assert "dependencies/resources/sidecars" in reviser


def test_mutation_tools_describe_the_same_authoring_contract():
    assert "nerve-skill-development" in SKILL_CREATE_SPEC.description
    assert "reviewed config PR" in SKILL_CREATE_SPEC.description
    assert "verified, reusable" in SKILL_AMEND_SPEC.description
    assert "nerve-skill-development" in SKILL_UPDATE_SPEC.description
    assert "expected_skill_revision" in SKILL_UPDATE_SPEC.description
    assert "resources, and sidecars" in SKILL_UPDATE_SPEC.description


def test_documentation_matches_session_based_skill_plan_approval():
    root = Path(__file__).parents[1]
    for relative in ("docs/cron.md", "docs/worker-guide.md", "docs/plans.md"):
        text = (root / relative).read_text(encoding="utf-8")
        assert "instead of spawning an implementation session" not in text
        assert "no implementation session needed" not in text
        assert "implementation session" in text


def test_skill_plan_prompts_preserve_the_authoring_contract():
    root = Path(__file__).parents[1]
    for relative in ("nerve/gateway/routes/plans.py", "nerve/agent/tools/handlers/plans.py"):
        text = (root / relative).read_text(encoding="utf-8")
        assert "Load `nerve-skill-development` before applying the proposal" in text
        assert "propose_config_change" in text
        assert "Do not discard canonical metadata" in text
        assert "Preserve dependencies" in text
        assert "resources, and sidecars" in text
        assert "expected_skill_revision" in text
