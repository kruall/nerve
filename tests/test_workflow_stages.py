"""Preflight tests for isolated preset-driven agent stages."""
from types import MappingProxyType

import pytest

from nerve.agent.tools.registry import ToolRegistry, ToolSpec
from nerve.skills.manager import SkillContent, SkillDependencyResolution
from nerve.workflows.presets import WorkflowStage
from nerve.workflows.stages import AgentStageResolutionError, AgentStageResolver, StageArtifactError, validate_artifact


class Skills:
    def __init__(self, bundle): self.bundle = bundle
    async def resolve_required_dependencies(self, name, *, for_model):
        assert for_model
        return SkillDependencyResolution(root=name, bundle=self.bundle)


async def _tool(ctx, args): pass


def _stage(**changes):
    raw = {"model": "gpt-test", "reasoning_effort": "high", "sandbox": "read-only",
           "skills": ["root"], "mcp": {"allow": ["nerve.lookup"]}}
    raw.update(changes)
    return WorkflowStage("research", (), "agent", MappingProxyType({}),
                         MappingProxyType({"type": "object"}), 60, MappingProxyType(raw))


def _resolver(bundle=()):
    registry = ToolRegistry()
    registry.register(ToolSpec("lookup", "x", {"type": "object"}, _tool))
    return AgentStageResolver(skills=Skills(list(bundle)), registry=registry,
                              configured_models={"gpt-test"})


@pytest.mark.asyncio
async def test_stage_pins_dependency_first_skills_tools_and_context_hash():
    dep = SkillContent(id="dep", name="dep", description="", skill_revision="a" * 64, raw="# dep")
    root = SkillContent(id="root", name="root", description="", skill_revision="b" * 64, raw="# root")
    spec = await _resolver([dep, root]).resolve(
        stage=_stage(), workflow={"preset_hash": "p"}, task_contract={"id": "T"},
        prompt="Investigate", artifacts={"source": {"type": "text", "value": "x"}}, budget_usd=1,
    )
    assert [s.id for s in spec.context.skills] == ["dep", "root"]
    assert [c.name for c in spec.context.capabilities] == ["nerve.lookup"]
    assert spec.context.context_hash
    assert "recalled_memories" not in spec.context.render()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage, message", [
    (_stage(sandbox="danger-full-access"), "sandbox"),
    (_stage(mcp={"allow": ["nerve.missing"]}), "unavailable"),
    (_stage(model="missing"), "model"),
])
async def test_stage_fails_before_launch_for_unavailable_dependencies(stage, message):
    with pytest.raises(AgentStageResolutionError, match=message):
        await _resolver().resolve(stage=stage, workflow={}, task_contract={}, prompt="p", artifacts={}, budget_usd=1)


def test_output_artifact_is_json_and_satisfies_declared_contract():
    schema = {"type": "object", "required": ["summary"],
              "properties": {"summary": {"type": "string"}}}
    assert validate_artifact('{"summary":"done"}', schema)["summary"] == "done"
    with pytest.raises(StageArtifactError, match="required"):
        validate_artifact("{}", schema)
