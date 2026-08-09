"""Immutable, preflight-resolved agent stages for workflow presets.

This module deliberately does not accept free-form requests from an agent.  A
controller passes a :class:`WorkflowStage` taken from a pinned preset and the
result is a value object which can be journaled before a backend is started.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from nerve.agent.tools.registry import ToolRegistry
from nerve.skills.manager import SkillManager
from nerve.workflows.presets import WorkflowStage

MAX_CONTEXT_BYTES = 256 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024
_SANDBOXES = frozenset(("read-only", "workspace-write"))


class AgentStageResolutionError(ValueError):
    """A stage cannot safely start; no model invocation has occurred."""


class StageArtifactError(ValueError):
    """The model response does not satisfy the declared output contract."""


def validate_artifact(text: str, schema: Mapping[str, Any]) -> Any:
    """Parse and validate the intentionally small JSON-schema contract subset.

    Presets may only rely on object/array/string/number/integer/boolean,
    ``required``, ``properties`` and ``items``.  Rejecting unsupported schema
    keywords is safer than accepting an output under a contract we did not
    actually enforce.
    """
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError) as original_error:
        # Codex stage sessions may preserve a short commentary preamble before
        # the final answer. Accept one complete JSON value only when it is the
        # terminal suffix; never salvage an intermediate object followed by
        # more model text.
        value = None
        if isinstance(text, str):
            decoder = json.JSONDecoder()
            for index, char in enumerate(text):
                if char not in "{[":
                    continue
                try:
                    candidate, end = decoder.raw_decode(text, index)
                except json.JSONDecodeError:
                    continue
                if not text[end:].strip():
                    value = candidate
                    break
        if value is None:
            raise StageArtifactError("final response is not JSON") from original_error

    def check(item: Any, node: Mapping[str, Any], path: str) -> None:
        unknown = set(node) - {"type", "required", "properties", "items"}
        if unknown:
            raise StageArtifactError(f"output schema uses unsupported keys: {', '.join(sorted(unknown))}")
        typ = node.get("type")
        matches = {
            "object": isinstance(item, dict), "array": isinstance(item, list),
            "string": isinstance(item, str), "number": isinstance(item, (int, float)) and not isinstance(item, bool),
            "integer": isinstance(item, int) and not isinstance(item, bool), "boolean": isinstance(item, bool),
        }
        if typ not in matches:
            raise StageArtifactError(f"{path}: unsupported or missing schema type")
        if not matches[typ]:
            raise StageArtifactError(f"{path}: expected {typ}")
        if typ == "object":
            properties = node.get("properties", {})
            if not isinstance(properties, dict): raise StageArtifactError("properties must be an object")
            for name in node.get("required", []):
                if name not in item: raise StageArtifactError(f"{path}.{name}: required")
            for name, child in properties.items():
                if name in item:
                    check(item[name], child, f"{path}.{name}")
        elif typ == "array" and "items" in node:
            for index, child in enumerate(item): check(child, node["items"], f"{path}[{index}]")
    check(value, schema, "$")
    return value


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _frozen(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class ResolvedSkill:
    id: str
    revision: str
    instructions: str

    def journal(self) -> dict[str, str]:
        return {"id": self.id, "revision": self.revision,
                "hash": hashlib.sha256(self.instructions.encode()).hexdigest()}


@dataclass(frozen=True)
class Capability:
    server: str
    tool: str
    schema: Mapping[str, Any]

    @property
    def name(self) -> str:
        return f"{self.server}.{self.tool}"


@dataclass(frozen=True)
class StageContext:
    """The complete bounded model input, excluding ambient conversation state."""
    workflow: Mapping[str, Any]
    task_contract: Mapping[str, Any]
    prompt: str
    repository_instructions: str
    artifacts: Mapping[str, Any]
    skills: tuple[ResolvedSkill, ...]
    capabilities: tuple[Capability, ...]
    output_schema: Mapping[str, Any]
    context_hash: str

    def render(self) -> str:
        payload = {
            "workflow": dict(self.workflow), "task_contract": dict(self.task_contract),
            "prompt": self.prompt, "repository_instructions": self.repository_instructions,
            "artifacts": dict(self.artifacts),
            "skills": [{"id": s.id, "instructions": s.instructions} for s in self.skills],
            "capabilities": [{"name": c.name, "input_schema": dict(c.schema)} for c in self.capabilities],
            "output_schema": dict(self.output_schema),
        }
        rendered = _canonical(payload)
        if len(rendered) > MAX_CONTEXT_BYTES:
            raise AgentStageResolutionError("compiled StageContext exceeds byte limit")
        return rendered.decode()

    def journal(self) -> dict[str, Any]:
        return {"context_hash": self.context_hash, "skills": [s.journal() for s in self.skills],
                "capabilities": [c.name for c in self.capabilities],
                "output_schema": dict(self.output_schema),
                "context_bytes": len(self.render().encode())}


@dataclass(frozen=True)
class AgentStageSpec:
    """Validated immutable launch specification consumed by a controller only."""
    stage_id: str
    model: str
    reasoning_effort: str
    sandbox: str
    cwd: str
    budget_usd: float
    context: StageContext


class AgentStageResolver:
    """Resolves every mutable dependency before returning a launch spec."""

    def __init__(self, *, skills: SkillManager, registry: ToolRegistry,
                 configured_models: set[str], external_servers: set[str] = frozenset()):
        self.skills = skills
        self.registry = registry
        self.configured_models = configured_models
        self.external_servers = external_servers

    async def resolve(self, *, stage: WorkflowStage, workflow: Mapping[str, Any],
                      task_contract: Mapping[str, Any], prompt: str,
                      artifacts: Mapping[str, Any], budget_usd: float,
                      repository_instructions: str = "", cwd: str = "") -> AgentStageSpec:
        if stage.runner != "agent":
            raise AgentStageResolutionError("stage is not an agent stage")
        raw = stage.spec
        model = str(raw.get("model") or "")
        if model not in self.configured_models:
            raise AgentStageResolutionError(f"model {model!r} is unavailable")
        sandbox = str(raw.get("sandbox") or "")
        if sandbox not in _SANDBOXES:
            raise AgentStageResolutionError("agent sandbox must be read-only or workspace-write")
        if not isinstance(budget_usd, (int, float)) or budget_usd <= 0:
            raise AgentStageResolutionError("stage budget must be positive")
        if not isinstance(prompt, str) or not prompt.strip():
            raise AgentStageResolutionError("stage prompt is required")
        artifact_bytes = len(_canonical(dict(artifacts)))
        if artifact_bytes > MAX_ARTIFACT_BYTES:
            raise AgentStageResolutionError("typed input artifacts exceed byte limit")

        resolved_skills: list[ResolvedSkill] = []
        for skill_id in sorted(set(raw.get("skills") or ())):
            resolution = await self.skills.resolve_required_dependencies(skill_id, for_model=True)
            if resolution.errors:
                raise AgentStageResolutionError(resolution.errors[0].message)
            for item in resolution.bundle:
                if item.id not in {s.id for s in resolved_skills}:
                    # ``raw`` retains the frontmatter and exact bytes whose
                    # revision was pinned; model-visible instructions must not
                    # silently differ from the journaled revision.
                    resolved_skills.append(ResolvedSkill(item.id, item.skill_revision, item.raw))

        allowed = ((raw.get("mcp") or {}).get("allow") or [])
        capabilities: list[Capability] = []
        for capability in sorted(set(allowed)):
            server, dot, tool = str(capability).partition(".")
            if not dot or not tool:
                raise AgentStageResolutionError(f"invalid MCP capability {capability!r}")
            if server == "nerve":
                spec = self.registry.get(tool)
                if spec is None:
                    raise AgentStageResolutionError(f"MCP tool {capability!r} is unavailable")
                capabilities.append(Capability(server, tool, _frozen(spec.input_schema)))
            elif server not in self.external_servers:
                raise AgentStageResolutionError(f"MCP server {server!r} is unavailable")
            else:
                # External MCPs cannot safely be granted without their advertised
                # schema snapshot; controller integrations must provide one.
                raise AgentStageResolutionError(f"MCP tool schema for {capability!r} is unavailable")

        output_schema = _frozen(stage.output_schema)
        if not output_schema:
            raise AgentStageResolutionError("agent stage requires an output schema")
        context_without_hash = StageContext(_frozen(workflow), _frozen(task_contract), prompt,
            repository_instructions, _frozen(artifacts), tuple(resolved_skills), tuple(capabilities),
            output_schema, "")
        digest = hashlib.sha256(context_without_hash.render().encode()).hexdigest()
        context = StageContext(
            context_without_hash.workflow, context_without_hash.task_contract,
            context_without_hash.prompt, context_without_hash.repository_instructions,
            context_without_hash.artifacts, context_without_hash.skills,
            context_without_hash.capabilities, context_without_hash.output_schema, digest,
        )
        return AgentStageSpec(stage.id, model, str(raw.get("reasoning_effort") or ""), sandbox,
                              str(cwd or ""), float(budget_usd), context)
