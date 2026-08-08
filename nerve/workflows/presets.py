"""Reviewed, immutable workflow-preset catalog.

The catalog is deliberately a compiler, not a scheduler.  A preset describes a
bounded static graph; callers receive a pinned :class:`ResolvedWorkflowPlan`
which a later controller may execute without consulting mutable configuration.
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import yaml

from nerve.executions import ExecutionCatalog, OperationValidationError

PRESET_DIR = Path("config/workflows/presets")
_ID = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_PRESET = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")


class WorkflowPresetError(ValueError):
    """A reviewed workflow preset is malformed or cannot be resolved."""


class WorkflowPresetValidationError(ValueError):
    """A start request does not satisfy a resolved preset."""


class _Loader(yaml.SafeLoader):
    pass


def _mapping(loader, node, deep=False):
    result = {}
    for key, value in node.value:
        key = loader.construct_object(key, deep=deep)
        if key in result:
            raise WorkflowPresetError(f"workflow preset contains duplicate key {key!r}")
        result[key] = loader.construct_object(value, deep=deep)
    return result


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _obj(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise WorkflowPresetError(f"{where} must be a mapping with string keys")
    return value


def _fields(data: Mapping[str, Any], where: str, allowed: set[str], required: set[str] = set()):
    unknown = sorted(set(data) - allowed)
    missing = sorted(required - set(data))
    if unknown:
        raise WorkflowPresetError(f"{where} has unknown field(s): {', '.join(unknown)}")
    if missing:
        raise WorkflowPresetError(f"{where} is missing required field(s): {', '.join(missing)}")


def _positive(value: Any, where: str, maximum: int = 86400) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise WorkflowPresetError(f"{where} must be an integer from 1 to {maximum}")
    return value


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


@dataclass(frozen=True)
class WorkflowStage:
    id: str
    depends_on: tuple[str, ...]
    runner: str
    input_schema: Mapping[str, Any]
    output_schema: Mapping[str, Any]
    timeout_seconds: int
    spec: Mapping[str, Any]


@dataclass(frozen=True)
class WorkflowPreset:
    name: str
    version: str
    title: str
    description: str
    inputs: Mapping[str, Any]
    stages: tuple[WorkflowStage, ...]
    budget_usd: float
    timeout_seconds: int
    terminal_policy: str
    source: str
    preset_hash: str = ""

    def summary(self) -> dict:
        return {"name": self.name, "version": self.version, "preset_hash": self.preset_hash,
                "title": self.title, "description": self.description, "stages": len(self.stages)}

    def describe(self) -> dict:
        return {**self.summary(), "inputs": dict(self.inputs), "budget_usd": self.budget_usd,
                "timeout_seconds": self.timeout_seconds, "terminal_policy": self.terminal_policy,
                "stages": [{"id": s.id, "depends_on": list(s.depends_on), "runner": s.runner,
                            "inputs": dict(s.input_schema), "outputs": dict(s.output_schema),
                            "timeout_seconds": s.timeout_seconds, "spec": dict(s.spec)} for s in self.stages]}


@dataclass(frozen=True)
class WorkflowCatalogSnapshot:
    generation: int
    presets: Mapping[str, WorkflowPreset]
    catalog_hash: str
    def summaries(self): return [p.summary() for p in self.presets.values()]


@dataclass(frozen=True)
class ResolvedWorkflowPlan:
    preset: WorkflowPreset
    inputs: Mapping[str, Any]
    preset_hash: str
    def as_dict(self): return {"name": self.preset.name, "version": self.preset.version,
                                "preset_hash": self.preset_hash, "inputs": dict(self.inputs),
                                "stages": [s.id for s in self.preset.stages]}


class WorkflowPresetCatalog:
    def __init__(self, workspace: Path | str, execution_catalog: ExecutionCatalog, config: Any = None):
        self.workspace, self.execution_catalog = Path(workspace), execution_catalog
        self.config = config
        self.directory = self.workspace / PRESET_DIR
        self._lock = threading.RLock()
        self._snapshot = WorkflowCatalogSnapshot(0, MappingProxyType({}), hashlib.sha256(b"{}").hexdigest())

    @property
    def snapshot(self): return self._snapshot

    def _stage(self, raw: Any, index: int) -> WorkflowStage:
        data = _obj(raw, f"stages[{index}]")
        _fields(data, f"stages[{index}]", {"id", "depends_on", "runner", "inputs", "outputs", "timeout_seconds", "agent", "execution"}, {"id", "runner"})
        ident = data["id"]
        if not isinstance(ident, str) or not _ID.fullmatch(ident): raise WorkflowPresetError(f"stages[{index}].id has an invalid identifier")
        deps = data.get("depends_on", [])
        if not isinstance(deps, list) or not all(isinstance(x, str) and _ID.fullmatch(x) for x in deps) or len(set(deps)) != len(deps):
            raise WorkflowPresetError(f"stages[{index}].depends_on must be a unique identifier list")
        runner = data["runner"]
        if runner not in {"agent", "execution"}: raise WorkflowPresetError(f"stages[{index}].runner must be 'agent' or 'execution'")
        spec_key = runner
        if set(data) & {"agent", "execution"} != {spec_key}: raise WorkflowPresetError(f"stages[{index}] must declare only its {runner} specification")
        spec = _obj(data.get(spec_key, {}), f"stages[{index}].{spec_key}")
        allowed = ({"model", "reasoning_effort", "sandbox", "skills", "mcp"} if runner == "agent" else {"kind", "arguments", "resources"})
        _fields(spec, f"stages[{index}].{spec_key}", allowed, {"model", "sandbox", "mcp"} if runner == "agent" else {"kind"})
        if runner == "agent":
            if not isinstance(spec["model"], str) or not spec["model"]: raise WorkflowPresetError(f"stages[{index}].agent.model must be a non-empty string")
            if not isinstance(spec["sandbox"], str) or not spec["sandbox"]: raise WorkflowPresetError(f"stages[{index}].agent.sandbox must be a non-empty string")
            if spec["sandbox"] not in {"read-only", "workspace-write"}:
                raise WorkflowPresetError(f"stages[{index}].agent.sandbox must be read-only or workspace-write")
            skills = spec.get("skills", [])
            if not isinstance(skills, list) or not all(isinstance(x, str) and x for x in skills) or len(set(skills)) != len(skills):
                raise WorkflowPresetError(f"stages[{index}].agent.skills must be a unique non-empty string list")
            effort = spec.get("reasoning_effort", "")
            if effort and (not isinstance(effort, str) or not effort.strip()):
                raise WorkflowPresetError(f"stages[{index}].agent.reasoning_effort must be a string")
            mcp = _obj(spec["mcp"], f"stages[{index}].agent.mcp")
            _fields(mcp, f"stages[{index}].agent.mcp", {"allow"}, {"allow"})
            if not isinstance(mcp["allow"], list) or not all(isinstance(x, str) and "." in x for x in mcp["allow"]):
                raise WorkflowPresetError(f"stages[{index}].agent.mcp.allow must be an explicit server.tool list")
            # Resolve named external servers at compile time. ``nerve`` is the
            # built-in server; its individual tools are validated by the tool
            # registry/controller when that runtime is installed.
            if self.config is not None:
                servers = {"nerve"} | {str(s.name) for s in getattr(self.config, "mcp_servers", []) if getattr(s, "enabled", True)}
                for capability in mcp["allow"]:
                    if capability.split(".", 1)[0] not in servers:
                        raise WorkflowPresetError(f"stages[{index}].agent.mcp references unknown server {capability!r}")
                model = spec["model"]
                configured = {str(getattr(self.config.agent, "model", ""))}
                configured.update(str(v) for v in (getattr(self.config.agent, "models", None) or []))
                configured.update(str(v) for v in getattr(self.config.codex, "pricing", {}))
                if model not in configured:
                    raise WorkflowPresetError(f"stages[{index}].agent.model {model!r} is not configured with known pricing")
        else:
            kind = spec["kind"]
            if not isinstance(kind, str): raise WorkflowPresetError(f"stages[{index}].execution.kind must be a string")
            try: self.execution_catalog.describe(kind)
            except OperationValidationError as e: raise WorkflowPresetError(str(e)) from e
        inputs, outputs = data.get("inputs", {}), data.get("outputs", {})
        _obj(inputs, f"stages[{index}].inputs"); _obj(outputs, f"stages[{index}].outputs")
        if runner == "agent" and not outputs:
            raise WorkflowPresetError(f"stages[{index}].outputs is required for agent stages")
        return WorkflowStage(ident, tuple(deps), runner, MappingProxyType(dict(inputs)), MappingProxyType(dict(outputs)), _positive(data.get("timeout_seconds", 3600), f"stages[{index}].timeout_seconds"), MappingProxyType(dict(spec)))

    def _parse(self, path: Path) -> WorkflowPreset:
        try: raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_Loader)
        except (OSError, yaml.YAMLError) as e: raise WorkflowPresetError(f"{path}: {e}") from e
        data = _obj(raw, str(path)); _fields(data, str(path), {"schema_version", "name", "version", "title", "description", "inputs", "stages", "budget_usd", "timeout_seconds", "terminal_policy"}, {"schema_version", "name", "version", "title", "stages", "budget_usd", "timeout_seconds", "terminal_policy"})
        if data["schema_version"] != 1: raise WorkflowPresetError(f"{path}: schema_version must be 1")
        if not isinstance(data["name"], str) or not _PRESET.fullmatch(data["name"]): raise WorkflowPresetError(f"{path}: name has an invalid identifier")
        if not isinstance(data["version"], (str, int)): raise WorkflowPresetError(f"{path}: version must be a string or integer")
        stages = tuple(self._stage(v, i) for i, v in enumerate(data["stages"] if isinstance(data["stages"], list) else []))
        if not stages: raise WorkflowPresetError(f"{path}: stages must be a non-empty list")
        ids = {s.id for s in stages}
        if len(ids) != len(stages): raise WorkflowPresetError(f"{path}: stage ids must be unique")
        for s in stages:
            if any(d not in ids for d in s.depends_on): raise WorkflowPresetError(f"{path}: stage {s.id!r} depends on an unknown stage")
        # Topological check and a single reachable root (all nodes must descend from one root).
        roots = [s.id for s in stages if not s.depends_on]
        if len(roots) != 1: raise WorkflowPresetError(f"{path}: graph must have exactly one root stage")
        seen, pending = set(), list(roots)
        children = {s.id: [] for s in stages}
        for s in stages:
            for d in s.depends_on: children[d].append(s.id)
        while pending:
            v = pending.pop()
            if v in seen: continue
            seen.add(v); pending.extend(children[v])
        if seen != ids: raise WorkflowPresetError(f"{path}: graph contains a cycle or unreachable stage")
        if not isinstance(data["budget_usd"], (int, float)) or isinstance(data["budget_usd"], bool) or data["budget_usd"] <= 0: raise WorkflowPresetError(f"{path}: budget_usd must be positive")
        policy = data["terminal_policy"]
        if policy not in {"fail_fast", "continue_independent"}: raise WorkflowPresetError(f"{path}: terminal_policy is invalid")
        canonical = {k: data[k] for k in sorted(data)}
        digest = hashlib.sha256(_canonical(canonical).encode()).hexdigest()
        return WorkflowPreset(data["name"], str(data["version"]), str(data["title"]), str(data.get("description", "")), MappingProxyType(dict(_obj(data.get("inputs", {}), f"{path}.inputs"))), stages, float(data["budget_usd"]), _positive(data["timeout_seconds"], f"{path}.timeout_seconds"), policy, str(path), digest)

    def build_candidate(self) -> WorkflowCatalogSnapshot:
        if self.directory.exists() and (not self.directory.is_dir() or self.directory.is_symlink()): raise WorkflowPresetError(f"preset directory {self.directory} must be a non-symlink directory")
        presets = {}
        for path in sorted(self.directory.glob("*.yaml")) if self.directory.exists() else []:
            if path.is_symlink() or not path.is_file(): raise WorkflowPresetError(f"preset file {path} must not be a symlink and must be regular")
            preset = self._parse(path)
            if preset.name in presets: raise WorkflowPresetError(f"duplicate workflow preset: {preset.name}")
            presets[preset.name] = preset
        return WorkflowCatalogSnapshot(self._snapshot.generation + 1, MappingProxyType(presets), hashlib.sha256(_canonical({k:v.preset_hash for k,v in presets.items()}).encode()).hexdigest())

    def reload(self):
        candidate = self.build_candidate()
        with self._lock: self._snapshot = candidate
        return candidate

    def describe(self, name: str):
        preset = self.snapshot.presets.get(name)
        if preset is None: raise WorkflowPresetValidationError(f"unknown workflow preset: {name}")
        return preset.describe()

    def compile(self, name: str, inputs: Mapping[str, Any] | None = None) -> ResolvedWorkflowPlan:
        preset = self.snapshot.presets.get(name)
        if preset is None: raise WorkflowPresetValidationError(f"unknown workflow preset: {name}")
        inputs = dict(inputs or {})
        if set(inputs) - set(preset.inputs): raise WorkflowPresetValidationError("unknown workflow input")
        return ResolvedWorkflowPlan(preset, MappingProxyType(inputs), preset.preset_hash)
