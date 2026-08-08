"""Strict loader and compiler for reviewed declarative execution kinds.

Profiles are code-equivalent configuration.  They are intentionally narrower
than a generic process description: a profile chooses the executable,
transport, working-directory context, resource slots, and argv structure.  An
operation may only fill typed argument positions and select a pool from a
profile-owned allowlist.  No profile or operation value is ever interpreted by
a shell or used as a string template.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any, Mapping, Protocol, runtime_checkable

import yaml


PROFILE_SCHEMA_VERSION = 1
PROFILE_DIR = Path("config/executions/kinds")
_KIND_RE = re.compile(r"^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$")
_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_CAPTURE_RE = _NAME_RE
_ARG_TYPES = frozenset({"string", "integer", "number", "boolean", "string_list", "path"})
_TOKEN_TYPES = frozenset({"literal", "arg", "spread", "context", "artifact", "captured_output"})
_CONTEXT_NAMES = frozenset({"workspace", "execution_dir", "execution_id", "session_id"})
_TRANSPORTS = frozenset({"local", "resource"})
_CLEANUP_WHEN = frozenset({"always", "success", "failure", "cancel"})
_CANCEL_MODES = frozenset({"terminate", "interrupt", "none"})
_MISSING = object()


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that refuses duplicate mapping keys."""


def _construct_unique_mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False):
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as e:
            raise ExecutionProfileError("execution profile mapping keys must be scalar") from e
        if duplicate:
            raise ExecutionProfileError(f"execution profile contains duplicate key {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


class ExecutionProfileError(ValueError):
    """A profile file or catalog is malformed."""


class OperationValidationError(ValueError):
    """Arguments/resources do not satisfy a selected execution kind."""


def _mapping(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExecutionProfileError(f"{where} must be a mapping")
    if not all(isinstance(k, str) for k in value):
        raise ExecutionProfileError(f"{where} keys must be strings")
    return value


def _sequence(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        raise ExecutionProfileError(f"{where} must be a list")
    return value


def _fields(data: Mapping[str, Any], where: str, *, allowed: set[str], required: set[str] = frozenset()) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ExecutionProfileError(f"{where} has unknown field(s): {', '.join(unknown)}")
    missing = sorted(required - set(data))
    if missing:
        raise ExecutionProfileError(f"{where} is missing required field(s): {', '.join(missing)}")


def _name(value: Any, where: str, pattern: re.Pattern[str] = _NAME_RE) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ExecutionProfileError(f"{where} has an invalid identifier")
    return value


def _positive_int(value: Any, where: str, *, minimum: int = 1, maximum: int = 86400) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ExecutionProfileError(f"{where} must be an integer from {minimum} to {maximum}")
    return value


def _safe_relative_path(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise ValueError(f"{where} must be a non-empty POSIX relative path")
    lexical_parts = value.split("/")
    path = PurePosixPath(value)
    if path.is_absolute() or value.startswith("/") or any(part in {"", ".", ".."} for part in lexical_parts):
        raise ValueError(f"{where} must not be absolute or contain '.', '..', or empty components")
    return path.as_posix()


def _freeze_map(values: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(values))


@dataclass(frozen=True)
class ArgumentSpec:
    name: str
    type: str
    description: str = ""
    required: bool = False
    default: Any = _MISSING
    secret: bool = False
    enum: tuple[Any, ...] = ()
    minimum: float | None = None
    maximum: float | None = None
    min_items: int | None = None
    max_items: int | None = None

    def schema(self) -> dict[str, Any]:
        types = {
            "string": "string", "path": "string", "integer": "integer",
            "number": "number", "boolean": "boolean", "string_list": "array",
        }
        result: dict[str, Any] = {"type": types[self.type]}
        if self.description:
            result["description"] = self.description
        if self.type == "string_list":
            result["items"] = {"type": "string"}
            if self.min_items is not None:
                result["minItems"] = self.min_items
            if self.max_items is not None:
                result["maxItems"] = self.max_items
        if self.enum:
            result["enum"] = list(self.enum)
        if self.minimum is not None:
            result["minimum"] = self.minimum
        if self.maximum is not None:
            result["maximum"] = self.maximum
        if self.default is not _MISSING and not self.secret:
            result["default"] = list(self.default) if isinstance(self.default, tuple) else self.default
        if self.secret:
            result["writeOnly"] = True
        return result


@dataclass(frozen=True)
class ResourceSlotSpec:
    name: str
    description: str
    allowed_pools: tuple[str, ...]
    default_pool: str | None
    required: bool


@dataclass(frozen=True)
class ArtifactSpec:
    name: str
    root: str
    path: str
    required: bool


@dataclass(frozen=True)
class ArgvToken:
    kind: str
    value: str


@dataclass(frozen=True)
class CommandStep:
    id: str
    transport: str
    executable: str
    argv: tuple[ArgvToken, ...]
    cwd: str
    resource_slot: str | None
    capture_stdout: str | None
    capture_stderr: str | None


@dataclass(frozen=True)
class ResultRules:
    success_exit_codes: tuple[int, ...] = (0,)
    required_artifacts: tuple[str, ...] = ()
    output_capture: str | None = None


@dataclass(frozen=True)
class CleanupPolicy:
    when: str = "always"
    timeout_seconds: int = 60
    steps: tuple[CommandStep, ...] = ()


@dataclass(frozen=True)
class CancellationPolicy:
    mode: str = "terminate"
    grace_seconds: int = 10
    run_cleanup: bool = True


@dataclass(frozen=True)
class ExecutionProfile:
    schema_version: int
    kind: str
    version: str
    title: str
    description: str
    arguments: Mapping[str, ArgumentSpec]
    resource_slots: Mapping[str, ResourceSlotSpec]
    artifacts: Mapping[str, ArtifactSpec]
    steps: tuple[CommandStep, ...]
    result: ResultRules
    timeout_seconds: int
    cleanup: CleanupPolicy
    cancellation: CancellationPolicy
    source: str
    profile_hash: str = ""

    def argument_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {name: spec.schema() for name, spec in self.arguments.items()},
            "required": [name for name, spec in self.arguments.items() if spec.required],
            "additionalProperties": False,
        }

    def summary(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "version": self.version,
            "profile_hash": self.profile_hash,
            "title": self.title,
            "description": self.description,
            "required_arguments": [name for name, spec in self.arguments.items() if spec.required],
            "resource_slots": list(self.resource_slots),
        }

    def describe(self) -> dict[str, Any]:
        return {
            **self.summary(),
            "schema_version": self.schema_version,
            "arguments": self.argument_schema(),
            "resources": {
                name: {
                    "description": slot.description,
                    "allowed_pools": list(slot.allowed_pools),
                    "default_pool": slot.default_pool,
                    "required": slot.required,
                }
                for name, slot in self.resource_slots.items()
            },
            "artifacts": {
                name: {"root": a.root, "path": a.path, "required": a.required}
                for name, a in self.artifacts.items()
            },
            "steps": len(self.steps),
            "timeout_seconds": self.timeout_seconds,
            "result": {
                "success_exit_codes": list(self.result.success_exit_codes),
                "required_artifacts": list(self.result.required_artifacts),
                "output_capture": self.result.output_capture,
            },
            "cleanup": {
                "when": self.cleanup.when,
                "timeout_seconds": self.cleanup.timeout_seconds,
                "steps": len(self.cleanup.steps),
            },
            "cancellation": {
                "mode": self.cancellation.mode,
                "grace_seconds": self.cancellation.grace_seconds,
                "run_cleanup": self.cancellation.run_cleanup,
            },
        }


@dataclass(frozen=True)
class CompiledArtifactRef:
    name: str
    root: str
    path: str

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "root": self.root, "path": self.path}


@dataclass(frozen=True)
class CompiledArgv:
    kind: str
    value: Any
    secret: bool = False

    def as_dict(self, *, redact_secrets: bool) -> dict[str, Any]:
        value = "<redacted>" if redact_secrets and self.secret else self.value
        if isinstance(value, CompiledArtifactRef):
            value = value.as_dict()
        return {"type": self.kind, "value": value}


@dataclass(frozen=True)
class CompiledCommand:
    id: str
    transport: str
    executable: str
    argv: tuple[CompiledArgv, ...]
    cwd: str
    resource_slot: str | None
    capture_stdout: str | None
    capture_stderr: str | None

    def as_dict(self, *, redact_secrets: bool) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": "command",
            "transport": self.transport,
            "executable": self.executable,
            "argv": [v.as_dict(redact_secrets=redact_secrets) for v in self.argv],
            "cwd": self.cwd,
            "resource_slot": self.resource_slot,
            "capture_stdout": self.capture_stdout,
            "capture_stderr": self.capture_stderr,
        }


@dataclass(frozen=True)
class CompiledCleanupPolicy:
    when: str
    timeout_seconds: int
    steps: tuple[CompiledCommand, ...]


@dataclass(frozen=True)
class CompiledExecutionPlan:
    kind: str
    profile_version: str
    profile_hash: str
    profile: ExecutionProfile = field(repr=False)
    arguments: Mapping[str, Any] = field(repr=False)
    resources: Mapping[str, str]
    artifacts: Mapping[str, ArtifactSpec]
    steps: tuple[CompiledCommand, ...]
    result: ResultRules
    timeout_seconds: int
    cleanup: CompiledCleanupPolicy
    cancellation: CancellationPolicy

    def as_dict(self, *, redact_secrets: bool = True) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "profile_version": self.profile_version,
            "profile_hash": self.profile_hash,
            "arguments": {
                name: (
                    "<redacted>" if redact_secrets and self.profile.arguments[name].secret
                    else list(value) if isinstance(value, tuple) else value
                )
                for name, value in self.arguments.items()
            },
            "resources": dict(self.resources),
            "artifacts": {
                name: {"root": a.root, "path": a.path, "required": a.required}
                for name, a in self.artifacts.items()
            },
            "steps": [s.as_dict(redact_secrets=redact_secrets) for s in self.steps],
            "result": {
                "success_exit_codes": list(self.result.success_exit_codes),
                "required_artifacts": list(self.result.required_artifacts),
                "output_capture": self.result.output_capture,
            },
            "timeout_seconds": self.timeout_seconds,
            "cleanup": {
                "when": self.cleanup.when,
                "timeout_seconds": self.cleanup.timeout_seconds,
                "steps": [s.as_dict(redact_secrets=redact_secrets) for s in self.cleanup.steps],
            },
            "cancellation": {
                "mode": self.cancellation.mode,
                "grace_seconds": self.cancellation.grace_seconds,
                "run_cleanup": self.cancellation.run_cleanup,
            },
        }


@dataclass(frozen=True)
class CatalogSnapshot:
    generation: int
    profiles: Mapping[str, ExecutionProfile]

    def get(self, kind: str) -> ExecutionProfile | None:
        return self.profiles.get(kind)

    def summaries(self) -> list[dict[str, Any]]:
        return [self.profiles[kind].summary() for kind in sorted(self.profiles)]


@runtime_checkable
class ExecutionService(Protocol):
    """Lifecycle boundary implemented by the separate execution-service task."""

    async def start(self, *, session_id: str, plan: CompiledExecutionPlan) -> Mapping[str, Any]: ...


def _parse_argument(name: str, raw: Any, where: str) -> ArgumentSpec:
    data = _mapping(raw, where)
    _fields(data, where, allowed={
        "type", "description", "required", "default", "secret", "enum",
        "minimum", "maximum", "min_items", "max_items",
    }, required={"type"})
    arg_type = data["type"]
    if arg_type not in _ARG_TYPES:
        raise ExecutionProfileError(f"{where}.type must be one of {sorted(_ARG_TYPES)}")
    description = data.get("description", "")
    if not isinstance(description, str):
        raise ExecutionProfileError(f"{where}.description must be a string")
    required = data.get("required", False)
    secret = data.get("secret", False)
    if not isinstance(required, bool) or not isinstance(secret, bool):
        raise ExecutionProfileError(f"{where}.required and .secret must be booleans")
    if required and "default" in data:
        raise ExecutionProfileError(f"{where} cannot be required and define a default")
    if secret and "default" in data:
        raise ExecutionProfileError(f"{where} secret arguments cannot define defaults")
    enum_raw = data.get("enum", [])
    if not isinstance(enum_raw, list):
        raise ExecutionProfileError(f"{where}.enum must be a list")
    if secret and enum_raw:
        raise ExecutionProfileError(f"{where} secret arguments cannot define enum values")
    minimum, maximum = data.get("minimum"), data.get("maximum")
    for label, value in (("minimum", minimum), ("maximum", maximum)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)):
            raise ExecutionProfileError(f"{where}.{label} must be a finite number")
    if (minimum is not None or maximum is not None) and arg_type not in {"integer", "number"}:
        raise ExecutionProfileError(f"{where} numeric bounds require type integer or number")
    if minimum is not None and maximum is not None and minimum > maximum:
        raise ExecutionProfileError(f"{where}.minimum must not exceed maximum")
    min_items, max_items = data.get("min_items"), data.get("max_items")
    for label, value in (("min_items", min_items), ("max_items", max_items)):
        if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0):
            raise ExecutionProfileError(f"{where}.{label} must be a non-negative integer")
    if (min_items is not None or max_items is not None) and arg_type != "string_list":
        raise ExecutionProfileError(f"{where} item bounds require type string_list")
    if min_items is not None and max_items is not None and min_items > max_items:
        raise ExecutionProfileError(f"{where}.min_items must not exceed max_items")
    spec = ArgumentSpec(
        name=name, type=arg_type, description=description, required=required,
        default=_MISSING, secret=secret, enum=(),
        minimum=minimum, maximum=maximum, min_items=min_items, max_items=max_items,
    )
    normalized_default = (
        _validate_argument_value(spec, data["default"], profile=True)
        if "default" in data else _MISSING
    )
    normalized_enum = tuple(
        _validate_argument_value(spec, member, profile=True) for member in enum_raw
    )
    return replace(spec, default=normalized_default, enum=normalized_enum)


def _validate_argument_value(spec: ArgumentSpec, value: Any, *, profile: bool = False) -> Any:
    error = ExecutionProfileError if profile else OperationValidationError
    where = f"argument {spec.name!r}"
    if spec.type in {"string", "path"}:
        if not isinstance(value, str):
            raise error(f"{where} must be a string")
        normalized: Any = value
        if spec.type == "path":
            try:
                normalized = _safe_relative_path(value, where)
            except ValueError as e:
                raise error(str(e)) from e
    elif spec.type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise error(f"{where} must be an integer")
        normalized = value
    elif spec.type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise error(f"{where} must be a finite number")
        normalized = value
    elif spec.type == "boolean":
        if not isinstance(value, bool):
            raise error(f"{where} must be a boolean")
        normalized = value
    else:
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise error(f"{where} must be a list of strings")
        normalized = tuple(value)
        if spec.min_items is not None and len(normalized) < spec.min_items:
            raise error(f"{where} must contain at least {spec.min_items} item(s)")
        if spec.max_items is not None and len(normalized) > spec.max_items:
            raise error(f"{where} must contain at most {spec.max_items} item(s)")
    if spec.enum and normalized not in spec.enum:
        raise error(f"{where} must be one of the profile's declared values")
    if spec.minimum is not None and normalized < spec.minimum:
        raise error(f"{where} is below the declared minimum")
    if spec.maximum is not None and normalized > spec.maximum:
        raise error(f"{where} exceeds the declared maximum")
    return normalized


def _parse_token(raw: Any, where: str, arguments: Mapping[str, ArgumentSpec], artifacts: Mapping[str, ArtifactSpec]) -> ArgvToken:
    data = _mapping(raw, where)
    if len(data) != 1 or next(iter(data), None) not in _TOKEN_TYPES:
        raise ExecutionProfileError(f"{where} must contain exactly one token: {', '.join(sorted(_TOKEN_TYPES))}")
    kind, value = next(iter(data.items()))
    if not isinstance(value, str):
        raise ExecutionProfileError(f"{where}.{kind} must be a string")
    if "\x00" in value:
        raise ExecutionProfileError(f"{where}.{kind} must not contain NUL")
    if kind == "arg":
        if value not in arguments:
            raise ExecutionProfileError(f"{where} references unknown argument {value!r}")
        if arguments[value].type == "string_list":
            raise ExecutionProfileError(f"{where} must use spread for string_list argument {value!r}")
        if not arguments[value].required and arguments[value].default is _MISSING:
            raise ExecutionProfileError(f"{where} references optional argument {value!r} without a default")
    elif kind == "spread":
        if value not in arguments:
            raise ExecutionProfileError(f"{where} references unknown argument {value!r}")
        if arguments[value].type != "string_list":
            raise ExecutionProfileError(f"{where} spread requires a string_list argument")
        if not arguments[value].required and arguments[value].default is _MISSING:
            raise ExecutionProfileError(f"{where} references optional argument {value!r} without a default")
    elif kind == "context" and value not in _CONTEXT_NAMES:
        raise ExecutionProfileError(f"{where} context must be one of {sorted(_CONTEXT_NAMES)}")
    elif kind == "artifact" and value not in artifacts:
        raise ExecutionProfileError(f"{where} references unknown artifact {value!r}")
    elif kind == "captured_output":
        _name(value, f"{where}.captured_output", _CAPTURE_RE)
    return ArgvToken(kind=kind, value=value)


def _parse_step(raw: Any, where: str, arguments: Mapping[str, ArgumentSpec], resources: Mapping[str, ResourceSlotSpec], artifacts: Mapping[str, ArtifactSpec], captures: set[str]) -> CommandStep:
    data = _mapping(raw, where)
    _fields(data, where, allowed={
        "id", "type", "transport", "executable", "argv", "cwd",
        "resource_slot", "capture_stdout", "capture_stderr",
    }, required={"id", "type", "transport", "executable", "argv"})
    if data["type"] != "command":
        raise ExecutionProfileError(f"{where}.type only supports 'command'")
    step_id = _name(data["id"], f"{where}.id")
    transport = data["transport"]
    if transport not in _TRANSPORTS:
        raise ExecutionProfileError(f"{where}.transport must be one of {sorted(_TRANSPORTS)}")
    executable = data["executable"]
    if not isinstance(executable, str) or not executable or "\x00" in executable or "\n" in executable:
        raise ExecutionProfileError(f"{where}.executable must be a non-empty literal without NUL/newline")
    slot = data.get("resource_slot")
    if transport == "resource":
        if slot not in resources:
            raise ExecutionProfileError(f"{where} resource transport requires a declared resource_slot")
    elif slot is not None:
        raise ExecutionProfileError(f"{where} local transport cannot set resource_slot")
    cwd = data.get("cwd", "execution_dir")
    if cwd not in {"workspace", "execution_dir"}:
        raise ExecutionProfileError(f"{where}.cwd must be 'workspace' or 'execution_dir'")
    argv = tuple(
        _parse_token(token, f"{where}.argv[{index}]", arguments, artifacts)
        for index, token in enumerate(_sequence(data["argv"], f"{where}.argv"))
    )
    for token in argv:
        if token.kind == "captured_output" and token.value not in captures:
            raise ExecutionProfileError(f"{where} references capture {token.value!r} before it is produced")
    output_names: list[str] = []
    for field_name in ("capture_stdout", "capture_stderr"):
        value = data.get(field_name)
        if value is not None:
            output_names.append(_name(value, f"{where}.{field_name}", _CAPTURE_RE))
    if len(set(output_names)) != len(output_names) or any(name in captures for name in output_names):
        raise ExecutionProfileError(f"{where} declares a duplicate captured-output name")
    captures.update(output_names)
    return CommandStep(
        id=step_id, transport=transport, executable=executable, argv=argv,
        cwd=cwd, resource_slot=slot, capture_stdout=data.get("capture_stdout"),
        capture_stderr=data.get("capture_stderr"),
    )


def _parse_profile(raw: Any, source: Path) -> ExecutionProfile:
    data = _mapping(raw, str(source))
    _fields(data, str(source), allowed={
        "schema_version", "kind", "version", "title", "description", "arguments",
        "resource_slots", "artifacts", "steps", "result", "timeout_seconds",
        "cleanup", "cancellation",
    }, required={"schema_version", "kind", "version", "title", "description", "steps"})
    if data["schema_version"] != PROFILE_SCHEMA_VERSION:
        raise ExecutionProfileError(f"{source}.schema_version must be {PROFILE_SCHEMA_VERSION}")
    kind = _name(data["kind"], f"{source}.kind", _KIND_RE)
    version = data["version"]
    if isinstance(version, bool) or not isinstance(version, (str, int)) or not str(version).strip():
        raise ExecutionProfileError(f"{source}.version must be a non-empty string or integer")
    title, description = data["title"], data["description"]
    if not isinstance(title, str) or not title.strip() or not isinstance(description, str):
        raise ExecutionProfileError(f"{source}.title must be non-empty and description must be a string")
    args_raw = _mapping(data.get("arguments", {}), f"{source}.arguments")
    arguments = {
        _name(name, f"{source}.arguments name"): _parse_argument(name, value, f"{source}.arguments.{name}")
        for name, value in args_raw.items()
    }
    resources_raw = _mapping(data.get("resource_slots", {}), f"{source}.resource_slots")
    resources: dict[str, ResourceSlotSpec] = {}
    for name, raw_slot in resources_raw.items():
        _name(name, f"{source}.resource_slots name")
        where = f"{source}.resource_slots.{name}"
        slot = _mapping(raw_slot, where)
        _fields(slot, where, allowed={"description", "allowed_pools", "default_pool", "required"}, required={"allowed_pools"})
        allowed = slot["allowed_pools"]
        if not isinstance(allowed, list) or not allowed or not all(isinstance(p, str) and _KIND_RE.fullmatch(p) for p in allowed):
            raise ExecutionProfileError(f"{where}.allowed_pools must be a non-empty list of identifiers")
        if len(set(allowed)) != len(allowed):
            raise ExecutionProfileError(f"{where}.allowed_pools contains duplicates")
        default = slot.get("default_pool")
        if default is not None and default not in allowed:
            raise ExecutionProfileError(f"{where}.default_pool must be in allowed_pools")
        required = slot.get("required", default is None)
        if not isinstance(required, bool):
            raise ExecutionProfileError(f"{where}.required must be a boolean")
        description_value = slot.get("description", "")
        if not isinstance(description_value, str):
            raise ExecutionProfileError(f"{where}.description must be a string")
        resources[name] = ResourceSlotSpec(name, description_value, tuple(allowed), default, required)
    artifacts_raw = _mapping(data.get("artifacts", {}), f"{source}.artifacts")
    artifacts: dict[str, ArtifactSpec] = {}
    for name, raw_artifact in artifacts_raw.items():
        _name(name, f"{source}.artifacts name")
        where = f"{source}.artifacts.{name}"
        artifact = _mapping(raw_artifact, where)
        _fields(artifact, where, allowed={"root", "path", "required"}, required={"path"})
        root = artifact.get("root", "execution_dir")
        if root not in {"workspace", "execution_dir"}:
            raise ExecutionProfileError(f"{where}.root must be 'workspace' or 'execution_dir'")
        try:
            artifact_path = _safe_relative_path(artifact["path"], f"{where}.path")
        except ValueError as e:
            raise ExecutionProfileError(str(e)) from e
        required = artifact.get("required", False)
        if not isinstance(required, bool):
            raise ExecutionProfileError(f"{where}.required must be a boolean")
        artifacts[name] = ArtifactSpec(name, root, artifact_path, required)
    captures: set[str] = set()
    steps = tuple(
        _parse_step(step, f"{source}.steps[{index}]", arguments, resources, artifacts, captures)
        for index, step in enumerate(_sequence(data["steps"], f"{source}.steps"))
    )
    if not steps:
        raise ExecutionProfileError(f"{source}.steps must not be empty")
    if len({step.id for step in steps}) != len(steps):
        raise ExecutionProfileError(f"{source}.steps contains duplicate ids")
    for step in steps:
        if step.resource_slot is None:
            continue
        slot = resources[step.resource_slot]
        if not slot.required and slot.default_pool is None:
            raise ExecutionProfileError(
                f"{source}.steps resource slot {step.resource_slot!r} is optional "
                "without a default but is required by a command"
            )
    result_raw = _mapping(data.get("result", {}), f"{source}.result")
    _fields(result_raw, f"{source}.result", allowed={"success_exit_codes", "required_artifacts", "output_capture"})
    success = result_raw.get("success_exit_codes", [0])
    if not isinstance(success, list) or not success or not all(isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 255 for v in success):
        raise ExecutionProfileError(f"{source}.result.success_exit_codes must be a non-empty list of integers from 0 to 255")
    if len(set(success)) != len(success):
        raise ExecutionProfileError(f"{source}.result.success_exit_codes contains duplicates")
    required_artifacts = result_raw.get("required_artifacts", [name for name, item in artifacts.items() if item.required])
    if not isinstance(required_artifacts, list) or not all(isinstance(v, str) and v in artifacts for v in required_artifacts):
        raise ExecutionProfileError(f"{source}.result.required_artifacts must reference declared artifacts")
    if len(set(required_artifacts)) != len(required_artifacts):
        raise ExecutionProfileError(f"{source}.result.required_artifacts contains duplicates")
    output_capture = result_raw.get("output_capture")
    if output_capture is not None and output_capture not in captures:
        raise ExecutionProfileError(f"{source}.result.output_capture must reference a declared capture")
    result = ResultRules(tuple(success), tuple(required_artifacts), output_capture)
    timeout = _positive_int(data.get("timeout_seconds", 3600), f"{source}.timeout_seconds")
    cleanup_raw = _mapping(data.get("cleanup", {}), f"{source}.cleanup")
    _fields(cleanup_raw, f"{source}.cleanup", allowed={"when", "timeout_seconds", "steps"})
    cleanup_when = cleanup_raw.get("when", "always")
    if cleanup_when not in _CLEANUP_WHEN:
        raise ExecutionProfileError(f"{source}.cleanup.when must be one of {sorted(_CLEANUP_WHEN)}")
    cleanup_captures = set(captures)
    cleanup_steps = tuple(
        _parse_step(step, f"{source}.cleanup.steps[{index}]", arguments, resources, artifacts, cleanup_captures)
        for index, step in enumerate(_sequence(cleanup_raw.get("steps", []), f"{source}.cleanup.steps"))
    )
    all_step_ids = [step.id for step in (*steps, *cleanup_steps)]
    if len(set(all_step_ids)) != len(all_step_ids):
        raise ExecutionProfileError(f"{source} contains duplicate step ids across main and cleanup steps")
    for step in cleanup_steps:
        if step.resource_slot is None:
            continue
        slot = resources[step.resource_slot]
        if not slot.required and slot.default_pool is None:
            raise ExecutionProfileError(
                f"{source}.cleanup.steps resource slot {step.resource_slot!r} is "
                "optional without a default but is required by a command"
            )
    cleanup = CleanupPolicy(
        cleanup_when,
        _positive_int(cleanup_raw.get("timeout_seconds", 60), f"{source}.cleanup.timeout_seconds"),
        cleanup_steps,
    )
    cancel_raw = _mapping(data.get("cancellation", {}), f"{source}.cancellation")
    _fields(cancel_raw, f"{source}.cancellation", allowed={"mode", "grace_seconds", "run_cleanup"})
    mode = cancel_raw.get("mode", "terminate")
    if mode not in _CANCEL_MODES:
        raise ExecutionProfileError(f"{source}.cancellation.mode must be one of {sorted(_CANCEL_MODES)}")
    run_cleanup = cancel_raw.get("run_cleanup", True)
    if not isinstance(run_cleanup, bool):
        raise ExecutionProfileError(f"{source}.cancellation.run_cleanup must be a boolean")
    cancellation = CancellationPolicy(
        mode,
        _positive_int(cancel_raw.get("grace_seconds", 10), f"{source}.cancellation.grace_seconds", minimum=0, maximum=300),
        run_cleanup,
    )
    profile = ExecutionProfile(
        PROFILE_SCHEMA_VERSION, kind, str(version), title.strip(), description,
        _freeze_map(arguments), _freeze_map(resources), _freeze_map(artifacts),
        steps, result, timeout, cleanup, cancellation, str(source),
    )
    digest = hashlib.sha256(
        json.dumps(_profile_canonical(profile), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return replace(profile, profile_hash=digest)


def _token_dict(token: ArgvToken) -> dict[str, str]:
    return {token.kind: token.value}


def _step_canonical(step: CommandStep) -> dict[str, Any]:
    return {
        "id": step.id, "type": "command", "transport": step.transport,
        "executable": step.executable, "argv": [_token_dict(v) for v in step.argv],
        "cwd": step.cwd, "resource_slot": step.resource_slot,
        "capture_stdout": step.capture_stdout, "capture_stderr": step.capture_stderr,
    }


def _profile_canonical(profile: ExecutionProfile) -> dict[str, Any]:
    return {
        "schema_version": profile.schema_version, "kind": profile.kind,
        "version": profile.version, "title": profile.title,
        "description": profile.description,
        "arguments": {
            name: {
                "type": a.type, "description": a.description, "required": a.required,
                "default": None if a.default is _MISSING else list(a.default) if isinstance(a.default, tuple) else a.default,
                "has_default": a.default is not _MISSING, "secret": a.secret,
                "enum": list(a.enum), "minimum": a.minimum, "maximum": a.maximum,
                "min_items": a.min_items, "max_items": a.max_items,
            } for name, a in profile.arguments.items()
        },
        "resource_slots": {
            name: {"description": r.description, "allowed_pools": list(r.allowed_pools), "default_pool": r.default_pool, "required": r.required}
            for name, r in profile.resource_slots.items()
        },
        "artifacts": {
            name: {"root": a.root, "path": a.path, "required": a.required}
            for name, a in profile.artifacts.items()
        },
        "steps": [_step_canonical(s) for s in profile.steps],
        "result": {"success_exit_codes": list(profile.result.success_exit_codes), "required_artifacts": list(profile.result.required_artifacts), "output_capture": profile.result.output_capture},
        "timeout_seconds": profile.timeout_seconds,
        "cleanup": {"when": profile.cleanup.when, "timeout_seconds": profile.cleanup.timeout_seconds, "steps": [_step_canonical(s) for s in profile.cleanup.steps]},
        "cancellation": {"mode": profile.cancellation.mode, "grace_seconds": profile.cancellation.grace_seconds, "run_cleanup": profile.cancellation.run_cleanup},
    }


def _render_arg(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _compile_step(step: CommandStep, profile: ExecutionProfile, arguments: Mapping[str, Any]) -> CompiledCommand:
    argv: list[CompiledArgv] = []
    for token in step.argv:
        if token.kind == "literal":
            argv.append(CompiledArgv("literal", token.value))
        elif token.kind == "arg":
            argv.append(CompiledArgv("value", _render_arg(arguments[token.value]), profile.arguments[token.value].secret))
        elif token.kind == "spread":
            argv.extend(CompiledArgv("value", item, profile.arguments[token.value].secret) for item in arguments[token.value])
        elif token.kind == "artifact":
            artifact = profile.artifacts[token.value]
            argv.append(CompiledArgv(
                "artifact",
                CompiledArtifactRef(artifact.name, artifact.root, artifact.path),
            ))
        else:
            argv.append(CompiledArgv(token.kind, token.value))
    return CompiledCommand(
        step.id, step.transport, step.executable, tuple(argv), step.cwd,
        step.resource_slot, step.capture_stdout, step.capture_stderr,
    )


def compile_plan(profile: ExecutionProfile, arguments: Any, resources: Any) -> CompiledExecutionPlan:
    if not isinstance(arguments, dict) or not all(isinstance(k, str) for k in arguments):
        raise OperationValidationError("arguments must be an object")
    if not isinstance(resources, dict) or not all(isinstance(k, str) for k in resources):
        raise OperationValidationError("resources must be an object")
    unknown_args = sorted(set(arguments) - set(profile.arguments))
    if unknown_args:
        raise OperationValidationError(f"unknown argument(s): {', '.join(unknown_args)}")
    normalized_args: dict[str, Any] = {}
    for name, spec in profile.arguments.items():
        if name in arguments:
            normalized_args[name] = _validate_argument_value(spec, arguments[name])
        elif spec.default is not _MISSING:
            value = list(spec.default) if isinstance(spec.default, tuple) else spec.default
            normalized_args[name] = _validate_argument_value(spec, value)
        elif spec.required:
            raise OperationValidationError(f"missing required argument: {name}")
    unknown_resources = sorted(set(resources) - set(profile.resource_slots))
    if unknown_resources:
        raise OperationValidationError(f"unknown resource slot(s): {', '.join(unknown_resources)}")
    normalized_resources: dict[str, str] = {}
    for name, slot in profile.resource_slots.items():
        selected = resources.get(name, slot.default_pool)
        if selected is None:
            if slot.required:
                raise OperationValidationError(f"missing required resource slot: {name}")
            continue
        if not isinstance(selected, str) or selected not in slot.allowed_pools:
            raise OperationValidationError(f"resource slot {name!r} must select one of its allowed pools")
        normalized_resources[name] = selected
    return CompiledExecutionPlan(
        kind=profile.kind, profile_version=profile.version,
        profile_hash=profile.profile_hash, profile=profile,
        arguments=_freeze_map(normalized_args), resources=_freeze_map(normalized_resources),
        artifacts=profile.artifacts,
        steps=tuple(_compile_step(s, profile, normalized_args) for s in profile.steps),
        result=profile.result, timeout_seconds=profile.timeout_seconds,
        cleanup=CompiledCleanupPolicy(
            profile.cleanup.when,
            profile.cleanup.timeout_seconds,
            tuple(_compile_step(s, profile, normalized_args) for s in profile.cleanup.steps),
        ),
        cancellation=profile.cancellation,
    )


class ExecutionCatalog:
    """Hot-reloadable immutable profile snapshot.

    Candidate parsing happens without touching the active snapshot.  A failed
    reload therefore raises while all callers continue seeing the last complete
    valid generation.
    """

    def __init__(self, workspace: Path):
        self.workspace = Path(workspace)
        self.directory = self.workspace / PROFILE_DIR
        self._lock = threading.RLock()
        self._snapshot = CatalogSnapshot(0, _freeze_map({}))

    @property
    def snapshot(self) -> CatalogSnapshot:
        with self._lock:
            return self._snapshot

    def _paths(self) -> list[Path]:
        directory = self.directory
        if not directory.exists():
            return []
        config_root = self.workspace / "config"
        for component in (config_root, config_root / "executions", directory):
            if component.is_symlink():
                raise ExecutionProfileError(
                    f"execution profile path component must not be a symlink: {component}"
                )
        if not directory.is_dir():
            raise ExecutionProfileError(f"execution profile path must be a real directory, not a symlink: {directory}")
        if not directory.resolve().is_relative_to(config_root.resolve()):
            raise ExecutionProfileError(f"execution profile directory resolves outside {config_root}")
        paths: list[Path] = []
        for path in sorted(directory.iterdir(), key=lambda p: p.name):
            if path.is_symlink():
                raise ExecutionProfileError(f"execution profile must not be a symlink: {path}")
            if not path.is_file() or path.suffix != ".yaml":
                raise ExecutionProfileError(f"execution profile directory contains unsupported entry: {path.name}")
            if not path.resolve().is_relative_to(directory.resolve()):
                raise ExecutionProfileError(f"execution profile resolves outside its catalog: {path}")
            paths.append(path)
        return paths

    def build_candidate(self) -> CatalogSnapshot:
        profiles: dict[str, ExecutionProfile] = {}
        for path in self._paths():
            try:
                raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
            except (yaml.YAMLError, ExecutionProfileError) as e:
                raise ExecutionProfileError(f"failed to parse execution profile {path}: {e}") from e
            except (OSError, UnicodeDecodeError) as e:
                raise ExecutionProfileError(f"cannot read execution profile {path}: {e}") from e
            if raw is None:
                raise ExecutionProfileError(f"execution profile is empty: {path}")
            profile = _parse_profile(raw, path)
            if profile.kind in profiles:
                raise ExecutionProfileError(
                    f"duplicate execution kind {profile.kind!r} in {profiles[profile.kind].source} and {path}"
                )
            profiles[profile.kind] = profile
        with self._lock:
            generation = self._snapshot.generation + 1
        return CatalogSnapshot(generation, _freeze_map(profiles))

    def reload(self) -> CatalogSnapshot:
        candidate = self.build_candidate()
        with self._lock:
            candidate = replace(candidate, generation=self._snapshot.generation + 1)
            self._snapshot = candidate
            return candidate

    def describe(self, kind: str) -> dict[str, Any]:
        profile = self.snapshot.get(kind)
        if profile is None:
            raise OperationValidationError(f"unknown execution kind: {kind}")
        return profile.describe()

    def compile(self, kind: str, arguments: Any = None, resources: Any = None) -> CompiledExecutionPlan:
        profile = self.snapshot.get(kind)
        if profile is None:
            raise OperationValidationError(f"unknown execution kind: {kind}")
        return compile_plan(profile, {} if arguments is None else arguments, {} if resources is None else resources)
