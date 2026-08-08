"""Skill manager — discovers, loads, and manages skills from the filesystem.

Skills are directories containing a SKILL.md file with YAML frontmatter.
Compatible with Claude SDK's skill format. The filesystem is the source of
truth; the DB indexes metadata and tracks usage statistics.

Directory structure:
    workspace/skills/<skill-id>/
        SKILL.md          (required — YAML frontmatter + markdown body)
        references/       (optional — documentation loaded on demand)
        scripts/          (optional — executable code)
        assets/           (optional — templates, images, etc.)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from nerve.config import ensure_not_locked
from nerve.db import Database

logger = logging.getLogger(__name__)

AMENDMENTS_REFERENCE = "AMENDMENTS.md"
AMENDMENTS_HEADER = "# Pending amendments\n"
MAX_DEPENDENCY_DEPTH = 5
MAX_DEPENDENCIES = 20
MAX_DESCRIPTION_LENGTH = 1024
_CANONICAL_SKILL_NAME_RE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
_RESOURCE_DIRS = ("references", "scripts", "assets", "agents")
_SEMVER_RE = re.compile(
    r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9]\d*|[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?\Z"
)
_UPDATE_JOURNAL = ".skill-update.json"
_UPDATE_OLD_SKILL = ".SKILL.md.rollback"
_UPDATE_OLD_AMENDMENTS = ".AMENDMENTS.md.rollback"


@dataclass(frozen=True)
class SkillDependency:
    """One skill-to-skill dependency declared in SKILL.md frontmatter."""

    skill: str
    mode: str = "required"
    when: str = ""


@dataclass(frozen=True)
class SkillDependencyIssue:
    """Actionable validation or graph-resolution failure."""

    code: str
    message: str
    skill: str
    dependency: str = ""
    path: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "skill": self.skill,
            "dependency": self.dependency or None,
            "path": list(self.path),
        }


@dataclass(frozen=True)
class SkillValidationIssue:
    """A package-level schema error or migration diagnostic."""

    code: str
    message: str
    path: tuple[str, ...] = ()
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "path": list(self.path),
            "severity": self.severity,
        }


class SkillValidationError(ValueError):
    """Raised before a mutating operation when a skill package is invalid."""

    def __init__(self, issues: list[SkillValidationIssue]):
        self.issues = issues
        super().__init__("; ".join(issue.message for issue in issues))


class SkillUpdateConflict(ValueError):
    """A replacement was prepared from stale installed state."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def skill_revision(raw: str | bytes) -> str:
    """Stable revision of the exact installed SKILL.md bytes."""
    payload = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _semver_key(value: str) -> tuple[int, int, int, tuple[tuple[int, object], ...]]:
    match = _SEMVER_RE.fullmatch(value)
    if not match:
        raise ValueError(f"invalid semantic version {value!r}; expected SemVer MAJOR.MINOR.PATCH")
    prerelease = match.group(4)
    # A release sorts after all of its prereleases.
    pre_key: tuple[tuple[int, object], ...] = ((2, ""),) if prerelease is None else tuple(
        (0, int(part)) if part.isdigit() else (1, part)
        for part in prerelease.split(".")
    )
    return int(match.group(1)), int(match.group(2)), int(match.group(3)), pre_key


@dataclass(frozen=True)
class SuggestedSkillDependency:
    """One advisory edge collected from a successfully resolved bundle."""

    skill: str
    when: str
    declared_by: str

    def to_dict(self) -> dict[str, str]:
        return {
            "skill": self.skill,
            "when": self.when,
            "declared_by": self.declared_by,
        }


@dataclass
class SkillMeta:
    """Skill metadata extracted from SKILL.md frontmatter."""
    id: str
    name: str
    description: str
    version: str = "1.0.0"
    enabled: bool = True
    user_invocable: bool = True
    model_invocable: bool = True
    allowed_tools: list[str] | None = None
    dependencies: list[SkillDependency] = field(default_factory=list)
    dependency_source: str = "none"
    dependency_errors: list[SkillDependencyIssue] = field(default_factory=list)
    has_references: bool = False
    has_scripts: bool = False
    has_assets: bool = False
    metadata: dict = field(default_factory=dict)
    schema_source: str = "canonical"
    diagnostics: list[SkillValidationIssue] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    skill_revision: str = ""
    git_commit: str = ""
    update_outcome: str = "loaded"


@dataclass
class SkillContent(SkillMeta):
    """Full skill including SKILL.md body content."""
    content: str = ""   # Markdown body (after frontmatter)
    raw: str = ""       # Full SKILL.md file


@dataclass
class SkillDependencyResolution:
    """Deterministic dependency-first bundle or explicit failures."""

    root: str
    bundle: list[SkillContent] = field(default_factory=list)
    suggested: list[SuggestedSkillDependency] = field(default_factory=list)
    errors: list[SkillDependencyIssue] = field(default_factory=list)
    max_depth: int = MAX_DEPENDENCY_DEPTH
    max_dependencies: int = MAX_DEPENDENCIES

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "root": self.root,
            "order": [skill.id for skill in self.bundle],
            "required": [
                {"id": skill.id, "name": skill.name, "version": skill.version}
                for skill in self.bundle
                if skill.id != self.root
            ],
            "suggested": [dependency.to_dict() for dependency in self.suggested],
            "errors": [issue.to_dict() for issue in self.errors],
            "limits": {
                "max_depth": self.max_depth,
                "max_dependencies": self.max_dependencies,
            },
        }


def _slugify(name: str) -> str:
    """Convert a name to a valid directory slug."""
    slug = name.lower().replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:60] or "unnamed-skill"


# One path component and nothing else: no separator, on either platform, and no
# drive or UNC prefix. Deliberately wider than what _slugify emits, because the
# filesystem is the source of truth here and discover() adopts whatever directory
# names it finds — a pre-existing `My_Skill` must stay editable.
_SKILL_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")


class SkillIdError(ValueError):
    """Raised for a skill id that could not name a directory under ``skills/``."""


def _skill_id(skill_id: str) -> str:
    """Validate a caller-supplied skill id before it is joined to a path.

    ``_slugify`` runs on *create* only; every other entry point takes the id
    straight from an HTTP path segment or a tool argument and joins it to
    ``skills_dir``. That made ``..`` a working path component — and since delete
    removes the whole directory tree it names, ``DELETE /api/skills/../config``
    took out the workspace's tracked config subtree.

    Checked as a *shape* rather than by containing the result afterwards: a skill
    id names one directory directly under ``skills/``, so anything that is not a
    single path component is not a skill id, whatever it would have resolved to.
    A leading dot is refused too, which keeps ``.`` and ``..`` out without
    special-casing them.
    """
    if not isinstance(skill_id, str) or not _SKILL_ID_RE.match(skill_id):
        raise SkillIdError(
            f"invalid skill id {skill_id!r}: a skill id names a single directory "
            f"under skills/ — letters, digits, '.', '_', '+' and '-', not "
            f"starting with a dot"
        )
    return skill_id


def _parse_skill_md(raw: str) -> tuple[dict, str]:
    """Parse SKILL.md into (frontmatter_dict, body_content).

    Frontmatter is delimited by --- lines at the top of the file.
    """
    frontmatter: dict = {}
    body = raw

    stripped = raw.strip()
    if stripped.startswith("---"):
        # Find the closing ---
        end_idx = stripped.find("---", 3)
        if end_idx != -1:
            yaml_block = stripped[3:end_idx].strip()
            body = stripped[end_idx + 3:].strip()
            try:
                frontmatter = yaml.safe_load(yaml_block) or {}
            except yaml.YAMLError as e:
                logger.warning("Failed to parse SKILL.md frontmatter: %s", e)

    return frontmatter, body


def _parse_skill_md_strict(raw: str) -> tuple[dict[str, Any], str]:
    """Parse a complete SKILL.md package without accepting malformed YAML.

    Discovery uses this same parser as the write paths.  The old reader logged
    a YAML error and then treated the package as empty frontmatter, which made
    malformed skills appear valid and allowed an update to destroy a good file.
    """
    match = re.match(r"\A---[ \t]*\r?\n(.*?)\r?\n---[ \t]*(?:\r?\n|\Z)", raw, re.DOTALL)
    if not match:
        raise SkillValidationError([SkillValidationIssue(
            "missing_frontmatter",
            "SKILL.md must start with a YAML frontmatter block delimited by ---",
        )])
    try:
        frontmatter = yaml.safe_load(match.group(1))
    except yaml.YAMLError as exc:
        raise SkillValidationError([SkillValidationIssue(
            "invalid_yaml", f"invalid YAML frontmatter: {exc}",
        )]) from exc
    if not isinstance(frontmatter, dict):
        raise SkillValidationError([SkillValidationIssue(
            "invalid_frontmatter", "SKILL.md frontmatter must be a mapping",
        )])
    return frontmatter, raw[match.end():].strip()


def _parse_dependencies(
    frontmatter: dict[str, Any], skill_id: str,
) -> tuple[list[SkillDependency], str, list[SkillDependencyIssue]]:
    """Normalize canonical and legacy dependency declarations.

    Canonical declarations live under ``metadata.nerve.dependencies`` and use
    ``required`` / ``suggested`` groups. The former top-level ``dependencies``
    field remains readable during migration. Ambiguous or malformed declarations
    are retained as explicit validation errors instead of being silently ignored.
    """

    issues: list[SkillDependencyIssue] = []
    canonical: Any = None
    has_canonical = False
    metadata = frontmatter.get("metadata")
    if metadata is not None:
        if not isinstance(metadata, dict):
            issues.append(SkillDependencyIssue(
                code="invalid_dependency_namespace",
                message="metadata must be a mapping to declare Nerve dependencies",
                skill=skill_id,
                path=("metadata",),
            ))
        elif "nerve" in metadata:
            nerve_metadata = metadata["nerve"]
            if not isinstance(nerve_metadata, dict):
                issues.append(SkillDependencyIssue(
                    code="invalid_dependency_namespace",
                    message="metadata.nerve must be a mapping",
                    skill=skill_id,
                    path=("metadata", "nerve"),
                ))
            else:
                if "dependencies" in nerve_metadata:
                    has_canonical = True
                    canonical = nerve_metadata["dependencies"]

    has_legacy = "dependencies" in frontmatter
    if has_canonical and has_legacy:
        issues.append(SkillDependencyIssue(
            code="conflicting_dependency_declarations",
            message=(
                "declare dependencies only once; migrate the top-level "
                "dependencies field to metadata.nerve.dependencies"
            ),
            skill=skill_id,
            path=("metadata", "nerve", "dependencies"),
        ))

    source = "canonical" if has_canonical else "legacy" if has_legacy else "none"
    raw = canonical if has_canonical else frontmatter.get("dependencies")
    if source == "none":
        return [], source, issues

    base_path = (
        ("metadata", "nerve", "dependencies")
        if source == "canonical" else ("dependencies",)
    )
    groups: list[tuple[str, Any, str]] = []
    if isinstance(raw, dict):
        allowed_groups = (
            {"required", "suggested"}
            if source == "canonical"
            else {"requires", "required", "suggests", "suggested"}
        )
        for key in sorted(set(raw) - allowed_groups, key=str):
            issues.append(SkillDependencyIssue(
                code="unsupported_dependency_field",
                message=f"unsupported dependency field: {key}",
                skill=skill_id,
                path=(*base_path, str(key)),
            ))
        required_keys = ("required",) if source == "canonical" else ("requires", "required")
        suggested_keys = ("suggested",) if source == "canonical" else ("suggests", "suggested")
        groups.extend(("required", raw[key], key) for key in required_keys if key in raw)
        groups.extend(("suggested", raw[key], key) for key in suggested_keys if key in raw)
    elif source == "legacy" and isinstance(raw, (list, str)):
        groups.append((
            "required",
            raw if isinstance(raw, list) else [raw],
            "required",
        ))
    else:
        issues.append(SkillDependencyIssue(
            code="invalid_dependencies_type",
            message=(
                "dependencies must be a mapping with required/suggested lists"
                if source == "canonical"
                else "legacy dependencies must be a list or mapping"
            ),
            skill=skill_id,
            path=base_path,
        ))
        return [], source, issues

    dependencies: list[SkillDependency] = []
    seen: dict[str, str] = {}
    for default_mode, items, group_name in groups:
        group_path = (*base_path, group_name)
        if not isinstance(items, list):
            if source == "legacy" and isinstance(items, (str, dict)):
                items = [items]
            else:
                issues.append(SkillDependencyIssue(
                    code="invalid_dependency_group",
                    message=f"{group_name} dependencies must be a list",
                    skill=skill_id,
                    path=group_path,
                ))
                continue
        for index, item in enumerate(items):
            item_path = (*group_path, str(index))
            mode = default_mode
            when = ""
            if isinstance(item, str):
                dependency_id = item.strip()
            elif isinstance(item, dict):
                allowed_fields = {"skill", "when"}
                if source == "legacy":
                    allowed_fields |= {"name", "mode"}
                for key in sorted(set(item) - allowed_fields, key=str):
                    issues.append(SkillDependencyIssue(
                        code="unsupported_dependency_field",
                        message=(
                            f"unsupported dependency field: {key}; "
                            "version constraints are deferred for this MVP"
                            if key in {"version", "version_constraint"}
                            else f"unsupported dependency field: {key}"
                        ),
                        skill=skill_id,
                        path=(*item_path, str(key)),
                    ))
                dependency_id = str(item.get("skill") or item.get("name") or "").strip()
                when_value = item.get("when", "")
                if when_value is not None and not isinstance(when_value, str):
                    issues.append(SkillDependencyIssue(
                        code="invalid_dependency_condition",
                        message="suggested dependency condition must be human-readable text",
                        skill=skill_id,
                        dependency=dependency_id,
                        path=(*item_path, "when"),
                    ))
                else:
                    when = str(when_value or "").strip()
                if source == "legacy" and "mode" in item:
                    mode = str(item["mode"]).strip().lower()
                    mode = {
                        "require": "required", "requires": "required",
                        "suggest": "suggested", "suggests": "suggested",
                        "recommend": "suggested", "recommended": "suggested",
                    }.get(mode, mode)
            else:
                issues.append(SkillDependencyIssue(
                    code="invalid_dependency_entry",
                    message="dependency entries must be skill IDs or mappings",
                    skill=skill_id,
                    path=item_path,
                ))
                continue

            if mode not in {"required", "suggested"}:
                issues.append(SkillDependencyIssue(
                    code="invalid_dependency_mode",
                    message=f"dependency mode must be required or suggested, got {mode!r}",
                    skill=skill_id,
                    dependency=dependency_id,
                    path=item_path,
                ))
                continue
            try:
                dependency_id = _skill_id(dependency_id)
            except SkillIdError:
                issues.append(SkillDependencyIssue(
                    code="invalid_dependency_id",
                    message=f"invalid dependency skill ID: {dependency_id!r}",
                    skill=skill_id,
                    dependency=dependency_id,
                    path=item_path,
                ))
                continue
            if dependency_id == skill_id:
                issues.append(SkillDependencyIssue(
                    code="self_dependency",
                    message=f"skill {skill_id!r} cannot depend on itself",
                    skill=skill_id,
                    dependency=dependency_id,
                    path=item_path,
                ))
                continue
            if mode == "required" and when:
                issues.append(SkillDependencyIssue(
                    code="conditional_required_dependency",
                    message="required dependencies cannot have advisory conditions",
                    skill=skill_id,
                    dependency=dependency_id,
                    path=(*item_path, "when"),
                ))
            previous_mode = seen.get(dependency_id)
            if previous_mode:
                code = (
                    "duplicate_dependency" if previous_mode == mode
                    else "conflicting_dependency_modes"
                )
                issues.append(SkillDependencyIssue(
                    code=code,
                    message=(
                        f"dependency {dependency_id!r} is declared more than once"
                        if previous_mode == mode
                        else f"dependency {dependency_id!r} cannot be both required and suggested"
                    ),
                    skill=skill_id,
                    dependency=dependency_id,
                    path=item_path,
                ))
                continue
            seen[dependency_id] = mode
            dependencies.append(SkillDependency(
                skill=dependency_id,
                mode=mode,
                when=when if mode == "suggested" else "",
            ))

    dependencies.sort(key=lambda dependency: (dependency.mode != "required", dependency.skill))
    return dependencies, source, issues


@dataclass(frozen=True)
class NormalizedSkillPackage:
    """The one internal representation used by discovery and all writers."""

    skill_id: str
    name: str
    description: str
    version: str
    body: str
    frontmatter: dict[str, Any]
    metadata: dict[str, Any]
    dependencies: list[SkillDependency]
    dependency_source: str
    dependency_errors: list[SkillDependencyIssue]
    user_invocable: bool
    model_invocable: bool
    allowed_tools: list[str] | None
    schema_source: str
    diagnostics: list[SkillValidationIssue]


def _resource_issues(skill_dir: Path) -> list[SkillValidationIssue]:
    """Reject optional resources that escape the package through symlinks."""
    issues: list[SkillValidationIssue] = []
    package_root = skill_dir.resolve()
    for dirname in _RESOURCE_DIRS:
        root = skill_dir / dirname
        if not root.exists():
            continue
        try:
            root.resolve().relative_to(package_root)
        except ValueError:
            issues.append(SkillValidationIssue(
                "unsafe_resource_path", f"{dirname} escapes the skill package",
                (dirname,),
            ))
            continue
        for path in root.rglob("*"):
            try:
                path.resolve().relative_to(package_root)
            except ValueError:
                issues.append(SkillValidationIssue(
                    "unsafe_resource_path",
                    f"resource path escapes the skill package: {path.relative_to(skill_dir)}",
                    tuple(path.relative_to(skill_dir).parts),
                ))
    return issues


def validate_skill_package(
    raw: str, skill_id: str, *, skill_dir: Path | None = None,
    allow_legacy: bool = True,
) -> NormalizedSkillPackage:
    """Validate and normalize a portable Nerve skill package in memory.

    Canonical packages use Codex-compatible ``name`` and ``description`` at
    the top level, with Nerve fields only under ``metadata.nerve``.  Old flat
    Nerve fields are intentionally read-only compatibility input; every such
    package carries an explicit migration diagnostic.
    """
    issues: list[SkillValidationIssue] = []
    try:
        skill_id = _skill_id(skill_id)
    except SkillIdError as exc:
        raise SkillValidationError([SkillValidationIssue("invalid_skill_id", str(exc))]) from exc
    frontmatter, body = _parse_skill_md_strict(raw)

    name = frontmatter.get("name")
    description = frontmatter.get("description")
    if not isinstance(name, str) or not name.strip():
        issues.append(SkillValidationIssue("missing_name", "name is required and must be text", ("name",)))
    if not isinstance(description, str) or not description.strip():
        issues.append(SkillValidationIssue(
            "missing_description", "description is required and must be text", ("description",),
        ))
    elif len(description) > MAX_DESCRIPTION_LENGTH:
        issues.append(SkillValidationIssue(
            "description_too_long",
            f"description must be at most {MAX_DESCRIPTION_LENGTH} characters",
            ("description",),
        ))
    if not body.strip():
        issues.append(SkillValidationIssue(
            "empty_body", "SKILL.md instructions must not be empty", ("body",),
        ))

    metadata_value = frontmatter.get("metadata", {})
    if not isinstance(metadata_value, dict):
        issues.append(SkillValidationIssue("invalid_metadata", "metadata must be a mapping", ("metadata",)))
        metadata_value = {}
    nerve_value = metadata_value.get("nerve", {})
    if "nerve" in metadata_value and not isinstance(nerve_value, dict):
        issues.append(SkillValidationIssue(
            "invalid_nerve_metadata", "metadata.nerve must be a mapping", ("metadata", "nerve"),
        ))
        nerve_value = {}

    has_nerve_namespace = "nerve" in metadata_value
    legacy_fields = {"version", "context", "dependencies", "agent"} & set(frontmatter)
    canonical_name = (
        isinstance(name, str)
        and bool(_CANONICAL_SKILL_NAME_RE.match(name))
        and name == skill_id
    )
    legacy_name = not has_nerve_namespace and not canonical_name
    canonical = not legacy_fields and canonical_name
    if legacy_fields or legacy_name:
        if not allow_legacy:
            issues.append(SkillValidationIssue(
                "legacy_schema_not_allowed",
                "legacy flat Nerve fields are not allowed for create or update",
                severity="error",
            ))
        else:
            issues.append(SkillValidationIssue(
                "legacy_schema",
                "legacy skill schema is supported temporarily; move Nerve fields under metadata.nerve and make name match the directory",
                severity="warning",
            ))

    version = nerve_value.get("version", frontmatter.get("version", "1.0.0"))
    context = nerve_value.get("context", frontmatter.get("context"))
    if not isinstance(version, str) or not version.strip():
        issues.append(SkillValidationIssue(
            "invalid_nerve_version", "metadata.nerve.version must be non-empty text", ("metadata", "nerve", "version"),
        ))
    else:
        try:
            _semver_key(version.strip())
        except ValueError as exc:
            issues.append(SkillValidationIssue(
                "invalid_nerve_version", str(exc), ("metadata", "nerve", "version"),
            ))
    if context is not None and not isinstance(context, str):
        issues.append(SkillValidationIssue(
            "invalid_nerve_context", "metadata.nerve.context must be text", ("metadata", "nerve", "context"),
        ))
    for field_name in ("user-invocable", "disable-model-invocation"):
        if field_name in frontmatter and not isinstance(frontmatter[field_name], bool):
            issues.append(SkillValidationIssue(
                "invalid_portable_field", f"{field_name} must be boolean", (field_name,),
            ))
    allowed_nerve = {"version", "context", "dependencies", "codex"}
    if isinstance(nerve_value, dict):
        for key in sorted(set(nerve_value) - allowed_nerve):
            issues.append(SkillValidationIssue(
                "unsupported_nerve_metadata", f"unsupported metadata.nerve field: {key}",
                ("metadata", "nerve", str(key)),
            ))
    if "codex" in nerve_value and not isinstance(nerve_value["codex"], bool):
        issues.append(SkillValidationIssue(
            "invalid_codex_metadata", "metadata.nerve.codex must be boolean", ("metadata", "nerve", "codex"),
        ))

    if has_nerve_namespace and not canonical:
        if not isinstance(name, str) or not _CANONICAL_SKILL_NAME_RE.match(name):
            issues.append(SkillValidationIssue(
                "invalid_canonical_name", "canonical name must be a lowercase hyphenated identifier", ("name",),
            ))
        elif name != skill_id:
            issues.append(SkillValidationIssue(
                "name_directory_mismatch", f"name {name!r} must match skill directory {skill_id!r}", ("name",),
            ))

    dependencies, dependency_source, dependency_errors = _parse_dependencies(frontmatter, skill_id)
    issues.extend(SkillValidationIssue(issue.code, issue.message, issue.path) for issue in dependency_errors)
    allowed_tools_raw = frontmatter.get("allowed-tools")
    allowed_tools: list[str] | None = None
    if allowed_tools_raw is not None:
        if isinstance(allowed_tools_raw, str):
            allowed_tools = [item.strip() for item in allowed_tools_raw.split(",") if item.strip()]
        elif isinstance(allowed_tools_raw, list) and all(isinstance(item, str) for item in allowed_tools_raw):
            allowed_tools = list(allowed_tools_raw)
        else:
            issues.append(SkillValidationIssue(
                "invalid_allowed_tools", "allowed-tools must be text or a list of text", ("allowed-tools",),
            ))

    if skill_dir is not None:
        issues.extend(_resource_issues(skill_dir))
        agent_file = skill_dir / "agents" / "openai.yaml"
        # Nerve-only packages ignore Codex' optional agent metadata.  A package
        # explicitly marked dual-use must at least carry a YAML mapping there.
        if nerve_value.get("codex") is True and agent_file.exists():
            try:
                agent_data = yaml.safe_load(agent_file.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                issues.append(SkillValidationIssue(
                    "invalid_codex_agent", f"invalid agents/openai.yaml: {exc}", ("agents", "openai.yaml"),
                ))
            else:
                if not isinstance(agent_data, dict):
                    issues.append(SkillValidationIssue(
                        "invalid_codex_agent", "agents/openai.yaml must be a mapping", ("agents", "openai.yaml"),
                    ))

    errors = [issue for issue in issues if issue.severity == "error"]
    if errors:
        raise SkillValidationError(issues)
    return NormalizedSkillPackage(
        skill_id=skill_id, name=name.strip(), description=description.strip(),
        version=version.strip(), body=body, frontmatter=frontmatter,
        metadata=metadata_value, dependencies=dependencies,
        dependency_source=dependency_source, dependency_errors=dependency_errors,
        user_invocable=frontmatter.get("user-invocable", True),
        model_invocable=not frontmatter.get("disable-model-invocation", False),
        allowed_tools=allowed_tools, schema_source="canonical" if canonical else "legacy",
        diagnostics=issues,
    )


def _build_skill_md(name: str, description: str, body: str = "", version: str = "1.0.0", **extra) -> str:
    """Build a SKILL.md file from components."""
    fm: dict[str, Any] = {
        "name": name,
        "description": description,
        "metadata": {"nerve": {"version": version}},
    }
    fm.update(extra)
    yaml_str = yaml.dump(fm, default_flow_style=False, allow_unicode=True).strip()
    parts = [f"---\n{yaml_str}\n---"]
    parts.append(body or f"# {name}\n\n{description}")
    return "\n\n".join(parts) + "\n"


class SkillManager:
    """Discovers, loads, and manages skills from the filesystem."""

    def __init__(self, workspace: Path, db: Database):
        self.workspace = workspace
        self.skills_dir = workspace / "skills"
        self.db = db
        self._cache: dict[str, SkillMeta] = {}
        self._diagnostics: dict[str, list[SkillValidationIssue]] = {}
        self._amendment_locks: dict[str, asyncio.Lock] = {}
        self._discovered_once = False

    def _meta_from_package(
        self, package: NormalizedSkillPackage, *, enabled: bool,
        skill_dir: Path, raw: str = "",
    ) -> SkillMeta:
        """Build index metadata from the shared normalized package model."""
        known_keys = {
            "name", "description", "user-invocable", "disable-model-invocation",
            "allowed-tools", "license", "argument-hint", "metadata",
        }
        extra_meta = {key: value for key, value in package.frontmatter.items() if key not in known_keys}
        extra_meta["nerve"] = package.metadata.get("nerve", {})
        git_commit = ""
        try:
            from nerve.config_history import skill_commit
            git_commit = skill_commit(self.workspace, package.skill_id)
        except Exception:  # Git provenance is optional and must not block loading.
            logger.debug("Could not determine Git provenance for skill %s", package.skill_id)
        return SkillMeta(
            id=package.skill_id, name=package.name, description=package.description,
            version=package.version, enabled=enabled,
            user_invocable=package.user_invocable,
            model_invocable=package.model_invocable,
            allowed_tools=package.allowed_tools,
            dependencies=package.dependencies,
            dependency_source=package.dependency_source,
            dependency_errors=package.dependency_errors,
            has_references=(skill_dir / "references").is_dir(),
            has_scripts=(skill_dir / "scripts").is_dir(),
            has_assets=(skill_dir / "assets").is_dir(),
            metadata=extra_meta, schema_source=package.schema_source,
            diagnostics=package.diagnostics,
            skill_revision=skill_revision(raw) if raw else "",
            git_commit=git_commit,
        )

    @staticmethod
    def _recover_update(skill_dir: Path) -> bool:
        """Rollback an interrupted filesystem transition before indexing it."""
        journal = skill_dir / _UPDATE_JOURNAL
        if not journal.exists():
            return False
        skill_md = skill_dir / "SKILL.md"
        old_skill = skill_dir / _UPDATE_OLD_SKILL
        old_amendments = skill_dir / _UPDATE_OLD_AMENDMENTS
        amendments = skill_dir / "references" / AMENDMENTS_REFERENCE
        if old_skill.exists():
            os.replace(old_skill, skill_md)
        try:
            data = json.loads(journal.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {"had_amendments": old_amendments.exists()}
        if data.get("had_amendments") and old_amendments.exists():
            amendments.parent.mkdir(parents=True, exist_ok=True)
            os.replace(old_amendments, amendments)
        old_amendments.unlink(missing_ok=True)
        journal.unlink(missing_ok=True)
        logger.warning("Recovered interrupted skill update for %s", skill_dir.name)
        return True

    async def discover(self) -> list[SkillMeta]:
        """Scan skills_dir for SKILL.md files, parse frontmatter, sync to DB.

        Returns all discovered skills. Also removes DB entries for skills
        that no longer exist on the filesystem.
        """
        await asyncio.to_thread(self.skills_dir.mkdir, parents=True, exist_ok=True)
        for skill_dir in await asyncio.to_thread(lambda: list(self.skills_dir.iterdir())):
            if skill_dir.is_dir():
                await asyncio.to_thread(self._recover_update, skill_dir)
        discovered: list[SkillMeta] = []
        found_ids: set[str] = set()
        self._cache.clear()
        self._diagnostics.clear()

        def _scan_skill_dirs() -> list[tuple[Path, str]]:
            """Collect (skill_dir, raw SKILL.md) pairs off the event loop."""
            out: list[tuple[Path, str]] = []
            for sdir in sorted(self.skills_dir.iterdir()):
                if not sdir.is_dir():
                    continue
                smd = sdir / "SKILL.md"
                if not smd.exists():
                    continue
                try:
                    out.append((sdir, smd.read_bytes().decode("utf-8")))
                except OSError as e:
                    logger.error("Failed to read skill %s: %s", sdir.name, e)
            return out

        for skill_dir, raw in await asyncio.to_thread(_scan_skill_dirs):
            skill_id = skill_dir.name
            found_ids.add(skill_id)

            try:
                # Preserve runtime state across filesystem re-discovery.
                existing = await self.db.get_skill_row(skill_id)
                enabled = existing["enabled"] if existing else True
                package = validate_skill_package(raw, skill_id, skill_dir=skill_dir)
                meta = self._meta_from_package(package, enabled=enabled, skill_dir=skill_dir, raw=raw)
                discovered.append(meta)
                self._cache[skill_id] = meta
                self._diagnostics[skill_id] = meta.diagnostics

                # Sync to DB
                await self.db.upsert_skill(
                    skill_id=skill_id,
                    name=meta.name,
                    description=meta.description,
                    version=meta.version,
                    enabled=enabled,
                    user_invocable=meta.user_invocable,
                    model_invocable=meta.model_invocable,
                    allowed_tools=meta.allowed_tools,
                    metadata=meta.metadata,
                )

            except SkillValidationError as e:
                self._cache.pop(skill_id, None)
                self._diagnostics[skill_id] = e.issues
                logger.warning("Invalid skill package %s: %s", skill_id, e)
            except Exception as e:
                logger.error("Failed to load skill %s: %s", skill_id, e)

        # Clean up DB entries for skills that no longer exist on filesystem
        db_skills = await self.db.list_skills()
        for db_skill in db_skills:
            if db_skill["id"] not in found_ids:
                logger.info("Removing stale skill from DB: %s", db_skill["id"])
                await self.db.delete_skill_row(db_skill["id"])

        logger.info("Discovered %d skills", len(discovered))
        self._discovered_once = True
        return discovered

    def diagnostics(self, skill_id: str) -> list[SkillValidationIssue]:
        """Return discovery diagnostics without treating an invalid package as usable."""
        return list(self._diagnostics.get(skill_id, ()))

    def all_diagnostics(self) -> dict[str, list[SkillValidationIssue]]:
        """Return a snapshot for discovery and API reporting."""
        return {skill_id: list(issues) for skill_id, issues in self._diagnostics.items() if issues}

    async def get_skill(self, skill_id: str) -> SkillContent | None:
        """Load full SKILL.md content + metadata for a skill."""
        skill_dir = self.skills_dir / _skill_id(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        raw = (await asyncio.to_thread(skill_md.read_bytes)).decode("utf-8")

        # Get metadata from cache or DB
        cached = self._cache.get(skill_id)
        if cached:
            return SkillContent(
                id=cached.id, name=cached.name, description=cached.description,
                version=cached.version, enabled=cached.enabled,
                user_invocable=cached.user_invocable,
                model_invocable=cached.model_invocable,
                allowed_tools=cached.allowed_tools,
                dependencies=list(cached.dependencies),
                dependency_source=cached.dependency_source,
                dependency_errors=list(cached.dependency_errors),
                has_references=cached.has_references,
                has_scripts=cached.has_scripts,
                has_assets=cached.has_assets,
                metadata=cached.metadata,
                schema_source=cached.schema_source,
                diagnostics=list(cached.diagnostics),
                created_at=cached.created_at,
                updated_at=cached.updated_at,
                skill_revision=skill_revision(raw),
                git_commit=cached.git_commit,
                content=_parse_skill_md_strict(raw)[1],
                raw=raw,
            )

        # Fallback is intentionally the same validator used by discovery.
        try:
            package = validate_skill_package(raw, skill_id, skill_dir=skill_dir)
        except SkillValidationError as exc:
            self._diagnostics[skill_id] = exc.issues
            return None
        db_row = await self.db.get_skill_row(skill_id)
        meta = self._meta_from_package(
            package, enabled=db_row["enabled"] if db_row else True, skill_dir=skill_dir, raw=raw,
        )
        self._cache[skill_id] = meta
        self._diagnostics[skill_id] = meta.diagnostics
        return SkillContent(**meta.__dict__, content=package.body, raw=raw)

    async def create_skill(
        self,
        name: str,
        description: str,
        content: str = "",
        version: str = "1.0.0",
    ) -> SkillMeta:
        """Create a new skill directory + SKILL.md and index in DB."""
        ensure_not_locked("create a skill")
        skill_id = _slugify(name)
        skill_dir = self.skills_dir / _skill_id(skill_id)

        # Creation always emits canonical packages; validate the complete
        # replacement before making a directory or touching the database.
        raw = _build_skill_md(skill_id, description, content, version)
        package = validate_skill_package(raw, skill_id, allow_legacy=False)
        if skill_dir.exists() or await self.db.get_skill_row(skill_id):
            raise FileExistsError(f"Skill already exists: {skill_id}")

        def _write_skill() -> None:
            self.skills_dir.mkdir(parents=True, exist_ok=True)
            skill_dir.mkdir()
            temp = skill_dir / f".SKILL.md.{uuid.uuid4().hex}.tmp"
            try:
                temp.write_text(raw, encoding="utf-8")
                os.replace(temp, skill_dir / "SKILL.md")
            except BaseException:
                temp.unlink(missing_ok=True)
                skill_dir.rmdir()
                raise

        await asyncio.to_thread(_write_skill)
        meta = self._meta_from_package(package, enabled=True, skill_dir=skill_dir, raw=raw)
        await self.db.upsert_skill(
            skill_id=meta.id, name=meta.name, description=meta.description,
            version=meta.version, user_invocable=meta.user_invocable,
            model_invocable=meta.model_invocable, allowed_tools=meta.allowed_tools,
            metadata=meta.metadata,
        )
        self._cache[skill_id] = meta
        self._diagnostics[skill_id] = meta.diagnostics
        return meta

    async def update_skill(
        self,
        skill_id: str,
        content: str,
        *,
        expected_skill_revision: str,
        clear_amendments: bool = False,
        amendments_revision: str = "",
    ) -> SkillMeta | None:
        """Update SKILL.md content (full raw file), re-parse frontmatter, sync DB."""
        ensure_not_locked("update a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        if not expected_skill_revision:
            raise ValueError("expected_skill_revision is required when replacing a skill")
        # Validate in memory before either SKILL.md or the DB can change.
        package = validate_skill_package(content, skill_id, skill_dir=skill_dir,
                                         allow_legacy=True)

        lock = self._amendment_locks.setdefault(skill_id, asyncio.Lock())
        async with lock:
            await asyncio.to_thread(self._recover_update, skill_dir)
            current_content = (await asyncio.to_thread(skill_md.read_bytes)).decode("utf-8")
            current_skill_revision = skill_revision(current_content)
            if current_skill_revision != expected_skill_revision:
                raise SkillUpdateConflict(
                    "stale_skill_revision",
                    "SKILL.md changed after the replacement was prepared; reload and retry",
                )
            if clear_amendments:
                current_revision = await self.amendments_revision(skill_id)
                if not amendments_revision:
                    raise ValueError(
                        "amendments_revision is required when clear_amendments is true"
                    )
                if current_revision != amendments_revision:
                    raise SkillUpdateConflict(
                        "stale_amendments_revision",
                        "pending amendments changed after the consolidated revision was "
                        "prepared; reload the skill and consolidate the new revision"
                    )

            if content == current_content:
                current = await self.get_skill(skill_id)
                if current is None:
                    return None
                current.update_outcome = "no_op"
                return current

            current_package = validate_skill_package(
                current_content, skill_id, skill_dir=skill_dir, allow_legacy=True,
            )
            candidate_key = _semver_key(package.version)
            current_key = _semver_key(current_package.version)
            if candidate_key <= current_key:
                raise ValueError(
                    f"replacement version must increase monotonically: "
                    f"{package.version!r} is not greater than {current_package.version!r}"
                )

            amendments = skill_dir / "references" / AMENDMENTS_REFERENCE
            def _replace() -> None:
                temp = skill_dir / f".SKILL.md.{uuid.uuid4().hex}.tmp"
                journal_temp = skill_dir / f".{_UPDATE_JOURNAL}.{uuid.uuid4().hex}.tmp"
                old_skill = skill_dir / _UPDATE_OLD_SKILL
                old_amendments = skill_dir / _UPDATE_OLD_AMENDMENTS
                journal = skill_dir / _UPDATE_JOURNAL
                def durable_write(path: Path, value: bytes) -> None:
                    with path.open("wb") as handle:
                        handle.write(value)
                        handle.flush()
                        os.fsync(handle.fileno())
                try:
                    durable_write(temp, content.encode("utf-8"))
                    durable_write(old_skill, current_content.encode("utf-8"))
                    had_amendments = clear_amendments and amendments.exists()
                    if had_amendments:
                        durable_write(old_amendments, amendments.read_bytes())
                    durable_write(
                        journal_temp,
                        json.dumps({"had_amendments": had_amendments}).encode("utf-8"),
                    )
                    os.replace(journal_temp, journal)
                    os.replace(temp, skill_md)
                    if clear_amendments:
                        amendments.unlink(missing_ok=True)
                    directory_fd = os.open(skill_dir, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                finally:
                    temp.unlink(missing_ok=True)
                    journal_temp.unlink(missing_ok=True)

            try:
                await asyncio.to_thread(_replace)
            except BaseException:
                await asyncio.to_thread(self._recover_update, skill_dir)
                raise
            existing = await self.db.get_skill_row(skill_id)
            meta = self._meta_from_package(
                package, enabled=existing["enabled"] if existing else True,
                skill_dir=skill_dir, raw=content,
            )
            try:
                await self.db.upsert_skill(
                    skill_id=meta.id, name=meta.name, description=meta.description,
                    version=meta.version, enabled=meta.enabled,
                    user_invocable=meta.user_invocable, model_invocable=meta.model_invocable,
                    allowed_tools=meta.allowed_tools, metadata=meta.metadata,
                )
            except BaseException:
                await asyncio.to_thread(self._recover_update, skill_dir)
                # A database adapter may report failure after committing. Best-effort
                # reindexing restores the old filesystem truth in that case; startup
                # discovery is the durable fallback if the database is unavailable.
                try:
                    old_package = validate_skill_package(
                        current_content, skill_id, skill_dir=skill_dir,
                        allow_legacy=True,
                    )
                    old_meta = self._meta_from_package(
                        old_package, enabled=existing["enabled"] if existing else True,
                        skill_dir=skill_dir, raw=current_content,
                    )
                    await self.db.upsert_skill(
                        skill_id=old_meta.id, name=old_meta.name,
                        description=old_meta.description, version=old_meta.version,
                        enabled=old_meta.enabled,
                        user_invocable=old_meta.user_invocable,
                        model_invocable=old_meta.model_invocable,
                        allowed_tools=old_meta.allowed_tools, metadata=old_meta.metadata,
                    )
                except Exception:
                    logger.exception("Failed to reindex rolled-back skill %s", skill_id)
                raise
            for path in (_UPDATE_OLD_SKILL, _UPDATE_OLD_AMENDMENTS, _UPDATE_JOURNAL):
                await asyncio.to_thread((skill_dir / path).unlink, missing_ok=True)
            meta.update_outcome = "updated"
            self._cache[skill_id] = meta
            self._diagnostics[skill_id] = meta.diagnostics
            return meta

    async def delete_skill(self, skill_id: str) -> bool:
        """Remove skill directory and DB record."""
        ensure_not_locked("delete a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        if skill_dir.exists():
            await asyncio.to_thread(shutil.rmtree, skill_dir)
        await self.db.delete_skill_row(skill_id)
        self._cache.pop(skill_id, None)
        self._diagnostics.pop(skill_id, None)
        return True

    async def toggle_skill(self, skill_id: str, enabled: bool) -> bool:
        """Enable or disable a skill."""
        ensure_not_locked("toggle a skill")
        existing = await self.db.get_skill_row(skill_id)
        if not existing:
            return False
        await self.db.update_skill_enabled(skill_id, enabled)
        if skill_id in self._cache:
            self._cache[skill_id].enabled = enabled
        return True

    async def list_references(self, skill_id: str) -> list[str]:
        """List reference files in a skill's references/ directory."""
        refs_dir = self.skills_dir / _skill_id(skill_id) / "references"
        if not refs_dir.is_dir():
            return []

        def _walk() -> list[str]:
            return sorted(
                str(f.relative_to(refs_dir))
                for f in refs_dir.rglob("*")
                if f.is_file()
            )

        return await asyncio.to_thread(_walk)

    async def read_amendments(self, skill_id: str) -> str | None:
        """Return pending amendments for a skill, excluding an empty header."""
        path = self.skills_dir / _skill_id(skill_id) / "references" / AMENDMENTS_REFERENCE
        if not path.is_file():
            return None
        content = await asyncio.to_thread(path.read_text, encoding="utf-8")
        if not content.strip() or content.strip() == AMENDMENTS_HEADER.strip():
            return None
        return content.strip()

    async def amendments_revision(self, skill_id: str) -> str:
        """Content revision used to make consolidation refuse stale snapshots."""
        content = await self.read_amendments(skill_id)
        if content is None:
            return ""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    async def append_amendment(
        self,
        skill_id: str,
        *,
        title: str,
        observation: str,
        change: str,
        evidence: list[str] | None = None,
        session_id: str = "",
    ) -> tuple[str, str]:
        """Append one structured, human-readable amendment to a skill."""
        ensure_not_locked("append a skill amendment")
        skill = await self.get_skill(skill_id)
        if skill is None:
            raise FileNotFoundError(f"Skill not found: {skill_id}")

        title = " ".join(title.strip().splitlines())
        observation = observation.strip()
        change = change.strip()
        if not title or not observation or not change:
            raise ValueError("title, observation, and change are required")

        amendment_id = str(uuid.uuid4())[:8]
        created_at = datetime.now(timezone.utc).isoformat()
        lines = [
            "",
            f"## {created_at} — {title}",
            "",
            f"- ID: `{amendment_id}`",
            f"- Base version: `{skill.version}`",
        ]
        if session_id:
            lines.append(f"- Session: `{session_id}`")
        lines.extend(["", "### Observation", "", observation, "", "### Change", "", change])
        clean_evidence = [str(item).strip() for item in (evidence or []) if str(item).strip()]
        if clean_evidence:
            lines.extend(["", "### Evidence", ""])
            lines.extend(f"- {item}" for item in clean_evidence)
        block = "\n".join(lines).rstrip() + "\n"

        lock = self._amendment_locks.setdefault(skill_id, asyncio.Lock())
        async with lock:
            path = self.skills_dir / skill_id / "references" / AMENDMENTS_REFERENCE

            def _append() -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                if not path.exists() or not path.read_text(encoding="utf-8").strip():
                    path.write_text(AMENDMENTS_HEADER, encoding="utf-8")
                with path.open("a", encoding="utf-8") as f:
                    f.write(block)

            await asyncio.to_thread(_append)

        return amendment_id, await self.amendments_revision(skill_id)

    async def resolve_required_dependencies(
        self,
        skill_id: str,
        *,
        max_depth: int = MAX_DEPENDENCY_DEPTH,
        max_dependencies: int = MAX_DEPENDENCIES,
        for_model: bool = True,
    ) -> SkillDependencyResolution:
        """Resolve a fail-closed, dependency-first instruction bundle.

        Required edges are traversed in lexical skill-ID order. Each skill is
        emitted once after its own requirements, giving a stable topological
        order for chains and diamonds. Suggested edges are collected only after
        successful required resolution and are never traversed.
        """
        root = _skill_id(skill_id)
        resolved: list[SkillContent] = []
        errors: list[SkillDependencyIssue] = []
        visited: set[str] = set()
        visiting: list[str] = []
        counted_dependencies: set[str] = set()

        async def _visit(current_id: str, depth: int, required_by: str = "") -> None:
            if current_id in visiting:
                cycle_start = visiting.index(current_id)
                cycle_path = (*visiting[cycle_start:], current_id)
                errors.append(SkillDependencyIssue(
                    code="dependency_cycle",
                    message=f"required dependency cycle: {' -> '.join(cycle_path)}",
                    skill=required_by or current_id,
                    dependency=current_id,
                    path=cycle_path,
                ))
                return
            if depth > max_depth:
                path = (*visiting, current_id)
                errors.append(SkillDependencyIssue(
                    code="dependency_depth_limit",
                    message=(
                        f"required dependency depth exceeds {max_depth} at {current_id!r}"
                    ),
                    skill=required_by or root,
                    dependency=current_id,
                    path=path,
                ))
                return
            if current_id in visited:
                return
            if current_id != root:
                if current_id not in counted_dependencies:
                    if len(counted_dependencies) >= max_dependencies:
                        errors.append(SkillDependencyIssue(
                            code="dependency_count_limit",
                            message=(
                                "required dependency count exceeds "
                                f"{max_dependencies} at {current_id!r}"
                            ),
                            skill=required_by or root,
                            dependency=current_id,
                            path=(*visiting, current_id),
                        ))
                        return
                    counted_dependencies.add(current_id)

            skill = await self.get_skill(current_id)
            if skill is None:
                errors.append(SkillDependencyIssue(
                    code="missing_required_dependency",
                    message=f"required skill {current_id!r} was not found",
                    skill=required_by or root,
                    dependency=current_id,
                    path=(*visiting, current_id),
                ))
                return
            errors.extend(skill.dependency_errors)
            if not skill.enabled:
                errors.append(SkillDependencyIssue(
                    code=("root_skill_disabled" if current_id == root else "required_dependency_disabled"),
                    message=(
                        f"skill {current_id!r} is disabled"
                        if current_id == root
                        else f"required skill {current_id!r} is disabled"
                    ),
                    skill=required_by or current_id,
                    dependency="" if current_id == root else current_id,
                    path=(*visiting, current_id),
                ))
            if for_model and not skill.model_invocable:
                errors.append(SkillDependencyIssue(
                    code=(
                        "root_skill_not_model_invocable"
                        if current_id == root else "required_dependency_not_model_invocable"
                    ),
                    message=(
                        f"skill {current_id!r} is not model-invocable"
                        if current_id == root
                        else f"required skill {current_id!r} is not model-invocable"
                    ),
                    skill=required_by or current_id,
                    dependency="" if current_id == root else current_id,
                    path=(*visiting, current_id),
                ))

            visiting.append(current_id)
            for dependency in sorted(
                (item for item in skill.dependencies if item.mode == "required"),
                key=lambda item: item.skill,
            ):
                await _visit(dependency.skill, depth + 1, current_id)
            visiting.pop()
            visited.add(current_id)
            resolved.append(skill)

        await _visit(root, 0)
        if errors:
            return SkillDependencyResolution(
                root=root,
                errors=errors,
                max_depth=max_depth,
                max_dependencies=max_dependencies,
            )

        suggestions: list[SuggestedSkillDependency] = []
        seen_suggestions: set[str] = set()
        for skill in resolved:
            for dependency in sorted(
                (item for item in skill.dependencies if item.mode == "suggested"),
                key=lambda item: item.skill,
            ):
                if dependency.skill in seen_suggestions:
                    continue
                seen_suggestions.add(dependency.skill)
                suggestions.append(SuggestedSkillDependency(
                    skill=dependency.skill,
                    when=dependency.when,
                    declared_by=skill.id,
                ))

        return SkillDependencyResolution(
            root=root,
            bundle=resolved,
            suggested=suggestions,
            max_depth=max_depth,
            max_dependencies=max_dependencies,
        )

    async def read_reference(self, skill_id: str, rel_path: str) -> str | None:
        """Read a reference file from a skill."""
        ref_file = self.skills_dir / _skill_id(skill_id) / "references" / rel_path
        # Prevent path traversal
        try:
            ref_file.resolve().relative_to((self.skills_dir / skill_id).resolve())
        except ValueError:
            return None
        if not ref_file.exists() or not ref_file.is_file():
            return None
        return await asyncio.to_thread(ref_file.read_text, encoding="utf-8")

    async def run_script(self, skill_id: str, rel_path: str, args: str = "") -> str:
        """Execute a script from a skill's scripts/ directory."""
        script_file = self.skills_dir / _skill_id(skill_id) / "scripts" / rel_path
        # Prevent path traversal
        try:
            script_file.resolve().relative_to((self.skills_dir / skill_id).resolve())
        except ValueError:
            return "Error: path traversal detected"
        if not script_file.exists() or not script_file.is_file():
            return f"Error: script not found: {rel_path}"

        # Detect interpreter
        suffix = script_file.suffix.lower()
        if suffix == ".py":
            cmd = ["python3", str(script_file)]
        elif suffix in (".sh", ".bash"):
            cmd = ["bash", str(script_file)]
        else:
            cmd = [str(script_file)]

        if args:
            cmd.extend(args.split())

        try:
            result = await asyncio.to_thread(
                subprocess.run,
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(self.skills_dir / _skill_id(skill_id)),
            )
            output = result.stdout
            if result.returncode != 0:
                output += f"\nSTDERR: {result.stderr}\nExit code: {result.returncode}"
            return output
        except subprocess.TimeoutExpired:
            return "Error: script timed out (30s)"
        except Exception as e:
            return f"Error running script: {e}"

    async def get_enabled_summaries(self) -> list[dict]:
        """Return name+description for all enabled model-invocable skills.

        Used for system prompt injection (progressive disclosure level 1).
        """
        db_skills = await self.db.list_skills()
        summaries = []
        for s in db_skills:
            if self._discovered_once and s["id"] not in self._cache:
                continue
            if s["enabled"] and s["model_invocable"]:
                summaries.append({
                    "id": s["id"],
                    "name": s["name"],
                    "description": s["description"],
                })
        return summaries

    async def record_usage(
        self,
        skill_id: str,
        session_id: str | None = None,
        invoked_by: str = "model",
        duration_ms: int | None = None,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        """Log a skill invocation for statistics."""
        await self.db.record_skill_usage(
            skill_id=skill_id,
            session_id=session_id,
            invoked_by=invoked_by,
            duration_ms=duration_ms,
            success=success,
            error=error,
        )
