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
import logging
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


@dataclass(frozen=True)
class SkillDependency:
    """One skill-to-skill dependency declared in SKILL.md frontmatter."""

    skill: str
    mode: str = "required"
    when: str = ""


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
    has_references: bool = False
    has_scripts: bool = False
    has_assets: bool = False
    metadata: dict = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""


@dataclass
class SkillContent(SkillMeta):
    """Full skill including SKILL.md body content."""
    content: str = ""   # Markdown body (after frontmatter)
    raw: str = ""       # Full SKILL.md file


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


def _parse_dependencies(raw: Any) -> list[SkillDependency]:
    """Normalize the small dependency syntax accepted in skill frontmatter.

    The preferred form keeps hard and advisory edges visibly separate::

        dependencies:
          requires: [python]
          suggests:
            - skill: django
              when: The repository uses Django

    A flat list with an explicit ``mode`` is accepted as a convenience. Invalid
    entries are ignored so one bad advisory edge cannot hide the skill itself.
    """

    groups: list[tuple[str, Any]] = []
    if isinstance(raw, dict):
        groups.extend(("required", raw.get(key)) for key in ("requires", "required"))
        groups.extend(("suggested", raw.get(key)) for key in ("suggests", "suggested"))
    elif isinstance(raw, list):
        groups.append(("required", raw))
    elif raw:
        groups.append(("required", [raw]))

    dependencies: list[SkillDependency] = []
    seen: set[tuple[str, str]] = set()
    for default_mode, items in groups:
        if items is None:
            continue
        if not isinstance(items, list):
            items = [items]
        for item in items:
            when = ""
            mode = default_mode
            if isinstance(item, str):
                skill_id = item.strip()
            elif isinstance(item, dict):
                skill_id = str(item.get("skill") or item.get("name") or "").strip()
                mode = str(item.get("mode") or default_mode).strip().lower()
                when = str(item.get("when") or "").strip()
            else:
                continue
            if mode in {"require", "requires"}:
                mode = "required"
            if mode in {"suggest", "suggests", "recommend", "recommended"}:
                mode = "suggested"
            if mode not in {"required", "suggested"}:
                continue
            try:
                skill_id = _skill_id(skill_id)
            except SkillIdError:
                continue
            key = (skill_id, mode)
            if key in seen:
                continue
            seen.add(key)
            dependencies.append(SkillDependency(skill=skill_id, mode=mode, when=when))
    return dependencies


def _build_skill_md(name: str, description: str, body: str = "", version: str = "1.0.0", **extra) -> str:
    """Build a SKILL.md file from components."""
    fm: dict[str, Any] = {"name": name, "description": description, "version": version}
    fm.update(extra)
    yaml_str = yaml.dump(fm, default_flow_style=False, allow_unicode=True).strip()
    parts = [f"---\n{yaml_str}\n---"]
    if body:
        parts.append(body)
    return "\n\n".join(parts) + "\n"


class SkillManager:
    """Discovers, loads, and manages skills from the filesystem."""

    def __init__(self, workspace: Path, db: Database):
        self.skills_dir = workspace / "skills"
        self.db = db
        self._cache: dict[str, SkillMeta] = {}
        self._amendment_locks: dict[str, asyncio.Lock] = {}

    async def discover(self) -> list[SkillMeta]:
        """Scan skills_dir for SKILL.md files, parse frontmatter, sync to DB.

        Returns all discovered skills. Also removes DB entries for skills
        that no longer exist on the filesystem.
        """
        await asyncio.to_thread(self.skills_dir.mkdir, parents=True, exist_ok=True)
        discovered: list[SkillMeta] = []
        found_ids: set[str] = set()

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
                    out.append((sdir, smd.read_text(encoding="utf-8")))
                except OSError as e:
                    logger.error("Failed to read skill %s: %s", sdir.name, e)
            return out

        for skill_dir, raw in await asyncio.to_thread(_scan_skill_dirs):
            skill_id = skill_dir.name
            found_ids.add(skill_id)

            try:
                fm, body = _parse_skill_md(raw)

                name = fm.get("name", skill_id)
                description = fm.get("description", "")
                if not description:
                    # Use first non-empty line of body as fallback
                    for line in body.split("\n"):
                        line = line.strip()
                        if line and not line.startswith("#"):
                            description = line[:200]
                            break

                version = fm.get("version", "1.0.0")
                dependencies = _parse_dependencies(fm.get("dependencies"))
                user_invocable = fm.get("user-invocable", True)
                model_invocable = not fm.get("disable-model-invocation", False)
                allowed_tools_raw = fm.get("allowed-tools")
                allowed_tools = None
                if allowed_tools_raw:
                    if isinstance(allowed_tools_raw, str):
                        allowed_tools = [t.strip() for t in allowed_tools_raw.split(",")]
                    elif isinstance(allowed_tools_raw, list):
                        allowed_tools = allowed_tools_raw

                # Check for optional subdirectories
                has_references = (skill_dir / "references").is_dir()
                has_scripts = (skill_dir / "scripts").is_dir()
                has_assets = (skill_dir / "assets").is_dir()

                # Extra metadata (everything not in known fields)
                known_keys = {"name", "description", "version", "user-invocable",
                              "disable-model-invocation", "allowed-tools", "license",
                              "argument-hint", "context", "agent"}
                extra_meta = {k: v for k, v in fm.items() if k not in known_keys}

                meta = SkillMeta(
                    id=skill_id,
                    name=name,
                    description=description,
                    version=str(version),
                    user_invocable=user_invocable,
                    model_invocable=model_invocable,
                    allowed_tools=allowed_tools,
                    dependencies=dependencies,
                    has_references=has_references,
                    has_scripts=has_scripts,
                    has_assets=has_assets,
                    metadata=extra_meta,
                )
                discovered.append(meta)
                self._cache[skill_id] = meta

                # Check if DB has the skill and preserve its enabled state
                existing = await self.db.get_skill_row(skill_id)
                enabled = existing["enabled"] if existing else True

                # Sync to DB
                await self.db.upsert_skill(
                    skill_id=skill_id,
                    name=name,
                    description=description,
                    version=str(version),
                    enabled=enabled,
                    user_invocable=user_invocable,
                    model_invocable=model_invocable,
                    allowed_tools=allowed_tools,
                    metadata=extra_meta,
                )

            except Exception as e:
                logger.error("Failed to load skill %s: %s", skill_id, e)

        # Clean up DB entries for skills that no longer exist on filesystem
        db_skills = await self.db.list_skills()
        for db_skill in db_skills:
            if db_skill["id"] not in found_ids:
                logger.info("Removing stale skill from DB: %s", db_skill["id"])
                await self.db.delete_skill_row(db_skill["id"])

        logger.info("Discovered %d skills", len(discovered))
        return discovered

    async def get_skill(self, skill_id: str) -> SkillContent | None:
        """Load full SKILL.md content + metadata for a skill."""
        skill_dir = self.skills_dir / _skill_id(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        raw = await asyncio.to_thread(skill_md.read_text, encoding="utf-8")
        fm, body = _parse_skill_md(raw)

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
                has_references=cached.has_references,
                has_scripts=cached.has_scripts,
                has_assets=cached.has_assets,
                metadata=cached.metadata,
                created_at=cached.created_at,
                updated_at=cached.updated_at,
                content=body,
                raw=raw,
            )

        # Fallback: parse from file
        name = fm.get("name", skill_id)
        description = fm.get("description", "")
        return SkillContent(
            id=skill_id, name=name, description=description,
            version=fm.get("version", "1.0.0"),
            dependencies=_parse_dependencies(fm.get("dependencies")),
            content=body, raw=raw,
            has_references=(skill_dir / "references").is_dir(),
            has_scripts=(skill_dir / "scripts").is_dir(),
            has_assets=(skill_dir / "assets").is_dir(),
        )

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

        # Build SKILL.md
        raw = _build_skill_md(name, description, content, version)

        def _write_skill() -> None:
            skill_dir.mkdir(parents=True, exist_ok=True)
            (skill_dir / "SKILL.md").write_text(raw, encoding="utf-8")

        await asyncio.to_thread(_write_skill)

        # Sync to DB
        await self.db.upsert_skill(
            skill_id=skill_id, name=name, description=description,
            version=version,
        )

        meta = SkillMeta(
            id=skill_id, name=name, description=description,
            version=version,
        )
        self._cache[skill_id] = meta
        return meta

    async def update_skill(
        self,
        skill_id: str,
        content: str,
        *,
        clear_amendments: bool = False,
        amendments_revision: str = "",
    ) -> SkillMeta | None:
        """Update SKILL.md content (full raw file), re-parse frontmatter, sync DB."""
        ensure_not_locked("update a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            return None

        lock = self._amendment_locks.setdefault(skill_id, asyncio.Lock())
        async with lock:
            if clear_amendments:
                current_revision = await self.amendments_revision(skill_id)
                if not amendments_revision:
                    raise ValueError(
                        "amendments_revision is required when clear_amendments is true"
                    )
                if current_revision != amendments_revision:
                    raise ValueError(
                        "pending amendments changed after the consolidated revision was "
                        "prepared; reload the skill and consolidate the new revision"
                    )

            await asyncio.to_thread(skill_md.write_text, content, encoding="utf-8")

            if clear_amendments:
                amendments = skill_dir / "references" / AMENDMENTS_REFERENCE
                if amendments.exists():
                    await asyncio.to_thread(amendments.unlink)

        # Re-parse and sync
        fm, body = _parse_skill_md(content)
        name = fm.get("name", skill_id)
        description = fm.get("description", "")
        version = fm.get("version", "1.0.0")
        dependencies = _parse_dependencies(fm.get("dependencies"))
        allowed_tools_raw = fm.get("allowed-tools")
        allowed_tools = None
        if isinstance(allowed_tools_raw, str):
            allowed_tools = [t.strip() for t in allowed_tools_raw.split(",") if t.strip()]
        elif isinstance(allowed_tools_raw, list):
            allowed_tools = allowed_tools_raw

        known_keys = {"name", "description", "version", "user-invocable",
                      "disable-model-invocation", "allowed-tools", "license",
                      "argument-hint", "context", "agent"}
        extra_meta = {k: v for k, v in fm.items() if k not in known_keys}

        await self.db.upsert_skill(
            skill_id=skill_id, name=name, description=description,
            version=str(version),
            user_invocable=fm.get("user-invocable", True),
            model_invocable=not fm.get("disable-model-invocation", False),
            allowed_tools=allowed_tools,
            metadata=extra_meta,
        )

        meta = SkillMeta(
            id=skill_id, name=name, description=description,
            version=str(version),
            user_invocable=fm.get("user-invocable", True),
            model_invocable=not fm.get("disable-model-invocation", False),
            allowed_tools=allowed_tools,
            dependencies=dependencies,
            has_references=(skill_dir / "references").is_dir(),
            has_scripts=(skill_dir / "scripts").is_dir(),
            has_assets=(skill_dir / "assets").is_dir(),
            metadata=extra_meta,
        )
        self._cache[skill_id] = meta
        return meta

    async def delete_skill(self, skill_id: str) -> bool:
        """Remove skill directory and DB record."""
        ensure_not_locked("delete a skill")
        skill_dir = self.skills_dir / _skill_id(skill_id)
        if skill_dir.exists():
            await asyncio.to_thread(shutil.rmtree, skill_dir)
        await self.db.delete_skill_row(skill_id)
        self._cache.pop(skill_id, None)
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
    ) -> tuple[list[SkillContent], list[str]]:
        """Return required skills in dependency-first order plus warnings."""
        root = _skill_id(skill_id)
        resolved: list[SkillContent] = []
        warnings: list[str] = []
        visited: set[str] = set()
        visiting: list[str] = []

        async def _visit(current_id: str, depth: int) -> None:
            if current_id in visited:
                return
            if current_id in visiting:
                cycle = " -> ".join([*visiting, current_id])
                warnings.append(f"Dependency cycle ignored: {cycle}")
                return
            if depth > max_depth:
                warnings.append(
                    f"Dependency depth limit ({max_depth}) reached at {current_id}"
                )
                return
            if len(visited) >= MAX_DEPENDENCIES:
                warnings.append(
                    f"Dependency count limit ({MAX_DEPENDENCIES}) reached"
                )
                return

            skill = await self.get_skill(current_id)
            if skill is None:
                warnings.append(f"Required skill not found: {current_id}")
                return

            visiting.append(current_id)
            for dependency in skill.dependencies:
                if dependency.mode == "required":
                    await _visit(dependency.skill, depth + 1)
            visiting.pop()
            visited.add(current_id)
            if current_id != root:
                resolved.append(skill)

        await _visit(root, 0)
        return resolved, warnings

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
