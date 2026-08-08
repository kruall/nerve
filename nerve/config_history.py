"""Git ownership checks and recoverable history for a Nerve config workspace.

The workspace root is intentionally the repository root.  Only this module's
allowlist is reviewed: runtime state and machine-local configuration must never
be made part of the history merely because they happen to live beside it.
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


REVIEWED_ROOT_FILES = frozenset({"SOUL.md", "IDENTITY.md", "USER.md", "AGENTS.md", "TOOLS.md"})
REVIEWED_TOP_LEVEL_DIRS = frozenset({"config", "skills"})
REVIEWED_REPOSITORY_FILES = frozenset({".gitignore", ".gitleaks.toml", "README.md"})
REVIEWED_REPOSITORY_DIRS = frozenset({".github"})


class ReviewedSurfaceError(ValueError):
    pass


def is_reviewed_path(path: str) -> bool:
    """Whether a repository-relative path is owned by the reviewed surface."""
    candidate = Path(path)
    if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
        return False
    return (len(candidate.parts) == 1 and candidate.name in (REVIEWED_ROOT_FILES | REVIEWED_REPOSITORY_FILES)) or (
        candidate.parts[0] in (REVIEWED_TOP_LEVEL_DIRS | REVIEWED_REPOSITORY_DIRS)
    )


def _git(workspace: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=workspace, capture_output=True,
                          text=True, encoding="utf-8", errors="replace")


def tracked_paths(workspace: Path, revision: str = "HEAD") -> list[str]:
    """Return tracked paths at *revision*, refusing a non-repository workspace."""
    result = _git(workspace, "ls-tree", "-r", "--name-only", revision)
    if result.returncode:
        raise ReviewedSurfaceError(result.stderr.strip() or "workspace is not a git repository")
    return [line for line in result.stdout.splitlines() if line]


def verify_reviewed_surface(workspace: Path, revision: str = "HEAD") -> list[str]:
    """Fail closed if any tracked path is outside the explicit reviewed surface."""
    unsafe = [path for path in tracked_paths(workspace, revision) if not is_reviewed_path(path)]
    if unsafe:
        raise ReviewedSurfaceError(
            "tracked paths outside Nerve's reviewed surface: " + ", ".join(sorted(unsafe))
        )
    return tracked_paths(workspace, revision)


def skill_commit(workspace: Path, skill_id: str) -> str:
    """Commit that last changed a skill package, or ``''`` outside a Git repo."""
    path = f"skills/{skill_id}"
    if not is_reviewed_path(path):
        return ""
    result = _git(workspace, "log", "-1", "--format=%H", "--", path)
    return result.stdout.strip() if result.returncode == 0 else ""


@dataclass(frozen=True)
class RollbackPlan:
    target: str
    current: str
    changed_paths: list[str]


def plan_rollback(workspace: Path, target: str) -> RollbackPlan:
    """Build a read-only rollback plan limited to the reviewed allowlist."""
    verify_reviewed_surface(workspace)
    target_paths = verify_reviewed_surface(workspace, target)
    current = _git(workspace, "rev-parse", "HEAD")
    if current.returncode:
        raise ReviewedSurfaceError("workspace has no HEAD commit")
    # Name-only diff identifies both additions and deletions.  The actual
    # application is deliberately left to the explicit CLI command below.
    reviewed = (*sorted(REVIEWED_TOP_LEVEL_DIRS), *sorted(REVIEWED_REPOSITORY_DIRS),
                *sorted(REVIEWED_ROOT_FILES), *sorted(REVIEWED_REPOSITORY_FILES))
    diff = _git(workspace, "diff", "--name-only", target, "HEAD", "--", *reviewed)
    if diff.returncode:
        raise ReviewedSurfaceError(diff.stderr.strip() or "cannot compare revisions")
    changed = [p for p in diff.stdout.splitlines() if p]
    if any(not is_reviewed_path(p) for p in changed):
        raise ReviewedSurfaceError("rollback diff escapes reviewed surface")
    # Make the target enumeration observable for callers/tests; it also proves
    # that rollback cannot resurrect an unreviewed file from old history.
    del target_paths
    return RollbackPlan(target=target, current=current.stdout.strip(), changed_paths=changed)


def apply_rollback(workspace: Path, target: str) -> RollbackPlan:
    """Apply a reverse reviewed-surface diff to the worktree, without committing.

    The caller must validate and create a new reviewable commit.  Git rejects
    conflicts; the existing checkout is not reset, cleaned, or rewritten.
    """
    plan = plan_rollback(workspace, target)
    if not plan.changed_paths:
        return plan
    diff = _git(workspace, "diff", "--binary", target, "HEAD", "--", *plan.changed_paths)
    applied = subprocess.run(["git", "apply", "--reverse", "--index"], cwd=workspace,
                              input=diff.stdout, capture_output=True, text=True,
                              encoding="utf-8", errors="replace")
    if applied.returncode:
        raise ReviewedSurfaceError(applied.stderr.strip() or "rollback patch could not be applied")
    return plan
