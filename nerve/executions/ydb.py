"""Pinned, shell-free YDB worktree snapshots for the reviewed builder pool."""
from __future__ import annotations

import base64
import os
import subprocess
import tempfile
from pathlib import Path


class YdbWorktreeError(ValueError):
    pass


def _git(worktree: Path, *argv: str, input: bytes | None = None,
         env: dict[str, str] | None = None) -> bytes:
    try:
        merged = os.environ.copy()
        if env:
            merged.update(env)
        return subprocess.run(["git", "-C", str(worktree), *argv], check=True,
                              input=input, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, env=merged).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise YdbWorktreeError("worktree is not a usable Git checkout") from exc


def validate_worktree(value: str, allowed_root: Path | None) -> Path:
    if allowed_root is None:
        raise YdbWorktreeError("YDB worktree root is not configured")
    try:
        path = Path(value).expanduser().resolve(strict=True)
        root = allowed_root.expanduser().resolve(strict=True)
    except OSError as exc:
        raise YdbWorktreeError("worktree or configured YDB root does not exist") from exc
    if path != root and root not in path.parents:
        raise YdbWorktreeError("worktree is outside the configured YDB worktree root")
    top = Path(_git(path, "rev-parse", "--show-toplevel").decode().strip()).resolve(strict=True)
    if top != path:
        raise YdbWorktreeError("worktree must be the Git top-level")
    return top


def snapshot(worktree: Path) -> dict[str, str]:
    """Create a deterministic, unreferenced commit and a thin delta pack.

    All objects and the index used to construct the commit live in a temporary
    object directory.  The real index, refs and worktree are read only.
    """
    head = _git(worktree, "rev-parse", "HEAD").decode().strip()
    git_dir = _git(worktree, "rev-parse", "--git-dir").decode().strip()
    object_dir = (worktree / git_dir / "objects").resolve()
    with tempfile.TemporaryDirectory(prefix="nerve-ydb-") as temporary:
        temp = Path(temporary)
        temp_objects = temp / "objects"; temp_objects.mkdir()
        env = {
            "GIT_INDEX_FILE": str(temp / "index"),
            "GIT_OBJECT_DIRECTORY": str(temp_objects),
            "GIT_ALTERNATE_OBJECT_DIRECTORIES": str(object_dir),
            "GIT_AUTHOR_NAME": "Nerve YDB Snapshot",
            "GIT_AUTHOR_EMAIL": "nerve-ydb@localhost",
            "GIT_COMMITTER_NAME": "Nerve YDB Snapshot",
            "GIT_COMMITTER_EMAIL": "nerve-ydb@localhost",
            "GIT_AUTHOR_DATE": "1970-01-01T00:00:00Z",
            "GIT_COMMITTER_DATE": "1970-01-01T00:00:00Z",
        }
        _git(worktree, "read-tree", head, env=env)
        # -A includes tracked modifications/deletions and untracked files while
        # respecting .gitignore, but writes only the temporary index above.
        _git(worktree, "add", "-A", env=env)
        tree = _git(worktree, "write-tree", env=env).decode().strip()
        commit = _git(worktree, "commit-tree", tree, "-p", head,
                      input=b"Nerve YDB snapshot\n", env=env).decode().strip()
        pack = _git(worktree, "pack-objects", "--thin", "--stdout", "--revs",
                    input=(commit + "\n^" + head + "\n").encode(), env=env)
    return {"head": head, "snapshot_id": commit,
            "pack_b64": base64.b64encode(pack).decode()}
