from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import nerve.executions.ydb as ydb
from nerve.executions.remote_supervisor import _sync
from nerve.executions.ydb import YdbWorktreeError, snapshot, validate_worktree


def _git(path, *args, input=None):
    return subprocess.run(["git", "-C", str(path), *args], check=True, input=input,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def _repo(tmp_path):
    repo = tmp_path / "tree"; repo.mkdir()
    _git(repo, "init"); _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test")
    (repo / "tracked").write_text("base")
    _git(repo, "add", "tracked"); _git(repo, "commit", "-m", "base")
    return repo


def test_snapshot_is_deterministic_and_leaves_real_index_and_status_unchanged(tmp_path):
    repo = _repo(tmp_path)
    (repo / "tracked").write_text("changed"); (repo / "untracked").write_text("extra")
    _git(repo, "add", "tracked")
    before_status = _git(repo, "status", "--porcelain=v1").stdout
    before_index = (repo / ".git" / "index").read_bytes()
    first = snapshot(validate_worktree(str(repo), tmp_path)); second = snapshot(repo)
    assert first == second
    assert first["head"] == _git(repo, "rev-parse", "HEAD").stdout.decode().strip()
    assert _git(repo, "status", "--porcelain=v1").stdout == before_status
    assert (repo / ".git" / "index").read_bytes() == before_index


def test_snapshot_pack_is_delta_only_not_repository_history(tmp_path):
    repo = _repo(tmp_path)
    for number in range(8):
        (repo / "history").write_text("x" * 10000 + str(number))
        _git(repo, "add", "history"); _git(repo, "commit", "-m", str(number))
    (repo / "tracked").write_text("changed")
    result = snapshot(repo)
    pack = result["pack"]
    assert isinstance(pack, bytes)
    assert len(pack) < 2000
    cache = tmp_path / "cache"; _git(repo, "clone", "--bare", str(repo), str(cache))
    _git(cache, "index-pack", "--stdin", "--fix-thin", input=pack)
    commits = _git(cache, "rev-list", result["snapshot_id"], "--not", result["head"]).stdout.splitlines()
    assert commits == [result["snapshot_id"].encode()]


def test_clean_snapshot_skips_full_temporary_index_refresh(tmp_path, monkeypatch):
    repo = _repo(tmp_path)
    calls = []
    real_git = ydb._git

    def recording_git(worktree, *argv, **kwargs):
        calls.append(argv)
        return real_git(worktree, *argv, **kwargs)

    monkeypatch.setattr(ydb, "_git", recording_git)
    result = snapshot(repo)
    assert result["pack"]
    assert ("add", "-A") not in calls


def test_snapshot_uses_common_object_store_for_linked_worktree(tmp_path):
    repo = _repo(tmp_path)
    linked = tmp_path / "linked"
    _git(repo, "worktree", "add", "-b", "linked-test", str(linked))
    result = snapshot(linked)
    assert result["head"] == _git(linked, "rev-parse", "HEAD").stdout.decode().strip()
    assert result["pack"]


def test_remote_materializes_snapshot_from_preseeded_cache_and_preserves_ignored(tmp_path):
    repo = _repo(tmp_path); (repo / ".gitignore").write_text("cache/\n")
    _git(repo, "add", ".gitignore"); _git(repo, "commit", "-m", "ignore")
    (repo / "tracked").write_text("changed"); (repo / "new").write_text("new")
    result = snapshot(repo); root = tmp_path / "remote"
    cache = root / ".nerve-ydb-object-cache"
    cache.parent.mkdir(); _git(repo, "clone", "--bare", str(repo), str(cache))
    pack = result.pop("pack")
    request = {"root": str(root), "session_id": "session-1", "fencing_token": 1, "snapshot": result}
    reply = _sync(request, pack); tree = Path(reply["workspace"])
    assert (tree / "tracked").read_text() == "changed" and (tree / "new").read_text() == "new"
    (tree / "cache").mkdir(); (tree / "cache" / "saved").write_text("keep")
    (repo / "new").unlink(); (repo / "tracked").write_text("again")
    next_snapshot = snapshot(repo); next_pack = next_snapshot.pop("pack")
    reply = _sync({**request, "fencing_token": 2, "snapshot": next_snapshot}, next_pack)
    tree = Path(reply["workspace"])
    assert not (tree / "new").exists() and (tree / "cache" / "saved").read_text() == "keep"


def test_remote_missing_base_cache_fails_without_full_history_fallback(tmp_path):
    repo = _repo(tmp_path); result = snapshot(repo); root = tmp_path / "remote"
    with pytest.raises(ValueError, match="object cache lacks requested base HEAD"):
        _sync({"root": str(root), "session_id": "session-1", "fencing_token": 1, "snapshot": result}, result["pack"])


def test_worktree_must_be_allowlisted_git_top_level(tmp_path):
    repo = _repo(tmp_path); (repo / "child").mkdir()
    elsewhere = tmp_path / "elsewhere"; elsewhere.mkdir()
    with pytest.raises(YdbWorktreeError, match="top-level"):
        validate_worktree(str(repo / "child"), tmp_path)
    with pytest.raises(YdbWorktreeError, match="outside"):
        validate_worktree(str(repo), elsewhere)
