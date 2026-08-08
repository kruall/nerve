import subprocess

import pytest

from nerve.config_history import (ReviewedSurfaceError, apply_rollback, plan_rollback,
                                  skill_commit, verify_reviewed_surface)


def _git(path, *args):
    return subprocess.run(["git", *args], cwd=path, check=True, capture_output=True, text=True)


def _repo(tmp_path):
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "test@example.invalid")
    _git(tmp_path, "config", "user.name", "Test")
    skill = tmp_path / "skills" / "demo"
    skill.mkdir(parents=True)
    (skill / "SKILL.md").write_text("---\nname: demo\ndescription: Demo\n---\n# Demo\n")
    _git(tmp_path, "add", "skills")
    _git(tmp_path, "commit", "-qm", "initial")
    return _git(tmp_path, "rev-parse", "HEAD").stdout.strip()


def test_verified_surface_and_skill_provenance(tmp_path):
    commit = _repo(tmp_path)
    assert verify_reviewed_surface(tmp_path) == ["skills/demo/SKILL.md"]
    assert skill_commit(tmp_path, "demo") == commit


def test_refuses_runtime_state_in_history(tmp_path):
    _repo(tmp_path)
    (tmp_path / "MEMORY.md").write_text("private")
    _git(tmp_path, "add", "MEMORY.md")
    _git(tmp_path, "commit", "-qm", "bad")
    with pytest.raises(ReviewedSurfaceError, match="MEMORY.md"):
        verify_reviewed_surface(tmp_path)


def test_rollback_plan_is_read_only_and_limited(tmp_path):
    initial = _repo(tmp_path)
    path = tmp_path / "skills" / "demo" / "SKILL.md"
    path.write_text("---\nname: demo\ndescription: New\n---\n# New\n")
    _git(tmp_path, "commit", "-am", "change skill")
    plan = plan_rollback(tmp_path, initial)
    assert plan.changed_paths == ["skills/demo/SKILL.md"]
    assert "description: New" in path.read_text()


def test_apply_rollback_stages_reverse_patch_without_rewriting_history(tmp_path):
    initial = _repo(tmp_path)
    path = tmp_path / "skills" / "demo" / "SKILL.md"
    path.write_text("---\nname: demo\ndescription: New\n---\n# New\n")
    _git(tmp_path, "commit", "-am", "change skill")
    head = _git(tmp_path, "rev-parse", "HEAD").stdout.strip()
    apply_rollback(tmp_path, initial)
    assert "description: Demo" in path.read_text()
    assert _git(tmp_path, "rev-parse", "HEAD").stdout.strip() == head
    assert _git(tmp_path, "diff", "--cached", "--name-only").stdout.splitlines() == ["skills/demo/SKILL.md"]
