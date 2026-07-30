"""Focused tests for configured remote worktree execution."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

from nerve.agent.remote_worktree_runner import (
    _Redactor,
    _checkout_key,
    create_snapshot,
    run_remote_worktree_command,
)
from nerve.agent.tools.handlers.remote_worktrees import remote_worktree_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig


def _git(repository: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *args],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repository(path: Path) -> Path:
    path.mkdir(parents=True)
    _git(path, "init")
    _git(path, "config", "user.name", "Test")
    _git(path, "config", "user.email", "test@example.test")
    (path / ".gitignore").write_text("build/\nignored.txt\n", encoding="utf-8")
    (path / "tracked.txt").write_text("original\n", encoding="utf-8")
    (path / "deleted.txt").write_text("delete me\n", encoding="utf-8")
    (path / "ya").write_text(
        "#!/bin/sh\n"
        "printf '%s\\n' \"$@\" > \"$NERVE_REMOTE_TEST_LOG\"\n"
        "exit \"${NERVE_REMOTE_TEST_EXIT:-0}\"\n",
        encoding="utf-8",
    )
    (path / "ya").chmod(0o755)
    _git(path, "add", ".")
    _git(path, "commit", "-m", "initial")
    return path


def _remote_dict(
    local_root: Path,
    remote_root: Path,
    *,
    fqdn: str = "builder.example.test",
) -> dict:
    return {
        "hosts": {
            "builder": {
                "fqdn": fqdn,
                "ssh_user": "build",
                "ssh_port": 22,
                "ssh_args": ["-o", "StrictHostKeyChecking=yes"],
                "repositories": {
                    "repo": {
                        "local_worktree_root": str(local_root),
                        "remote_bare_repo": str(remote_root / "repo.git"),
                        "remote_checkout_root": str(remote_root / "checkouts"),
                    },
                },
            },
        },
    }


def _config(local_root: Path, remote_root: Path) -> NerveConfig:
    return NerveConfig.from_dict({
        "remote_worktrees": _remote_dict(local_root, remote_root),
    })


def test_config_parses_aliases_and_expands_local_root(
    tmp_path, monkeypatch,
):
    local_root = tmp_path / "local"
    remote_root = tmp_path / "remote"
    monkeypatch.setenv("NERVE_REMOTE_LOCAL_ROOT", str(local_root))
    raw = _remote_dict(local_root, remote_root)
    raw["hosts"]["builder"]["repositories"]["repo"][
        "local_worktree_root"
    ] = "$NERVE_REMOTE_LOCAL_ROOT"
    config = NerveConfig.from_dict({"remote_worktrees": raw})
    assert config.remote_worktrees.aliases == ("builder",)
    repository = config.remote_worktrees.host("builder").repositories[0]
    assert repository.local_worktree_root == local_root.resolve()


@pytest.mark.parametrize(
    "key,value",
    [
        ("remote_bare_repo", "/"),
        ("remote_bare_repo", "relative/repo.git"),
        ("remote_bare_repo", "/tmp/a/../repo.git"),
        ("remote_checkout_root", ""),
        ("remote_checkout_root", "/tmp/checkouts/"),
    ],
)
def test_config_rejects_unsafe_remote_paths(tmp_path, key, value):
    raw = _remote_dict(tmp_path / "local", tmp_path / "remote")
    raw["hosts"]["builder"]["repositories"]["repo"][key] = value
    with pytest.raises(ValueError, match="remote"):
        NerveConfig.from_dict({"remote_worktrees": raw})


def test_config_rejects_ambiguous_local_roots(tmp_path):
    raw = _remote_dict(tmp_path / "local", tmp_path / "remote")
    raw["hosts"]["builder"]["repositories"]["nested"] = {
        "local_worktree_root": str(tmp_path / "local" / "nested"),
        "remote_bare_repo": str(tmp_path / "other.git"),
        "remote_checkout_root": str(tmp_path / "other-checkouts"),
    }
    with pytest.raises(ValueError, match="ambiguous"):
        NerveConfig.from_dict({"remote_worktrees": raw})


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        (lambda raw: raw["hosts"].update({"bad alias": raw["hosts"].pop("builder")}),
         "host alias"),
        (lambda raw: raw["hosts"]["builder"].update({"fqdn": "not-a-fqdn"}),
         "fully-qualified"),
        (lambda raw: raw["hosts"]["builder"].update({"ssh_port": 70000}),
         "ssh_port"),
    ],
)
def test_config_rejects_invalid_host_identity(tmp_path, mutation, error):
    raw = _remote_dict(tmp_path / "local", tmp_path / "remote")
    mutation(raw)
    with pytest.raises(ValueError, match=error):
        NerveConfig.from_dict({"remote_worktrees": raw})


def test_config_rejects_non_mapping_remote_worktrees():
    with pytest.raises(ValueError, match="must be a mapping"):
        NerveConfig.from_dict({"remote_worktrees": []})


def test_empty_config_disables_hosts():
    config = NerveConfig.from_dict({})
    assert config.remote_worktrees.aliases == ()


def test_stream_redaction_covers_chunk_boundaries(capsys):
    redactor = _Redactor(
        ["build@builder.example.test", "builder.example.test"],
        "[host:builder]",
    )
    redactor.feed(b"ssh build@builder.exam")
    redactor.feed(b"ple.test failed")
    redactor.finish()
    output = capsys.readouterr().out
    assert output == "ssh [host:builder] failed"
    assert "example.test" not in output


@pytest.mark.asyncio
async def test_unknown_alias_does_not_leak_fqdn(tmp_path):
    config = _config(tmp_path / "local", tmp_path / "remote")
    result = await remote_worktree_handler(
        ToolContext(
            session_id="s1", config=config, db=object(),
            engine=SimpleNamespace(),
        ),
        {"host": "unknown", "worktree": "/tmp/x", "operation": "sync"},
    )
    text = result.content[0]["text"]
    assert result.is_error
    assert "builder" in text
    assert "builder.example.test" not in text


@pytest.mark.parametrize(
    ("operation", "arguments", "skip_sync"),
    [
        ("sync", [], False),
        ("make", ["target"], False),
        ("test", ["target"], False),
        ("execute", ["git", "status"], True),
    ],
)
@pytest.mark.asyncio
async def test_handler_starts_all_operations(
    tmp_path, operation, arguments, skip_sync,
):
    local_root = tmp_path / "worktrees"
    worktree = _init_repository(local_root / "task")
    engine = SimpleNamespace(start_remote_worktree_command=AsyncMock(
        return_value={"id": "job-1", "output_path": "/tmp/job.log"},
    ))
    result = await remote_worktree_handler(
        ToolContext(
            session_id="s1", config=_config(local_root, tmp_path / "remote"),
            db=object(), engine=engine,
            runtime_metadata={"runtime": "codex"},
        ),
        {
            "host": "builder", "worktree": str(worktree),
            "operation": operation, "arguments": arguments,
            "remoteCwd": ".", "skipSync": skip_sync,
            "timeoutSeconds": 90, "prompt": "continue",
        },
    )
    assert not result.is_error
    engine.start_remote_worktree_command.assert_awaited_once_with(
        session_id="s1", host_alias="builder", repository_name="repo",
        worktree=str(worktree.resolve()), operation=operation,
        arguments=arguments, remote_cwd=".", skip_sync=skip_sync,
        timeout_seconds=90, prompt="continue",
    )


@pytest.mark.parametrize(
    "arguments,error",
    [
        (
            {"operation": "make", "arguments": [], "skipSync": False},
            "requires at least one",
        ),
        (
            {"operation": "sync", "arguments": ["x"], "skipSync": False},
            "does not accept",
        ),
        (
            {"operation": "test", "arguments": ["x"], "skipSync": True},
            "only for",
        ),
        (
            {
                "operation": "execute", "arguments": ["echo", "x"],
                "remoteCwd": "../escape",
            },
            "inside",
        ),
        (
            {"operation": "execute", "arguments": ["bad\x00arg"]},
            "argv",
        ),
    ],
)
@pytest.mark.asyncio
async def test_handler_rejects_invalid_arguments(tmp_path, arguments, error):
    local_root = tmp_path / "worktrees"
    worktree = _init_repository(local_root / "task")
    engine = SimpleNamespace(start_remote_worktree_command=AsyncMock())
    result = await remote_worktree_handler(
        ToolContext(
            session_id="s1", config=_config(local_root, tmp_path / "remote"),
            db=object(), engine=engine,
        ),
        {"host": "builder", "worktree": str(worktree), **arguments},
    )
    assert result.is_error
    assert error in result.content[0]["text"]
    engine.start_remote_worktree_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_handler_rejects_external_runtime(tmp_path):
    engine = SimpleNamespace(start_remote_worktree_command=AsyncMock())
    result = await remote_worktree_handler(
        ToolContext(
            session_id="external", config=NerveConfig.from_dict({}),
            db=object(), engine=engine,
            runtime_metadata={"runtime": "external"},
        ),
        {"host": "builder", "worktree": "/tmp/x", "operation": "sync"},
    )
    assert result.is_error
    engine.start_remote_worktree_command.assert_not_awaited()


def test_snapshot_includes_worktree_changes_without_mutating_git_state(tmp_path):
    worktree = _init_repository(tmp_path / "repo")
    (worktree / "staged.txt").write_text("staged locally\n", encoding="utf-8")
    _git(worktree, "add", "staged.txt")
    (worktree / "tracked.txt").write_text("modified\n", encoding="utf-8")
    (worktree / "deleted.txt").unlink()
    (worktree / "untracked.txt").write_text("new\n", encoding="utf-8")
    (worktree / "ignored.txt").write_text("ignored\n", encoding="utf-8")
    before_head = _git(worktree, "rev-parse", "HEAD")
    before_branch = _git(worktree, "branch", "--show-current")
    before_status = _git(worktree, "status", "--short")
    before_index = _git(worktree, "write-tree")

    snapshot = create_snapshot(worktree)
    assert create_snapshot(worktree) == snapshot
    names = set(_git(worktree, "ls-tree", "-r", "--name-only", snapshot).splitlines())
    assert "tracked.txt" in names
    assert "untracked.txt" in names
    assert "staged.txt" in names
    assert "deleted.txt" not in names
    assert "ignored.txt" not in names
    assert _git(worktree, "show", f"{snapshot}:tracked.txt") == "modified"
    assert _git(worktree, "rev-parse", "HEAD") == before_head
    assert _git(worktree, "branch", "--show-current") == before_branch
    assert _git(worktree, "status", "--short") == before_status
    assert _git(worktree, "write-tree") == before_index


def _write_fake_ssh(path: Path) -> None:
    path.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        "with open(os.environ['NERVE_FAKE_SSH_LOG'], 'a', encoding='utf-8') as f:\n"
        "    f.write(repr(sys.argv[1:]) + '\\n')\n"
        "os.execv('/bin/sh', ['sh', '-c', sys.argv[-1]])\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _write_config(
    config_dir: Path, local_root: Path, remote_root: Path,
) -> None:
    config_dir.mkdir()
    (config_dir / "config.yaml").write_text(
        yaml.safe_dump({
            "workspace": str(config_dir / "workspace"),
            "remote_worktrees": _remote_dict(local_root, remote_root),
        }),
        encoding="utf-8",
    )


def test_runner_syncs_incrementally_preserves_cache_and_exit_code(
    tmp_path, monkeypatch,
):
    local_root = tmp_path / "worktrees"
    worktree = _init_repository(local_root / "task")
    remote_root = tmp_path / "remote"
    config_dir = tmp_path / "config"
    _write_config(config_dir, local_root, remote_root)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    _write_fake_ssh(fake_bin / "ssh")
    ssh_log = tmp_path / "ssh.log"
    command_log = tmp_path / "command.log"
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("NERVE_FAKE_SSH_LOG", str(ssh_log))
    monkeypatch.setenv("NERVE_REMOTE_TEST_LOG", str(command_log))

    common = {
        "config_dir": config_dir,
        "host_alias": "builder",
        "repository_name": "repo",
        "worktree": worktree,
        "session_id": "session-1",
        "remote_cwd": ".",
    }
    assert run_remote_worktree_command(
        **common, operation="sync", arguments=[], skip_sync=False,
    ) == 0
    checkout = (
        remote_root / "checkouts" / _checkout_key("session-1", worktree)
    )
    assert (checkout / "tracked.txt").read_text() == "original\n"
    cache = checkout / "build" / "cache.bin"
    cache.parent.mkdir()
    cache.write_text("keep\n", encoding="utf-8")

    (worktree / "tracked.txt").write_text("changed\n", encoding="utf-8")
    (worktree / "deleted.txt").unlink()
    (worktree / "new.txt").write_text("new\n", encoding="utf-8")
    assert run_remote_worktree_command(
        **common, operation="sync", arguments=[], skip_sync=False,
    ) == 0
    assert (checkout / "tracked.txt").read_text() == "changed\n"
    assert not (checkout / "deleted.txt").exists()
    assert (checkout / "new.txt").read_text() == "new\n"
    assert cache.read_text() == "keep\n"

    assert run_remote_worktree_command(
        **common, operation="make", arguments=["target"], skip_sync=False,
    ) == 0
    assert command_log.read_text().splitlines() == [
        "make", "--build", "relwithdebinfo", "target",
    ]
    assert run_remote_worktree_command(
        **common, operation="test", arguments=["target"], skip_sync=False,
    ) == 0
    assert command_log.read_text().splitlines() == [
        "make", "--build", "relwithdebinfo", "-tA", "target",
    ]
    assert run_remote_worktree_command(
        **common, operation="execute",
        arguments=["sh", "-c", "exit 7"], skip_sync=True,
    ) == 7
    assert "builder.example.test" in ssh_log.read_text()
