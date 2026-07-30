"""Snapshot, synchronize, and execute in an allowlisted remote worktree."""

from __future__ import annotations

import argparse
import codecs
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence
from urllib.parse import quote

from nerve.config import (
    RemoteWorktreeHostConfig,
    RemoteWorktreeRepositoryConfig,
    load_config,
)


_SNAPSHOT_IDENTITY = {
    "GIT_AUTHOR_NAME": "Nerve",
    "GIT_AUTHOR_EMAIL": "nerve-snapshot@localhost",
    "GIT_AUTHOR_DATE": "2000-01-01T00:00:00+00:00",
    "GIT_COMMITTER_NAME": "Nerve",
    "GIT_COMMITTER_EMAIL": "nerve-snapshot@localhost",
    "GIT_COMMITTER_DATE": "2000-01-01T00:00:00+00:00",
}


def _capture(
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> str:
    result = subprocess.run(
        list(command),
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(detail or f"{command[0]} exited with {result.returncode}")
    return result.stdout.decode("utf-8", errors="strict").strip()


def create_snapshot(worktree: Path) -> str:
    """Create a deterministic synthetic commit without changing worktree state."""
    worktree = worktree.resolve(strict=True)
    top = Path(
        _capture(["git", "-C", str(worktree), "rev-parse", "--show-toplevel"])
    ).resolve()
    if top != worktree:
        raise ValueError("worktree must name the Git worktree top-level")
    head = _capture(["git", "-C", str(worktree), "rev-parse", "HEAD"])
    staged = _capture(["git", "-C", str(worktree), "ls-files", "--stage"])
    if any(line.startswith("160000 ") for line in staged.splitlines()):
        raise ValueError("Git submodules are not supported by remote worktree snapshots")

    with tempfile.TemporaryDirectory(prefix="nerve-remote-snapshot-") as tmp:
        index = Path(tmp) / "index"
        env = os.environ.copy()
        env["GIT_INDEX_FILE"] = str(index)
        _capture(["git", "-C", str(worktree), "read-tree", head], env=env)
        _capture(["git", "-C", str(worktree), "add", "-A", "--", "."], env=env)
        snapshot_staged = _capture(
            ["git", "-C", str(worktree), "ls-files", "--stage"], env=env,
        )
        if any(line.startswith("160000 ") for line in snapshot_staged.splitlines()):
            raise ValueError(
                "Git submodules are not supported by remote worktree snapshots"
            )
        tree = _capture(["git", "-C", str(worktree), "write-tree"], env=env)
        commit_env = {**env, **_SNAPSHOT_IDENTITY}
        return _capture(
            [
                "git", "-C", str(worktree), "commit-tree", tree,
                "-p", head, "-m", "Nerve remote worktree snapshot",
            ],
            env=commit_env,
        )


class _Redactor:
    def __init__(self, tokens: Sequence[str], replacement: str) -> None:
        self._tokens = tuple(token for token in tokens if token)
        self._replacement = replacement
        self._pending = ""
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._keep = max((len(token) for token in self._tokens), default=1) - 1

    def feed(self, chunk: bytes) -> None:
        self._pending += self._decoder.decode(chunk)
        self._flush(final=False)

    def finish(self) -> None:
        self._pending += self._decoder.decode(b"", final=True)
        self._flush(final=True)

    def _flush(self, *, final: bool) -> None:
        for token in self._tokens:
            self._pending = self._pending.replace(token, self._replacement)
        length = len(self._pending) if final else max(0, len(self._pending) - self._keep)
        if length:
            sys.stdout.write(self._pending[:length])
            sys.stdout.flush()
            self._pending = self._pending[length:]


def _stream(
    command: Sequence[str],
    *,
    redactions: Sequence[str],
    alias: str,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    process = subprocess.Popen(
        list(command),
        cwd=str(cwd) if cwd is not None else None,
        env=dict(env) if env is not None else None,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert process.stdout is not None
    redactor = _Redactor(redactions, f"[host:{alias}]")
    while chunk := process.stdout.read(8192):
        redactor.feed(chunk)
    redactor.finish()
    return process.wait()


def _ssh_base(host: RemoteWorktreeHostConfig) -> list[str]:
    return [
        "ssh", *host.ssh_args,
        "-o", "BatchMode=yes",
        "-p", str(host.ssh_port),
    ]


def _destination(host: RemoteWorktreeHostConfig) -> str:
    return f"{host.ssh_user}@{host.fqdn}"


def _ssh(
    host: RemoteWorktreeHostConfig,
    remote_argv: Sequence[str],
    *,
    redactions: Sequence[str],
) -> int:
    remote_command = shlex.join(list(remote_argv))
    return _stream(
        [*_ssh_base(host), _destination(host), remote_command],
        redactions=redactions,
        alias=host.alias,
    )


def _remote_url(
    host: RemoteWorktreeHostConfig,
    repository: RemoteWorktreeRepositoryConfig,
) -> str:
    return (
        f"ssh://{host.ssh_user}@{host.fqdn}:{host.ssh_port}"
        f"{quote(repository.remote_bare_repo)}"
    )


def _checkout_key(session_id: str, worktree: Path) -> str:
    value = f"{session_id}\x00{worktree}".encode()
    return hashlib.sha256(value).hexdigest()[:24]


def _require_success(exit_code: int, message: str) -> None:
    if exit_code != 0:
        raise RuntimeError(f"{message} (exit code {exit_code})")


_INITIALIZE_BARE_SCRIPT = r"""
set -eu
bare=$1
if [ -L "$bare" ]; then
    echo "configured bare repository path is a symlink" >&2
    exit 20
fi
if [ -e "$bare" ]; then
    if [ ! -d "$bare" ] || [ "$(git -C "$bare" rev-parse --is-bare-repository 2>/dev/null || true)" != true ]; then
        echo "configured repository exists but is not bare" >&2
        exit 21
    fi
else
    mkdir -p "$(dirname "$bare")"
    git init --bare "$bare"
fi
"""


_PREPARE_CHECKOUT_SCRIPT = r"""
set -eu
checkout_root=$1
checkout=$2
bare=$3
snapshot_ref=$4
if [ -L "$checkout_root" ] || [ -L "$checkout" ]; then
    echo "configured checkout path is a symlink" >&2
    exit 30
fi
mkdir -p "$checkout_root"
if [ -e "$checkout" ] && [ ! -d "$checkout" ]; then
    echo "checkout path exists but is not a directory" >&2
    exit 31
fi
mkdir -p "$checkout"
if [ ! -d "$checkout/.git" ]; then
    if [ -n "$(find "$checkout" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "checkout path is non-empty and is not a Git repository" >&2
        exit 32
    fi
    git -C "$checkout" init
fi
if [ "$(git -C "$checkout" rev-parse --is-bare-repository 2>/dev/null || true)" != false ]; then
    echo "checkout path is not a non-bare Git repository" >&2
    exit 33
fi
git -C "$checkout" remote remove nerve-source >/dev/null 2>&1 || true
git -C "$checkout" remote add nerve-source "$bare"
git -C "$checkout" fetch --force --no-tags nerve-source "$snapshot_ref"
git -C "$checkout" reset --hard FETCH_HEAD
git -C "$checkout" clean -ffd
"""


_EXECUTE_SCRIPT = r"""
set -eu
checkout=$1
remote_cwd=$2
shift 2
if [ -L "$checkout" ] || [ ! -d "$checkout/.git" ]; then
    echo "remote checkout is missing or unsafe; synchronize it first" >&2
    exit 40
fi
if [ ! -d "$remote_cwd" ]; then
    echo "remoteCwd does not exist inside the checkout" >&2
    exit 41
fi
cd "$remote_cwd"
exec "$@"
"""


def run_remote_worktree_command(
    *,
    config_dir: Path,
    host_alias: str,
    repository_name: str,
    worktree: Path,
    session_id: str,
    operation: str,
    arguments: list[str],
    remote_cwd: str,
    skip_sync: bool,
) -> int:
    if operation not in {"sync", "make", "test", "execute"}:
        raise ValueError(f"unsupported operation: {operation}")
    if any("\x00" in part for part in arguments):
        raise ValueError("arguments must not contain NUL bytes")
    if operation == "sync" and arguments:
        raise ValueError("sync does not accept arguments")
    if operation != "sync" and not arguments:
        raise ValueError(f"{operation} requires at least one argument")
    if skip_sync and operation != "execute":
        raise ValueError("skipSync is valid only for execute")
    remote_cwd_path = PurePosixPath(remote_cwd)
    if (
        "\x00" in remote_cwd
        or "\n" in remote_cwd
        or "\r" in remote_cwd
        or remote_cwd_path.is_absolute()
        or ".." in remote_cwd_path.parts
        or remote_cwd != str(remote_cwd_path)
    ):
        raise ValueError("remoteCwd must remain inside the remote checkout")

    config = load_config(config_dir)
    host = config.remote_worktrees.host(host_alias)
    if host is None:
        aliases = ", ".join(config.remote_worktrees.aliases) or "(none)"
        raise ValueError(
            f"unknown remote host alias {host_alias!r}; allowed aliases: {aliases}"
        )
    repository = next(
        (item for item in host.repositories if item.name == repository_name),
        None,
    )
    if repository is None:
        raise ValueError(
            f"repository is not configured for host alias {host.alias!r}"
        )
    worktree = worktree.resolve(strict=True)
    if not worktree.is_relative_to(repository.local_worktree_root):
        raise ValueError(
            f"worktree is outside the repository root for host alias {host.alias!r}"
        )
    top = Path(
        _capture(["git", "-C", str(worktree), "rev-parse", "--show-toplevel"])
    ).resolve()
    if top != worktree:
        raise ValueError("worktree must name the Git worktree top-level")

    key = _checkout_key(session_id, worktree)
    snapshot_ref = f"refs/nerve/snapshots/{key}"
    checkout = PurePosixPath(repository.remote_checkout_root) / key
    cwd = checkout / remote_cwd_path
    destination = _destination(host)
    remote_url = _remote_url(host, repository)
    redactions = (destination, host.fqdn, remote_url)

    if not skip_sync:
        snapshot = create_snapshot(worktree)
        _require_success(
            _ssh(
                host,
                [
                    "sh", "-c", _INITIALIZE_BARE_SCRIPT, "nerve-init-bare",
                    repository.remote_bare_repo,
                ],
                redactions=redactions,
            ),
            f"could not initialize remote repository for alias {host.alias!r}",
        )
        git_ssh_command = shlex.join(_ssh_base(host))
        _require_success(
            _stream(
                [
                    "git", "-C", str(worktree),
                    "-c", f"core.sshCommand={git_ssh_command}",
                    "push", "--force", remote_url,
                    f"{snapshot}:{snapshot_ref}",
                ],
                redactions=redactions,
                alias=host.alias,
            ),
            f"could not push snapshot to alias {host.alias!r}",
        )
        _require_success(
            _ssh(
                host,
                [
                    "sh", "-c", _PREPARE_CHECKOUT_SCRIPT, "nerve-checkout",
                    repository.remote_checkout_root, str(checkout),
                    repository.remote_bare_repo, snapshot_ref,
                ],
                redactions=redactions,
            ),
            f"could not prepare checkout on alias {host.alias!r}",
        )

    if operation == "sync":
        return 0
    if operation == "make":
        command = ["./ya", "make", "--build", "relwithdebinfo", *arguments]
    elif operation == "test":
        command = [
            "./ya", "make", "--build", "relwithdebinfo", "-tA", *arguments,
        ]
    elif operation == "execute":
        command = arguments
    return _ssh(
        host,
        [
            "sh", "-c", _EXECUTE_SCRIPT, "nerve-execute",
            str(checkout), str(cwd), *command,
        ],
        redactions=redactions,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-dir", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--worktree", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--arguments-json", required=True)
    parser.add_argument("--remote-cwd", default=".")
    parser.add_argument("--skip-sync", action="store_true")
    args = parser.parse_args(argv)
    try:
        arguments = json.loads(args.arguments_json)
        if not isinstance(arguments, list) or not all(
            isinstance(part, str) and "\x00" not in part for part in arguments
        ):
            raise ValueError("arguments must be a JSON argv array")
        return run_remote_worktree_command(
            config_dir=Path(args.config_dir),
            host_alias=args.host,
            repository_name=args.repository,
            worktree=Path(args.worktree),
            session_id=args.session_id,
            operation=args.operation,
            arguments=arguments,
            remote_cwd=args.remote_cwd,
            skip_sync=args.skip_sync,
        )
    except Exception as e:
        print(f"Remote worktree command failed for alias {args.host!r}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
