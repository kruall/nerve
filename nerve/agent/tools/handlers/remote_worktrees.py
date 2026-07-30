"""Detached commands in allowlisted remote Git worktrees."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path, PurePosixPath

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.config import (
    RemoteWorktreeHostConfig,
    RemoteWorktreeRepositoryConfig,
)


REMOTE_WORKTREE_SCHEMA = {
    "type": "object",
    "properties": {
        "host": {
            "type": "string",
            "description": "Configured remote host alias; raw hostnames are rejected.",
        },
        "worktree": {
            "type": "string",
            "description": "Absolute local Git worktree inside an allowed repository root.",
        },
        "operation": {
            "type": "string",
            "enum": ["sync", "make", "test", "execute"],
        },
        "arguments": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Operation arguments as argv, never a shell string.",
            "default": [],
        },
        "remoteCwd": {
            "type": "string",
            "description": "Directory relative to the isolated remote checkout.",
            "default": ".",
        },
        "skipSync": {
            "type": "boolean",
            "description": "Reuse the existing checkout; valid only for execute.",
            "default": False,
        },
        "timeoutSeconds": {
            "type": "number",
            "description": "Maximum run time, clamped to 60–14400 seconds.",
            "default": 1800,
        },
        "prompt": {
            "type": "string",
            "description": "Instruction for the continuation turn.",
            "default": "Inspect the remote command result and continue the task.",
        },
    },
    "required": ["host", "worktree", "operation"],
}


def _aliases(hosts: tuple[RemoteWorktreeHostConfig, ...]) -> str:
    return ", ".join(host.alias for host in hosts) or "(none)"


def _validate_arguments(operation: str, value: object) -> list[str]:
    if not isinstance(value, list) or not all(
        isinstance(part, str) and part and "\x00" not in part
        for part in value
    ):
        raise ValueError("`arguments` must be an argv array of non-empty strings.")
    if operation == "sync" and value:
        raise ValueError("`sync` does not accept arguments.")
    if operation != "sync" and not value:
        raise ValueError(f"`{operation}` requires at least one argument.")
    return list(value)


def _validate_remote_cwd(value: object) -> str:
    remote_cwd = str(value or ".")
    if "\x00" in remote_cwd or "\n" in remote_cwd or "\r" in remote_cwd:
        raise ValueError("`remoteCwd` must not contain control characters.")
    path = PurePosixPath(remote_cwd)
    if path.is_absolute() or ".." in path.parts or remote_cwd != str(path):
        raise ValueError("`remoteCwd` must remain inside the remote checkout.")
    return remote_cwd


async def _git_toplevel(worktree: Path) -> Path:
    process = await asyncio.create_subprocess_exec(
        "git", "-C", str(worktree), "rev-parse", "--show-toplevel",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _stderr = await process.communicate()
    if process.returncode != 0:
        raise ValueError("`worktree` must be a valid Git worktree.")
    return Path(
        os.path.expandvars(os.path.expanduser(stdout.decode().strip()))
    ).resolve()


async def _resolve_repository(
    host: RemoteWorktreeHostConfig, value: object,
) -> tuple[Path, RemoteWorktreeRepositoryConfig]:
    raw = str(value or "")
    if not raw or "\x00" in raw:
        raise ValueError("`worktree` must be a non-empty absolute local path.")
    path = Path(os.path.expandvars(os.path.expanduser(raw)))
    if not path.is_absolute():
        raise ValueError("`worktree` must be an absolute local path.")
    try:
        worktree = path.resolve(strict=True)
    except OSError as e:
        raise ValueError("`worktree` does not exist.") from e
    if not worktree.is_dir():
        raise ValueError("`worktree` must be a directory.")
    matches = [
        repository for repository in host.repositories
        if worktree.is_relative_to(repository.local_worktree_root)
    ]
    if len(matches) != 1:
        raise ValueError(
            "`worktree` is not inside exactly one repository root allowed "
            f"for host alias {host.alias!r}."
        )
    if await _git_toplevel(worktree) != worktree:
        raise ValueError("`worktree` must name the Git worktree top-level.")
    return worktree, matches[0]


async def remote_worktree_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.engine is None or ctx.db is None or ctx.config is None:
        return ToolResult.text(
            "run_remote_worktree_command is unavailable: engine not wired",
            is_error=True,
        )
    if ctx.runtime_metadata.get("runtime") not in (None, "codex"):
        return ToolResult.text(
            "run_remote_worktree_command is available only to "
            "engine-owned sessions.",
            is_error=True,
        )

    hosts = ctx.config.remote_worktrees.hosts
    alias = args.get("host")
    if not isinstance(alias, str) or not alias or "\x00" in alias:
        return ToolResult.text(
            f"`host` must be a configured alias. Allowed aliases: {_aliases(hosts)}.",
            is_error=True,
        )
    host = ctx.config.remote_worktrees.host(alias)
    if host is None:
        return ToolResult.text(
            f"Unknown remote host alias {alias!r}. "
            f"Allowed aliases: {_aliases(hosts)}.",
            is_error=True,
        )

    operation = args.get("operation")
    if operation not in {"sync", "make", "test", "execute"}:
        return ToolResult.text(
            "`operation` must be one of: sync, make, test, execute.",
            is_error=True,
        )
    try:
        arguments = _validate_arguments(operation, args.get("arguments", []))
        remote_cwd = _validate_remote_cwd(args.get("remoteCwd", "."))
        skip_sync = args.get("skipSync", False)
        if not isinstance(skip_sync, bool):
            raise ValueError("`skipSync` must be a boolean.")
        if skip_sync and operation != "execute":
            raise ValueError("`skipSync` is valid only for `execute`.")
        worktree, repository = await _resolve_repository(host, args.get("worktree"))
        job = await ctx.engine.start_remote_worktree_command(
            session_id=ctx.session_id,
            host_alias=host.alias,
            repository_name=repository.name,
            worktree=str(worktree),
            operation=operation,
            arguments=arguments,
            remote_cwd=remote_cwd,
            skip_sync=skip_sync,
            timeout_seconds=args.get("timeoutSeconds", 1800),
            prompt=str(
                args.get("prompt")
                or "Inspect the remote command result and continue the task."
            ),
        )
    except ValueError as e:
        return ToolResult.text(str(e), is_error=True)
    except Exception as e:
        return ToolResult.text(
            f"Could not start remote command for alias {host.alias!r}: {e}",
            is_error=True,
        )
    return ToolResult.text(
        f"Remote worktree command {job['id']} for alias {host.alias!r} is "
        "running in the background. This turn may end; Nerve will re-invoke "
        "this same session when it finishes or times out."
    )


REMOTE_WORKTREE_SPECS = [
    ToolSpec(
        name="run_remote_worktree_command",
        description=(
            "Snapshot an allowed local Git worktree, synchronize it to a "
            "configured remote host alias, and optionally run make, test, or "
            "an exact argv command there. Completion arrives in a separate "
            "continuation turn for this same session."
        ),
        input_schema=REMOTE_WORKTREE_SCHEMA,
        handler=remote_worktree_handler,
    ),
]
