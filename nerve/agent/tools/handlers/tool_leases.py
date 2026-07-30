"""MCP handlers for durable exclusive-tool leases."""

from __future__ import annotations

import re

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    TOOL_LEASE_ACQUIRE_SCHEMA,
    TOOL_LEASE_RELEASE_SCHEMA,
    TOOL_LEASE_RENEW_SCHEMA,
    TOOL_LEASE_STATUS_SCHEMA,
    TOOL_LEASE_SUBSCRIBE_SCHEMA,
    TOOL_LEASE_UNSUBSCRIBE_SCHEMA,
)

_TOOL_NAME_RE = re.compile(r"^[A-Za-z0-9_.:/-]{1,200}$")
_MIN_LEASE_SECONDS = 60
_MAX_LEASE_SECONDS = 3600


def _tool_name(args: dict) -> str | None:
    name = str(args.get("tool_name") or "").strip()
    return name if _TOOL_NAME_RE.fullmatch(name) else None


def _seconds(args: dict, field: str, default: int) -> int | None:
    value = args.get(field, default)
    if isinstance(value, bool):
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    if not _MIN_LEASE_SECONDS <= seconds <= _MAX_LEASE_SECONDS:
        return None
    return seconds


def _unavailable() -> ToolResult:
    return ToolResult.text("Tool leases are unavailable: database not wired.", is_error=True)


def _invalid_name() -> ToolResult:
    return ToolResult.text(
        "Invalid tool_name. Use 1–200 letters, digits, '.', '_', ':', '/', or '-'.",
        is_error=True,
    )


def _invalid_seconds(field: str) -> ToolResult:
    return ToolResult.text(
        f"Invalid {field}. It must be an integer from {_MIN_LEASE_SECONDS} to {_MAX_LEASE_SECONDS} seconds.",
        is_error=True,
    )


async def tool_lease_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    lease = await ctx.db.get_tool_lease(name)
    if lease is None:
        return ToolResult.text(f"Lease for `{name}` is available.")
    if lease["session_id"] == ctx.session_id:
        return ToolResult.text(f"This session owns `{name}` until {lease['expires_at']}.")
    return ToolResult.text(f"Lease for `{name}` is busy until {lease['expires_at']}.")


async def tool_lease_acquire_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    seconds = _seconds(args, "lease_seconds", 300)
    if seconds is None:
        return _invalid_seconds("lease_seconds")
    outcome, lease = await ctx.db.acquire_tool_lease(name, ctx.session_id, seconds)
    if outcome == "acquired":
        return ToolResult.text(f"Lease acquired for `{name}` until {lease['expires_at']}.")
    if outcome == "already_owned":
        return ToolResult.text(
            f"This session already owns `{name}` until {lease['expires_at']}. Use tool_lease_renew to extend it."
        )
    return ToolResult.text(
        f"Lease for `{name}` is busy until {lease['expires_at']}. Subscribe with tool_lease_subscribe to continue automatically after handoff."
    )


async def tool_lease_renew_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    seconds = _seconds(args, "lease_seconds", 300)
    if seconds is None:
        return _invalid_seconds("lease_seconds")
    lease = await ctx.db.renew_tool_lease(name, ctx.session_id, seconds)
    if lease is None:
        return ToolResult.text(
            f"Cannot renew `{name}`: this session does not own a live lease. Acquire it again.",
            is_error=True,
        )
    return ToolResult.text(f"Lease for `{name}` renewed until {lease['expires_at']}.")


async def tool_lease_release_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    released = await ctx.db.release_tool_lease(name, ctx.session_id)
    if released:
        return ToolResult.text(
            f"Lease released for `{name}`. The first waiting session will be resumed by the next lease sweep."
        )
    return ToolResult.text(f"This session does not own `{name}`; nothing was released.")


async def tool_lease_subscribe_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    lease_seconds = _seconds(args, "lease_seconds", 300)
    wait_seconds = _seconds(args, "wait_seconds", 3600)
    if lease_seconds is None:
        return _invalid_seconds("lease_seconds")
    if wait_seconds is None:
        return _invalid_seconds("wait_seconds")
    session = await ctx.db.get_session(ctx.session_id)
    if session and session.get("source") == "external":
        return ToolResult.text(
            "tool_lease_subscribe is unavailable for external client sessions: "
            "Nerve cannot safely wake a session it does not own.",
            is_error=True,
        )
    if await ctx.db.can_acquire_tool_lease(name, ctx.session_id):
        outcome, lease = await ctx.db.acquire_tool_lease(name, ctx.session_id, lease_seconds)
        if outcome in {"acquired", "already_owned"}:
            return ToolResult.text(f"Lease acquired for `{name}` until {lease['expires_at']}; no subscription is needed.")
    prompt = str(args.get("prompt") or "").strip()
    if not prompt:
        prompt = (
            f"The exclusive lease for `{name}` was handed to this session. "
            "Continue the deferred work now; renew or release the lease when appropriate."
        )
    subscription = await ctx.db.subscribe_tool_lease(
        name, ctx.session_id, prompt, lease_seconds, wait_seconds,
    )
    return ToolResult.text(
        f"Subscribed to `{name}` until {subscription['expires_at']}. "
        "When it becomes free, the lease will be reserved for this session and it will be woken automatically."
    )


async def tool_lease_unsubscribe_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.db is None:
        return _unavailable()
    name = _tool_name(args)
    if name is None:
        return _invalid_name()
    removed = await ctx.db.unsubscribe_tool_lease(name, ctx.session_id)
    return ToolResult.text(
        f"Subscription removed for `{name}`." if removed else f"No subscription exists for `{name}`."
    )


TOOL_LEASE_SPECS = [
    ToolSpec("tool_lease_status", "Check whether an exclusive named tool lease is available now.", TOOL_LEASE_STATUS_SCHEMA, tool_lease_status_handler),
    ToolSpec("tool_lease_acquire", "Atomically acquire an exclusive named tool lease for this session.", TOOL_LEASE_ACQUIRE_SCHEMA, tool_lease_acquire_handler),
    ToolSpec("tool_lease_renew", "Extend this session's live exclusive tool lease before it expires.", TOOL_LEASE_RENEW_SCHEMA, tool_lease_renew_handler),
    ToolSpec("tool_lease_release", "Release this session's exclusive tool lease when its work is complete.", TOOL_LEASE_RELEASE_SCHEMA, tool_lease_release_handler),
    ToolSpec("tool_lease_subscribe", "Wait for an exclusive tool: its lease is atomically handed to this session and it is resumed when free.", TOOL_LEASE_SUBSCRIBE_SCHEMA, tool_lease_subscribe_handler),
    ToolSpec("tool_lease_unsubscribe", "Cancel this session's pending exclusive-tool lease subscription.", TOOL_LEASE_UNSUBSCRIBE_SCHEMA, tool_lease_unsubscribe_handler),
]
