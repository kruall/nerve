"""Agent tools for graceful Nerve restart scheduling."""

from __future__ import annotations

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    RESTART_READY_SCHEMA,
    RESTART_SCHEDULE_SCHEMA,
    RESTART_WAIT_SCHEMA,
)
from nerve.restart import DEFAULT_PROMPT_AFTER_SECONDS


async def schedule_restart_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.engine is None:
        return ToolResult.text("Restart scheduling is unavailable.", is_error=True)
    result = await ctx.engine.restart_coordinator.schedule(
        ctx.session_id,
        prompt_after_seconds=args.get(
            "prompt_after_seconds", DEFAULT_PROMPT_AFTER_SECONDS,
        ),
    )
    if result["already_pending"]:
        return ToolResult.text(
            "A graceful restart is already pending; new and resumed sessions "
            "remain paused."
        )
    return ToolResult.text(
        "Graceful restart scheduled. New and resumed sessions are paused; "
        f"the {result['active_sessions']} active session(s) may finish. "
        "Long-running sessions will be asked for a restart decision after "
        f"{result['prompt_after_seconds']} seconds."
    )


async def restart_ready_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.engine is None:
        return ToolResult.text("Restart scheduling is unavailable.", is_error=True)
    if not await ctx.engine.restart_coordinator.mark_ready(ctx.session_id):
        return ToolResult.text(
            "This session has no pending scheduled-restart question.",
            is_error=True,
        )
    return ToolResult.text(
        "Restart permission recorded. This turn will now be stopped so the "
        "graceful restart can proceed."
    )


async def restart_wait_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.engine is None:
        return ToolResult.text("Restart scheduling is unavailable.", is_error=True)
    seconds = await ctx.engine.restart_coordinator.request_more_time(
        ctx.session_id, args["seconds"],
    )
    if seconds is None:
        return ToolResult.text(
            "This session has no pending scheduled-restart question.",
            is_error=True,
        )
    return ToolResult.text(
        f"Restart deferred for this session for {seconds} seconds. Finish the "
        "current work and wait for the next restart question."
    )


RESTART_SPECS = [
    ToolSpec(
        name="schedule_restart",
        description=(
            "Schedule a graceful Nerve daemon restart after a user explicitly "
            "requests one. It pauses all new and resumed sessions immediately, "
            "waits for active turns to finish, then invokes the normal restart."
        ),
        input_schema=RESTART_SCHEDULE_SCHEMA,
        handler=schedule_restart_handler,
    ),
    ToolSpec(
        name="restart_ready",
        description=(
            "Answer a scheduled-restart steer: this active session is safe to "
            "stop now. Only call it after receiving that steer."
        ),
        input_schema=RESTART_READY_SCHEMA,
        handler=restart_ready_handler,
    ),
    ToolSpec(
        name="restart_wait",
        description=(
            "Answer a scheduled-restart steer: this active session needs a "
            "bounded amount of additional time before another restart question."
        ),
        input_schema=RESTART_WAIT_SCHEMA,
        handler=restart_wait_handler,
    ),
]
