"""Durable build/test command launcher for engine-owned sessions."""

from __future__ import annotations

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec


LONG_COMMAND_SCHEMA = {
    "type": "object",
    "properties": {
        "command": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "description": "Build or test command as argv, never a shell string.",
        },
        "cwd": {
            "type": "string",
            "description": "Optional directory relative to the configured workspace.",
            "default": ".",
        },
        "timeoutSeconds": {
            "type": "number",
            "description": "Maximum run time in seconds, clamped to 60–14400.",
            "default": 1800,
        },
        "prompt": {
            "type": "string",
            "description": "Optional instruction for this session after the command settles.",
            "default": "Inspect the command result and continue the task.",
        },
    },
    "required": ["command"],
}


async def long_command_handler(ctx: ToolContext, args: dict) -> ToolResult:
    if ctx.engine is None or ctx.db is None or ctx.workspace is None:
        return ToolResult.text(
            "run_long_command is unavailable: engine not wired", is_error=True,
        )
    if ctx.runtime_metadata.get("runtime") not in (None, "codex"):
        return ToolResult.text(
            "run_long_command is available only to engine-owned sessions.",
            is_error=True,
        )
    command = args.get("command")
    if not isinstance(command, list) or not command or not all(
        isinstance(part, str) and part for part in command
    ):
        return ToolResult.text("`command` must be a non-empty argv array.", is_error=True)
    try:
        job = await ctx.engine.start_long_command(
            session_id=ctx.session_id,
            command=command,
            cwd=str(args.get("cwd") or "."),
            timeout_seconds=args.get("timeoutSeconds", 1800),
            prompt=str(args.get("prompt") or "Inspect the command result and continue the task."),
        )
    except ValueError as e:
        return ToolResult.text(str(e), is_error=True)
    except Exception as e:
        return ToolResult.text(f"Could not start long command: {e}", is_error=True)
    return ToolResult.text(
        f"Long command {job['id']} is running in the background. This turn may end; "
        f"Nerve will re-invoke this same session when it finishes or times out. "
        f"Output: {job['output_path']}"
    )


LONG_COMMAND_SPECS = [
    ToolSpec(
        name="run_long_command",
        description=(
            "Run a potentially long build or test command without keeping this "
            "agent turn active. The command must be argv (not a shell string) and "
            "runs below the configured workspace. Nerve resumes this exact session "
            "on completion or timeout with the output tail."
        ),
        input_schema=LONG_COMMAND_SCHEMA,
        handler=long_command_handler,
    ),
]
