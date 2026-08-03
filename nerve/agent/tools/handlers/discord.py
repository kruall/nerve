"""Discord project/audit-forum management tools.

``discord_forum_tags`` is read-only. ``discord_forum_tag_action`` never
mutates Discord directly: it prepares a fully described action and files an
approval notification whose server-side dispatcher performs the mutation
only after an explicit Approve decision.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    COMPLETE_DISCORD_PROJECT_TASK_AUDIT_SCHEMA,
    DISCORD_FORUM_TAG_ACTION_SCHEMA,
    DISCORD_FORUM_TAGS_SCHEMA,
    DISCORD_PROJECT_TASK_AUDIT_SCHEMA,
    DISCORD_PROJECT_TASK_CREATE_SCHEMA,
    DISCORD_PROJECT_TASK_STATUS_SCHEMA,
)
from nerve.channels.discord_project_tasks import DiscordProjectTaskCreateError
from nerve.channels.discord_project_task_audit import DiscordProjectTaskAuditError
from nerve.discord_tags import (
    DISCORD_FORUM_TAG_METADATA_KEY,
    DISCORD_FORUM_TAG_TARGET_KIND,
    DiscordForumTagError,
    DiscordForumTagManager,
    DiscordProjectTaskStatusError,
    transition_project_task_status,
)

logger = logging.getLogger(__name__)


def _current_discord_target(ctx: ToolContext) -> str:
    if ctx.engine is None:
        return ""
    if ctx.engine.get_active_channel(ctx.session_id) != "discord":
        return ""
    context = ctx.engine.router.get_message_context(ctx.session_id)
    if not context or context.get("channel_name") != "discord":
        return ""
    return str(context.get("target") or "")


async def discord_forum_tags_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    if ctx.config is None:
        return ToolResult.text(
            "discord_forum_tags: Nerve config is unavailable.",
            is_error=True,
        )
    project = str(args.get("project") or "").strip()
    thread_id = str(args.get("thread_id") or "").strip()
    if not project and not thread_id:
        thread_id = _current_discord_target(ctx)

    try:
        result = await asyncio.to_thread(
            DiscordForumTagManager(ctx.config).inspect,
            project=project,
            thread_id=thread_id or None,
        )
    except DiscordForumTagError as exc:
        return ToolResult.text(
            f"discord_forum_tags: {exc}",
            is_error=True,
        )
    return ToolResult.text(json.dumps(result, ensure_ascii=False, indent=2))


async def discord_forum_tag_action_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    """Prepare a Discord mutation and create its mandatory approval."""
    if ctx.config is None:
        return ToolResult.text(
            "discord_forum_tag_action: Nerve config is unavailable.",
            is_error=True,
        )
    if ctx.notification_service is None:
        return ToolResult.text(
            "discord_forum_tag_action: notification service is unavailable.",
            is_error=True,
        )

    operation = str(args.get("operation") or "").strip()
    implicit_thread_id = _current_discord_target(ctx)
    try:
        prepared = await asyncio.to_thread(
            DiscordForumTagManager(ctx.config).prepare,
            operation,
            args,
            implicit_thread_id=implicit_thread_id or None,
        )
    except DiscordForumTagError as exc:
        return ToolResult.text(
            f"discord_forum_tag_action: {exc}",
            is_error=True,
        )

    action_id = f"discord-tag-{uuid.uuid4().hex[:12]}"
    action = dict(prepared.action)
    action["action_id"] = action_id
    body = (
        prepared.body
        + "\n\nNothing changes until this approval is accepted. At execution "
        "time Nerve will re-fetch Discord state and re-check that the forum "
        "is still configured in `discord.task_forums`."
    )
    try:
        result = await ctx.notification_service.propose_action(
            session_id=ctx.session_id,
            target_kind=DISCORD_FORUM_TAG_TARGET_KIND,
            target_id=action_id,
            title=prepared.title,
            body=body,
            options=[
                {"label": "Approve", "value": "approve"},
                {"label": "Decline", "value": "decline"},
            ],
            priority="high",
            metadata={DISCORD_FORUM_TAG_METADATA_KEY: action},
        )
    except Exception as exc:
        return ToolResult.text(
            f"discord_forum_tag_action: failed to create approval: {exc}",
            is_error=True,
        )

    return ToolResult.text(
        f"Discord mutation queued as {action_id}; approval "
        f"{result['notification_id']} is pending. No Discord state was changed."
    )


async def discord_project_task_status_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    """Advance one Discord project task through the agreed state machine."""
    if ctx.config is None:
        return ToolResult.text(
            "discord_project_task_status: Nerve config is unavailable.",
            is_error=True,
        )
    thread_id = str(args.get("thread_id") or _current_discord_target(ctx)).strip()
    if not thread_id:
        return ToolResult.text(
            "discord_project_task_status: this tool requires a Discord project thread.",
            is_error=True,
        )
    target_status = str(args.get("status") or "").strip().casefold()
    if target_status == "completed":
        return ToolResult.text(
            "discord_project_task_status: agents cannot complete project tasks "
            "directly; ask the user to run /close_task in the task thread.",
            is_error=True,
        )
    try:
        result = await asyncio.to_thread(
            transition_project_task_status,
            ctx.config,
            thread_id=thread_id,
            target_status=target_status,
            audit_reason=f"Nerve project task lifecycle {ctx.session_id[:32]}",
        )
    except DiscordProjectTaskStatusError as exc:
        return ToolResult.text(
            f"discord_project_task_status: {exc}",
            is_error=True,
        )
    except DiscordForumTagError as exc:
        return ToolResult.text(
            f"discord_project_task_status: {exc}",
            is_error=True,
        )
    return ToolResult.text(json.dumps(result, ensure_ascii=False, indent=2))


async def discord_project_task_create_handler(
    ctx: ToolContext,
    args: dict,
) -> ToolResult:
    """Create a new untagged task in a configured Discord project forum."""
    if ctx.config is None:
        return ToolResult.text(
            "discord_project_task_create: Nerve config is unavailable.",
            is_error=True,
        )
    if not ctx.config.discord.enabled:
        return ToolResult.text(
            "discord_project_task_create: Discord integration is disabled.",
            is_error=True,
        )
    if ctx.engine is None:
        return ToolResult.text(
            "discord_project_task_create: engine is unavailable.",
            is_error=True,
        )

    create_task = getattr(
        ctx.engine.router.get_channel("discord"), "create_project_task", None,
    )
    if not callable(create_task):
        return ToolResult.text(
            "discord_project_task_create: Discord channel is unavailable.",
            is_error=True,
        )

    try:
        task_id, _thread_id = await create_task(
            project=str(args.get("project") or ""),
            title=str(args.get("title") or ""),
            description=str(args.get("description") or ""),
        )
    except DiscordProjectTaskCreateError as exc:
        return ToolResult.text(
            f"discord_project_task_create: {exc}",
            is_error=True,
        )
    except Exception:
        logger.exception("Discord project-task creation failed")
        return ToolResult.text(
            "discord_project_task_create: Discord could not create the task.",
            is_error=True,
        )
    return ToolResult.text(
        f"Created Discord project task {task_id} in its configured forum."
    )


async def _require_project_task_auditor(ctx: ToolContext) -> str | None:
    if ctx.db is None or ctx.engine is None or ctx.config is None:
        return "Discord project-task audit tools are unavailable: engine not wired."
    session = await ctx.db.get_session(ctx.session_id)
    if (
        not session
        or session.get("source") != "cron"
        or not ctx.session_id.startswith("cron:project-task-auditor:")
    ):
        return (
            "This tool is restricted to the project-task-auditor cron session."
        )
    return None


def _discord_channel_for_audit(ctx: ToolContext):
    if ctx.engine is None:
        return None
    return ctx.engine.router.get_channel("discord")


async def discord_project_task_audit_handler(
    ctx: ToolContext, args: dict,
) -> ToolResult:
    denied = await _require_project_task_auditor(ctx)
    if denied:
        return ToolResult.text(denied, is_error=True)
    channel = _discord_channel_for_audit(ctx)
    audit = getattr(channel, "audit_project_tasks", None)
    if not callable(audit):
        audit = getattr(channel, "get_project_task_audit_batch", None)
    if not callable(audit):
        return ToolResult.text(
            "discord_project_task_audit: Discord channel is unavailable.",
            is_error=True,
        )
    try:
        result = await audit(
            limit=max(1, min(int(args.get("limit", 20) or 20), 50)),
        )
    except DiscordProjectTaskAuditError as exc:
        return ToolResult.text(
            f"discord_project_task_audit: {exc}", is_error=True,
        )
    except Exception:
        logger.exception("Discord project-task audit read failed")
        return ToolResult.text(
            "discord_project_task_audit: Discord state could not be read.",
            is_error=True,
        )
    return ToolResult.text(json.dumps(result, ensure_ascii=False, indent=2))


async def complete_discord_project_task_audit_handler(
    ctx: ToolContext, args: dict,
) -> ToolResult:
    denied = await _require_project_task_auditor(ctx)
    if denied:
        return ToolResult.text(denied, is_error=True)
    thread_id = str(args.get("thread_id") or "").strip()
    result_name = str(args.get("result") or args.get("outcome") or "").strip()
    summary = str(args.get("summary") or "").strip()
    follow_up_ids = [
        str(item).strip()
        for item in (args.get("follow_up_task_ids") or [])
        if str(item).strip()
    ]
    if not thread_id or not thread_id.isdigit():
        return ToolResult.text(
            "thread_id must be a numeric Discord thread ID", is_error=True,
        )
    if result_name not in {"verified", "follow-up-created"}:
        return ToolResult.text(
            "result must be 'verified' or 'follow-up-created'", is_error=True,
        )
    if not summary:
        return ToolResult.text("summary is required", is_error=True)
    if result_name == "follow-up-created" and not follow_up_ids:
        return ToolResult.text(
            "follow_up_task_ids is required for follow-up-created",
            is_error=True,
        )

    existing = await ctx.db.get_discord_project_task_audit(thread_id)
    if existing is not None:
        return ToolResult.text(json.dumps({
            "completed": True,
            "idempotent": True,
            "audit": existing,
        }, ensure_ascii=False))

    channel = _discord_channel_for_audit(ctx)
    read_task = getattr(channel, "read_project_task_for_audit", None)
    if not callable(read_task):
        read_task = getattr(channel, "get_project_task_audit_task", None)
    if not callable(read_task):
        return ToolResult.text(
            "complete_discord_project_task_audit: Discord channel is unavailable.",
            is_error=True,
        )
    try:
        task = await read_task(thread_id=thread_id)
    except DiscordProjectTaskAuditError as exc:
        return ToolResult.text(
            f"complete_discord_project_task_audit: {exc}", is_error=True,
        )
    except Exception:
        logger.exception("Discord project-task audit validation failed")
        return ToolResult.text(
            "complete_discord_project_task_audit: Discord state could not be read.",
            is_error=True,
        )
    if not task:
        return ToolResult.text(
            "complete_discord_project_task_audit: unknown or non-auditable task.",
            is_error=True,
        )

    completion = task.get("completion") or {}
    stored = await ctx.db.record_discord_project_task_audit(
        thread_id=thread_id,
        guild_id=ctx.config.discord.guild_id,
        project=str(task.get("project") or ""),
        completion_notification_id=str(completion.get("notification_id") or ""),
        completion_record=completion,
        result=result_name,
        summary=summary,
        follow_up_task_ids=follow_up_ids,
    )
    return ToolResult.text(json.dumps({
        "completed": True,
        "idempotent": False,
        "audit": stored,
    }, ensure_ascii=False))


DISCORD_FORUM_TAGS_SPEC = ToolSpec(
    name="discord_forum_tags",
    description=(
        "Read configured Discord project/audit-forum tags and, optionally, "
        "the tags applied to a forum thread. This is read-only and needs no "
        "approval. Use project=AUDIT for discord.audit_forum_id. When called "
        "from a Discord forum thread, project/thread can be inferred."
    ),
    input_schema=DISCORD_FORUM_TAGS_SCHEMA,
    handler=discord_forum_tags_handler,
)

DISCORD_PROJECT_TASK_STATUS_SPEC = ToolSpec(
    name="discord_project_task_status",
    description=(
        "Move a Discord project task through its configured lifecycle. "
        "Discord tags are the only task state; this applies one validated "
        "transition, preserves unrelated tags, and fails closed if status tags "
        "are missing or ambiguous."
    ),
    input_schema=DISCORD_PROJECT_TASK_STATUS_SCHEMA,
    handler=discord_project_task_status_handler,
)

DISCORD_PROJECT_TASK_CREATE_SPEC = ToolSpec(
    name="discord_project_task_create",
    description=(
        "Create a new untagged task in a configured Discord project forum. "
        "Use when the user asks to create a task or when an actionable problem "
        "should be captured for follow-up. The task starts in the new-task state."
    ),
    input_schema=DISCORD_PROJECT_TASK_CREATE_SCHEMA,
    handler=discord_project_task_create_handler,
)

DISCORD_PROJECT_TASK_AUDIT_SPEC = ToolSpec(
    name="discord_project_task_audit",
    description=(
        "Read the next bounded batch of completed Discord project tasks and "
        "their actual tags, archive state, starter message, session binding, "
        "and limited untrusted transcript evidence. Cron-only."
    ),
    input_schema=DISCORD_PROJECT_TASK_AUDIT_SCHEMA,
    handler=discord_project_task_audit_handler,
)

COMPLETE_DISCORD_PROJECT_TASK_AUDIT_SPEC = ToolSpec(
    name="complete_discord_project_task_audit",
    description=(
        "Record an evidence-based audit result for one task returned by "
        "discord_project_task_audit. Cron-only and idempotent."
    ),
    input_schema=COMPLETE_DISCORD_PROJECT_TASK_AUDIT_SCHEMA,
    handler=complete_discord_project_task_audit_handler,
)

DISCORD_FORUM_TAG_ACTION_SPEC = ToolSpec(
    name="discord_forum_tag_action",
    description=(
        "Request creation, update, deletion, assignment, removal, or replacement "
        "of Discord project/audit-forum tags. Use project=AUDIT for "
        "discord.audit_forum_id. This tool NEVER mutates Discord directly: every "
        "call creates a mandatory approval, and a server-side dispatcher executes "
        "only after the user selects Approve. Use discord_forum_tags first when "
        "tag IDs are unknown."
    ),
    input_schema=DISCORD_FORUM_TAG_ACTION_SCHEMA,
    handler=discord_forum_tag_action_handler,
)

DISCORD_SPECS = [
    DISCORD_FORUM_TAGS_SPEC,
    DISCORD_PROJECT_TASK_STATUS_SPEC,
    DISCORD_PROJECT_TASK_CREATE_SPEC,
    DISCORD_PROJECT_TASK_AUDIT_SPEC,
    COMPLETE_DISCORD_PROJECT_TASK_AUDIT_SPEC,
    DISCORD_FORUM_TAG_ACTION_SPEC,
]
