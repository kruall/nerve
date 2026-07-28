"""Discord project-forum management tools.

``discord_forum_tags`` is read-only. ``discord_forum_tag_action`` never
mutates Discord directly: it prepares a fully described action and files an
approval notification whose server-side dispatcher performs the mutation
only after an explicit Approve decision.
"""

from __future__ import annotations

import asyncio
import json
import uuid

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    DISCORD_FORUM_TAG_ACTION_SCHEMA,
    DISCORD_FORUM_TAGS_SCHEMA,
)
from nerve.discord_tags import (
    DISCORD_FORUM_TAG_METADATA_KEY,
    DISCORD_FORUM_TAG_TARGET_KIND,
    DiscordForumTagError,
    DiscordForumTagManager,
)


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


DISCORD_FORUM_TAGS_SPEC = ToolSpec(
    name="discord_forum_tags",
    description=(
        "Read configured Discord project-forum tags and, optionally, the tags "
        "applied to a forum thread. This is read-only and needs no approval. "
        "When called from a Discord forum thread, project/thread can be inferred."
    ),
    input_schema=DISCORD_FORUM_TAGS_SCHEMA,
    handler=discord_forum_tags_handler,
)

DISCORD_FORUM_TAG_ACTION_SPEC = ToolSpec(
    name="discord_forum_tag_action",
    description=(
        "Request creation, update, deletion, assignment, removal, or replacement "
        "of Discord project-forum tags. This tool NEVER mutates Discord directly: "
        "every call creates a mandatory approval, and a server-side dispatcher "
        "executes only after the user selects Approve. Use discord_forum_tags "
        "first when tag IDs are unknown."
    ),
    input_schema=DISCORD_FORUM_TAG_ACTION_SCHEMA,
    handler=discord_forum_tag_action_handler,
)

DISCORD_SPECS = [
    DISCORD_FORUM_TAGS_SPEC,
    DISCORD_FORUM_TAG_ACTION_SPEC,
]
