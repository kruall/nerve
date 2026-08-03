"""Discord approval inbox with persistent buttons and feedback modals.

All actionable approvals are collected in one pinned forum post under the
configured Discord audit forum.  Views are restored from notification
metadata after a restart, so an outstanding approval does not turn into a
dead set of buttons when Nerve reconnects.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import discord

from nerve.channels.discord_inbox import (
    resolve_inbox_tag,
    tags_with_inbox_tag,
)

logger = logging.getLogger(__name__)

_THREAD_NAME = "Approvals"
_THREAD_INTRO = (
    "Nerve approval inbox. Pending plans and protected actions appear here. "
    "Approve executes immediately; decline and request-changes actions ask "
    "for written feedback first."
)
_MAX_MESSAGE_LENGTH = 2000
_MAX_ACTION_CARD_LENGTH = 1500
_MAX_PLAN_SUMMARY_LENGTH = 600
_MAX_EMBED_TITLE_LENGTH = 256
_MAX_EMBED_DESCRIPTION_LENGTH = 4096
_MAX_EMBED_FIELD_VALUE_LENGTH = 1024
_FEEDBACK_DECISIONS = frozenset({"decline", "revise", "request_changes"})
_TASK_COMPLETION_TARGET_KIND = "discord-project-task-completion"
_TASK_RECOVERY_TARGET_KIND = "discord-project-task-recovery"
_DISPATCH_OUTCOME_KEY = "approval_dispatch"

_BUTTON_STYLES = {
    "approve": discord.ButtonStyle.success,
    "decline": discord.ButtonStyle.danger,
    "revise": discord.ButtonStyle.primary,
    "request_changes": discord.ButtonStyle.primary,
    "snooze_24h": discord.ButtonStyle.secondary,
}
_BUTTON_EMOJIS = {
    "approve": "✅",
    "decline": "❌",
    "revise": "✏️",
    "request_changes": "✏️",
    "snooze_24h": "💤",
}
_PRIORITY_COLOURS = {
    "urgent": discord.Colour.red(),
    "high": discord.Colour.orange(),
    "normal": discord.Colour.blurple(),
    "low": discord.Colour.light_grey(),
}


def _metadata(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("metadata")
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _option_labels(row: dict[str, Any]) -> dict[str, str]:
    labels = _metadata(row).get("option_labels")
    if not isinstance(labels, dict):
        return {}
    return {
        str(value): str(label)
        for value, label in labels.items()
        if value and label
    }


def _option_values(row: dict[str, Any]) -> list[str]:
    raw = row.get("options")
    if isinstance(raw, str) and raw:
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw if str(value)]


def _delivery_coordinates(row: dict[str, Any]) -> list[dict[str, Any]]:
    """Return every persisted Discord card for one approval notification."""
    metadata = _metadata(row)
    coordinates = []
    for key in (
        "discord_approval",
        "discord_project_task_completion",
        "discord_project_task_recovery",
    ):
        value = metadata.get(key)
        if isinstance(value, dict):
            coordinates.append(value)
    return coordinates


def _plan_summary(row: dict[str, Any]) -> str:
    """Return the model-authored brief description for a plan approval."""
    return str(_metadata(row).get("plan_summary") or "").strip()[
        :_MAX_PLAN_SUMMARY_LENGTH
    ]


def _safe_label(value: str, labels: dict[str, str]) -> str:
    label = labels.get(value) or value.replace("_", " ").title()
    return label[:80]


def _split_message(text: str) -> list[str]:
    remaining = text.strip()
    chunks: list[str] = []
    while remaining:
        if len(remaining) <= _MAX_MESSAGE_LENGTH:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, _MAX_MESSAGE_LENGTH + 1)
        if cut < _MAX_MESSAGE_LENGTH // 2:
            cut = remaining.rfind(" ", 0, _MAX_MESSAGE_LENGTH + 1)
        if cut <= 0:
            cut = _MAX_MESSAGE_LENGTH
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    return chunks


def _append_embed_status(
    source_message: discord.Message,
    label: str,
    value: str,
) -> discord.Embed | None:
    """Copy an embed card and append one terminal decision status.

    Legacy content-based cards remain actionable after an upgrade: their
    handlers receive ``None`` and retain the existing text-edit path.
    """
    embeds = getattr(source_message, "embeds", ())
    if not isinstance(embeds, (list, tuple)):
        return None
    source = next(
        (embed for embed in embeds if isinstance(embed, discord.Embed)),
        None,
    )
    if source is None:
        return None
    card = source.copy()
    for index, field in enumerate(card.fields):
        if field.name == label:
            card.set_field_at(
                index,
                name=label,
                value=str(value)[:_MAX_EMBED_FIELD_VALUE_LENGTH] or "—",
                inline=False,
            )
            return card
    card.add_field(
        name=label,
        value=str(value)[:_MAX_EMBED_FIELD_VALUE_LENGTH] or "—",
        inline=False,
    )
    return card


class ApprovalFeedbackModal(discord.ui.Modal):
    """Collect decline/revision feedback before dispatching the decision."""

    def __init__(
        self,
        inbox: "DiscordApprovalInbox",
        notification_id: str,
        decision: str,
        source_message: discord.Message,
        *,
        feedback_required: bool = False,
        suppress_ephemeral_outcome: bool = False,
    ) -> None:
        label = (
            "Request changes"
            if decision in {"revise", "request_changes"}
            else "Decline approval"
        )
        super().__init__(
            title=label,
            custom_id=f"nerve:approval:feedback:{notification_id}:{decision}",
            timeout=900,
        )
        self.inbox = inbox
        self.notification_id = notification_id
        self.decision = decision
        self.source_message = source_message
        self.suppress_ephemeral_outcome = suppress_ephemeral_outcome
        self.feedback = discord.ui.TextInput(
            label="What should change?" if decision != "decline" else "Reason",
            placeholder=(
                "Describe the required changes"
                if decision != "decline"
                else (
                    "Explain why this task should remain open"
                    if feedback_required
                    else "Optional: explain why this is declined"
                )
            ),
            style=discord.TextStyle.paragraph,
            required=decision != "decline" or feedback_required,
            max_length=2000,
        )
        self.add_item(self.feedback)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if self.suppress_ephemeral_outcome:
            await interaction.response.defer()
        else:
            await interaction.response.defer(ephemeral=True)
        feedback = str(self.feedback.value or "").strip()
        success = await self.inbox.answer(
            interaction=interaction,
            notification_id=self.notification_id,
            decision=self.decision,
            feedback=feedback,
            source_message=self.source_message,
        )
        if self.suppress_ephemeral_outcome:
            return
        if success:
            await interaction.followup.send(
                "Decision recorded.", ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "This approval is no longer pending.", ephemeral=True,
            )

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
    ) -> None:
        logger.exception("Discord approval modal failed", exc_info=error)
        if interaction.response.is_done():
            await interaction.followup.send(
                "Failed to record the decision.", ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "Failed to record the decision.", ephemeral=True,
            )


class ApprovalButton(discord.ui.Button["ApprovalView"]):
    """One persistent approval decision button."""

    def __init__(
        self,
        notification_id: str,
        decision: str,
        label: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=_BUTTON_STYLES.get(
                decision, discord.ButtonStyle.secondary,
            ),
            label=label,
            emoji=_BUTTON_EMOJIS.get(decision),
            custom_id=f"nerve:approval:{notification_id}:{decision}",
            disabled=disabled,
            row=0,
        )
        self.notification_id = notification_id
        self.decision = decision

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to act on Nerve approvals.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.message, discord.Message):
            await interaction.response.send_message(
                "The approval message is unavailable.", ephemeral=True,
            )
            return
        if self.decision in _FEEDBACK_DECISIONS:
            await interaction.response.send_modal(ApprovalFeedbackModal(
                view.inbox,
                self.notification_id,
                self.decision,
                interaction.message,
                feedback_required=(
                    self.decision == "decline"
                    and view.target_kind == _TASK_COMPLETION_TARGET_KIND
                ),
                suppress_ephemeral_outcome=(
                    view.target_kind == _TASK_COMPLETION_TARGET_KIND
                ),
            ))
            return

        if view.target_kind == _TASK_COMPLETION_TARGET_KIND:
            completed = _append_embed_status(
                interaction.message,
                "Status",
                "✅ Completed",
            )
            edit_kwargs: dict[str, Any] = {
                "view": None,
                "allowed_mentions": discord.AllowedMentions.none(),
            }
            if completed is not None:
                edit_kwargs["embed"] = completed
            await interaction.response.edit_message(**edit_kwargs)
            success = await view.inbox.answer(
                interaction=interaction,
                notification_id=self.notification_id,
                decision=self.decision,
                feedback="",
                source_message=interaction.message,
                source_card_preclosed=True,
            )
            if not success:
                failed = _append_embed_status(
                    interaction.message,
                    "Status",
                    "❌ Not completed: approval is no longer pending",
                )
                if failed is not None:
                    await interaction.message.edit(
                        embed=failed,
                        view=None,
                        allowed_mentions=discord.AllowedMentions.none(),
                    )
            return

        await interaction.response.defer(ephemeral=True)
        success = await view.inbox.answer(
            interaction=interaction,
            notification_id=self.notification_id,
            decision=self.decision,
            feedback="",
            source_message=interaction.message,
        )
        if success:
            await interaction.followup.send(
                "Decision recorded.", ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "This approval is no longer pending.", ephemeral=True,
            )


class ApprovalPlanButton(discord.ui.Button["ApprovalView"]):
    """Show the full plan privately without expanding the public card."""

    def __init__(
        self,
        notification_id: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Show plan",
            emoji="📄",
            custom_id=f"nerve:approval:{notification_id}:show_plan",
            disabled=disabled,
            row=1,
        )
        self.notification_id = notification_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to view Nerve approval details.",
                ephemeral=True,
            )
            return

        row = await view.inbox.db.get_notification(self.notification_id)
        body = str((row or {}).get("body") or "").strip()
        if not body:
            await interaction.response.send_message(
                "Plan details are unavailable.", ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        title = str(
            (row or {}).get("title") or "Plan details"
        ).strip()
        chunks = _split_message(f"**{title}**\n\n{body}")
        for chunk in chunks:
            await interaction.followup.send(
                chunk,
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )


class ApprovalDescribeButton(discord.ui.Button["ApprovalView"]):
    """Show the model-authored plan overview privately."""

    def __init__(
        self,
        notification_id: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Describe",
            emoji="🧭",
            custom_id=f"nerve:approval:{notification_id}:describe",
            disabled=disabled,
            row=1,
        )
        self.notification_id = notification_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to view Nerve approval details.",
                ephemeral=True,
            )
            return

        row = await view.inbox.db.get_notification(self.notification_id)
        summary = _plan_summary(row or {})
        if not summary:
            await interaction.response.send_message(
                "A brief description is unavailable for this plan.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"**Plan overview**\n\n{summary}",
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )


class ApprovalView(discord.ui.View):
    """Persistent view reconstructed from a notification row."""

    def __init__(
        self,
        inbox: "DiscordApprovalInbox",
        notification_id: str,
        options: list[str],
        labels: dict[str, str],
        *,
        target_kind: str = "",
        disabled: bool = False,
        show_plan: bool = False,
        show_describe: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.inbox = inbox
        self.notification_id = notification_id
        self.options = list(options)
        self.labels = dict(labels)
        self.target_kind = target_kind
        for value in options[:5]:
            self.add_item(ApprovalButton(
                notification_id,
                value,
                _safe_label(value, labels),
                disabled=disabled,
            ))
        if show_plan:
            self.add_item(ApprovalPlanButton(
                notification_id,
                disabled=disabled,
            ))
        if show_describe:
            self.add_item(ApprovalDescribeButton(
                notification_id,
                disabled=disabled,
            ))


class DiscordApprovalInbox:
    """Own the pinned audit-forum thread and its actionable messages."""

    def __init__(
        self,
        *,
        client: discord.Client,
        db: Any,
        notification_service: Any,
        guild_id: int,
        forum_id: int,
        allowed_author_ids: set[int],
    ) -> None:
        self.client = client
        self.db = db
        self.notification_service = notification_service
        self.guild_id = guild_id
        self.forum_id = forum_id
        self.allowed_author_ids = set(allowed_author_ids)
        self._thread: discord.Thread | None = None
        self._thread_lock = asyncio.Lock()
        self._answer_locks: dict[str, asyncio.Lock] = {}

    async def start(self, guild: discord.Guild) -> None:
        await self._ensure_thread(guild)
        await self._restore_pending_views()

    def interaction_allowed(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        return bool(
            guild is not None
            and int(guild.id) == self.guild_id
            and int(interaction.user.id) in self.allowed_author_ids
        )

    async def _ensure_thread(
        self,
        guild: discord.Guild | None = None,
    ) -> discord.Thread:
        if self._thread is not None:
            if self._thread.archived:
                self._thread = await self._thread.edit(
                    archived=False,
                    pinned=True,
                    reason="Restore Nerve approval inbox",
                )
            return self._thread

        async with self._thread_lock:
            if self._thread is not None:
                return self._thread
            guild = guild or self.client.get_guild(self.guild_id)
            if guild is None:
                raise RuntimeError("Discord approval guild is unavailable")
            forum = guild.get_channel(self.forum_id)
            if not isinstance(forum, discord.ForumChannel):
                raise RuntimeError(
                    "Discord approval inbox requires audit_forum_id "
                    "to identify a forum channel"
                )

            inbox_tag = resolve_inbox_tag(forum)
            thread = await self._find_thread(guild, forum)
            if thread is None:
                create_kwargs: dict[str, Any] = {}
                if inbox_tag is not None:
                    create_kwargs["applied_tags"] = [inbox_tag]
                created = await forum.create_thread(
                    name=_THREAD_NAME,
                    content=_THREAD_INTRO,
                    auto_archive_duration=10080,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason="Create Nerve approval inbox",
                    **create_kwargs,
                )
                thread = created.thread

            edit_kwargs: dict[str, Any] = {
                "archived": False,
                "pinned": True,
                "reason": "Pin Nerve approval inbox",
            }
            applied_tags = tags_with_inbox_tag(thread, inbox_tag)
            if applied_tags is not None:
                edit_kwargs["applied_tags"] = applied_tags
            self._thread = await thread.edit(**edit_kwargs)
            return self._thread

    async def _find_thread(
        self,
        guild: discord.Guild,
        forum: discord.ForumChannel,
    ) -> discord.Thread | None:
        active = await guild.active_threads()
        for thread in active:
            if (
                int(getattr(thread, "parent_id", 0) or 0) == self.forum_id
                and getattr(thread, "name", "") == _THREAD_NAME
            ):
                return thread
        async for thread in forum.archived_threads(limit=100):
            if getattr(thread, "name", "") == _THREAD_NAME:
                return thread
        return None

    async def deliver(self, row: dict[str, Any]) -> str:
        """Post one approval card to the pinned audit thread."""
        return await self.deliver_to_thread(
            row,
            await self._ensure_thread(),
            metadata_key="discord_approval",
        )

    async def deliver_to_thread(
        self,
        row: dict[str, Any],
        thread: discord.Thread,
        *,
        metadata_key: str,
    ) -> str:
        """Post one restart-safe approval card in an explicitly resolved thread."""
        metadata = _metadata(row)
        existing = metadata.get(metadata_key)
        if isinstance(existing, dict):
            message_id = str(existing.get("message_id") or "").strip()
            if message_id:
                return message_id
        options = _option_values(row)
        labels = _option_labels(row)
        if not options:
            raise ValueError("Discord approval has no options")
        show_plan = self._has_plan_details(row)
        show_describe = bool(_plan_summary(row))
        view = ApprovalView(
            self,
            row["id"],
            options,
            labels,
            target_kind=str(row.get("target_kind") or "").strip(),
            show_plan=show_plan,
            show_describe=show_describe,
        )
        content = self._render_card(row)
        if len(content) > _MAX_ACTION_CARD_LENGTH:
            for chunk in _split_message(content):
                await thread.send(
                    embed=discord.Embed(
                        description=chunk[:_MAX_EMBED_DESCRIPTION_LENGTH],
                        colour=_PRIORITY_COLOURS.get(
                            str(row.get("priority") or "normal"),
                            discord.Colour.blurple(),
                        ),
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            compact = True
        else:
            compact = False
        message = await thread.send(
            embed=self._card_embed(row, compact=compact),
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )

        metadata[metadata_key] = {
            "thread_id": str(thread.id),
            "message_id": str(message.id),
        }
        await self.db.update_notification(
            row["id"], metadata=json.dumps(metadata),
        )
        return str(message.id)

    async def _restore_pending_views(self) -> None:
        rows = await self.db.list_notifications(
            status="pending",
            type="approval",
            limit=500,
        )
        for row in rows:
            options = _option_values(row)
            if not options:
                continue
            for coords in _delivery_coordinates(row):
                try:
                    message_id = int(coords["message_id"])
                except (KeyError, TypeError, ValueError):
                    continue
                self.client.add_view(
                    ApprovalView(
                        self,
                        row["id"],
                        options,
                        _option_labels(row),
                        target_kind=str(
                            row.get("target_kind") or "",
                        ).strip(),
                        show_plan=self._has_plan_details(row),
                        show_describe=bool(_plan_summary(row)),
                    ),
                    message_id=message_id,
                )

    async def answer(
        self,
        *,
        interaction: discord.Interaction,
        notification_id: str,
        decision: str,
        feedback: str,
        source_message: discord.Message,
        source_card_preclosed: bool = False,
    ) -> bool:
        """Serialize one decision, dispatch it, then close the card."""
        if not self.interaction_allowed(interaction):
            return False
        lock = self._answer_locks.setdefault(
            notification_id, asyncio.Lock(),
        )
        async with lock:
            success = await self.notification_service.handle_answer(
                notification_id=notification_id,
                answer=decision,
                answered_by=f"discord:{interaction.user.id}",
                feedback=feedback,
            )
            if not success:
                return False

            row = await self.db.get_notification(notification_id)
            await self._close_cards(
                row or {},
                notification_id=notification_id,
                decision=decision,
                feedback=feedback,
                actor_id=int(interaction.user.id),
                source_message=source_message,
                source_card_preclosed=source_card_preclosed,
            )
            return True

    async def _close_cards(
        self,
        row: dict[str, Any],
        *,
        notification_id: str,
        decision: str,
        feedback: str,
        actor_id: int,
        source_message: discord.Message,
        source_card_preclosed: bool,
    ) -> None:
        """Render a terminal outcome and deactivate every delivered copy."""
        source_id = str(getattr(source_message, "id", "") or "")
        dispatch_outcome = _metadata(row).get(_DISPATCH_OUTCOME_KEY)
        task_completion_succeeded = (
            str(row.get("target_kind") or "").strip()
            == _TASK_COMPLETION_TARGET_KIND
            and decision == "approve"
            and isinstance(dispatch_outcome, dict)
            and bool(dispatch_outcome.get("ok"))
        )
        task_card = _metadata(row).get("discord_project_task_completion")
        source_is_task_card = (
            isinstance(task_card, dict)
            and str(task_card.get("message_id") or "") == source_id
        )
        source_was_preclosed_task_card = (
            task_completion_succeeded
            and source_card_preclosed
            and source_is_task_card
        )
        if not source_was_preclosed_task_card:
            await self._close_one_card(
                source_message,
                row,
                notification_id=notification_id,
                decision=decision,
                feedback=feedback,
                actor_id=actor_id,
            )
        for coords in _delivery_coordinates(row):
            message_id = str(coords.get("message_id") or "")
            if not message_id or message_id == source_id:
                continue
            if (
                source_was_preclosed_task_card
                and isinstance(task_card, dict)
                and str(task_card.get("message_id") or "") == message_id
            ):
                # The original interaction has already changed this card
                # before the dispatcher archives its task thread. Never
                # reopen a completed task only to edit it afterwards.
                continue
            try:
                thread_id = int(coords["thread_id"])
                thread = self.client.get_channel(thread_id)
                if thread is None:
                    thread = await self.client.fetch_channel(thread_id)
                message = await thread.fetch_message(int(message_id))
            except Exception as exc:  # one stale copy must not block another
                logger.warning(
                    "Could not fetch Discord approval copy %s/%s: %s",
                    coords.get("thread_id"), message_id, exc,
                )
                continue
            await self._close_one_card(
                message,
                row,
                notification_id=notification_id,
                decision=decision,
                feedback=feedback,
                actor_id=actor_id,
            )

    async def _close_one_card(
        self,
        message: discord.Message,
        row: dict[str, Any],
        *,
        notification_id: str,
        decision: str,
        feedback: str,
        actor_id: int,
    ) -> None:
        label, value, suffix = self._decision_outcome(
            row,
            decision=decision,
            feedback=feedback,
            actor_id=actor_id,
        )
        embed = _append_embed_status(message, label, value)
        edit_kwargs: dict[str, Any] = {
            # A terminal approval is no longer actionable. Remove its view
            # altogether instead of leaving inert controls in the card.
            "view": None,
            "allowed_mentions": discord.AllowedMentions.none(),
        }
        if embed is not None:
            if feedback and label == "Decision":
                embed.add_field(
                    name="Feedback",
                    value=feedback[:_MAX_EMBED_FIELD_VALUE_LENGTH] or "—",
                    inline=False,
                )
            edit_kwargs["embed"] = embed
        else:
            content = str(getattr(message, "content", "") or "")
            if suffix not in content:
                room = _MAX_MESSAGE_LENGTH - len(content)
                if room > 0:
                    edit_kwargs["content"] = content + suffix[:room]
        try:
            await message.edit(**edit_kwargs)
            return
        except discord.HTTPException as exc:
            if getattr(exc, "code", None) != 50083:
                logger.warning(
                    "Could not close Discord approval card %s: %s",
                    getattr(message, "id", "unknown"), exc,
                )
                return

            if self._is_successfully_archived_task_card(
                message,
                row,
                decision=decision,
            ):
                logger.info(
                    "Leaving archived Discord task completion card %s "
                    "unchanged after successful dispatch",
                    getattr(message, "id", "unknown"),
                )
                return

        # Legacy or unrelated archived approval cards may still need a brief
        # reopen to record their terminal result. A successfully dispatched
        # task-completion card was handled above without changing its archive
        # state.
        thread = getattr(message, "channel", None)
        if not bool(getattr(thread, "archived", False)):
            logger.warning(
                "Could not close archived Discord approval card %s: "
                "its thread is unavailable",
                getattr(message, "id", "unknown"),
            )
            return
        try:
            await thread.edit(
                archived=False,
                reason="Record Nerve task completion outcome",
            )
            await message.edit(**edit_kwargs)
        except Exception as exc:
            logger.warning(
                "Could not update archived Discord approval card %s: %s",
                getattr(message, "id", "unknown"), exc,
            )
        finally:
            try:
                await thread.edit(
                    archived=True,
                    reason="Archive completed Nerve project task",
                )
            except Exception as exc:
                logger.warning(
                    "Could not re-archive completed Discord task thread: %s",
                    exc,
                )

    @staticmethod
    def _is_successfully_archived_task_card(
        message: discord.Message,
        row: dict[str, Any],
        *,
        decision: str,
    ) -> bool:
        if (
            str(row.get("target_kind") or "").strip()
            != _TASK_COMPLETION_TARGET_KIND
            or decision != "approve"
        ):
            return False
        outcome = _metadata(row).get(_DISPATCH_OUTCOME_KEY)
        if not isinstance(outcome, dict) or not bool(outcome.get("ok")):
            return False
        target_id = str(row.get("target_id") or "").strip()
        channel_id = str(
            getattr(getattr(message, "channel", None), "id", "") or "",
        ).strip()
        return bool(target_id) and target_id == channel_id

    @staticmethod
    def _decision_outcome(
        row: dict[str, Any],
        *,
        decision: str,
        feedback: str,
        actor_id: int,
    ) -> tuple[str, str, str]:
        target_kind = str(row.get("target_kind") or "").strip()
        if target_kind == _TASK_COMPLETION_TARGET_KIND:
            outcome = _metadata(row).get(_DISPATCH_OUTCOME_KEY)
            dispatch_ok = (
                isinstance(outcome, dict) and bool(outcome.get("ok"))
            )
            if decision == "approve" and dispatch_ok:
                return "Status", "✅ Completed", "\n\n✅ **Completed.**"
            if decision == "decline":
                reason = feedback or str(
                    _metadata(row).get("decision_feedback") or "",
                ).strip()
                if not reason:
                    reason = "Completion was declined."
            elif isinstance(outcome, dict):
                reason = str(outcome.get("error") or "").strip()
            else:
                reason = ""
            if not reason:
                reason = "The completion action failed."
            return (
                "Status",
                f"❌ Not completed: {reason}",
                f"\n\n❌ **Not completed:** {reason}",
            )

        status = _safe_label(decision, _option_labels(row))
        suffix = f"\n\n**Decision:** {status} by <@{actor_id}>"
        if feedback:
            suffix += f"\n**Feedback:** {feedback}"
        return "Decision", f"{status} by <@{actor_id}>", suffix

    @staticmethod
    def _has_plan_details(row: dict[str, Any]) -> bool:
        return (
            str(row.get("target_kind") or "").strip() == "plan"
            and bool(str(row.get("body") or "").strip())
        )

    @staticmethod
    def _render_card(row: dict[str, Any]) -> str:
        priority = str(row.get("priority") or "normal")
        prefix = {"urgent": "🚨 ", "high": "⚠️ "}.get(priority, "")
        title = str(row.get("title") or "Approval required").strip()
        body = str(row.get("body") or "").strip()
        target_kind = str(row.get("target_kind") or "").strip()
        target_id = str(row.get("target_id") or "").strip()
        parts = [f"{prefix}**{title}**"]
        if body and target_kind == "plan":
            actions = "**Show plan** to view the full plan privately"
            if _plan_summary(row):
                actions = "**Describe** for a short overview or " + actions
            parts.append(f"Use {actions}.")
        elif body:
            parts.append(body)
        if (
            (target_kind or target_id)
            and target_kind != "discord-project-task-completion"
        ):
            parts.append(f"`{target_kind}:{target_id}`")
        text = "\n\n".join(parts)
        return text

    @staticmethod
    def _card_embed(
        row: dict[str, Any],
        *,
        compact: bool = False,
    ) -> discord.Embed:
        """Render a card for the persistent audit approval inbox only."""
        priority = str(row.get("priority") or "normal")
        prefix = {"urgent": "🚨 ", "high": "⚠️ "}.get(priority, "")
        title = str(row.get("title") or "Approval required").strip()
        title = title or "Approval required"
        body = str(row.get("body") or "").strip()
        target_kind = str(row.get("target_kind") or "").strip()
        if compact:
            description = (
                "Decision required. Full details are in the embeds "
                "immediately above."
            )
        elif body and target_kind == "plan":
            description = "Use **Show plan** to view the full plan privately."
            if _plan_summary(row):
                description = (
                    "Use **Describe** for a short overview or **Show plan** "
                    "to view the full plan privately."
                )
        else:
            description = body or None
        card = discord.Embed(
            title=(prefix + title)[:_MAX_EMBED_TITLE_LENGTH],
            description=(
                description[:_MAX_EMBED_DESCRIPTION_LENGTH]
                if description
                else None
            ),
            colour=_PRIORITY_COLOURS.get(priority, discord.Colour.blurple()),
        )
        target_id = str(row.get("target_id") or "").strip()
        if (
            (target_kind or target_id)
            and target_kind != _TASK_COMPLETION_TARGET_KIND
        ):
            card.set_footer(text=f"{target_kind}:{target_id}"[:2048])
        return card
