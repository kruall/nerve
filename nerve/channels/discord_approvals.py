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

logger = logging.getLogger(__name__)

_THREAD_NAME = "Approvals"
_THREAD_INTRO = (
    "Nerve approval inbox. Pending plans and protected actions appear here. "
    "Approve executes immediately; decline and request-changes actions ask "
    "for written feedback first."
)
_MAX_MESSAGE_LENGTH = 2000
_MAX_ACTION_CARD_LENGTH = 1500
_FEEDBACK_DECISIONS = frozenset({"decline", "revise", "request_changes"})

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


class ApprovalFeedbackModal(discord.ui.Modal):
    """Collect decline/revision feedback before dispatching the decision."""

    def __init__(
        self,
        inbox: "DiscordApprovalInbox",
        notification_id: str,
        decision: str,
        source_message: discord.Message,
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
        self.feedback = discord.ui.TextInput(
            label="What should change?" if decision != "decline" else "Reason",
            placeholder=(
                "Describe the required changes"
                if decision != "decline"
                else "Optional: explain why this is declined"
            ),
            style=discord.TextStyle.paragraph,
            required=decision != "decline",
            max_length=2000,
        )
        self.add_item(self.feedback)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        feedback = str(self.feedback.value or "").strip()
        success = await self.inbox.answer(
            interaction=interaction,
            notification_id=self.notification_id,
            decision=self.decision,
            feedback=feedback,
            source_message=self.source_message,
        )
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
            ))
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


class ApprovalView(discord.ui.View):
    """Persistent view reconstructed from a notification row."""

    def __init__(
        self,
        inbox: "DiscordApprovalInbox",
        notification_id: str,
        options: list[str],
        labels: dict[str, str],
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.inbox = inbox
        self.notification_id = notification_id
        self.options = list(options)
        self.labels = dict(labels)
        for value in options[:5]:
            self.add_item(ApprovalButton(
                notification_id,
                value,
                _safe_label(value, labels),
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

            thread = await self._find_thread(guild, forum)
            if thread is None:
                created = await forum.create_thread(
                    name=_THREAD_NAME,
                    content=_THREAD_INTRO,
                    auto_archive_duration=10080,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason="Create Nerve approval inbox",
                )
                thread = created.thread

            self._thread = await thread.edit(
                archived=False,
                pinned=True,
                reason="Pin Nerve approval inbox",
            )
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
        """Post one approval card and persist its Discord coordinates."""
        thread = await self._ensure_thread()
        options = _option_values(row)
        labels = _option_labels(row)
        if not options:
            raise ValueError("Discord approval has no options")
        view = ApprovalView(self, row["id"], options, labels)
        content = self._render_card(row)
        if len(content) > _MAX_ACTION_CARD_LENGTH:
            for chunk in _split_message(content):
                await thread.send(
                    chunk,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            target_kind = str(row.get("target_kind") or "").strip()
            target_id = str(row.get("target_id") or "").strip()
            title = str(row.get("title") or "Approval")[:500]
            content = (
                f"**Decision required: {title}**"
                "\n\nFull details are in the messages immediately above."
            )
            if target_kind or target_id:
                content += f"\n\n`{target_kind}:{target_id}`"
        message = await thread.send(
            content,
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )

        meta = _metadata(row)
        meta["discord_approval"] = {
            "thread_id": str(thread.id),
            "message_id": str(message.id),
        }
        await self.db.update_notification(
            row["id"], metadata=json.dumps(meta),
        )
        return str(message.id)

    async def _restore_pending_views(self) -> None:
        rows = await self.db.list_notifications(
            status="pending",
            type="approval",
            limit=500,
        )
        for row in rows:
            coords = _metadata(row).get("discord_approval")
            if not isinstance(coords, dict):
                continue
            try:
                message_id = int(coords["message_id"])
            except (KeyError, TypeError, ValueError):
                continue
            options = _option_values(row)
            if not options:
                continue
            self.client.add_view(
                ApprovalView(
                    self,
                    row["id"],
                    options,
                    _option_labels(row),
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
            options = _option_values(row or {})
            labels = _option_labels(row or {})
            status = _safe_label(decision, labels)
            suffix = f"\n\n**Decision:** {status} by <@{interaction.user.id}>"
            source_content = str(source_message.content or "")
            if feedback:
                room = (
                    _MAX_MESSAGE_LENGTH
                    - len(source_content)
                    - len(suffix)
                    - len("\n**Feedback:** ")
                )
                if room > 0:
                    rendered_feedback = feedback[:room]
                    if len(rendered_feedback) < len(feedback):
                        rendered_feedback = rendered_feedback[:-1] + "…"
                    suffix += f"\n**Feedback:** {rendered_feedback}"
            content = source_content
            if "**Decision:**" not in content:
                content = content + suffix
            await source_message.edit(
                content=content,
                view=ApprovalView(
                    self,
                    notification_id,
                    options,
                    labels,
                    disabled=True,
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return True

    @staticmethod
    def _render_card(row: dict[str, Any]) -> str:
        priority = str(row.get("priority") or "normal")
        prefix = {"urgent": "🚨 ", "high": "⚠️ "}.get(priority, "")
        title = str(row.get("title") or "Approval required").strip()
        body = str(row.get("body") or "").strip()
        target_kind = str(row.get("target_kind") or "").strip()
        target_id = str(row.get("target_id") or "").strip()
        parts = [f"{prefix}**{title}**"]
        if body:
            parts.append(body)
        if target_kind or target_id:
            parts.append(f"`{target_kind}:{target_id}`")
        text = "\n\n".join(parts)
        return text
