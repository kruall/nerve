"""Discord inboxes for notifications and questions.

The configured Discord audit forum contains one thread per notification kind.
This module owns the ``Notifications`` and ``Questions`` threads; approvals
keep their richer dispatcher-specific UI in :mod:`discord_approvals`.

Pending interactive views are reconstructed from notification metadata after
a restart so questions and dismiss buttons remain usable.
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

_THREADS = {
    "notify": (
        "Notifications",
        "Nerve notification inbox. Informational alerts appear here and can "
        "be dismissed after they are reviewed.",
    ),
    "question": (
        "Questions",
        "Nerve question inbox. Choose a suggested answer or use "
        "**Write answer** to send a free-form response.",
    ),
}
_MAX_MESSAGE_LENGTH = 2000
_MAX_ACTION_CARD_LENGTH = 1500
_MAX_QUESTION_OPTIONS = 20


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


def _append_status(content: str, label: str, value: str) -> str:
    prefix = f"\n\n**{label}:** "
    room = _MAX_MESSAGE_LENGTH - len(content) - len(prefix)
    if room <= 0:
        return content[: _MAX_MESSAGE_LENGTH - 1] + "…"
    rendered = value[:room]
    if len(rendered) < len(value) and rendered:
        rendered = rendered[:-1] + "…"
    return content + prefix + rendered


class QuestionAnswerModal(discord.ui.Modal):
    """Collect a free-form answer for one pending question."""

    def __init__(
        self,
        inbox: "DiscordNotificationInbox",
        notification_id: str,
        source_message: discord.Message,
    ) -> None:
        super().__init__(
            title="Answer Nerve question",
            custom_id=f"nerve:question:answer:{notification_id}",
            timeout=900,
        )
        self.inbox = inbox
        self.notification_id = notification_id
        self.source_message = source_message
        self.answer = discord.ui.TextInput(
            label="Answer",
            placeholder="Write your response",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=2000,
        )
        self.add_item(self.answer)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        success = await self.inbox.answer_question(
            interaction=interaction,
            notification_id=self.notification_id,
            answer=str(self.answer.value or "").strip(),
            source_message=self.source_message,
        )
        await interaction.followup.send(
            "Answer recorded."
            if success
            else "This question is no longer pending.",
            ephemeral=True,
        )

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
    ) -> None:
        logger.exception("Discord question modal failed", exc_info=error)
        if interaction.response.is_done():
            await interaction.followup.send(
                "Failed to record the answer.", ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "Failed to record the answer.", ephemeral=True,
            )


class QuestionOptionButton(discord.ui.Button["QuestionView"]):
    """One suggested answer for a pending question."""

    def __init__(
        self,
        notification_id: str,
        answer: str,
        index: int,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.primary,
            label=answer[:80],
            custom_id=(
                f"nerve:question:{notification_id}:option:{index}"
            ),
            disabled=disabled,
            row=index // 5,
        )
        self.notification_id = notification_id
        self.answer = answer

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to answer Nerve questions.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.message, discord.Message):
            await interaction.response.send_message(
                "The question message is unavailable.", ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        success = await view.inbox.answer_question(
            interaction=interaction,
            notification_id=self.notification_id,
            answer=self.answer,
            source_message=interaction.message,
        )
        await interaction.followup.send(
            "Answer recorded."
            if success
            else "This question is no longer pending.",
            ephemeral=True,
        )


class QuestionWriteButton(discord.ui.Button["QuestionView"]):
    """Open a modal for a free-form question answer."""

    def __init__(
        self,
        notification_id: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Write answer",
            emoji="✏️",
            custom_id=f"nerve:question:{notification_id}:write",
            disabled=disabled,
            row=4,
        )
        self.notification_id = notification_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to answer Nerve questions.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.message, discord.Message):
            await interaction.response.send_message(
                "The question message is unavailable.", ephemeral=True,
            )
            return
        await interaction.response.send_modal(QuestionAnswerModal(
            view.inbox,
            self.notification_id,
            interaction.message,
        ))


class QuestionView(discord.ui.View):
    """Persistent suggested-answer and free-form controls."""

    def __init__(
        self,
        inbox: "DiscordNotificationInbox",
        notification_id: str,
        options: list[str],
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.inbox = inbox
        self.notification_id = notification_id
        self.options = list(options)
        for index, answer in enumerate(options[:_MAX_QUESTION_OPTIONS]):
            self.add_item(QuestionOptionButton(
                notification_id,
                answer,
                index,
                disabled=disabled,
            ))
        self.add_item(QuestionWriteButton(
            notification_id,
            disabled=disabled,
        ))


class NotificationDismissButton(discord.ui.Button["NotificationView"]):
    """Dismiss one informational notification."""

    def __init__(
        self,
        notification_id: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(
            style=discord.ButtonStyle.secondary,
            label="Dismiss",
            emoji="✅",
            custom_id=f"nerve:notification:{notification_id}:dismiss",
            disabled=disabled,
        )
        self.notification_id = notification_id

    async def callback(self, interaction: discord.Interaction) -> None:
        view = self.view
        if view is None:
            return
        if not view.inbox.interaction_allowed(interaction):
            await interaction.response.send_message(
                "You are not allowed to dismiss Nerve notifications.",
                ephemeral=True,
            )
            return
        if not isinstance(interaction.message, discord.Message):
            await interaction.response.send_message(
                "The notification message is unavailable.", ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True)
        success = await view.inbox.dismiss_notification(
            interaction=interaction,
            notification_id=self.notification_id,
            source_message=interaction.message,
        )
        await interaction.followup.send(
            "Notification dismissed."
            if success
            else "This notification is no longer pending.",
            ephemeral=True,
        )


class NotificationView(discord.ui.View):
    """Persistent dismiss control for a notification."""

    def __init__(
        self,
        inbox: "DiscordNotificationInbox",
        notification_id: str,
        *,
        disabled: bool = False,
    ) -> None:
        super().__init__(timeout=None)
        self.inbox = inbox
        self.notification_id = notification_id
        self.add_item(NotificationDismissButton(
            notification_id,
            disabled=disabled,
        ))


class DiscordNotificationInbox:
    """Own the notify/question threads and their persistent views."""

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
        self._threads: dict[str, discord.Thread] = {}
        self._thread_locks = {
            kind: asyncio.Lock() for kind in _THREADS
        }
        self._answer_locks: dict[str, asyncio.Lock] = {}

    async def start(self, guild: discord.Guild) -> None:
        started = 0
        for kind in _THREADS:
            try:
                await self._ensure_thread(kind, guild)
                started += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Discord %s inbox failed to start; continuing with "
                    "other notification kinds",
                    kind,
                )
        if not started:
            raise RuntimeError("No Discord notification inbox could start")
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
        kind: str,
        guild: discord.Guild | None = None,
    ) -> discord.Thread:
        if kind not in _THREADS:
            raise ValueError(f"Unsupported Discord notification type: {kind}")
        thread = self._threads.get(kind)
        if thread is not None:
            if thread.archived:
                thread = await thread.edit(
                    archived=False,
                    pinned=False,
                    reason=f"Restore Nerve {kind} inbox",
                )
                self._threads[kind] = thread
            return thread

        async with self._thread_locks[kind]:
            thread = self._threads.get(kind)
            if thread is not None:
                return thread
            guild = guild or self.client.get_guild(self.guild_id)
            if guild is None:
                raise RuntimeError("Discord notification guild is unavailable")
            forum = guild.get_channel(self.forum_id)
            if not isinstance(forum, discord.ForumChannel):
                raise RuntimeError(
                    "Discord notification inbox requires audit_forum_id "
                    "to identify a forum channel"
                )

            name, intro = _THREADS[kind]
            inbox_tag = resolve_inbox_tag(forum)
            thread = await self._find_thread(guild, forum, name)
            if thread is None:
                create_kwargs: dict[str, Any] = {}
                if inbox_tag is not None:
                    create_kwargs["applied_tags"] = [inbox_tag]
                created = await forum.create_thread(
                    name=name,
                    content=intro,
                    auto_archive_duration=10080,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason=f"Create Nerve {kind} inbox",
                    **create_kwargs,
                )
                thread = created.thread

            edit_kwargs: dict[str, Any] = {
                "archived": False,
                "pinned": False,
                "reason": f"Prepare Nerve {kind} inbox",
            }
            applied_tags = tags_with_inbox_tag(thread, inbox_tag)
            if applied_tags is not None:
                edit_kwargs["applied_tags"] = applied_tags
            thread = await thread.edit(
                **edit_kwargs,
            )
            self._threads[kind] = thread
            return thread

    async def _find_thread(
        self,
        guild: discord.Guild,
        forum: discord.ForumChannel,
        name: str,
    ) -> discord.Thread | None:
        active = await guild.active_threads()
        for thread in active:
            if (
                int(getattr(thread, "parent_id", 0) or 0) == self.forum_id
                and getattr(thread, "name", "") == name
            ):
                return thread
        async for thread in forum.archived_threads(limit=100):
            if getattr(thread, "name", "") == name:
                return thread
        return None

    async def deliver(self, row: dict[str, Any]) -> str:
        """Post a notification/question card and persist its coordinates."""
        kind = str(row.get("type") or "")
        if kind not in _THREADS:
            raise ValueError(f"Unsupported Discord notification type: {kind}")
        thread = await self._ensure_thread(kind)
        options = _option_values(row)
        view: discord.ui.View
        if kind == "question":
            view = QuestionView(self, row["id"], options)
        else:
            view = NotificationView(self, row["id"])

        content = self._render_card(row)
        if len(content) > _MAX_ACTION_CARD_LENGTH:
            for chunk in _split_message(content):
                await thread.send(
                    chunk,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            title = str(row.get("title") or "Nerve")[:500]
            action = "Answer required" if kind == "question" else "Notification"
            content = (
                f"**{action}: {title}**"
                "\n\nFull details are in the messages immediately above."
            )

        message = await thread.send(
            content,
            view=view,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        meta = _metadata(row)
        meta["discord_notification"] = {
            "thread_id": str(thread.id),
            "message_id": str(message.id),
        }
        await self.db.update_notification(
            row["id"], metadata=json.dumps(meta),
        )
        return str(message.id)

    async def _restore_pending_views(self) -> None:
        for kind in _THREADS:
            rows = await self.db.list_notifications(
                status="pending",
                type=kind,
                limit=500,
            )
            for row in rows:
                coords = _metadata(row).get("discord_notification")
                if not isinstance(coords, dict):
                    continue
                try:
                    message_id = int(coords["message_id"])
                except (KeyError, TypeError, ValueError):
                    continue
                view = (
                    QuestionView(self, row["id"], _option_values(row))
                    if kind == "question"
                    else NotificationView(self, row["id"])
                )
                self.client.add_view(view, message_id=message_id)

    async def answer_question(
        self,
        *,
        interaction: discord.Interaction,
        notification_id: str,
        answer: str,
        source_message: discord.Message,
    ) -> bool:
        """Serialize one question answer, inject it, and close the card."""
        if not self.interaction_allowed(interaction) or not answer:
            return False
        lock = self._answer_locks.setdefault(
            notification_id, asyncio.Lock(),
        )
        async with lock:
            success = await self.notification_service.handle_answer(
                notification_id=notification_id,
                answer=answer,
                answered_by=f"discord:{interaction.user.id}",
            )
            if not success:
                return False
            row = await self.db.get_notification(notification_id)
            content = str(source_message.content or "")
            status = f"{answer} — by <@{interaction.user.id}>"
            if "**Answer:**" not in content:
                content = _append_status(content, "Answer", status)
            await source_message.edit(
                content=content,
                view=QuestionView(
                    self,
                    notification_id,
                    _option_values(row or {}),
                    disabled=True,
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return True

    async def dismiss_notification(
        self,
        *,
        interaction: discord.Interaction,
        notification_id: str,
        source_message: discord.Message,
    ) -> bool:
        """Serialize one dismissal and close the notification card."""
        if not self.interaction_allowed(interaction):
            return False
        lock = self._answer_locks.setdefault(
            notification_id, asyncio.Lock(),
        )
        async with lock:
            success = await self.notification_service.handle_dismiss(
                notification_id,
            )
            if not success:
                return False
            content = str(source_message.content or "")
            if "**Dismissed by:**" not in content:
                content = _append_status(
                    content,
                    "Dismissed by",
                    f"<@{interaction.user.id}>",
                )
            await source_message.edit(
                content=content,
                view=NotificationView(
                    self,
                    notification_id,
                    disabled=True,
                ),
                allowed_mentions=discord.AllowedMentions.none(),
            )
            return True

    @staticmethod
    def _render_card(row: dict[str, Any]) -> str:
        priority = str(row.get("priority") or "normal")
        prefix = {"urgent": "🚨 ", "high": "⚠️ "}.get(priority, "")
        fallback = (
            "Question"
            if row.get("type") == "question"
            else "Notification"
        )
        title = str(row.get("title") or fallback).strip()
        body = str(row.get("body") or "").strip()
        session_id = str(row.get("session_id") or "").strip()
        parts = [f"{prefix}**{title}**"]
        if body:
            parts.append(body)
        if session_id:
            parts.append(f"`Session: {session_id}`")
        return "\n\n".join(parts)
