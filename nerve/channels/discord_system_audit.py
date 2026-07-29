"""Persistent system-lifecycle feed in the Discord audit forum."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import discord

logger = logging.getLogger(__name__)

_THREAD_NAME = "System"
_THREAD_INTRO = (
    "Nerve system audit. Process lifecycle events and important service "
    "state changes appear here."
)
_SYSTEM_TAG_NAME = "system"
_MAX_MESSAGE_LENGTH = 2000
_MAX_THREAD_TAGS = 5
_LEVEL_EMOJIS = {
    "info": "ℹ️",
    "success": "🟢",
    "warning": "🟡",
    "error": "🔴",
}


def _tag_id(tag: Any) -> int:
    value = tag.get("id") if isinstance(tag, dict) else getattr(tag, "id")
    return int(value)


def _tag_name(tag: Any) -> str:
    value = tag.get("name") if isinstance(tag, dict) else getattr(tag, "name")
    return str(value or "")


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
        if cut < _MAX_MESSAGE_LENGTH // 2:
            cut = _MAX_MESSAGE_LENGTH
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    return chunks


class DiscordSystemAudit:
    """Own the unpinned ``System`` thread and append lifecycle events."""

    def __init__(
        self,
        *,
        client: discord.Client,
        guild_id: int,
        forum_id: int,
    ) -> None:
        self.client = client
        self.guild_id = guild_id
        self.forum_id = forum_id
        self._thread: discord.Thread | None = None
        self._thread_lock = asyncio.Lock()
        self._event_lock = asyncio.Lock()

    async def start(
        self,
        guild: discord.Guild | None = None,
    ) -> None:
        """Create or restore the persistent system thread."""
        await self._ensure_thread(guild)

    async def emit(
        self,
        title: str,
        *,
        details: str = "",
        level: str = "info",
        occurred_at: int | None = None,
    ) -> None:
        """Append one timestamped system event."""
        title = " ".join(str(title or "").split())
        if not title:
            raise ValueError("Discord system audit event title is required")
        if level not in _LEVEL_EMOJIS:
            raise ValueError(
                f"Unsupported Discord system audit level: {level}"
            )

        timestamp = int(time.time()) if occurred_at is None else occurred_at
        content = (
            f"{_LEVEL_EMOJIS[level]} **{title}** · <t:{timestamp}:F>"
        )
        details = str(details or "").strip()
        if details:
            content += f"\n{details}"

        async with self._event_lock:
            thread = await self._ensure_thread()
            for chunk in _split_message(content):
                await thread.send(
                    chunk,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

    async def _ensure_thread(
        self,
        guild: discord.Guild | None = None,
    ) -> discord.Thread:
        thread = self._thread
        if thread is not None and not thread.archived:
            return thread

        async with self._thread_lock:
            thread = self._thread
            if thread is not None and not thread.archived:
                return thread

            guild = guild or self.client.get_guild(self.guild_id)
            if guild is None:
                raise RuntimeError("Discord system audit guild is unavailable")
            forum = guild.get_channel(self.forum_id)
            if not isinstance(forum, discord.ForumChannel):
                raise RuntimeError(
                    "Discord system audit requires audit_forum_id "
                    "to identify a forum channel"
                )

            thread = await self._find_thread(guild, forum)
            system_tag = self._resolve_system_tag(forum)
            if thread is None:
                create_kwargs: dict[str, Any] = {}
                if system_tag is not None:
                    create_kwargs["applied_tags"] = [system_tag]
                created = await forum.create_thread(
                    name=_THREAD_NAME,
                    content=_THREAD_INTRO,
                    auto_archive_duration=10080,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason="Create Nerve system audit",
                    **create_kwargs,
                )
                thread = created.thread

            edit_kwargs: dict[str, Any] = {
                "archived": False,
                "pinned": False,
                "reason": "Prepare Nerve system audit",
            }
            applied_tags = self._tags_with_system_tag(thread, system_tag)
            if applied_tags is not None:
                edit_kwargs["applied_tags"] = applied_tags
            updated = await thread.edit(**edit_kwargs)
            self._thread = updated or thread
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

    @staticmethod
    def _resolve_system_tag(forum: discord.ForumChannel) -> Any | None:
        matches = [
            tag
            for tag in list(getattr(forum, "available_tags", []) or [])
            if _tag_name(tag).casefold() == _SYSTEM_TAG_NAME.casefold()
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            logger.warning(
                "Discord audit forum has duplicate %s tags; the System "
                "thread will preserve its current tags",
                _SYSTEM_TAG_NAME,
            )
        else:
            logger.warning(
                "Discord audit forum is missing the %s tag; the System "
                "thread will remain untagged",
                _SYSTEM_TAG_NAME,
            )
        return None

    @staticmethod
    def _tags_with_system_tag(
        thread: discord.Thread,
        system_tag: Any | None,
    ) -> list[Any] | None:
        if system_tag is None:
            return None
        current = list(getattr(thread, "applied_tags", []) or [])
        system_tag_id = _tag_id(system_tag)
        if any(_tag_id(tag) == system_tag_id for tag in current):
            return None
        if len(current) >= _MAX_THREAD_TAGS:
            logger.warning(
                "Discord System thread %s already has five tags; cannot add %s",
                getattr(thread, "id", "unknown"),
                _SYSTEM_TAG_NAME,
            )
            return None
        return [*current, system_tag]
