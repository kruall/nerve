"""Persistent system-lifecycle feed in the Discord audit forum."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

import discord

from nerve.agent.streaming import StreamBroadcaster, broadcaster
from nerve.channels.discord_inbox import resolve_inbox_tag
from nerve.db import Database

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
_LEVEL_COLOURS = {
    "info": discord.Colour.blurple(),
    "success": discord.Colour.green(),
    "warning": discord.Colour.orange(),
    "error": discord.Colour.red(),
}
_MAX_EMBED_TITLE_LENGTH = 256
_MAX_EMBED_DESCRIPTION_LENGTH = 4096
_MAX_SESSION_TITLE_LENGTH = 160
_MAX_SESSION_FIELD_LENGTH = 80
_MAX_SESSION_ERROR_LENGTH = 1500


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
    """Own the unpinned ``System`` thread and append operational events."""

    def __init__(
        self,
        *,
        client: discord.Client,
        db: Database,
        guild_id: int,
        forum_id: int,
        stream: StreamBroadcaster = broadcaster,
    ) -> None:
        self.client = client
        self.db = db
        self.guild_id = guild_id
        self.forum_id = forum_id
        self.stream = stream
        self._thread: discord.Thread | None = None
        self._thread_lock = asyncio.Lock()
        self._event_lock = asyncio.Lock()
        self._lifecycle_lock = asyncio.Lock()
        self._error_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None
        self._listener_id = f"discord-system-audit:{forum_id}"

    async def start(
        self,
        guild: discord.Guild | None = None,
    ) -> None:
        """Create or restore the persistent System thread and listener."""
        async with self._lifecycle_lock:
            if self._worker_task is not None:
                return
            await self._ensure_thread(guild)
            await self.stream.register_global(
                self._listener_id,
                self._on_stream_event,
            )
            try:
                self._worker_task = asyncio.create_task(
                    self._error_worker(),
                    name="discord-system-audit-errors",
                )
            except BaseException:
                await self.stream.unregister_global(self._listener_id)
                raise

    async def stop(self) -> None:
        """Remove the stream listener and stop its error-delivery worker."""
        async with self._lifecycle_lock:
            await self.stream.unregister_global(self._listener_id)
            worker = self._worker_task
            self._worker_task = None
            if worker is not None:
                worker.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker
            while True:
                try:
                    self._error_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                else:
                    self._error_queue.task_done()

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
        content = f"<t:{timestamp}:F>"
        details = str(details or "").strip()
        if details:
            content += f"\n\n{details}"

        async with self._event_lock:
            thread = await self._ensure_thread()
            chunks = _split_message(content)
            for index, chunk in enumerate(chunks):
                suffix = "" if index == 0 else " (continued)"
                await thread.send(
                    embed=discord.Embed(
                        title=(
                            f"{_LEVEL_EMOJIS[level]} {title}{suffix}"
                        )[:_MAX_EMBED_TITLE_LENGTH],
                        description=chunk[:_MAX_EMBED_DESCRIPTION_LENGTH],
                        colour=_LEVEL_COLOURS[level],
                    ),
                    allowed_mentions=discord.AllowedMentions.none(),
                )

    async def _on_stream_event(
        self,
        session_id: str,
        event: dict[str, Any],
    ) -> None:
        """Enqueue terminal session errors without delaying agent streaming."""
        if session_id == "__global__" or event.get("type") != "error":
            return
        error = str(event.get("error") or event.get("message") or "")
        self._error_queue.put_nowait((str(session_id), error))

    async def _error_worker(self) -> None:
        while True:
            session_id, error = await self._error_queue.get()
            try:
                await self._emit_session_error(session_id, error)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Discord System audit could not deliver session error for %s",
                    session_id,
                    exc_info=True,
                )
            finally:
                self._error_queue.task_done()

    async def _emit_session_error(self, session_id: str, error: str) -> None:
        try:
            session = await self.db.get_session(session_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning(
                "Discord System audit could not read session %s",
                session_id,
                exc_info=True,
            )
            return
        if session is None:
            logger.warning(
                "Discord System audit received an error for missing session %s",
                session_id,
            )
            return

        await self.emit(
            "Session error",
            details=self._session_error_details(session, session_id, error),
            level="error",
        )

    @staticmethod
    def _session_error_details(
        session: dict[str, Any],
        session_id: str,
        error: str,
    ) -> str:
        def compact(value: Any, limit: int) -> str:
            text = " ".join(str(value or "").split())
            if len(text) <= limit:
                return text
            return text[: limit - 1].rstrip() + "…"

        title = compact(
            session.get("title") or session_id,
            _MAX_SESSION_TITLE_LENGTH,
        )
        source = compact(session.get("source") or "unknown", _MAX_SESSION_FIELD_LENGTH)
        backend = compact(session.get("backend") or "unknown", _MAX_SESSION_FIELD_LENGTH)
        message = compact(error or "Unknown error", _MAX_SESSION_ERROR_LENGTH)
        return "\n".join((
            f"Session: {title}",
            f"ID: `{session_id[:8]}`",
            f"Source: `{source}` · Backend: `{backend}`",
            f"Error: {message}",
        ))

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
            managed_tags = [
                tag
                for tag in (
                    self._resolve_system_tag(forum),
                    resolve_inbox_tag(forum),
                )
                if tag is not None
            ]
            if thread is None:
                create_kwargs: dict[str, Any] = {}
                if managed_tags:
                    create_kwargs["applied_tags"] = managed_tags
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
            applied_tags = self._tags_with_managed_tags(
                thread,
                managed_tags,
            )
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
    def _tags_with_managed_tags(
        thread: discord.Thread,
        managed_tags: list[Any],
    ) -> list[Any] | None:
        if not managed_tags:
            return None
        current = list(getattr(thread, "applied_tags", []) or [])
        current_ids = {_tag_id(tag) for tag in current}
        missing = [
            tag for tag in managed_tags
            if _tag_id(tag) not in current_ids
        ]
        if not missing:
            return None
        if len(current) + len(missing) > _MAX_THREAD_TAGS:
            logger.warning(
                "Discord System thread %s has too many tags to add its "
                "managed audit tags",
                getattr(thread, "id", "unknown"),
            )
            return None
        return [*current, *missing]
