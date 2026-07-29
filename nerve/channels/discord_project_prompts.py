"""Editable, pinned instructions for configured Discord project forums."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import discord

logger = logging.getLogger(__name__)

_THREAD_NAME = "Project prompt"
_ARCHIVE_DURATION_MINUTES = 10080


class DiscordProjectPrompts:
    """Maintain one reserved, pinned prompt thread per project forum.

    The bot owns the thread's explanatory starter post, but an allowed human
    owns the prompt message itself. That lets the human edit the prompt in
    Discord without granting anyone access to the bot token or local config.
    """

    def __init__(
        self,
        *,
        client: discord.Client,
        db: Any,
        guild_id: int,
        project_forums: dict[int, str],
        allowed_author_ids: set[int],
    ) -> None:
        self.client = client
        self.db = db
        self.guild_id = guild_id
        self.project_forums = dict(project_forums)
        self.allowed_author_ids = set(allowed_author_ids)
        self._prompts: dict[int, dict[str, Any]] = {}
        self._forum_locks: dict[int, asyncio.Lock] = {}

    async def start(self, guild: discord.Guild) -> None:
        """Create or restore prompt threads without blocking other forums."""
        for forum_id, project in sorted(self.project_forums.items()):
            try:
                await self._ensure_prompt(guild, forum_id, project)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Discord project prompt failed to initialize for %s",
                    project,
                )

    def is_prompt_thread(self, thread_id: int) -> bool:
        return any(
            int(prompt["thread_id"]) == int(thread_id)
            for prompt in self._prompts.values()
        )

    def prompt_for_thread(self, forum_id: int | None, thread_id: int) -> str:
        """Render the current project prompt for a normal project thread."""
        if forum_id is None:
            return ""
        prompt = self._prompts.get(int(forum_id))
        if prompt is None or int(prompt["thread_id"]) == int(thread_id):
            return ""
        content = str(prompt.get("content") or "").strip()
        if not content:
            return ""
        project = str(prompt["project"])
        return (
            f"[Project prompt for {project}. It is maintained in the pinned "
            "project-prompt thread and is higher-priority working guidance for "
            "this project.]\n\n"
            f"{content}\n\n"
            "[End project prompt]"
        )

    async def observe_message(self, message: Any) -> bool:
        """Capture the first allowed human message in a prompt thread.

        Returns whether the message belongs to a reserved prompt thread and
        must therefore not be handled as ordinary project discussion.
        """
        prompt = self._prompt_for_thread_id(
            int(getattr(getattr(message, "channel", None), "id", 0) or 0),
        )
        if prompt is None:
            return False
        if str(prompt.get("message_id") or ""):
            return True

        author_id = int(
            getattr(getattr(message, "author", None), "id", 0) or 0,
        )
        content = str(getattr(message, "content", "") or "")
        if author_id not in self.allowed_author_ids or not content.strip():
            return True

        updated = {**prompt, "message_id": str(message.id), "content": content}
        await self._persist(updated)
        self._prompts[int(updated["forum_id"])] = updated
        return True

    async def observe_edit(self, message: Any) -> bool:
        """Refresh the prompt after its human-owned source message is edited."""
        message_id = str(getattr(message, "id", "") or "")
        prompt = next(
            (
                candidate
                for candidate in self._prompts.values()
                if str(candidate.get("message_id") or "") == message_id
            ),
            None,
        )
        if prompt is None:
            return False
        updated = {**prompt, "content": str(getattr(message, "content", "") or "")}
        await self._persist(updated)
        self._prompts[int(updated["forum_id"])] = updated
        return True

    async def observe_delete(self, message: Any) -> bool:
        """Clear a deleted prompt so an allowed author can post a replacement."""
        message_id = str(getattr(message, "id", "") or "")
        prompt = next(
            (
                candidate
                for candidate in self._prompts.values()
                if str(candidate.get("message_id") or "") == message_id
            ),
            None,
        )
        if prompt is None:
            return False
        updated = {**prompt, "message_id": "", "content": ""}
        await self._persist(updated)
        self._prompts[int(updated["forum_id"])] = updated
        return True

    async def _ensure_prompt(
        self,
        guild: discord.Guild,
        forum_id: int,
        project: str,
    ) -> None:
        lock = self._forum_locks.setdefault(forum_id, asyncio.Lock())
        async with lock:
            forum = guild.get_channel(forum_id)
            if not isinstance(forum, discord.ForumChannel):
                raise RuntimeError(
                    "Discord project prompt requires task_forums to identify "
                    "a forum channel"
                )

            saved = await self.db.get_discord_project_prompt(forum_id)
            thread = await self._resolve_saved_thread(saved, forum_id)
            if thread is None:
                thread = await self._find_recoverable_thread(guild, forum)
            if thread is None:
                await self._ensure_pin_is_available(guild, forum_id)
                created = await forum.create_thread(
                    name=_THREAD_NAME,
                    content=(
                        f"Project prompt for **{project}**.\n\n"
                        "Send one message with the working instructions Nerve "
                        "should apply in this project, then edit that message "
                        "whenever the instructions change. This reserved thread "
                        "never starts an agent session."
                    ),
                    auto_archive_duration=_ARCHIVE_DURATION_MINUTES,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason=f"Create Nerve project prompt for {project}",
                )
                thread = created.thread
                saved = None

            updated_thread = await thread.edit(
                archived=False,
                pinned=True,
                reason=f"Pin Nerve project prompt for {project}",
            )
            thread = updated_thread or thread
            prompt = {
                "guild_id": str(self.guild_id),
                "forum_id": str(forum_id),
                "project": project,
                "thread_id": str(thread.id),
                "message_id": str((saved or {}).get("message_id") or ""),
                "content": str((saved or {}).get("content") or ""),
            }
            await self._refresh_content(thread, prompt)
            await self._persist(prompt)
            self._prompts[forum_id] = prompt

    async def _resolve_saved_thread(
        self,
        saved: dict[str, Any] | None,
        forum_id: int,
    ) -> discord.Thread | None:
        if saved is None:
            return None
        try:
            thread_id = int(saved["thread_id"])
        except (KeyError, TypeError, ValueError):
            return None
        thread = self.client.get_channel(thread_id)
        if thread is None:
            try:
                thread = await self.client.fetch_channel(thread_id)
            except Exception:
                return None
        if int(getattr(thread, "parent_id", 0) or 0) != forum_id:
            logger.warning(
                "Discord project prompt thread %s no longer belongs to forum %s",
                thread_id,
                forum_id,
            )
            return None
        return thread

    async def _find_recoverable_thread(
        self,
        guild: discord.Guild,
        forum: discord.ForumChannel,
    ) -> discord.Thread | None:
        matches: list[discord.Thread] = []
        for thread in await guild.active_threads():
            if (
                int(getattr(thread, "parent_id", 0) or 0) == int(forum.id)
                and getattr(thread, "name", "") == _THREAD_NAME
            ):
                matches.append(thread)
        async for thread in forum.archived_threads(limit=100):
            if getattr(thread, "name", "") == _THREAD_NAME:
                matches.append(thread)
        if len(matches) > 1:
            raise RuntimeError(
                "Discord project forum has duplicate Project prompt threads; "
                "refusing to choose one"
            )
        return matches[0] if matches else None

    async def _ensure_pin_is_available(
        self,
        guild: discord.Guild,
        forum_id: int,
    ) -> None:
        for thread in await guild.active_threads():
            if (
                int(getattr(thread, "parent_id", 0) or 0) == forum_id
                and bool(getattr(thread, "pinned", False))
            ):
                raise RuntimeError(
                    "Discord project forum already has a pinned thread; "
                    "cannot create the managed Project prompt"
                )

    async def _refresh_content(
        self,
        thread: discord.Thread,
        prompt: dict[str, Any],
    ) -> None:
        message_id = str(prompt.get("message_id") or "")
        if not message_id:
            return
        try:
            message = await thread.fetch_message(int(message_id))
        except Exception:
            logger.warning(
                "Discord project prompt source message %s is unavailable; "
                "waiting for a replacement",
                message_id,
            )
            prompt["message_id"] = ""
            prompt["content"] = ""
            return
        prompt["content"] = str(getattr(message, "content", "") or "")

    async def _persist(self, prompt: dict[str, Any]) -> None:
        await self.db.upsert_discord_project_prompt(
            guild_id=int(prompt["guild_id"]),
            forum_id=int(prompt["forum_id"]),
            project=str(prompt["project"]),
            thread_id=int(prompt["thread_id"]),
            message_id=(
                int(prompt["message_id"])
                if str(prompt.get("message_id") or "")
                else None
            ),
            content=str(prompt.get("content") or ""),
        )

    def _prompt_for_thread_id(self, thread_id: int) -> dict[str, Any] | None:
        return next(
            (
                prompt
                for prompt in self._prompts.values()
                if int(prompt["thread_id"]) == thread_id
            ),
            None,
        )
