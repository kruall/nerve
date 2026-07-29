"""Shared Discord forum projection for Nerve skills.

Each local skill is represented by one forum thread named after its stable
skill ID. Exact SKILL.md revisions are attached as files, while the thread
remains available for human and peer-agent discussion. The filesystem stays
the source of truth: Discord is a discoverable exchange and collaboration
surface, not a writable replica.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import logging
import re
from typing import Any

import discord

from nerve.skills.manager import SkillManager

logger = logging.getLogger(__name__)

_SKILL_MARKER = re.compile(
    r"Nerve skill(?: snapshot)?:\s*`([^`]+)`",
    re.IGNORECASE,
)
_HASH_MARKER = re.compile(
    r"SHA-256:\s*`([0-9a-f]{64})`",
    re.IGNORECASE,
)
_MAX_DESCRIPTION_LENGTH = 900
_ARCHIVE_DURATION_MINUTES = 10080


def _digest(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _bounded_description(value: str) -> str:
    text = str(value or "").strip()
    if len(text) <= _MAX_DESCRIPTION_LENGTH:
        return text
    return text[: _MAX_DESCRIPTION_LENGTH - 1].rstrip() + "…"


class DiscordSkillForum:
    """Project local skills into a shared, restart-safe Discord forum."""

    def __init__(
        self,
        *,
        client: discord.Client,
        skill_manager: SkillManager,
        guild_id: int,
        forum_id: int,
    ):
        self.client = client
        self.skill_manager = skill_manager
        self.guild_id = guild_id
        self.forum_id = forum_id
        self._forum: discord.ForumChannel | None = None
        self._threads: dict[str, discord.Thread] = {}
        self._skill_by_thread: dict[int, str] = {}
        self._skill_locks: dict[str, asyncio.Lock] = {}
        self._reconcile_lock = asyncio.Lock()
        self._started = False

    async def start(self, guild: discord.Guild) -> None:
        forum = guild.get_channel(self.forum_id)
        if not isinstance(forum, discord.ForumChannel):
            raise RuntimeError(
                "Discord skill forum requires skills_forum_id "
                "to identify a forum channel"
            )
        self._forum = forum
        self.skill_manager.add_change_listener(self._on_skill_change)
        self._started = True
        try:
            await self.reconcile(guild)
        except BaseException:
            self._started = False
            self.skill_manager.remove_change_listener(self._on_skill_change)
            raise

    async def stop(self) -> None:
        if self._started:
            self.skill_manager.remove_change_listener(self._on_skill_change)
        self._started = False

    def skill_id_for_thread(self, thread_id: int) -> str:
        """Return the skill represented by a managed thread, if known."""
        return self._skill_by_thread.get(int(thread_id), "")

    async def register_thread(self, thread: Any) -> str:
        """Discover a managed thread created by another connected agent."""
        thread_id = int(getattr(thread, "id", 0) or 0)
        if (
            not thread_id
            or int(getattr(thread, "parent_id", 0) or 0) != self.forum_id
        ):
            return ""
        existing = self._skill_by_thread.get(thread_id)
        if existing:
            return existing
        try:
            starter = await thread.fetch_message(thread_id)
        except discord.HTTPException:
            return ""
        content = str(getattr(starter, "content", "") or "")
        match = _SKILL_MARKER.search(content)
        if match is None:
            return ""
        skill_id = match.group(1).strip()
        if not skill_id:
            return ""
        self._remember_thread(skill_id, thread)
        return skill_id

    async def reconcile(self, guild: discord.Guild | None = None) -> None:
        """Discover shared threads and publish every current local skill."""
        async with self._reconcile_lock:
            guild = guild or self.client.get_guild(self.guild_id)
            if guild is None:
                raise RuntimeError("Discord skill forum guild is unavailable")
            forum = self._forum or guild.get_channel(self.forum_id)
            if not isinstance(forum, discord.ForumChannel):
                raise RuntimeError("Discord skill forum is unavailable")
            self._forum = forum

            await self._discover_threads(guild, forum)
            rows = await self.skill_manager.db.list_skills()
            failures = 0
            for row in rows:
                skill_id = str(row["id"])
                try:
                    await self._ensure_skill(skill_id, guild)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    failures += 1
                    logger.exception(
                        "Discord skill thread sync failed for %s",
                        skill_id,
                    )
            if rows and failures == len(rows):
                raise RuntimeError("No local skill could be synced to Discord")

    async def _on_skill_change(
        self,
        action: str,
        skill_id: str | None,
    ) -> None:
        if not self._started:
            return
        if action == "sync":
            await self.reconcile()
        elif action in {"create", "update"} and skill_id:
            await self._ensure_skill(skill_id)
        # A local delete or enabled-state toggle must not remove or rewrite a
        # shared thread that another agent may still use.

    async def _discover_threads(
        self,
        guild: discord.Guild,
        forum: discord.ForumChannel,
    ) -> None:
        seen: set[int] = set()
        for thread in await guild.active_threads():
            if int(getattr(thread, "parent_id", 0) or 0) != self.forum_id:
                continue
            seen.add(int(thread.id))
            await self.register_thread(thread)
        async for thread in forum.archived_threads(limit=None):
            if int(thread.id) in seen:
                continue
            await self.register_thread(thread)

    def _remember_thread(self, skill_id: str, thread: discord.Thread) -> None:
        thread_id = int(thread.id)
        existing = self._threads.get(skill_id)
        if existing is not None and int(existing.id) != thread_id:
            logger.warning(
                "Discord skill forum has duplicate managed threads for %s; "
                "keeping thread %s and ignoring %s",
                skill_id,
                existing.id,
                thread_id,
            )
            return
        self._threads[skill_id] = thread
        self._skill_by_thread[thread_id] = skill_id

    async def _ensure_skill(
        self,
        skill_id: str,
        guild: discord.Guild | None = None,
    ) -> discord.Thread | None:
        lock = self._skill_locks.setdefault(skill_id, asyncio.Lock())
        async with lock:
            skill = await self.skill_manager.get_skill(skill_id)
            if skill is None:
                return None
            raw = skill.raw
            snapshot_hash = _digest(raw)

            thread = self._threads.get(skill_id)
            if thread is None:
                guild = guild or self.client.get_guild(self.guild_id)
                if guild is None:
                    raise RuntimeError(
                        "Discord skill forum guild is unavailable"
                    )
                forum = self._forum or guild.get_channel(self.forum_id)
                if not isinstance(forum, discord.ForumChannel):
                    raise RuntimeError("Discord skill forum is unavailable")
                created = await forum.create_thread(
                    name=skill_id,
                    content=self._render_starter(skill, snapshot_hash),
                    file=self._snapshot_file(skill_id, raw),
                    auto_archive_duration=_ARCHIVE_DURATION_MINUTES,
                    allowed_mentions=discord.AllowedMentions.none(),
                    reason=f"Create Nerve skill thread {skill_id}",
                )
                thread = created.thread
                self._remember_thread(skill_id, thread)
                return thread

            if bool(getattr(thread, "archived", False)):
                updated = await thread.edit(
                    archived=False,
                    reason=f"Restore Nerve skill thread {skill_id}",
                )
                thread = updated or thread
                self._remember_thread(skill_id, thread)

            if not await self._thread_has_snapshot(
                thread,
                skill_id,
                snapshot_hash,
            ):
                await thread.send(
                    self._render_snapshot(skill, snapshot_hash),
                    file=self._snapshot_file(skill_id, raw),
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            return thread

    async def _thread_has_snapshot(
        self,
        thread: discord.Thread,
        skill_id: str,
        snapshot_hash: str,
    ) -> bool:
        async for message in thread.history(limit=None):
            content = str(getattr(message, "content", "") or "")
            skill_match = _SKILL_MARKER.search(content)
            hash_match = _HASH_MARKER.search(content)
            if (
                skill_match is not None
                and hash_match is not None
                and skill_match.group(1).strip() == skill_id
                and hash_match.group(1).lower() == snapshot_hash
            ):
                return True
        return False

    @staticmethod
    def _snapshot_file(skill_id: str, raw: str) -> discord.File:
        return discord.File(
            io.BytesIO(raw.encode("utf-8")),
            filename=f"{skill_id}-SKILL.md",
            description=f"Exact SKILL.md snapshot for {skill_id}",
        )

    @staticmethod
    def _render_starter(skill: Any, snapshot_hash: str) -> str:
        description = _bounded_description(skill.description)
        return (
            f"**Nerve skill: `{skill.id}`**\n"
            f"Name: **{skill.name}** · Version: `{skill.version}`\n"
            f"SHA-256: `{snapshot_hash}`\n\n"
            f"{description}\n\n"
            "The attached file is the exact initial `SKILL.md` snapshot. "
            "Use this thread to hand the skill to another agent or discuss "
            "changes. Mention or reply to an agent to invoke it; the local "
            "workspace copy remains that agent's source of truth."
        )

    @staticmethod
    def _render_snapshot(skill: Any, snapshot_hash: str) -> str:
        description = _bounded_description(skill.description)
        return (
            f"**Nerve skill snapshot: `{skill.id}`**\n"
            f"Name: **{skill.name}** · Version: `{skill.version}`\n"
            f"SHA-256: `{snapshot_hash}`\n\n"
            f"{description}\n\n"
            "The attached file is the exact current `SKILL.md` snapshot."
        )
