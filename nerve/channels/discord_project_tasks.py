"""Discord modal flow for creating numbered project-forum tasks."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from typing import Any

import discord

logger = logging.getLogger(__name__)

_MAX_THREAD_NAME_LENGTH = 100
_MAX_TASK_TITLE_LENGTH = 80
_MAX_TASK_DESCRIPTION_LENGTH = 2000
_AUTO_ARCHIVE_DURATION_MINUTES = 10080
_READY_FOR_AGENT_TAG_NAME = "ready-for-agent"


class DiscordProjectTaskCreateError(ValueError):
    """A safe, user-facing failure while creating a project task."""


class DiscordProjectTaskCreator:
    """Create one numbered forum thread at a time for every project."""

    def __init__(
        self,
        *,
        guild_id: int,
        task_forums: dict[str, int],
        allowed_author_ids: set[int],
        client: Callable[[], discord.Client | None],
    ) -> None:
        self.guild_id = guild_id
        self.task_forums = dict(task_forums)
        self.allowed_author_ids = set(allowed_author_ids)
        self._client = client
        self._forum_locks: dict[int, asyncio.Lock] = {}
        self._last_created_number: dict[int, int] = {}

    def project_name(self, project: str) -> str:
        requested = project.strip().casefold()
        for name in self.task_forums:
            if name.casefold() == requested:
                return name
        configured = ", ".join(sorted(self.task_forums)) or "(none)"
        raise DiscordProjectTaskCreateError(
            f"Неизвестный проект {project!r}. Доступны: {configured}."
        )

    def command_allowed(self, interaction: discord.Interaction) -> bool:
        guild_id = int(getattr(interaction, "guild_id", 0) or 0)
        user_id = int(
            getattr(getattr(interaction, "user", None), "id", 0) or 0
        )
        return guild_id == self.guild_id and user_id in self.allowed_author_ids

    def project_choices(self, current: str) -> list[discord.app_commands.Choice[str]]:
        needle = current.strip().casefold()
        return [
            discord.app_commands.Choice(name=name, value=name)
            for name in sorted(self.task_forums)
            if not needle or needle in name.casefold()
        ][:25]

    async def create(
        self,
        interaction: discord.Interaction,
        *,
        project: str,
        title: str,
        description: str,
        ready_for_agent: bool = False,
    ) -> tuple[str, int]:
        """Create the task and return its rendered identifier and thread ID."""
        if not self.command_allowed(interaction):
            raise DiscordProjectTaskCreateError(
                "У вас нет доступа к созданию задач Nerve."
            )

        return await self.create_for_agent(
            project=project,
            title=title,
            description=description,
            author_id=int(
                getattr(getattr(interaction, "user", None), "id", 0) or 0
            ),
            ready_for_agent=ready_for_agent,
        )

    async def create_for_agent(
        self,
        *,
        project: str,
        title: str,
        description: str,
        author_id: int | None = None,
        ready_for_agent: bool = False,
    ) -> tuple[str, int]:
        """Create a task for a Nerve agent using the connected Discord client.

        The caller is responsible for authorizing the external action. This
        shares the same numbering lock and validation path as the slash command.
        """
        project = self.project_name(project)
        title = " ".join(title.split())
        description = description.strip()
        if not title:
            raise DiscordProjectTaskCreateError("Укажите непустой заголовок задачи.")
        if len(title) > _MAX_TASK_TITLE_LENGTH:
            raise DiscordProjectTaskCreateError(
                "Заголовок задачи не должен быть длиннее "
                f"{_MAX_TASK_TITLE_LENGTH} символов."
            )
        if not description:
            raise DiscordProjectTaskCreateError("Укажите описание задачи.")
        if len(description) > _MAX_TASK_DESCRIPTION_LENGTH:
            raise DiscordProjectTaskCreateError(
                "Описание задачи не должно быть длиннее 2000 символов."
            )

        client = self._client()
        if client is None:
            raise DiscordProjectTaskCreateError("Discord-клиент Nerve не запущен.")
        guild = client.get_guild(self.guild_id)
        if guild is None:
            raise DiscordProjectTaskCreateError(
                "Nerve не видит настроенный Discord-сервер."
            )

        forum_id = int(self.task_forums[project])
        forum = guild.get_channel(forum_id)
        if forum is None or not hasattr(forum, "create_thread"):
            raise DiscordProjectTaskCreateError(
                "Nerve не видит форум выбранного проекта."
            )
        ready_for_agent_tag = (
            self._ready_for_agent_tag(forum) if ready_for_agent else None
        )

        lock = self._forum_locks.setdefault(forum_id, asyncio.Lock())
        async with lock:
            maximum = await self._maximum_existing_number(
                guild, forum, project, forum_id,
            )
            number = max(maximum, self._last_created_number.get(forum_id, 0)) + 1
            task_id = f"{project}-{number}"
            thread_name = f"{task_id} {title}"
            starter_content = description
            allowed_mentions = discord.AllowedMentions.none()
            if author_id:
                starter_content = f"<@{author_id}>\n\n{description}"
                allowed_mentions = discord.AllowedMentions(
                    users=[discord.Object(id=author_id)],
                )
            if len(thread_name) > _MAX_THREAD_NAME_LENGTH:
                raise DiscordProjectTaskCreateError(
                    "Заголовок слишком длинный для имени Discord-треда."
                )
            create_kwargs: dict[str, Any] = {}
            if ready_for_agent_tag is not None:
                create_kwargs["applied_tags"] = [ready_for_agent_tag]
            try:
                created = await forum.create_thread(
                    name=thread_name,
                    content=starter_content,
                    auto_archive_duration=_AUTO_ARCHIVE_DURATION_MINUTES,
                    allowed_mentions=allowed_mentions,
                    reason=f"Create Nerve project task {task_id}",
                    **create_kwargs,
                )
            except discord.HTTPException as exc:
                logger.warning(
                    "Discord failed to create project task %s: %s",
                    task_id,
                    exc,
                )
                raise DiscordProjectTaskCreateError(
                    "Discord не создал задачу. Проверьте права бота и попробуйте снова."
                ) from exc
            self._last_created_number[forum_id] = number
            return task_id, int(created.thread.id)

    @staticmethod
    def _ready_for_agent_tag(forum: Any) -> Any:
        matches = [
            tag
            for tag in list(getattr(forum, "available_tags", []) or [])
            if str(getattr(tag, "name", "") or "").casefold()
            == _READY_FOR_AGENT_TAG_NAME
        ]
        if len(matches) != 1:
            raise DiscordProjectTaskCreateError(
                "Форум проекта должен содержать ровно один тег ready-for-agent."
            )
        return matches[0]

    async def _maximum_existing_number(
        self,
        guild: discord.Guild,
        forum: Any,
        project: str,
        forum_id: int,
    ) -> int:
        pattern = re.compile(rf"^{re.escape(project)}-(\d+)(?:\s|$)")
        maximum = 0
        seen: set[int] = set()

        def observe(thread: Any) -> None:
            nonlocal maximum
            if int(getattr(thread, "parent_id", 0) or 0) != forum_id:
                return
            thread_id = int(getattr(thread, "id", 0) or 0)
            if not thread_id or thread_id in seen:
                return
            seen.add(thread_id)
            match = pattern.match(str(getattr(thread, "name", "") or ""))
            if match is not None:
                maximum = max(maximum, int(match.group(1)))

        try:
            for thread in await guild.active_threads():
                observe(thread)
            async for thread in forum.archived_threads(limit=None):
                observe(thread)
        except Exception as exc:
            logger.warning(
                "Unable to determine the next task number for project %s",
                project,
                exc_info=exc,
            )
            raise DiscordProjectTaskCreateError(
                "Не удалось определить следующий номер задачи; задача не создана."
            ) from exc
        return maximum


class ProjectTaskCreateModal(discord.ui.Modal):
    """Collect the title and description after the project is selected."""

    def __init__(
        self,
        creator: DiscordProjectTaskCreator,
        project: str,
    ) -> None:
        super().__init__(
            title=f"Новая задача: {project}"[:45],
            custom_id=f"nerve:project-task:create:{project}"[:100],
            timeout=900,
        )
        self.creator = creator
        self.project = project
        self.task_title = discord.ui.TextInput(
            label="Заголовок",
            placeholder="Кратко опишите задачу",
            required=True,
            max_length=_MAX_TASK_TITLE_LENGTH,
        )
        self.description = discord.ui.TextInput(
            label="Описание",
            placeholder="Что нужно сделать и какой ожидается результат",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=_MAX_TASK_DESCRIPTION_LENGTH,
        )
        self.ready_for_agent = discord.ui.Checkbox(
            custom_id="nerve:project-task:ready-for-agent",
            default=False,
        )
        self.add_item(self.task_title)
        self.add_item(self.description)
        self.add_item(discord.ui.Label(
            text="Готово для агента",
            description="Сразу передать задачу автономному агенту",
            component=self.ready_for_agent,
        ))

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        try:
            task_id, thread_id = await self.creator.create(
                interaction,
                project=self.project,
                title=str(self.task_title.value or ""),
                description=str(self.description.value or ""),
                ready_for_agent=self.ready_for_agent.value,
            )
        except DiscordProjectTaskCreateError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        await interaction.followup.send(
            f"Создана задача **{task_id}**: <#{thread_id}>",
            ephemeral=True,
        )

    async def on_error(
        self,
        interaction: discord.Interaction,
        error: Exception,
    ) -> None:
        logger.exception("Discord project-task creation modal failed", exc_info=error)
        if interaction.response.is_done():
            await interaction.followup.send(
                "Не удалось создать задачу.", ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "Не удалось создать задачу.", ephemeral=True,
            )
