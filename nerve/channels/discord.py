"""Native Discord channel backed by discord.py.

The adapter is intentionally fail-closed. It accepts messages only from one
configured guild, explicitly allowed text channels or project forums, and an
explicit author allowlist. Its optional audit forum is outbound-only. A direct
bot mention in a text channel starts a conversation thread. Messages inside
conversation threads do not require further mentions, while project-forum
threads accept either a direct mention or a reply to the bot.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import stat
from collections import OrderedDict
from typing import Any, TYPE_CHECKING

import discord
from discord import app_commands

from nerve.agent.streaming import broadcaster
from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.channels.discord_context import (
    ContextSummarizer,
    DiscordThreadContext,
)
from nerve.channels.discord_project_tasks import (
    DiscordProjectTaskCreateError,
    DiscordProjectTaskCreator,
    ProjectTaskCreateModal,
)
from nerve.config import NerveConfig
from nerve.discord_tags import (
    DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
    DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
    DISCORD_PROJECT_TASK_VALIDATION_TARGET_KIND,
    DiscordForumTagError,
    complete_project_task,
    resume_project_task,
    transition_project_task_status,
)

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.db import Database
    from nerve.skills.manager import SkillManager

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 30.0
_MAX_MESSAGE_LENGTH = 2000
_MAX_THREAD_NAME_LENGTH = 100
_MAX_REPLY_CONTEXT_LENGTH = 500
_RECENT_MESSAGE_IDS = 4096
_THREAD_NAME_PREFIX = "Nerve · "
_AUTO_MODEL_TIER = "auto"
_PROJECT_TASK_LIFECYCLE_CONTEXT = """[Discord project-task lifecycle]
Discord forum tags are the sole source of task state; do not use Plane or
Backlog.md. An untagged thread is `new-task`. When the task state actually
changes, call `mcp__nerve__discord_project_task_status` exactly once with the
next allowed status:
`new-task` -> `ready-for-agent` / `blocked` / `cancelled`;
`ready-for-agent` -> `in-progress`;
`in-progress` -> `ready-for-user` / `backlog` / `blocked` / `cancelled`;
`ready-for-user` -> `blocked` / `cancelled`;
`blocked` -> `ready-for-agent`.
Do not infer task completion from a transient session ending. The agent hands
work back with `ready-for-user`; it does not create a completion approval and
cannot set `completed`. The user closes the task with `/close_task`, which
changes it to `completed` and archives the thread without another model turn.
A direct mention or true reply in `ready-for-user` first changes the task back
to `in-progress`; other states are not automatically claimed.]
"""
_TERMINAL_PROJECT_TASK_STATUSES = frozenset({"completed", "cancelled"})


def split_discord_message(text: str, limit: int = _MAX_MESSAGE_LENGTH) -> list[str]:
    """Split text into non-empty Discord-sized messages."""
    remaining = text.strip()
    chunks: list[str] = []
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break

        cut = remaining.rfind("\n", 0, limit + 1)
        if cut < limit // 2:
            cut = remaining.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit

        chunk = remaining[:cut].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[cut:].lstrip()
    return chunks


class DiscordChannel(BaseChannel):
    """Connect one allowlisted Discord guild to Nerve's channel router."""

    def __init__(
        self,
        config: NerveConfig,
        router: ChannelRouter,
        db: Database,
        *,
        context_summarizer: ContextSummarizer | None = None,
        skill_manager: SkillManager | None = None,
    ):
        self._nerve_config = config
        self.config = config.discord
        self.router = router
        self.db = db
        self._client: discord.Client | None = None
        self._command_tree: app_commands.CommandTree | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._post_ready_task: asyncio.Task[None] | None = None
        self._session_mirror: Any | None = None
        self._presence: Any | None = None
        self._approval_inbox: Any | None = None
        self._notification_inbox: Any | None = None
        self._system_audit: Any | None = None
        self._skill_forum: Any | None = None
        self._project_prompts: Any | None = None
        self._project_task_runner: Any | None = None
        self._skill_manager = skill_manager
        self._system_audit_lock = asyncio.Lock()
        self._notification_service: Any | None = None
        self._ready = asyncio.Event()
        self._startup_error: Exception | None = None
        self._ingest_guard = asyncio.Lock()
        self._recent_message_ids: OrderedDict[int, None] = OrderedDict()
        self._bot_user_id = 0
        self._allowed_authors = set(self.config.allowed_author_ids)
        self._project_task_creator = DiscordProjectTaskCreator(
            guild_id=self.config.guild_id,
            task_forums=self.config.task_forums,
            allowed_author_ids=self._allowed_authors,
            client=lambda: self._client,
        )
        self._text_channels = set(self.config.channel_ids)
        self._project_forums = {
            channel_id: project
            for project, channel_id in self.config.task_forums.items()
        }
        self._thread_context = DiscordThreadContext(
            db=db,
            guild_id=self.config.guild_id,
            project_forum_ids=(
                set(self._project_forums)
                | (
                    {self.config.skills_forum_id}
                    if self.config.skills_forum_id
                    else set()
                )
            ),
            allowed_author_ids=self._allowed_authors,
            bot_user_id=lambda: self._bot_user_id,
            message_text=self._message_text,
            safe_label=self._safe_label,
            summarizer=context_summarizer,
        )

    @property
    def name(self) -> str:
        return "discord"

    @property
    def capabilities(self) -> ChannelCapability:
        return (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.MARKDOWN
            | ChannelCapability.TYPING_INDICATOR
        )

    @property
    def automatic_responses(self) -> bool:
        """Discord messages are published only through the explicit MCP tool."""
        return False

    def set_notification_service(self, service: Any) -> None:
        """Wire notification answer routing before the gateway starts."""
        self._notification_service = service

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(max_message_length=_MAX_MESSAGE_LENGTH)

    def _validate_config(self) -> None:
        missing: list[str] = []
        if not self.config.guild_id:
            missing.append("guild_id")
        has_inbound_targets = bool(
            self._text_channels
            or self._project_forums
            or self.config.skills_forum_id
        )
        if not has_inbound_targets and not self.config.audit_forum_id:
            missing.append(
                "channel_ids, task_forums, skills_forum_id, or audit_forum_id"
            )
        if has_inbound_targets and not self._allowed_authors:
            missing.append("allowed_author_ids")
        if not (self.config.bot_token or self.config.bot_token_file):
            missing.append("bot_token or bot_token_file")
        if missing:
            raise ValueError(
                "discord is enabled but missing: " + ", ".join(missing)
            )
        forum_ids = list(self.config.task_forums.values())
        if len(forum_ids) != len(set(forum_ids)):
            raise ValueError(
                "discord.task_forums must use a different channel for each project"
            )
        if (
            self.config.skills_forum_id
            and self.config.skills_forum_id in set(forum_ids)
        ):
            raise ValueError(
                "discord.skills_forum_id must differ from task_forums"
            )
        overlap = self._text_channels & set(self._project_forums)
        if overlap:
            raise ValueError(
                "Discord channel IDs cannot be both text channels and project forums"
            )
        if (
            self.config.skills_forum_id
            and self.config.skills_forum_id in self._text_channels
        ):
            raise ValueError(
                "discord.skills_forum_id cannot also be a text channel"
            )
        if (
            self.config.audit_forum_id
            and self.config.audit_forum_id in (
                self._text_channels
                | set(self._project_forums)
                | (
                    {self.config.skills_forum_id}
                    if self.config.skills_forum_id
                    else set()
                )
            )
        ):
            raise ValueError(
                "discord.audit_forum_id must be an outbound-only forum and "
                "differ from every inbound target"
            )
        if self.config.audit_batch_window_seconds < 0:
            raise ValueError(
                "discord.audit_batch_window_seconds must be non-negative"
            )
        if self.config.presence_refresh_interval_seconds < 60:
            raise ValueError(
                "discord.presence_refresh_interval_seconds must be at least 60"
            )
        if len(self._nerve_config.codex.model_tiers) > 24:
            raise ValueError(
                "Discord supports at most 24 codex.model_tiers plus Auto"
            )

    def _load_token(self) -> str:
        if self.config.bot_token:
            token = self.config.bot_token.strip()
        else:
            path = self.config.bot_token_file
            if path is None:
                raise ValueError("Discord bot token is not configured")
            try:
                token = path.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise ValueError(
                    f"Cannot read Discord bot token file {path}: {exc}"
                ) from exc
            try:
                mode = stat.S_IMODE(path.stat().st_mode)
                if mode & 0o077:
                    logger.warning(
                        "Discord bot token file permissions are %03o; use 600",
                        mode,
                    )
            except OSError:
                pass
        if not token or "\n" in token:
            raise ValueError("Discord bot token file must contain one token")
        return token

    def _build_client(self) -> discord.Client:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True
        client = discord.Client(intents=intents)
        tree = app_commands.CommandTree(client)
        self._command_tree = tree
        guild = discord.Object(id=self.config.guild_id)

        @tree.command(
            name="model",
            description="Выбрать модель для текущей Nerve-сессии",
            guild=guild,
        )
        @app_commands.describe(
            tier="Auto включает маршрутизацию; остальные варианты фиксируют tier",
        )
        @app_commands.choices(tier=[
            app_commands.Choice(
                name="Auto — адаптивный выбор", value=_AUTO_MODEL_TIER,
            ),
            *[
                app_commands.Choice(name=tier.id, value=tier.id)
                for tier in self._nerve_config.codex.model_tiers
            ],
        ])
        async def model_command(
            interaction: discord.Interaction,
            tier: app_commands.Choice[str],
        ) -> None:
            await self._handle_model_command(interaction, tier.value)

        @tree.command(
            name="create-task",
            description="Создать задачу в проекте Nerve",
            guild=guild,
        )
        @app_commands.describe(project="Проект, в котором будет создана задача")
        async def create_task_command(
            interaction: discord.Interaction,
            project: str,
        ) -> None:
            await self._handle_create_task_command(interaction, project)

        @create_task_command.autocomplete("project")
        async def create_task_project_autocomplete(
            interaction: discord.Interaction,
            current: str,
        ) -> list[app_commands.Choice[str]]:
            if not self._project_task_creator.command_allowed(interaction):
                return []
            return self._project_task_creator.project_choices(current)

        @tree.command(
            name="to_work",
            description="Передать задачу в работу",
            guild=guild,
        )
        @app_commands.describe(
            force="Разрешить переход вне обычного графа статусов",
        )
        async def to_work_command(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            await self._handle_to_work_command(interaction, force=force)

        @tree.command(
            name="postpone",
            description="Отложить задачу в backlog",
            guild=guild,
        )
        @app_commands.describe(
            force="Разрешить переход вне обычного графа статусов",
        )
        async def postpone_command(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            await self._handle_postpone_command(interaction, force=force)

        @tree.command(
            name="close_task",
            description="Закрыть готовую задачу Nerve и архивировать тему",
            guild=guild,
        )
        @app_commands.describe(
            force="Разрешить закрытие вне обычного графа статусов",
        )
        async def close_task_command(
            interaction: discord.Interaction,
            force: bool = False,
        ) -> None:
            await self._handle_close_task_command(interaction, force=force)

        @client.event
        async def on_ready() -> None:
            await self._on_ready()

        @client.event
        async def on_message(message: discord.Message) -> None:
            await self._on_message(message)

        @client.event
        async def on_message_edit(
            _before: discord.Message,
            after: discord.Message,
        ) -> None:
            await self._on_message_edit(after)

        @client.event
        async def on_message_delete(message: discord.Message) -> None:
            await self._on_message_delete(message)

        @client.event
        async def on_thread_update(
            _before: discord.Thread,
            after: discord.Thread,
        ) -> None:
            runner = self._project_task_runner
            if runner is not None:
                runner.notify_thread_update(after)

        return client

    async def _sync_application_commands(self) -> None:
        """Synchronize Nerve's guild-only commands without global propagation."""
        if self._command_tree is None:
            return
        guild = discord.Object(id=self.config.guild_id)
        commands = await self._command_tree.sync(guild=guild)
        logger.info(
            "Synced %d Discord application command(s) to guild %s",
            len(commands), self.config.guild_id,
        )

    async def _respond_to_interaction(
        self,
        interaction: discord.Interaction,
        text: str,
    ) -> None:
        """Send one private interaction response, including test doubles."""
        await interaction.response.send_message(text, ephemeral=True)

    async def _handle_model_command(
        self,
        interaction: discord.Interaction,
        tier_id: str,
    ) -> None:
        """Apply a user-selected Codex tier to this Discord thread's session."""
        guild_id = int(getattr(interaction, "guild_id", 0) or 0)
        channel_id = int(getattr(interaction, "channel_id", 0) or 0)
        user_id = int(getattr(getattr(interaction, "user", None), "id", 0) or 0)
        if guild_id != self.config.guild_id:
            await self._respond_to_interaction(
                interaction, "Эта команда доступна только в настроенном сервере Nerve.",
            )
            return
        if user_id not in self._allowed_authors:
            await self._respond_to_interaction(
                interaction, "У вас нет доступа к настройке модели Nerve.",
            )
            return
        if not channel_id:
            await self._respond_to_interaction(
                interaction, "Откройте команду внутри форумной темы Nerve.",
            )
            return

        mapping = await self.db.get_channel_session(
            f"discord:{guild_id}:{channel_id}",
        )
        session_id = (mapping or {}).get("session_id")
        if not session_id:
            await self._respond_to_interaction(
                interaction,
                "В этой теме ещё нет Nerve-сессии. Сначала отправьте сообщение боту.",
            )
            return
        binding = await self.db.get_discord_session_binding(session_id)
        if (
            binding is None
            or str(binding["guild_id"]) != str(guild_id)
            or str(binding["thread_id"]) != str(channel_id)
        ):
            await self._respond_to_interaction(
                interaction, "Эта тема не привязана к текущей Nerve-сессии.",
            )
            return
        session = await self.db.get_session(session_id)
        if session is None:
            await self._respond_to_interaction(
                interaction, "Nerve-сессия для этой темы больше не существует.",
            )
            return
        if session.get("backend") != "codex":
            await self._respond_to_interaction(
                interaction, "Выбор tier доступен только для Codex-сессий.",
            )
            return
        if self.router.engine.sessions.is_running(session_id):
            await self._respond_to_interaction(
                interaction,
                "Дождитесь завершения текущего хода, затем выберите модель.",
            )
            return

        if tier_id == _AUTO_MODEL_TIER:
            await self.db.update_session_fields(session_id, {"model_pinned": 0})
            await self._respond_to_interaction(
                interaction,
                "Модель: Auto. Для следующих ходов включён адаптивный routing.",
            )
            return

        tier = self._nerve_config.codex.tier(tier_id)
        if tier is None:
            await self._respond_to_interaction(
                interaction, "Неизвестный tier модели.",
            )
            return
        await self.db.update_session_fields(session_id, {
            "model": tier.model,
            "model_tier": tier.id,
            "reasoning_effort": tier.effort,
            "model_pinned": 1,
        })
        await self._respond_to_interaction(
            interaction, f"Модель: {tier.id}. Tier закреплён для следующих ходов.",
        )

    async def _handle_create_task_command(
        self,
        interaction: discord.Interaction,
        project: str,
    ) -> None:
        """Open the project-task modal after validating the command context."""
        guild_id = int(getattr(interaction, "guild_id", 0) or 0)
        if guild_id != self.config.guild_id:
            await self._respond_to_interaction(
                interaction, "Эта команда доступна только в настроенном сервере Nerve.",
            )
            return
        if not self._project_task_creator.command_allowed(interaction):
            await self._respond_to_interaction(
                interaction, "У вас нет доступа к созданию задач Nerve.",
            )
            return
        try:
            project_name = self._project_task_creator.project_name(project)
        except DiscordProjectTaskCreateError as exc:
            await self._respond_to_interaction(interaction, str(exc))
            return
        await interaction.response.send_modal(
            ProjectTaskCreateModal(
                self._project_task_creator,
                project_name,
            ),
        )

    async def _project_task_command_context(
        self,
        interaction: discord.Interaction,
        *,
        command_name: str,
        access_message: str,
    ) -> tuple[int, int] | None:
        """Validate a guild command that changes a project task lifecycle."""
        guild_id = int(getattr(interaction, "guild_id", 0) or 0)
        user_id = int(getattr(getattr(interaction, "user", None), "id", 0) or 0)
        channel_id = int(getattr(interaction, "channel_id", 0) or 0)
        if guild_id != self.config.guild_id:
            await self._respond_to_interaction(
                interaction,
                "Эта команда доступна только в настроенном сервере Nerve.",
            )
            return
        if user_id not in self._allowed_authors:
            await self._respond_to_interaction(
                interaction,
                access_message,
            )
            return None

        channel = getattr(interaction, "channel", None)
        if channel is None and self._client is not None:
            channel = self._client.get_channel(channel_id)
        parent_id = getattr(channel, "parent_id", None)
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError):
            parent_id = 0
        if (
            not channel_id
            or not parent_id
            or parent_id not in self._project_forums
        ):
            await self._respond_to_interaction(
                interaction,
                f"Откройте /{command_name} внутри темы задачи проекта Nerve.",
            )
            return None
        return user_id, channel_id

    async def _handle_project_task_status_command(
        self,
        interaction: discord.Interaction,
        *,
        command_name: str,
        target_status: str,
        force: bool,
        access_message: str,
    ) -> None:
        """Apply one authenticated project-task lifecycle transition."""
        context = await self._project_task_command_context(
            interaction,
            command_name=command_name,
            access_message=access_message,
        )
        if context is None:
            return
        user_id, channel_id = context

        await interaction.response.defer(ephemeral=True)
        try:
            result = await asyncio.to_thread(
                transition_project_task_status,
                self._nerve_config,
                thread_id=channel_id,
                target_status=target_status,
                audit_reason=f"Nerve /{command_name} by {user_id}",
                force=force,
            )
        except DiscordForumTagError as exc:
            await interaction.followup.send(
                f"Статус задачи не изменён: {exc}", ephemeral=True,
            )
            return
        except Exception:
            logger.exception(
                "Unexpected /%s failure for Discord thread %s",
                command_name, channel_id,
            )
            await interaction.followup.send(
                "Статус задачи не изменён из-за внутренней ошибки Nerve.",
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            f"Статус задачи: {result['current_status']}.", ephemeral=True,
        )

    async def _handle_to_work_command(
        self,
        interaction: discord.Interaction,
        *,
        force: bool = False,
    ) -> None:
        await self._handle_project_task_status_command(
            interaction,
            command_name="to_work",
            target_status="ready-for-agent",
            force=force,
            access_message="У вас нет доступа к управлению задачами Nerve.",
        )

    async def _handle_postpone_command(
        self,
        interaction: discord.Interaction,
        *,
        force: bool = False,
    ) -> None:
        await self._handle_project_task_status_command(
            interaction,
            command_name="postpone",
            target_status="backlog",
            force=force,
            access_message="У вас нет доступа к управлению задачами Nerve.",
        )

    async def _handle_close_task_command(
        self,
        interaction: discord.Interaction,
        *,
        force: bool = False,
    ) -> None:
        """Close the current ready-for-user project task mechanically."""
        context = await self._project_task_command_context(
            interaction,
            command_name="close_task",
            access_message="У вас нет доступа к закрытию задач Nerve.",
        )
        if context is None:
            return
        user_id, channel_id = context

        await interaction.response.defer(ephemeral=True)
        try:
            result = await asyncio.to_thread(
                complete_project_task,
                self._nerve_config,
                thread_id=channel_id,
                audit_reason=f"Nerve /close_task by {user_id}",
                force=force,
            )
        except DiscordForumTagError as exc:
            await interaction.followup.send(
                f"Задача не закрыта: {exc}", ephemeral=True,
            )
            return
        except Exception:
            logger.exception(
                "Unexpected /close_task failure for Discord thread %s",
                channel_id,
            )
            await interaction.followup.send(
                "Задача не закрыта из-за внутренней ошибки Nerve.",
                ephemeral=True,
            )
            return

        binding = None
        try:
            binding = await self.db.get_discord_session_binding_by_thread(
                self.config.guild_id, channel_id,
            )
        except Exception:
            logger.exception(
                "Could not resolve the session bound to closed Discord task %s",
                channel_id,
            )
        session_id = str((binding or {}).get("session_id") or "").strip()
        if session_id and self._notification_service is not None:
            try:
                await self._notification_service.retire_completed_project_task_session(
                    session_id,
                )
            except Exception:
                logger.exception(
                    "Could not retire session %s after closing Discord task %s",
                    session_id[:8], channel_id,
                )
        await interaction.followup.send(
            f"Задача закрыта и тема архивирована (статус: "
            f"{result['current_status']}).",
            ephemeral=True,
        )

    async def create_project_task(
        self,
        *,
        project: str,
        title: str,
        description: str,
    ) -> tuple[str, int]:
        """Create a project task for an authorized agent tool call."""
        return await self._project_task_creator.create_for_agent(
            project=project,
            title=title,
            description=description,
            initial_lifecycle_tag="backlog",
        )

    def _project_task_auditor(self) -> Any:
        from nerve.channels.discord_project_task_audit import (
            DiscordProjectTaskAuditor,
        )

        return DiscordProjectTaskAuditor(
            client=self._client,
            db=self.db,
            guild_id=self.config.guild_id,
            project_forums=self._project_forums,
        )

    async def audit_project_tasks(self, *, limit: int = 20) -> dict[str, Any]:
        """Read the next bounded batch of completed project tasks."""
        return await self._project_task_auditor().get_batch(limit=limit)

    async def get_project_task_audit_batch(
        self, *, limit: int = 20,
    ) -> dict[str, Any]:
        return await self.audit_project_tasks(limit=limit)

    async def read_project_task_for_audit(
        self, *, thread_id: str,
    ) -> dict[str, Any] | None:
        """Read one task only when the auditor currently considers it eligible."""
        return await self._project_task_auditor().get_task(thread_id)

    async def get_project_task_audit_task(
        self, *, thread_id: str,
    ) -> dict[str, Any] | None:
        return await self.read_project_task_for_audit(thread_id=thread_id)

    async def start(self) -> None:
        if self._client_task is not None:
            return

        self._validate_config()
        token = self._load_token()
        self._ready.clear()
        self._startup_error = None
        self._client = self._build_client()
        self._client_task = asyncio.create_task(
            self._client.start(token, reconnect=True),
            name="discord-channel",
        )

        ready_wait = asyncio.create_task(self._ready.wait())
        done, _ = await asyncio.wait(
            {ready_wait, self._client_task},
            timeout=_CONNECT_TIMEOUT_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if ready_wait in done:
            if self._startup_error is not None:
                await self.stop()
                raise self._startup_error
            logger.info(
                "Discord channel connected to one guild with %d text channel(s) "
                "and %d project forum(s)%s",
                len(self._text_channels),
                len(self._project_forums),
                " plus the skill forum"
                if self.config.skills_forum_id
                else "",
            )
            return

        ready_wait.cancel()
        if self._client_task in done:
            await self._client_task
            raise RuntimeError("Discord client stopped before becoming ready")

        await self.stop()
        raise TimeoutError(
            f"Discord client did not become ready within "
            f"{_CONNECT_TIMEOUT_SECONDS:.0f} seconds"
        )

    async def stop(self) -> None:
        client = self._client
        task = self._client_task
        post_ready_task = self._post_ready_task
        self._client = None
        self._client_task = None
        self._post_ready_task = None

        if post_ready_task is not None:
            post_ready_task.cancel()
            try:
                await post_ready_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug(
                    "Discord post-ready task stopped with an error",
                    exc_info=True,
                )

        mirror = self._session_mirror
        self._session_mirror = None
        presence = self._presence
        self._presence = None
        self._approval_inbox = None
        self._notification_inbox = None
        system_audit = self._system_audit
        self._system_audit = None
        skill_forum = self._skill_forum
        self._skill_forum = None
        self._project_prompts = None
        project_task_runner = self._project_task_runner
        self._project_task_runner = None

        if mirror is not None:
            await mirror.stop()
        if system_audit is not None:
            await system_audit.stop()
        if skill_forum is not None:
            await skill_forum.stop()
        if project_task_runner is not None:
            await project_task_runner.stop()
        if presence is not None:
            await presence.stop()
        if client is not None and not client.is_closed():
            await client.close()
        if task is not None:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug("Discord client stopped with an error", exc_info=True)

    async def _on_ready(self) -> None:
        try:
            if self._client is None or self._client.user is None:
                raise RuntimeError("Discord client became ready without a bot user")
            self._bot_user_id = int(self._client.user.id)
            guild = self._client.get_guild(self.config.guild_id)
            if guild is None:
                raise ValueError(
                    "Discord bot cannot access the configured guild"
                )

            configured = self._text_channels | set(self._project_forums)
            if self.config.skills_forum_id:
                configured.add(self.config.skills_forum_id)
            if self.config.audit_forum_id:
                configured.add(self.config.audit_forum_id)
            missing = [
                channel_id
                for channel_id in configured
                if guild.get_channel(channel_id) is None
            ]
            if missing:
                raise ValueError(
                    "Discord bot cannot access configured channel(s): "
                    + ", ".join(str(value) for value in sorted(missing))
                )
            if (
                self._post_ready_task is None
                or self._post_ready_task.done()
            ):
                self._post_ready_task = asyncio.create_task(
                    self._run_post_ready(guild),
                    name="discord-post-ready",
                )
        except Exception as exc:
            self._startup_error = exc
        finally:
            self._ready.set()

    async def _run_post_ready(self, guild: discord.Guild) -> None:
        """Start optional integrations and catch up without blocking startup."""
        try:
            await self._sync_application_commands()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Discord slash-command sync failed; continuing without "
                "updated application commands"
            )

        if self.config.audit_forum_id:
            try:
                await self._ensure_system_audit(guild)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Discord system audit failed to start; "
                    "continuing without system events"
                )

        if self.config.presence_enabled and self._presence is None:
            try:
                from nerve.channels.discord_presence import DiscordPresence

                self._presence = DiscordPresence(
                    client=self._client,
                    running_session_count=self._running_session_count,
                    rate_limit_reader=self._read_codex_rate_limits,
                    refresh_interval_seconds=(
                        self.config.presence_refresh_interval_seconds
                    ),
                )
                await self._presence.start()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._presence = None
                logger.exception(
                    "Discord presence failed to start; "
                    "continuing without operational status"
                )

        if self.config.audit_forum_id and self._session_mirror is None:
            try:
                from nerve.channels.discord_mirror import DiscordSessionMirror

                self._session_mirror = DiscordSessionMirror(
                    client=self._client,
                    db=self.db,
                    guild_id=self.config.guild_id,
                    forum_id=self.config.audit_forum_id,
                    batch_window_seconds=(
                        self.config.audit_batch_window_seconds
                    ),
                    is_session_running=self.router.engine.is_session_running,
                )
                await self._session_mirror.start()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._session_mirror = None
                logger.exception(
                    "Discord session mirror failed to start; "
                    "continuing without the audit mirror"
                )

        if (
            self.config.audit_forum_id
            and self._notification_service is not None
            and self._approval_inbox is None
        ):
            try:
                from nerve.channels.discord_approvals import (
                    DiscordApprovalInbox,
                )

                self._approval_inbox = DiscordApprovalInbox(
                    client=self._client,
                    db=self.db,
                    notification_service=self._notification_service,
                    guild_id=self.config.guild_id,
                    forum_id=self.config.audit_forum_id,
                    allowed_author_ids=self._allowed_authors,
                )
                await self._approval_inbox.start(guild)
                await self._restore_project_task_completion_cards()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._approval_inbox = None
                logger.exception(
                    "Discord approval inbox failed to start; "
                    "continuing without Discord approval delivery"
                )

        if (
            self.config.audit_forum_id
            and self._notification_service is not None
            and self._notification_inbox is None
        ):
            try:
                from nerve.channels.discord_notifications import (
                    DiscordNotificationInbox,
                )

                self._notification_inbox = DiscordNotificationInbox(
                    client=self._client,
                    db=self.db,
                    notification_service=self._notification_service,
                    guild_id=self.config.guild_id,
                    forum_id=self.config.audit_forum_id,
                    allowed_author_ids=self._allowed_authors,
                )
                await self._notification_inbox.start(guild)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._notification_inbox = None
                logger.exception(
                    "Discord notification inbox failed to start; "
                    "continuing without Discord notify/question delivery"
                )

        if self._project_forums and self._project_prompts is None:
            try:
                from nerve.channels.discord_project_prompts import (
                    DiscordProjectPrompts,
                )

                self._project_prompts = DiscordProjectPrompts(
                    client=self._client,
                    db=self.db,
                    guild_id=self.config.guild_id,
                    project_forums=self._project_forums,
                    allowed_author_ids=self._allowed_authors,
                )
                await self._project_prompts.start(guild)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._project_prompts = None
                logger.exception(
                    "Discord project prompts failed to start; continuing "
                    "without project-level instructions"
                )

        # Skill reconciliation may scan long thread histories. Keep it after
        # the user-facing inboxes so enabling the skill forum cannot delay
        # notification and approval readiness during startup.
        if (
            self.config.skills_forum_id
            and self._skill_manager is not None
            and self._skill_forum is None
        ):
            try:
                from nerve.channels.discord_skills import DiscordSkillForum

                self._skill_forum = DiscordSkillForum(
                    client=self._client,
                    skill_manager=self._skill_manager,
                    guild_id=self.config.guild_id,
                    forum_id=self.config.skills_forum_id,
                )
                await self._skill_forum.start(guild)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._skill_forum = None
                logger.exception(
                    "Discord skill forum failed to start; "
                    "continuing without skill threads"
                )

        try:
            await self._sync_backlog(guild)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Discord backlog catch-up failed; live messages remain available"
            )

        if (
            self.config.project_task_runner_enabled
            and self._project_forums
            and self._project_task_runner is None
        ):
            try:
                from nerve.channels.discord_project_task_runner import (
                    DiscordProjectTaskRunner,
                )

                prompts = self._project_prompts
                self._project_task_runner = DiscordProjectTaskRunner(
                    config=self._nerve_config,
                    router=self.router,
                    db=self.db,
                    project_forums=self._project_forums,
                    notification_service=self._notification_service,
                    system_audit=self.emit_system_event,
                    retire_recovery_action=(
                        self._approval_inbox.retire
                        if self._approval_inbox is not None else None
                    ),
                    project_prompt=(
                        prompts.prompt_for_thread if prompts is not None else None
                    ),
                )
                await self._project_task_runner.start(guild)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._project_task_runner = None
                logger.exception(
                    "Discord project-task runner failed to start; "
                    "continuing without autonomous task execution"
                )

    async def _ensure_system_audit(
        self,
        guild: discord.Guild | None = None,
    ) -> Any:
        audit = self._system_audit
        if audit is not None:
            return audit
        if not self.config.audit_forum_id:
            raise RuntimeError("Discord audit forum is not configured")

        async with self._system_audit_lock:
            audit = self._system_audit
            if audit is not None:
                return audit
            if self._client is None:
                raise RuntimeError("Discord client is not running")

            from nerve.channels.discord_system_audit import DiscordSystemAudit

            audit = DiscordSystemAudit(
                client=self._client,
                db=self.db,
                guild_id=self.config.guild_id,
                forum_id=self.config.audit_forum_id,
                stream=broadcaster,
            )
            await audit.start(guild)
            self._system_audit = audit
            return audit

    async def emit_system_event(
        self,
        title: str,
        *,
        details: str = "",
        level: str = "info",
    ) -> None:
        """Append an important process event to the AUDIT System thread."""
        audit = await self._ensure_system_audit()
        await audit.emit(title, details=details, level=level)

    def _running_session_count(self) -> int:
        return len(self.router.engine.sessions.get_running_ids())

    async def _read_codex_rate_limits(self) -> dict[str, Any] | None:
        backend = self.router.engine._backends.get("codex")
        if backend is None:
            return None
        status = await backend.preflight(
            force=True,
            validate_default_model=False,
        )
        rate_limits = status.get("rate_limits")
        return rate_limits if isinstance(rate_limits, dict) else None

    async def _on_message(self, message: discord.Message) -> None:
        try:
            await self._ingest(message)
        except Exception:
            logger.exception(
                "Discord message dispatch failed for channel %s",
                getattr(message.channel, "id", "unknown"),
            )

    async def _on_message_edit(self, message: discord.Message) -> None:
        prompts = self._project_prompts
        if prompts is None:
            return
        try:
            await prompts.observe_edit(message)
        except Exception:
            logger.exception("Discord project prompt edit handling failed")

    async def _on_message_delete(self, message: discord.Message) -> None:
        prompts = self._project_prompts
        if prompts is None:
            return
        try:
            await prompts.observe_delete(message)
        except Exception:
            logger.exception("Discord project prompt deletion handling failed")

    async def _ingest(self, message: discord.Message) -> None:
        message_id = int(message.id)
        async with self._ingest_guard:
            if message_id in self._recent_message_ids:
                return
            self._recent_message_ids[message_id] = None
            if len(self._recent_message_ids) > _RECENT_MESSAGE_IDS:
                self._recent_message_ids.popitem(last=False)

        try:
            if self._project_prompts is not None:
                if await self._project_prompts.observe_message(message):
                    await self._advance_cursor(
                        int(message.channel.id),
                        message_id,
                    )
                    return
            if (
                self._skill_forum is not None
                and getattr(message.channel, "parent_id", None)
                == self.config.skills_forum_id
            ):
                await self._skill_forum.register_thread(message.channel)
            accepted = self._accepts(message)
            context_block = ""
            context_message = self._thread_context.accepts_message(message)
            if context_message:
                if accepted:
                    context_block = await self._thread_context.prepare(message)
                else:
                    await self._thread_context.record(message)

            if accepted:
                target = message.channel
                if int(message.channel.id) in self._text_channels:
                    target = await self._ensure_conversation_thread(message)
                if (
                    self._message_scope(message) == "project_forum_thread"
                    and (
                        self._has_direct_mention(message)
                        or self._is_reply_to_bot(message)
                    )
                ):
                    resume_result = await asyncio.to_thread(
                        resume_project_task,
                        self._nerve_config,
                        thread_id=int(message.channel.id),
                        audit_reason=(
                            "Nerve Discord project-task mention/reply "
                            f"{message.id}"
                        ),
                    )
                    if (
                        resume_result.get("current_status")
                        in _TERMINAL_PROJECT_TASK_STATUSES
                    ):
                        await self._advance_cursor(
                            int(message.channel.id), message_id,
                        )
                        return
                await self._dispatch(
                    message,
                    target=target,
                    context_block=context_block,
                )
                if context_message:
                    await self._thread_context.mark_delivered(message)
                if int(target.id) != int(message.channel.id):
                    await self._advance_cursor(
                        int(target.id),
                        message_id,
                    )
            await self._advance_cursor(
                int(message.channel.id),
                message_id,
            )
        except BaseException:
            # A failed dispatch must remain retryable on reconnect. Concurrent
            # gateway/backlog delivery is still collapsed while it is running.
            async with self._ingest_guard:
                self._recent_message_ids.pop(message_id, None)
            raise

    async def _sync_backlog(self, guild: discord.Guild) -> None:
        """Prime new targets and replay messages missed after a restart."""
        for channel_id in sorted(self._text_channels):
            channel = guild.get_channel(channel_id)
            if channel is not None:
                await self._sync_target(channel, process_existing=False)

        guild_threads = await guild.active_threads()
        active_threads = {
            int(thread.id): thread
            for thread in guild_threads
            if (
                getattr(thread, "parent_id", None) in self._text_channels
                or getattr(thread, "parent_id", None) in self._project_forums
                or (
                    self.config.skills_forum_id
                    and getattr(thread, "parent_id", None)
                    == self.config.skills_forum_id
                )
            )
        }
        for thread in active_threads.values():
            if getattr(thread, "parent_id", None) in self._text_channels:
                await self._sync_target(thread, process_existing=False)

        forum_ids = set(self._project_forums)
        if self.config.skills_forum_id:
            forum_ids.add(self.config.skills_forum_id)
        for forum_id in sorted(forum_ids):
            forum = guild.get_channel(forum_id)
            if forum is None:
                continue
            forum_cursor = await self.db.get_sync_cursor(
                self._forum_source_name(forum_id),
            )
            previous_thread_id = int(forum_cursor or 0)
            discovered = {
                thread_id: thread
                for thread_id, thread in active_threads.items()
                if getattr(thread, "parent_id", None) == forum_id
            }
            if previous_thread_id:
                try:
                    async for thread in forum.archived_threads(limit=100):
                        if int(thread.id) <= previous_thread_id:
                            break
                        discovered[int(thread.id)] = thread
                except Exception:
                    logger.warning(
                        "Discord archived-thread catch-up failed for one forum",
                        exc_info=True,
                    )

            for thread_id, thread in sorted(discovered.items()):
                await self._sync_target(
                    thread,
                    process_existing=bool(
                        previous_thread_id and thread_id > previous_thread_id
                    ),
                )

            forum_head = int(
                getattr(forum, "last_message_id", None)
                or max(discovered, default=previous_thread_id)
            )
            if forum_head > previous_thread_id:
                await self.db.set_sync_cursor(
                    self._forum_source_name(forum_id),
                    str(forum_head),
                )

    async def _sync_target(
        self,
        channel: Any,
        *,
        process_existing: bool,
    ) -> None:
        channel_id = int(channel.id)
        cursor = await self.db.get_sync_cursor(
            self._source_name(channel_id),
        )
        if cursor is None and not process_existing:
            head = int(getattr(channel, "last_message_id", None) or 0)
            await self.db.set_sync_cursor(
                self._source_name(channel_id),
                str(head),
            )
            return

        cursor_id = int(cursor or 0)
        after = discord.Object(id=cursor_id) if cursor_id else None
        try:
            async for message in channel.history(
                limit=None,
                after=after,
                oldest_first=True,
            ):
                await self._ingest(message)
        except Exception:
            # Leave the cursor at the last successfully handled message so the
            # failed item is retried on the next reconnect/restart.
            logger.warning(
                "Discord backlog catch-up stopped for one channel",
                exc_info=True,
            )

    async def _advance_cursor(self, channel_id: int, message_id: int) -> None:
        source = self._source_name(channel_id)
        current = int(await self.db.get_sync_cursor(source) or 0)
        if message_id > current:
            await self.db.set_sync_cursor(source, str(message_id))

    def _source_name(self, channel_id: int) -> str:
        return f"discord:{self.config.guild_id}:{channel_id}"

    def _forum_source_name(self, forum_id: int) -> str:
        return f"discord-forum:{self.config.guild_id}:{forum_id}"

    def _message_scope(self, message: discord.Message) -> str:
        channel_id = int(message.channel.id)
        parent_id = getattr(message.channel, "parent_id", None)
        if channel_id in self._text_channels:
            return "text_channel"
        if parent_id in self._text_channels:
            return "conversation_thread"
        if parent_id in self._project_forums:
            return "project_forum_thread"
        if (
            parent_id == self.config.skills_forum_id
            and self._skill_forum is not None
            and self._skill_forum.skill_id_for_thread(channel_id)
        ):
            return "skill_forum_thread"
        return ""

    def _has_direct_mention(self, message: discord.Message) -> bool:
        raw_mentions = {
            int(value) for value in getattr(message, "raw_mentions", [])
        }
        return self._bot_user_id in raw_mentions

    @staticmethod
    def _referenced_reply(message: discord.Message) -> Any | None:
        reference = getattr(message, "reference", None)
        if reference is None:
            return None
        if (
            getattr(reference, "type", discord.MessageReferenceType.reply)
            is not discord.MessageReferenceType.reply
        ):
            return None

        referenced_message = getattr(reference, "resolved", None)
        if referenced_message is None:
            referenced_message = getattr(reference, "cached_message", None)
        return referenced_message

    def _is_reply_to_bot(self, message: discord.Message) -> bool:
        referenced_message = self._referenced_reply(message)
        author = getattr(referenced_message, "author", None)
        author_id = int(getattr(author, "id", 0) or 0)
        return bool(self._bot_user_id and author_id == self._bot_user_id)

    def _reply_context(self, message: discord.Message) -> str:
        referenced_message = self._referenced_reply(message)
        if referenced_message is None:
            return ""

        referenced_author = getattr(referenced_message, "author", None)
        referenced_author_id = int(
            getattr(referenced_author, "id", 0) or 0,
        )
        if self._bot_user_id and referenced_author_id == self._bot_user_id:
            sender = "assistant"
        else:
            sender = self._safe_label(
                getattr(referenced_author, "display_name", "")
                or getattr(referenced_author, "name", "")
            )
            if not sender:
                sender = str(referenced_author_id or "user")

        original = str(getattr(referenced_message, "content", "") or "")
        if not original:
            return f"[Reply to {sender}'s message]"
        display = original
        if len(display) > _MAX_REPLY_CONTEXT_LENGTH:
            display = display[:_MAX_REPLY_CONTEXT_LENGTH] + "…"
        return f'[Reply to {sender}: "{display}"]'

    def _accepts(self, message: discord.Message) -> bool:
        guild = message.guild
        author = message.author

        if guild is None or int(guild.id) != self.config.guild_id:
            return False
        author_id = int(author.id)
        if author_id == self._bot_user_id or author_id not in self._allowed_authors:
            return False

        scope = self._message_scope(message)
        if not scope:
            return False

        if (
            self.config.require_mention
            and scope != "conversation_thread"
            and not self._has_direct_mention(message)
            and not self._is_reply_to_bot(message)
        ):
            return False

        return bool(self._message_text(message))

    def _message_text(self, message: discord.Message) -> str:
        text = str(getattr(message, "content", "") or "")
        if self._bot_user_id:
            mention = re.compile(rf"<@!?{self._bot_user_id}>")
            text = mention.sub("", text)
        attachment_lines: list[str] = []
        for attachment in list(
            getattr(message, "attachments", []) or []
        ):
            filename = " ".join(
                str(getattr(attachment, "filename", "") or "").split()
            )[:200]
            url = str(getattr(attachment, "url", "") or "")[:2048]
            size = int(getattr(attachment, "size", 0) or 0)
            if filename and url:
                attachment_lines.append(
                    f"[Discord attachment: {filename}; "
                    f"{size} bytes; {url}]"
                )
        return "\n".join(
            part for part in (text.strip(), *attachment_lines) if part
        )

    def _conversation_thread_name(self, message: discord.Message) -> str:
        subject = " ".join(self._message_text(message).split())
        if not subject:
            subject = "conversation"
        available = _MAX_THREAD_NAME_LENGTH - len(_THREAD_NAME_PREFIX)
        return _THREAD_NAME_PREFIX + subject[:available].rstrip()

    async def _ensure_conversation_thread(
        self,
        message: discord.Message,
    ) -> Any:
        existing = getattr(message, "thread", None)
        if existing is not None:
            return existing

        if self._client is not None:
            existing = self._client.get_channel(int(message.id))
            if (
                existing is not None
                and getattr(existing, "parent_id", None)
                == int(message.channel.id)
            ):
                return existing

        try:
            return await message.create_thread(
                name=self._conversation_thread_name(message),
                reason="Nerve Discord conversation",
            )
        except discord.HTTPException as exc:
            # Discord returns 160004 when another event handler or process
            # created the message thread between the cache check and this call.
            if exc.code == 160004 and self._client is not None:
                existing = await self._client.fetch_channel(int(message.id))
                if (
                    getattr(existing, "parent_id", None)
                    == int(message.channel.id)
                ):
                    return existing
            raise

    async def _dispatch(
        self,
        message: discord.Message,
        *,
        target: Any | None = None,
        context_block: str = "",
    ) -> None:
        guild_id = int(message.guild.id)
        target = target or message.channel
        channel_id = int(target.id)
        parent_id = getattr(target, "parent_id", None)
        project = self._project_forums.get(parent_id, "")
        skill_id = ""
        if self._skill_forum is not None:
            skill_id = self._skill_forum.skill_id_for_thread(channel_id)
        author_name = self._safe_label(
            getattr(message.author, "display_name", "")
        )
        channel_name = self._safe_label(
            getattr(target, "name", "")
        )
        author = author_name or str(message.author.id)
        if project:
            location = f"форумной темы проекта {project}"
        elif skill_id:
            location = f"форумной темы скилла `{skill_id}`"
        elif parent_id in self._text_channels:
            location = "диалогового треда обычного канала"
        else:
            location = "общего текстового канала"
        response_hint = (
            " Бот получает все сообщения треда; публичный ответ нужен только "
            "когда он полезен."
            if parent_id in self._text_channels
            else ""
        )
        context = (
            f"[Это сообщение Discord из {location}; ответ увидят участники "
            f"канала.{response_hint} Автор: {author}.]\n\n"
        )
        if skill_id:
            local_skill = (
                await self._skill_manager.get_skill(skill_id)
                if self._skill_manager is not None
                else None
            )
            if local_skill is not None:
                skill_context = (
                    f"[Тред привязан к локальному скиллу `{skill_id}`. "
                    "Перед изменением загрузи его через skill_get; изменения "
                    "вноси через skill_update полным содержимым SKILL.md. "
                    "Локальный файл — источник истины, Discord — поверхность "
                    "обсуждения и обмена снимками.]"
                )
            else:
                skill_context = (
                    f"[Тред представляет скилл `{skill_id}`, которого сейчас "
                    "нет в локальном workspace этого агента. Точные снимки "
                    "SKILL.md прикреплены к сообщениям треда. Не импортируй "
                    "его без явной просьбы; при запросе на передачу создай "
                    "локальный скилл из последнего снимка.]"
                )
            context += skill_context + "\n\n"
        project_prompt = (
            self._project_prompts.prompt_for_thread(parent_id, channel_id)
            if self._project_prompts is not None
            else ""
        )
        task_lifecycle_context = (
            _PROJECT_TASK_LIFECYCLE_CONTEXT if project else ""
        )
        text = self._message_text(message)
        reply_context = self._reply_context(message)
        text = "\n\n".join(
            part
            for part in (
                project_prompt,
                task_lifecycle_context,
                context_block,
                reply_context,
                text,
            )
            if part
        )
        title = (
            f"Discord · {project} · {channel_name or channel_id}"
            if project
            else (
                f"Discord · SKILL · {skill_id}"
                if skill_id
                else (
                    f"Discord · thread · {channel_name or channel_id}"
                    if parent_id in self._text_channels
                    else f"Discord · {channel_name or channel_id}"
                )
            )
        )
        metadata = {
            "message_id": str(message.id),
            "discord_guild_id": guild_id,
            "discord_channel_id": channel_id,
            "discord_parent_channel_id": parent_id,
            "discord_origin_channel_id": int(message.channel.id),
            "discord_project": project,
            "discord_skill_id": skill_id,
            "discord_author_id": int(message.author.id),
            "discord_author_name": author_name,
        }
        if project and self._nerve_config.agent.backend == "codex":
            tier = self._nerve_config.codex.tier(
                self.config.project_model_tiers.get(project),
            )
            if tier is not None:
                metadata.update({
                    "initial_model": tier.model,
                    "initial_model_tier": tier.id,
                    "initial_reasoning_effort": tier.effort,
                })

        await self.router.handle_message(InboundMessage(
            channel_name=self.name,
            channel_key=f"discord:{guild_id}:{channel_id}",
            sender_id=str(channel_id),
            text=context + text,
            session_title=title,
            metadata=metadata,
            steer_if_busy=True,
        ))

    async def _resolve_messageable(self, target: str) -> Any:
        if self._client is None:
            raise RuntimeError("Discord client is not running")
        channel_id = int(target)
        channel = self._client.get_channel(channel_id)
        if channel is None:
            channel = await self._client.fetch_channel(channel_id)
        if not hasattr(channel, "send"):
            raise ValueError("Discord target is not a message channel or thread")
        return channel

    async def send(self, message: OutboundMessage) -> None:
        channel = await self._resolve_messageable(message.target)
        channel = await self._active_project_thread(channel, message.target)
        if channel is None:
            return
        model = message.metadata.get("model")
        reasoning_effort = message.metadata.get("reasoning_effort")
        chunks: list[str]
        if isinstance(model, str) and model:
            header = f"-# {model}"
            if isinstance(reasoning_effort, str) and reasoning_effort:
                header += f" · {reasoning_effort}"
            chunks = split_discord_message(
                message.text,
                limit=max(1, _MAX_MESSAGE_LENGTH - len(header) - 1),
            )
            if chunks:
                chunks[0] = f"{header}\n{chunks[0]}"
            else:
                chunks = [header]
        else:
            chunks = split_discord_message(message.text)
        for chunk in chunks:
            await channel.send(
                chunk,
                allowed_mentions=discord.AllowedMentions.none(),
            )

    async def deliver_approval(self, row: dict[str, Any]) -> str:
        """Deliver an actionable notification to the pinned audit thread."""
        if row.get("target_kind") == DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND:
            return await self._deliver_project_task_completion(row)
        if row.get("target_kind") in {
            DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
            DISCORD_PROJECT_TASK_VALIDATION_TARGET_KIND,
        }:
            return await self._deliver_project_task_recovery(row)
        inbox = self._approval_inbox
        if inbox is None:
            raise RuntimeError("Discord approval inbox is not available")
        return await inbox.deliver(row)

    async def _restore_project_task_completion_cards(self) -> None:
        """Backfill source-thread cards for pending completion approvals."""
        rows = await self.db.list_notifications(
            status="pending", type="approval", limit=500,
        )
        for row in rows:
            if row.get("target_kind") not in {
                DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
                DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
                DISCORD_PROJECT_TASK_VALIDATION_TARGET_KIND,
            }:
                continue
            try:
                metadata = json.loads(row.get("metadata") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            if isinstance(metadata, dict) and metadata.get(
                "defer_discord_until_turn_end",
            ):
                # A restart can happen while the originating turn still has
                # a durable recovery checkpoint.  Let that turn finish and
                # flush its card in order; otherwise this is a prior failed
                # delivery and post-ready restoration may safely retry it.
                if await self.db.get_session_run_recovery(row["session_id"]):
                    continue
            try:
                if row.get("target_kind") == DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND:
                    await self._deliver_project_task_completion(
                        row, duplicate_to_audit=False,
                    )
                else:
                    await self._deliver_project_task_recovery(
                        row, duplicate_to_audit=False,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Failed to restore project task completion card for %s",
                    row.get("id"),
                )

    async def _deliver_project_task_completion(
        self,
        row: dict[str, Any],
        *,
        duplicate_to_audit: bool = True,
    ) -> str:
        """Deliver task completion approval in both its task and audit threads."""
        inbox = self._approval_inbox
        if inbox is None:
            raise RuntimeError("Discord approval inbox is not available")
        target = str(row.get("target_id") or "").strip()
        if not target:
            raise ValueError("Project task completion approval has no thread target")
        thread = await self._resolve_messageable(target)
        if (
            not isinstance(thread, discord.Thread)
            or int(getattr(thread, "parent_id", 0) or 0) not in self._project_forums
        ):
            raise ValueError("Project task completion target is not a project thread")

        message_id = await inbox.deliver_to_thread(
            row,
            thread,
            metadata_key="discord_project_task_completion",
        )
        if duplicate_to_audit:
            refreshed = await self.db.get_notification(row["id"])
            if refreshed is None:
                raise RuntimeError("Completion approval disappeared after task delivery")
            await inbox.deliver(refreshed)
        return message_id

    async def _deliver_project_task_recovery(
        self,
        row: dict[str, Any],
        *,
        duplicate_to_audit: bool = True,
    ) -> str:
        """Deliver a blocked-task decision card in its source thread."""
        inbox = self._approval_inbox
        if inbox is None:
            raise RuntimeError("Discord approval inbox is not available")
        target = str(row.get("target_id") or "").strip()
        if not target:
            raise ValueError("Project task recovery target has no thread target")
        thread = await self._resolve_messageable(target)
        if (
            not isinstance(thread, discord.Thread)
            or int(getattr(thread, "parent_id", 0) or 0)
            not in self._project_forums
        ):
            raise ValueError("Project task recovery target is not a project thread")

        metadata_key = (
            "discord_project_task_validation"
            if row.get("target_kind") == DISCORD_PROJECT_TASK_VALIDATION_TARGET_KIND
            else "discord_project_task_recovery"
        )
        message_id = await inbox.deliver_to_thread(
            row,
            thread,
            metadata_key=metadata_key,
        )
        if duplicate_to_audit:
            refreshed = await self.db.get_notification(row["id"])
            if refreshed is None:
                raise RuntimeError("Recovery action disappeared after delivery")
            await inbox.deliver(refreshed)
        return message_id

    async def deliver_notification(self, row: dict[str, Any]) -> str:
        """Deliver any notification kind to its pinned audit-forum inbox."""
        if row.get("type") == "approval":
            return await self.deliver_approval(row)
        inbox = self._notification_inbox
        if inbox is None:
            raise RuntimeError("Discord notification inbox is not available")
        return await inbox.deliver(row)

    async def send_typing(self, target: str) -> None:
        channel = await self._resolve_messageable(target)
        channel = await self._active_project_thread(channel, target)
        if channel is None:
            return
        await channel.typing()

    async def _active_project_thread(self, channel: Any, target: str) -> Any | None:
        """Return a project thread only when it may receive bot activity.

        Completion is applied by the mechanical-action dispatcher, outside of
        discord.py's cache. Reading a fresh thread here is load-bearing: a
        stale cached archive flag would let a scheduled wakeup's typing request
        reopen the task immediately after completion. Ordinary channels and
        conversational threads keep the cached fast path; only configured
        project-forum threads need this guard.
        """
        if getattr(channel, "parent_id", None) not in self._project_forums:
            return channel
        if self._client is None:  # _resolve_messageable already checked this
            return None
        try:
            fresh = await self._client.fetch_channel(int(target))
        except discord.HTTPException as exc:
            # A project task may disappear from active listings once it is
            # archived. Do not make an outbound side effect while its state
            # cannot be confirmed.
            logger.warning(
                "Skipping Discord activity for project thread %s: %s",
                target, exc,
            )
            return None

        status_names = {
            str(getattr(tag, "name", "") or "").casefold()
            for tag in (getattr(fresh, "applied_tags", None) or [])
        }
        if bool(getattr(fresh, "archived", False)) or (
            status_names & _TERMINAL_PROJECT_TASK_STATUSES
        ):
            logger.info(
                "Skipping Discord activity for terminal project thread %s",
                target,
            )
            return None
        return fresh

    @staticmethod
    def _safe_label(value: Any) -> str:
        text = " ".join(str(value or "").split())
        if not text or len(text) > 100 or any(char in text for char in "[]"):
            return ""
        return text
