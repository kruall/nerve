"""Native Discord channel backed by discord.py.

The adapter is intentionally fail-closed. It accepts messages only from one
configured guild, explicitly allowed text channels or project forums, and an
explicit author allowlist. Its optional audit forum is outbound-only. A direct
bot mention in a text channel starts a conversation thread. Messages inside
conversation threads do not require further mentions, while project-forum
threads keep the mention requirement.
"""

from __future__ import annotations

import asyncio
import logging
import re
import stat
from typing import Any, TYPE_CHECKING

import discord

from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.config import NerveConfig

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.db import Database

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 30.0
_MAX_MESSAGE_LENGTH = 2000
_MAX_THREAD_NAME_LENGTH = 100
_THREAD_NAME_PREFIX = "Nerve · "


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
    ):
        self.config = config.discord
        self.router = router
        self.db = db
        self._client: discord.Client | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._session_mirror: Any | None = None
        self._ready = asyncio.Event()
        self._startup_error: Exception | None = None
        self._bot_user_id = 0
        self._allowed_authors = set(self.config.allowed_author_ids)
        self._text_channels = set(self.config.channel_ids)
        self._project_forums = {
            channel_id: project
            for project, channel_id in self.config.task_forums.items()
        }

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

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(max_message_length=_MAX_MESSAGE_LENGTH)

    def _validate_config(self) -> None:
        missing: list[str] = []
        if not self.config.guild_id:
            missing.append("guild_id")
        has_inbound_targets = bool(self._text_channels or self._project_forums)
        if not has_inbound_targets and not self.config.audit_forum_id:
            missing.append("channel_ids, task_forums, or audit_forum_id")
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
        overlap = self._text_channels & set(self._project_forums)
        if overlap:
            raise ValueError(
                "Discord channel IDs cannot be both text channels and project forums"
            )
        if (
            self.config.audit_forum_id
            and self.config.audit_forum_id
            in (self._text_channels | set(self._project_forums))
        ):
            raise ValueError(
                "discord.audit_forum_id must be an outbound-only forum"
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

        @client.event
        async def on_ready() -> None:
            await self._on_ready()

        @client.event
        async def on_message(message: discord.Message) -> None:
            await self._on_message(message)

        return client

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
                "and %d project forum(s)",
                len(self._text_channels),
                len(self._project_forums),
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
        mirror = self._session_mirror
        self._client = None
        self._client_task = None
        self._session_mirror = None

        if mirror is not None:
            await mirror.stop()
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
            await self._sync_backlog(guild)
            if self.config.audit_forum_id and self._session_mirror is None:
                from nerve.channels.discord_mirror import DiscordSessionMirror

                self._session_mirror = DiscordSessionMirror(
                    client=self._client,
                    db=self.db,
                    guild_id=self.config.guild_id,
                    forum_id=self.config.audit_forum_id,
                )
                await self._session_mirror.start()
        except Exception as exc:
            self._startup_error = exc
        finally:
            self._ready.set()

    async def _on_message(self, message: discord.Message) -> None:
        try:
            await self._ingest(message)
        except Exception:
            logger.exception(
                "Discord message dispatch failed for channel %s",
                getattr(message.channel, "id", "unknown"),
            )

    async def _ingest(self, message: discord.Message) -> None:
        if self._accepts(message):
            target = message.channel
            if int(message.channel.id) in self._text_channels:
                target = await self._ensure_conversation_thread(message)
            await self._dispatch(message, target=target)
            if int(target.id) != int(message.channel.id):
                await self._advance_cursor(
                    int(target.id),
                    int(message.id),
                )
        await self._advance_cursor(
            int(message.channel.id),
            int(message.id),
        )

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
            )
        }
        for thread in active_threads.values():
            if getattr(thread, "parent_id", None) in self._text_channels:
                await self._sync_target(thread, process_existing=False)

        for forum_id in sorted(self._project_forums):
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
        return ""

    def _has_direct_mention(self, message: discord.Message) -> bool:
        raw_mentions = {
            int(value) for value in getattr(message, "raw_mentions", [])
        }
        return self._bot_user_id in raw_mentions

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
        ):
            return False

        return bool(self._message_text(message))

    def _message_text(self, message: discord.Message) -> str:
        text = str(getattr(message, "content", "") or "")
        if self._bot_user_id:
            mention = re.compile(rf"<@!?{self._bot_user_id}>")
            text = mention.sub("", text)
        return text.strip()

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
    ) -> None:
        guild_id = int(message.guild.id)
        target = target or message.channel
        channel_id = int(target.id)
        parent_id = getattr(target, "parent_id", None)
        project = self._project_forums.get(parent_id, "")
        author_name = self._safe_label(
            getattr(message.author, "display_name", "")
        )
        channel_name = self._safe_label(
            getattr(target, "name", "")
        )
        author = author_name or str(message.author.id)
        if project:
            location = f"форумной темы проекта {project}"
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
        title = (
            f"Discord · {project} · {channel_name or channel_id}"
            if project
            else (
                f"Discord · thread · {channel_name or channel_id}"
                if parent_id in self._text_channels
                else f"Discord · {channel_name or channel_id}"
            )
        )
        await self.router.handle_message(InboundMessage(
            channel_name=self.name,
            channel_key=f"discord:{guild_id}:{channel_id}",
            sender_id=str(channel_id),
            text=context + self._message_text(message),
            session_title=title,
            metadata={
                "message_id": str(message.id),
                "discord_guild_id": guild_id,
                "discord_channel_id": channel_id,
                "discord_parent_channel_id": parent_id,
                "discord_origin_channel_id": int(message.channel.id),
                "discord_project": project,
                "discord_author_id": int(message.author.id),
                "discord_author_name": author_name,
            },
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
        for chunk in split_discord_message(message.text):
            await channel.send(
                chunk,
                allowed_mentions=discord.AllowedMentions.none(),
            )

    async def send_typing(self, target: str) -> None:
        channel = await self._resolve_messageable(target)
        await channel.trigger_typing()

    @staticmethod
    def _safe_label(value: Any) -> str:
        text = " ".join(str(value or "").split())
        if not text or len(text) > 100 or any(char in text for char in "[]"):
            return ""
        return text
