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
import logging
import re
import stat
from collections import OrderedDict
from typing import Any, TYPE_CHECKING

import discord

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
from nerve.config import NerveConfig

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.db import Database

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 30.0
_MAX_MESSAGE_LENGTH = 2000
_MAX_THREAD_NAME_LENGTH = 100
_MAX_REPLY_CONTEXT_LENGTH = 500
_RECENT_MESSAGE_IDS = 4096
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
        *,
        context_summarizer: ContextSummarizer | None = None,
    ):
        self.config = config.discord
        self.router = router
        self.db = db
        self._client: discord.Client | None = None
        self._client_task: asyncio.Task[None] | None = None
        self._post_ready_task: asyncio.Task[None] | None = None
        self._session_mirror: Any | None = None
        self._presence: Any | None = None
        self._approval_inbox: Any | None = None
        self._notification_inbox: Any | None = None
        self._system_audit: Any | None = None
        self._system_audit_lock = asyncio.Lock()
        self._notification_service: Any | None = None
        self._ready = asyncio.Event()
        self._startup_error: Exception | None = None
        self._ingest_guard = asyncio.Lock()
        self._recent_message_ids: OrderedDict[int, None] = OrderedDict()
        self._bot_user_id = 0
        self._allowed_authors = set(self.config.allowed_author_ids)
        self._text_channels = set(self.config.channel_ids)
        self._project_forums = {
            channel_id: project
            for project, channel_id in self.config.task_forums.items()
        }
        self._thread_context = DiscordThreadContext(
            db=db,
            guild_id=self.config.guild_id,
            project_forum_ids=set(self._project_forums),
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
        if self.config.audit_batch_window_seconds < 0:
            raise ValueError(
                "discord.audit_batch_window_seconds must be non-negative"
            )
        if self.config.presence_refresh_interval_seconds < 60:
            raise ValueError(
                "discord.presence_refresh_interval_seconds must be at least 60"
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
        self._system_audit = None

        if mirror is not None:
            await mirror.stop()
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

        try:
            await self._sync_backlog(guild)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Discord backlog catch-up failed; live messages remain available"
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
                guild_id=self.config.guild_id,
                forum_id=self.config.audit_forum_id,
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

    async def _ingest(self, message: discord.Message) -> None:
        message_id = int(message.id)
        async with self._ingest_guard:
            if message_id in self._recent_message_ids:
                return
            self._recent_message_ids[message_id] = None
            if len(self._recent_message_ids) > _RECENT_MESSAGE_IDS:
                self._recent_message_ids.popitem(last=False)

        try:
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
        context_block: str = "",
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
        text = self._message_text(message)
        reply_context = self._reply_context(message)
        text = "\n\n".join(
            part for part in (context_block, reply_context, text) if part
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
            text=context + text,
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

    async def deliver_approval(self, row: dict[str, Any]) -> str:
        """Deliver an actionable notification to the pinned audit thread."""
        inbox = self._approval_inbox
        if inbox is None:
            raise RuntimeError("Discord approval inbox is not available")
        return await inbox.deliver(row)

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
        await channel.trigger_typing()

    @staticmethod
    def _safe_label(value: Any) -> str:
        text = " ".join(str(value or "").split())
        if not text or len(text) > 100 or any(char in text for char in "[]"):
            return ""
        return text
