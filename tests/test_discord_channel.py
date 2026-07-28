from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.discord import DiscordChannel, split_discord_message
from nerve.config import NerveConfig

GUILD = 100
TEXT_CHANNEL = 200
CONVERSATION_THREAD = 201
YDB_FORUM = 300
YDB_THREAD = 301
AUDIT_FORUM = 350
USER = 400
PEER_BOT = 401
DOGGY = 900


class _HistoryChannel:
    def __init__(
        self,
        channel_id: int,
        messages: list,
        *,
        last_message_id: int = 0,
        parent_id: int | None = None,
        name: str = "channel",
    ):
        self.id = channel_id
        self.last_message_id = last_message_id
        self.parent_id = parent_id
        self.name = name
        self.messages = messages
        self.history_calls: list[dict] = []

    async def history(self, **kwargs):
        self.history_calls.append(kwargs)
        for message in self.messages:
            yield message


class _ForumChannel:
    def __init__(self, archived_threads: list, *, last_message_id: int):
        self.id = YDB_FORUM
        self.last_message_id = last_message_id
        self._archived_threads = archived_threads

    async def archived_threads(self, **kwargs):
        for thread in self._archived_threads:
            yield thread


def _channel() -> DiscordChannel:
    cfg = NerveConfig.from_dict({"discord": {
        "enabled": True,
        "bot_token": "synthetic-token",
        "guild_id": GUILD,
        "channel_ids": [TEXT_CHANNEL],
        "task_forums": {"YDB": YDB_FORUM},
        "allowed_author_ids": [USER, PEER_BOT],
        "require_mention": True,
    }})
    db = MagicMock()
    db.get_sync_cursor = AsyncMock(return_value=None)
    db.set_sync_cursor = AsyncMock()
    channel = DiscordChannel(cfg, MagicMock(), db)
    channel._bot_user_id = DOGGY
    return channel


def _message(
    *,
    channel_id: int = TEXT_CHANNEL,
    parent_id: int | None = None,
    author_id: int = USER,
    guild_id: int = GUILD,
    content: str = f"<@{DOGGY}> ping",
    mentions: list[int] | None = None,
    reply_author_id: int | None = None,
    unresolved_reply: bool = False,
    reference_type: discord.MessageReferenceType = (
        discord.MessageReferenceType.reply
    ),
):
    reference = None
    if reply_author_id is not None or unresolved_reply:
        resolved = (
            None
            if unresolved_reply
            else SimpleNamespace(author=SimpleNamespace(id=reply_author_id))
        )
        reference = SimpleNamespace(
            type=reference_type,
            resolved=resolved,
            cached_message=None,
        )
    message = SimpleNamespace(
        id=700,
        guild=SimpleNamespace(id=guild_id),
        author=SimpleNamespace(
            id=author_id,
            display_name="kruall" if author_id == USER else "Kitty",
        ),
        channel=SimpleNamespace(
            id=channel_id,
            parent_id=parent_id,
            name="general" if parent_id is None else "move actors",
        ),
        content=content,
        raw_mentions=[DOGGY] if mentions is None else mentions,
        reference=reference,
        thread=None,
    )
    message.create_thread = AsyncMock()
    return message


def test_accepts_allowed_explicit_mention_in_text_channel():
    assert _channel()._accepts(_message()) is True


def test_accepts_conversation_thread_message_without_mention():
    assert _channel()._accepts(_message(
        channel_id=CONVERSATION_THREAD,
        parent_id=TEXT_CHANNEL,
        content="follow-up without mention",
        mentions=[],
    )) is True


def test_discord_responses_require_explicit_mcp_send():
    assert _channel().automatic_responses is False


def test_accepts_allowed_peer_bot_in_project_forum_thread():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        author_id=PEER_BOT,
    )) is True


def test_project_forum_thread_still_requires_mention():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content="unmentioned forum update",
        mentions=[],
    )) is False


def test_project_forum_thread_accepts_reply_to_bot_without_mention():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content="reply without mention",
        mentions=[],
        reply_author_id=DOGGY,
    )) is True


def test_project_forum_thread_rejects_reply_to_other_author_without_mention():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content="reply to another participant",
        mentions=[],
        reply_author_id=USER,
    )) is False


def test_project_forum_thread_rejects_unresolved_reply_without_mention():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content="reply whose target is unavailable",
        mentions=[],
        unresolved_reply=True,
    )) is False


def test_project_forum_thread_rejects_forwarded_bot_message_without_mention():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content="forwarded bot message",
        mentions=[],
        reply_author_id=DOGGY,
        reference_type=discord.MessageReferenceType.forward,
    )) is False


@pytest.mark.parametrize("message", [
    _message(guild_id=999),
    _message(channel_id=999),
    _message(author_id=999),
    _message(author_id=DOGGY),
    _message(mentions=[]),
    _message(content=f"<@{DOGGY}>   "),
])
def test_rejects_wrong_scope_self_unmentioned_and_empty_messages(message):
    assert _channel()._accepts(message) is False


def test_mention_is_removed_from_agent_prompt():
    assert _channel()._message_text(
        _message(content=f"hello <@!{DOGGY}> there"),
    ) == "hello  there"


@pytest.mark.asyncio
async def test_text_channel_mention_creates_thread_and_dispatches_there():
    channel = _channel()
    channel.router.handle_message = AsyncMock()
    message = _message()
    thread = SimpleNamespace(
        id=CONVERSATION_THREAD,
        parent_id=TEXT_CHANNEL,
        name="Nerve · ping",
    )
    message.create_thread.return_value = thread

    await channel._ingest(message)

    message.create_thread.assert_awaited_once_with(
        name="Nerve · ping",
        reason="Nerve Discord conversation",
    )
    inbound = channel.router.handle_message.await_args.args[0]
    assert inbound.channel_name == "discord"
    assert inbound.channel_key == f"discord:{GUILD}:{CONVERSATION_THREAD}"
    assert inbound.sender_id == str(CONVERSATION_THREAD)
    assert inbound.session_title == "Discord · thread · Nerve · ping"
    assert inbound.steer_if_busy is True
    assert inbound.metadata["discord_channel_id"] == CONVERSATION_THREAD
    assert inbound.metadata["discord_parent_channel_id"] == TEXT_CHANNEL
    assert inbound.metadata["discord_origin_channel_id"] == TEXT_CHANNEL
    assert inbound.metadata["discord_project"] == ""
    assert inbound.metadata["discord_author_id"] == USER
    assert inbound.text.endswith("ping")
    assert channel.db.set_sync_cursor.await_count == 2


@pytest.mark.asyncio
async def test_existing_message_thread_is_reused():
    channel = _channel()
    channel.router.handle_message = AsyncMock()
    message = _message()
    message.thread = SimpleNamespace(
        id=CONVERSATION_THREAD,
        parent_id=TEXT_CHANNEL,
        name="existing",
    )

    await channel._ingest(message)

    message.create_thread.assert_not_awaited()
    inbound = channel.router.handle_message.await_args.args[0]
    assert inbound.channel_key == f"discord:{GUILD}:{CONVERSATION_THREAD}"


@pytest.mark.asyncio
async def test_conversation_thread_follow_up_dispatches_without_mention():
    channel = _channel()
    channel.router.handle_message = AsyncMock()
    message = _message(
        channel_id=CONVERSATION_THREAD,
        parent_id=TEXT_CHANNEL,
        content="I have more context",
        mentions=[],
    )

    await channel._ingest(message)

    message.create_thread.assert_not_awaited()
    inbound = channel.router.handle_message.await_args.args[0]
    assert inbound.channel_key == f"discord:{GUILD}:{CONVERSATION_THREAD}"
    assert inbound.text.endswith("I have more context")
    assert "публичный ответ нужен только когда он полезен" in inbound.text


@pytest.mark.asyncio
async def test_concurrent_gateway_and_backlog_delivery_is_dispatched_once():
    channel = _channel()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_dispatch(_inbound):
        entered.set()
        await release.wait()

    channel.router.handle_message = AsyncMock(side_effect=slow_dispatch)
    message = _message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
    )

    first = asyncio.create_task(channel._ingest(message))
    await entered.wait()
    await channel._ingest(message)
    release.set()
    await first

    channel.router.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_maps_forum_thread_to_project():
    channel = _channel()
    channel.router.handle_message = AsyncMock()

    await channel._dispatch(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
    ))

    inbound = channel.router.handle_message.await_args.args[0]
    assert inbound.channel_key == f"discord:{GUILD}:{YDB_THREAD}"
    assert inbound.sender_id == str(YDB_THREAD)
    assert inbound.session_title == "Discord · YDB · move actors"
    assert inbound.metadata["discord_parent_channel_id"] == YDB_FORUM
    assert inbound.metadata["discord_project"] == "YDB"
    assert "проекта YDB" in inbound.text


def test_conversation_thread_name_fits_discord_limit():
    channel = _channel()
    name = channel._conversation_thread_name(_message(
        content=f"<@{DOGGY}> " + "long subject " * 20,
    ))
    assert name.startswith("Nerve · ")
    assert len(name) == 100


@pytest.mark.asyncio
async def test_send_splits_long_messages_and_disables_mentions():
    channel = _channel()
    target = SimpleNamespace(send=AsyncMock())
    channel._client = MagicMock()
    channel._client.get_channel.return_value = target

    await channel.send(OutboundMessage(
        target=str(TEXT_CHANNEL),
        text="a" * 2100,
    ))

    assert target.send.await_count == 2
    chunks = [call.args[0] for call in target.send.await_args_list]
    assert "".join(chunks) == "a" * 2100
    assert all(len(chunk) <= 2000 for chunk in chunks)
    assert all("allowed_mentions" in call.kwargs for call in target.send.await_args_list)


@pytest.mark.asyncio
async def test_first_start_primes_channel_without_replaying_old_messages():
    channel = _channel()
    target = _HistoryChannel(TEXT_CHANNEL, [], last_message_id=777)

    await channel._sync_target(target, process_existing=False)

    assert target.history_calls == []
    channel.db.set_sync_cursor.assert_awaited_once_with(
        f"discord:{GUILD}:{TEXT_CHANNEL}",
        "777",
    )


@pytest.mark.asyncio
async def test_restart_replays_messages_after_durable_cursor():
    channel = _channel()
    missed = _message(content=f"<@{DOGGY}> missed")
    missed.id = 778
    missed.create_thread.return_value = SimpleNamespace(
        id=778,
        parent_id=TEXT_CHANNEL,
        name="Nerve · missed",
    )
    target = _HistoryChannel(TEXT_CHANNEL, [missed], last_message_id=778)
    channel.db.get_sync_cursor = AsyncMock(side_effect=["777", None, "777"])
    channel.router.handle_message = AsyncMock()

    await channel._sync_target(target, process_existing=False)

    channel.router.handle_message.assert_awaited_once()
    after = target.history_calls[0]["after"]
    assert after.id == 777
    assert channel.db.set_sync_cursor.await_count == 2
    assert channel.db.set_sync_cursor.await_args_list[-1].args == (
        f"discord:{GUILD}:{TEXT_CHANNEL}",
        "778",
    )


@pytest.mark.asyncio
async def test_ready_primes_active_conversation_threads():
    channel = _channel()
    thread = _HistoryChannel(
        CONVERSATION_THREAD,
        [],
        last_message_id=778,
        parent_id=TEXT_CHANNEL,
        name="conversation",
    )
    forum = _ForumChannel([], last_message_id=0)
    guild = MagicMock()
    guild.active_threads = AsyncMock(return_value=[thread])
    guild.get_channel.side_effect = lambda channel_id: (
        _HistoryChannel(TEXT_CHANNEL, [], last_message_id=777)
        if channel_id == TEXT_CHANNEL
        else forum
    )

    await channel._sync_backlog(guild)

    assert thread.history_calls == []
    assert any(
        call.args == (
            f"discord:{GUILD}:{CONVERSATION_THREAD}",
            "778",
        )
        for call in channel.db.set_sync_cursor.await_args_list
    )


@pytest.mark.asyncio
async def test_new_forum_thread_created_while_offline_is_replayed():
    channel = _channel()
    channel._text_channels.clear()
    thread = _HistoryChannel(
        YDB_THREAD,
        [],
        last_message_id=778,
        parent_id=YDB_FORUM,
        name="offline task",
    )
    missed = _message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        content=f"<@{DOGGY}> missed task",
    )
    missed.id = 778
    missed.channel = thread
    thread.messages = [missed]
    forum = _ForumChannel([], last_message_id=YDB_THREAD)
    guild = MagicMock()
    guild.active_threads = AsyncMock(return_value=[thread])
    guild.get_channel.return_value = forum
    channel.db.get_sync_cursor = AsyncMock(side_effect=[
        str(YDB_THREAD - 1),
        None,
        None,
    ])
    channel.router.handle_message = AsyncMock()

    await channel._sync_backlog(guild)

    channel.router.handle_message.assert_awaited_once()
    assert channel.router.handle_message.await_args.args[0].metadata[
        "discord_project"
    ] == "YDB"
    assert channel.db.set_sync_cursor.await_count == 2
    assert channel.db.set_sync_cursor.await_args_list[-1].args == (
        f"discord-forum:{GUILD}:{YDB_FORUM}",
        str(YDB_THREAD),
    )


def test_split_prefers_newline_and_never_returns_empty_chunks():
    chunks = split_discord_message("alpha\nbeta gamma", limit=10)
    assert chunks == ["alpha", "beta gamma"]


def test_token_loader_reads_one_line_file(tmp_path: Path):
    token_file = tmp_path / "discord-token"
    token_file.write_text("synthetic-token\n")
    channel = _channel()
    channel.config.bot_token = ""
    channel.config.bot_token_file = token_file
    assert channel._load_token() == "synthetic-token"


@pytest.mark.asyncio
async def test_ready_validates_bot_guild_and_configured_channels():
    channel = _channel()
    channel._sync_backlog = AsyncMock()
    guild = MagicMock()
    guild.get_channel.side_effect = lambda channel_id: (
        SimpleNamespace(id=channel_id)
        if channel_id in {TEXT_CHANNEL, YDB_FORUM}
        else None
    )
    channel._client = MagicMock()
    channel._client.user = SimpleNamespace(id=DOGGY)
    channel._client.get_guild.return_value = guild

    await channel._on_ready()

    assert channel._ready.is_set()
    assert channel._startup_error is None
    assert channel._bot_user_id == DOGGY
    assert channel._post_ready_task is not None
    await channel._post_ready_task
    channel._sync_backlog.assert_awaited_once_with(guild)


@pytest.mark.asyncio
async def test_slow_backlog_does_not_delay_discord_readiness():
    channel = _channel()
    backlog_release = asyncio.Event()

    async def slow_backlog(_guild):
        await backlog_release.wait()

    channel._sync_backlog = AsyncMock(side_effect=slow_backlog)
    guild = MagicMock()
    guild.get_channel.side_effect = lambda channel_id: (
        SimpleNamespace(id=channel_id)
        if channel_id in {TEXT_CHANNEL, YDB_FORUM}
        else None
    )
    channel._client = MagicMock()
    channel._client.user = SimpleNamespace(id=DOGGY)
    channel._client.get_guild.return_value = guild

    await channel._on_ready()

    assert channel._ready.is_set()
    assert channel._startup_error is None
    assert channel._post_ready_task is not None
    assert not channel._post_ready_task.done()

    backlog_release.set()
    await channel._post_ready_task


@pytest.mark.asyncio
async def test_ready_fails_when_bot_cannot_access_configured_forum():
    channel = _channel()
    guild = MagicMock()
    guild.get_channel.side_effect = lambda channel_id: (
        SimpleNamespace(id=channel_id)
        if channel_id == TEXT_CHANNEL
        else None
    )
    channel._client = MagicMock()
    channel._client.user = SimpleNamespace(id=DOGGY)
    channel._client.get_guild.return_value = guild

    await channel._on_ready()

    assert channel._ready.is_set()
    assert isinstance(channel._startup_error, ValueError)
    assert str(YDB_FORUM) in str(channel._startup_error)


@pytest.mark.asyncio
async def test_ready_starts_audit_mirror_only_once_across_reconnects():
    channel = _channel()
    channel.config.audit_forum_id = AUDIT_FORUM
    channel.config.audit_batch_window_seconds = 45
    channel._sync_backlog = AsyncMock()
    guild = MagicMock()
    guild.get_channel.side_effect = lambda channel_id: (
        SimpleNamespace(id=channel_id)
        if channel_id in {TEXT_CHANNEL, YDB_FORUM, AUDIT_FORUM}
        else None
    )
    channel._client = MagicMock()
    channel._client.user = SimpleNamespace(id=DOGGY)
    channel._client.get_guild.return_value = guild

    with patch(
        "nerve.channels.discord_mirror.DiscordSessionMirror"
    ) as mirror_cls:
        mirror_cls.return_value.start = AsyncMock()
        await channel._on_ready()
        await channel._on_ready()
        assert channel._post_ready_task is not None
        await channel._post_ready_task

    mirror_cls.assert_called_once()
    assert mirror_cls.call_args.kwargs["batch_window_seconds"] == 45
    mirror_cls.return_value.start.assert_awaited_once()


def test_validation_is_fail_closed():
    cfg = NerveConfig.from_dict({"discord": {"enabled": True}})
    channel = DiscordChannel(cfg, MagicMock(), MagicMock())
    with pytest.raises(ValueError, match="guild_id"):
        channel._validate_config()


def test_validation_allows_outbound_only_audit_forum():
    cfg = NerveConfig.from_dict({"discord": {
        "enabled": True,
        "bot_token": "synthetic-token",
        "guild_id": GUILD,
        "audit_forum_id": 350,
    }})
    DiscordChannel(cfg, MagicMock(), MagicMock())._validate_config()


def test_validation_rejects_negative_audit_batch_window():
    cfg = NerveConfig.from_dict({"discord": {
        "enabled": True,
        "bot_token": "synthetic-token",
        "guild_id": GUILD,
        "audit_forum_id": 350,
        "audit_batch_window_seconds": -1,
    }})
    with pytest.raises(ValueError, match="audit_batch_window_seconds"):
        DiscordChannel(cfg, MagicMock(), MagicMock())._validate_config()


def test_validation_rejects_one_forum_used_by_two_projects():
    cfg = NerveConfig.from_dict({"discord": {
        "enabled": True,
        "bot_token": "synthetic-token",
        "guild_id": GUILD,
        "task_forums": {"YDB": YDB_FORUM, "AW": YDB_FORUM},
        "allowed_author_ids": [USER],
    }})
    channel = DiscordChannel(cfg, MagicMock(), MagicMock())
    with pytest.raises(ValueError, match="different channel"):
        channel._validate_config()


def test_validation_rejects_audit_forum_as_inbound_project_forum():
    cfg = NerveConfig.from_dict({"discord": {
        "enabled": True,
        "bot_token": "synthetic-token",
        "guild_id": GUILD,
        "task_forums": {"YDB": YDB_FORUM},
        "audit_forum_id": YDB_FORUM,
        "allowed_author_ids": [USER],
    }})
    channel = DiscordChannel(cfg, MagicMock(), MagicMock())
    with pytest.raises(ValueError, match="outbound-only"):
        channel._validate_config()
