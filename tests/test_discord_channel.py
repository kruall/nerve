from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.discord import DiscordChannel, split_discord_message
from nerve.config import NerveConfig

GUILD = 100
TEXT_CHANNEL = 200
YDB_FORUM = 300
YDB_THREAD = 301
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
):
    return SimpleNamespace(
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
    )


def test_accepts_allowed_explicit_mention_in_text_channel():
    assert _channel()._accepts(_message()) is True


def test_discord_responses_require_explicit_mcp_send():
    assert _channel().automatic_responses is False


def test_accepts_allowed_peer_bot_in_project_forum_thread():
    assert _channel()._accepts(_message(
        channel_id=YDB_THREAD,
        parent_id=YDB_FORUM,
        author_id=PEER_BOT,
    )) is True


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
async def test_dispatch_uses_one_session_per_text_channel():
    channel = _channel()
    channel.router.handle_message = AsyncMock()

    await channel._dispatch(_message())

    inbound = channel.router.handle_message.await_args.args[0]
    assert inbound.channel_name == "discord"
    assert inbound.channel_key == f"discord:{GUILD}:{TEXT_CHANNEL}"
    assert inbound.sender_id == str(TEXT_CHANNEL)
    assert inbound.session_title == "Discord · general"
    assert inbound.steer_if_busy is True
    assert inbound.metadata["discord_project"] == ""
    assert inbound.metadata["discord_author_id"] == USER
    assert inbound.text.endswith("ping")


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
    target = _HistoryChannel(TEXT_CHANNEL, [missed], last_message_id=778)
    channel.db.get_sync_cursor = AsyncMock(side_effect=["777", "777"])
    channel.router.handle_message = AsyncMock()

    await channel._sync_target(target, process_existing=False)

    channel.router.handle_message.assert_awaited_once()
    after = target.history_calls[0]["after"]
    assert after.id == 777
    channel.db.set_sync_cursor.assert_awaited_once_with(
        f"discord:{GUILD}:{TEXT_CHANNEL}",
        "778",
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
    channel._sync_backlog.assert_awaited_once_with(guild)


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


def test_validation_is_fail_closed():
    cfg = NerveConfig.from_dict({"discord": {"enabled": True}})
    channel = DiscordChannel(cfg, MagicMock(), MagicMock())
    with pytest.raises(ValueError, match="guild_id"):
        channel._validate_config()


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
