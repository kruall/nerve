from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.buzz import BuzzChannel
from nerve.config import NerveConfig

BOT = "7cfe357723892013ae3198fa99ffd353524081992d1cf9ca42b127d9fedb2403"
USER = "b50fabe8acbb9aaafa8c4bc32995a7f8d1acb4221a5136d1feb8d8dccec3b4eb"
CHANNEL = "71560b67-4553-5a3a-a0c5-1dc813fb52b6"
DM_CHANNEL = "06d9ef66-7f50-48e4-b764-9f605709b6a9"


def _channel() -> BuzzChannel:
    cfg = NerveConfig.from_dict({"buzz": {
        "enabled": True, "relay_url": "http://relay", "binary_path": "/bin/true",
        "community_name": "Acme Team",
        "private_key": "test-key", "bot_pubkey": BOT, "channel_ids": [CHANNEL],
        "allowed_pubkeys": [USER], "direct_messages": True,
    }})
    return BuzzChannel(cfg, MagicMock(), MagicMock())


def _event(**overrides):
    event = {"id": "event-1", "pubkey": USER, "content": "@Doggy ping", "created_at": 10,
             "kind": 9, "tags": [["h", CHANNEL], ["p", BOT]]}
    event.update(overrides)
    return event


def test_accepts_allowed_explicit_mention():
    assert _channel()._accepts(_event()) is True


def test_disables_automatic_session_responses():
    assert _channel().automatic_responses is False


def test_accepts_allowed_dm_without_mention():
    assert _channel()._accepts(
        _event(content="private ping", tags=[["h", DM_CHANNEL]]),
        is_dm=True,
    ) is True


@pytest.mark.parametrize("event", [
    _event(pubkey="other"),
    _event(pubkey=BOT),
    _event(tags=[["h", CHANNEL]]),
])
def test_rejects_untrusted_self_and_unmentioned_events(event):
    assert _channel()._accepts(event) is False


def test_private_key_loader_accepts_identity_file(tmp_path: Path):
    identity = tmp_path / "identity"
    identity.write_text("Public key:  x\nSecret key:  test-key\n")
    channel = _channel()
    channel.config.private_key = ""
    channel.config.private_key_file = identity
    assert channel._load_private_key() == "test-key"


def test_validation_allows_dm_only_configuration():
    channel = _channel()
    channel.config.channel_ids = []
    channel._validate_config()


@pytest.mark.asyncio
async def test_fetch_dm_channel_ids_uses_relay_confirmed_conversations():
    channel = _channel()
    channel._run_cli = AsyncMock(return_value=json.dumps([
        {"dm_id": DM_CHANNEL, "participants": [BOT, USER]},
        {"dm_id": "stranger", "participants": [BOT, "other"]},
        {"dm_id": "mixed-group", "participants": [BOT, USER, "other"]},
        {"dm_id": "", "participants": [BOT]},
        {"participants": [BOT]},
    ]))

    assert await channel._fetch_dm_channel_ids() == {DM_CHANNEL}
    channel._run_cli.assert_awaited_once_with("dms", "list", "--limit", "200")


@pytest.mark.asyncio
async def test_disabled_direct_messages_do_not_call_cli():
    channel = _channel()
    channel.config.direct_messages = False
    channel._dm_channel_ids.add(DM_CHANNEL)
    channel._fetch_dm_channel_ids = AsyncMock()

    await channel._refresh_dm_channel_ids(force=True)

    assert channel._dm_channel_ids == set()
    channel._fetch_dm_channel_ids.assert_not_awaited()


@pytest.mark.asyncio
async def test_send_allows_discovered_dm_channel():
    channel = _channel()
    channel._dm_channel_ids.add(DM_CHANNEL)
    channel._run_cli = AsyncMock(return_value="{}")

    await channel.send(OutboundMessage(target=DM_CHANNEL, text="private reply"))

    channel._run_cli.assert_awaited_once_with(
        "messages", "send", "--channel", DM_CHANNEL,
        "--content", "private reply",
    )


@pytest.mark.asyncio
async def test_poll_dm_dispatches_allowed_message_without_mention():
    channel = _channel()
    channel.db.get_sync_cursor = AsyncMock(return_value="0")
    channel.db.get_source_message = AsyncMock(return_value=None)
    channel.db.insert_source_messages = AsyncMock()
    channel.db.set_sync_cursor = AsyncMock()
    channel._fetch_events = AsyncMock(return_value=[
        _event(content="private ping", tags=[["h", DM_CHANNEL]]),
    ])
    channel._start_dispatch = MagicMock()

    await channel._poll_channel(DM_CHANNEL, is_dm=True)

    channel._start_dispatch.assert_called_once_with(
        DM_CHANNEL,
        _event(content="private ping", tags=[["h", DM_CHANNEL]]),
        is_dm=True,
    )
    record = channel.db.insert_source_messages.await_args.args[0][0]
    assert record.metadata["is_dm"] is True


@pytest.mark.asyncio
async def test_dispatch_uses_one_steerable_session_per_channel():
    channel = _channel()
    channel._channel_names[CHANNEL] = "General"
    channel._source_names[CHANNEL] = "buzz:acme-team:general"
    channel.router.handle_message = AsyncMock()
    other_user = "a" * 64
    await channel._dispatch(CHANNEL, _event())
    await channel._dispatch(CHANNEL, _event(id="event-2", pubkey=other_user))

    first, second = [
        call.args[0] for call in channel.router.handle_message.await_args_list
    ]
    assert first.sender_id == second.sender_id == CHANNEL
    assert first.channel_key == second.channel_key == f"buzz:{CHANNEL}"
    assert first.session_title == second.session_title == "Buzz · General"
    assert first.steer_if_busy is second.steer_if_busy is True
    assert first.metadata["message_id"] == "event-1"
    assert first.metadata["buzz_channel_name"] == "General"
    assert first.metadata["buzz_source"] == "buzz:acme-team:general"
    assert first.metadata["buzz_is_dm"] is False
    assert first.metadata["buzz_author_pubkey"] == USER
    assert second.metadata["buzz_author_pubkey"] == other_user
    assert USER in first.text
    assert other_user in second.text
    assert "ответ увидят все" in first.text


@pytest.mark.asyncio
async def test_load_author_names_caches_buzz_profile_names():
    channel = _channel()
    other_user = "a" * 64
    channel.config.allowed_pubkeys.append(other_user)
    channel._run_cli = AsyncMock(return_value=json.dumps([
        {"pubkey": USER, "display_name": "kruall"},
        {"pubkey": other_user, "name": "Other User"},
        {"pubkey": "c" * 64, "display_name": "Untrusted"},
    ]))

    await channel._load_author_names()

    assert channel._author_names == {
        USER: "kruall",
        other_user: "Other User",
    }
    channel._run_cli.assert_awaited_once_with(
        "--format", "json", "users", "get",
        "--pubkey", other_user,
        "--pubkey", USER,
    )


@pytest.mark.asyncio
async def test_dispatch_uses_profile_name_for_author_mentions():
    channel = _channel()
    channel._author_names[USER] = "kruall"
    channel.router.handle_message = AsyncMock()

    await channel._dispatch(CHANNEL, _event())

    message = channel.router.handle_message.await_args.args[0]
    assert f"[Автор Buzz: @kruall (pubkey: {USER})" in message.text
    assert "Для обращения используйте @kruall, не pubkey." in message.text
    assert message.metadata["buzz_author_name"] == "kruall"
    assert message.metadata["buzz_author_pubkey"] == USER


def test_profile_mention_name_is_single_line_and_rejects_context_delimiters():
    channel = _channel()

    assert channel._profile_mention_name(
        {"display_name": "  @Will \n Pfleger  "},
    ) == "Will Pfleger"
    assert channel._profile_mention_name({"display_name": "[spoof]"}) == ""
    assert channel._profile_mention_name({"display_name": "x" * 101}) == ""


@pytest.mark.asyncio
async def test_load_channel_names_builds_human_readable_sources_and_migrates_legacy():
    channel = _channel()
    channel._run_cli = AsyncMock(return_value=(
        f'[{{"channel_id":"{CHANNEL}","name":"Nerve Dev"}}]'
    ))
    channel.db.rename_source = AsyncMock()

    await channel._load_channel_names()
    await channel._migrate_legacy_source_names()

    assert channel._source_name(CHANNEL) == "buzz:acme-team:nerve-dev"
    channel.db.rename_source.assert_awaited_once_with(
        f"buzz:{CHANNEL}", "buzz:acme-team:nerve-dev",
    )


@pytest.mark.asyncio
async def test_load_channel_names_disambiguates_normalized_collisions():
    other = "12345678-1234-4234-8234-123456789abc"
    channel = _channel()
    channel.config.channel_ids.append(other)
    channel._run_cli = AsyncMock(return_value=json.dumps([
        {"channel_id": CHANNEL, "name": "Nerve Dev"},
        {"channel_id": other, "name": "nerve-dev"},
    ]))

    await channel._load_channel_names()

    assert channel._source_name(CHANNEL) == f"buzz:acme-team:nerve-dev-{CHANNEL[:8]}"
    assert channel._source_name(other) == f"buzz:acme-team:nerve-dev-{other[:8]}"


def test_relay_host_is_used_when_community_name_is_not_configured():
    channel = _channel()
    channel.config.community_name = ""
    channel.config.relay_url = "https://relay.example:8443"
    assert channel._slug(channel._community_from_relay_url(channel.config.relay_url)) == (
        "relay.example"
    )


def test_public_session_title_falls_back_to_short_channel_id():
    channel = _channel()
    assert channel._session_title(
        CHANNEL, peer_pubkey=USER, is_dm=False,
    ) == f"Buzz · {CHANNEL[:12]}"


@pytest.mark.asyncio
async def test_dispatch_marks_dm_private_and_replies_to_dm_channel():
    channel = _channel()
    channel.router.handle_message = AsyncMock()
    await channel._dispatch(DM_CHANNEL, _event(content="private ping"), is_dm=True)
    message = channel.router.handle_message.await_args.args[0]
    assert message.sender_id == DM_CHANNEL
    assert message.channel_key == f"buzz:{DM_CHANNEL}"
    assert message.session_title == f"Buzz DM · {USER[:12]}"
    assert message.steer_if_busy is True
    assert message.metadata["buzz_is_dm"] is True
    assert "личное сообщение" in message.text
    assert "ответ увидит только этот чат" in message.text
