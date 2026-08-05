"""Unit coverage for the fail-closed Rocket.Chat channel."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nerve.channels.base import OutboundMessage
from nerve.channels.rocketchat import (
    RocketChatChannel,
    split_rocketchat_message,
)
from nerve.config import NerveConfig, RocketChatConfig


class _Router:
    def __init__(self) -> None:
        self.messages = []

    async def handle_message(self, message):
        self.messages.append(message)
        return ""


def _channel(router: _Router) -> RocketChatChannel:
    config = NerveConfig(
        rocketchat=RocketChatConfig(
            enabled=True,
            url="http://chat.test:3000",
            username="litty",
            password="test-password",
            room_ids=["room-1"],
            allowed_author_ids=["human-1"],
        ),
    )
    channel = RocketChatChannel(config, router)
    channel._bot_user_id = "bot-1"
    return channel


@pytest.mark.asyncio
async def test_ingest_accepts_only_configured_room_and_author():
    router = _Router()
    channel = _channel(router)

    await channel._ingest({
        "_id": "message-1",
        "rid": "room-1",
        "msg": "please check this",
        "u": {"_id": "human-1"},
    })
    await asyncio.sleep(0)

    assert len(router.messages) == 1
    message = router.messages[0]
    assert message.channel_name == "rocketchat"
    assert message.channel_key == "rocketchat:room-1:root"
    assert message.sender_id == "room-1|"
    assert message.metadata["rocketchat_author_id"] == "human-1"
    assert message.text.endswith("please check this")


@pytest.mark.asyncio
async def test_ingest_rejects_other_room_author_and_bot_messages():
    router = _Router()
    channel = _channel(router)
    for message in (
        {"_id": "wrong-room", "rid": "room-2", "msg": "x", "u": {"_id": "human-1"}},
        {"_id": "wrong-user", "rid": "room-1", "msg": "x", "u": {"_id": "human-2"}},
        {"_id": "self", "rid": "room-1", "msg": "x", "u": {"_id": "bot-1"}},
    ):
        await channel._ingest(message)
    await asyncio.sleep(0)
    assert router.messages == []


@pytest.mark.asyncio
async def test_ingest_keeps_thread_destination():
    router = _Router()
    channel = _channel(router)
    await channel._ingest({
        "_id": "thread-message",
        "rid": "room-1",
        "tmid": "root-message",
        "msg": "inside a thread",
        "u": {"_id": "human-1"},
    })
    await asyncio.sleep(0)
    assert router.messages[0].channel_key == "rocketchat:room-1:root-message"
    assert router.messages[0].sender_id == "room-1|root-message"


class _Response:
    def raise_for_status(self) -> None:
        return None

    def json(self):
        return {"success": True}


class _Client:
    def __init__(self) -> None:
        self.requests = []

    async def post(self, path, *, headers, json):
        self.requests.append((path, headers, json))
        return _Response()


@pytest.mark.asyncio
async def test_send_uses_session_bound_room_and_thread():
    router = _Router()
    channel = _channel(router)
    client = _Client()
    channel._client = client
    channel._auth_token = "session-token"

    await channel.send(OutboundMessage(
        target="room-1|root-message",
        text="deliberate reply",
    ))

    assert client.requests == [(
        "/api/v1/chat.sendMessage",
        {"X-Auth-Token": "session-token", "X-User-Id": "bot-1"},
        {"message": {
            "rid": "room-1", "tmid": "root-message", "msg": "deliberate reply",
        }},
    )]


def test_split_rocketchat_message_prefers_a_readable_boundary():
    assert split_rocketchat_message("one two three", limit=7) == ["one two", "three"]


def test_rocketchat_config_reads_separate_secret_file(tmp_path: Path):
    secret = tmp_path / "bot-password"
    secret.write_text("secret\n", encoding="utf-8")
    secret.chmod(0o600)
    config = NerveConfig.from_dict({
        "rocketchat": {
            "enabled": True,
            "url": "http://chat.test:3000/",
            "username": "doggy",
            "password_file": str(secret),
            "room_ids": ["room-1"],
            "allowed_author_ids": ["human-1"],
        },
    })
    assert config.rocketchat.url == "http://chat.test:3000"
    assert config.rocketchat.password_file == secret


def test_rocketchat_token_auth_requires_its_user_id():
    config = RocketChatConfig(
        enabled=True,
        url="http://chat.test:3000",
        username="doggy",
        auth_token="token",
        room_ids=["room-1"],
        allowed_author_ids=["human-1"],
    )
    with pytest.raises(ValueError, match="user_id"):
        RocketChatChannel(NerveConfig(rocketchat=config), _Router())._validate_config()
