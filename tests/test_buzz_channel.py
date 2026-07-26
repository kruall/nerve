from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.buzz import BuzzChannel
from nerve.config import NerveConfig

BOT = "7cfe357723892013ae3198fa99ffd353524081992d1cf9ca42b127d9fedb2403"
USER = "b50fabe8acbb9aaafa8c4bc32995a7f8d1acb4221a5136d1feb8d8dccec3b4eb"
CHANNEL = "71560b67-4553-5a3a-a0c5-1dc813fb52b6"


def _channel() -> BuzzChannel:
    cfg = NerveConfig.from_dict({"buzz": {
        "enabled": True, "relay_url": "http://relay", "binary_path": "/bin/true",
        "private_key": "test-key", "bot_pubkey": BOT, "channel_ids": [CHANNEL],
        "allowed_pubkeys": [USER],
    }})
    return BuzzChannel(cfg, MagicMock(), MagicMock())


def _event(**overrides):
    event = {"id": "event-1", "pubkey": USER, "content": "@Doggy ping", "created_at": 10,
             "kind": 9, "tags": [["h", CHANNEL], ["p", BOT]]}
    event.update(overrides)
    return event


def test_accepts_allowed_explicit_mention():
    assert _channel()._accepts(_event()) is True


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


@pytest.mark.asyncio
async def test_dispatch_keeps_sessions_per_author_and_replies_to_channel():
    channel = _channel()
    channel.router.handle_message = AsyncMock()
    await channel._dispatch(CHANNEL, _event())
    message = channel.router.handle_message.await_args.args[0]
    assert message.sender_id == CHANNEL
    assert message.channel_key == f"buzz:{CHANNEL}:{USER}"
    assert message.metadata["message_id"] == "event-1"
    assert "ответ увидят все" in message.text
