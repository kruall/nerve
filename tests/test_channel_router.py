from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    InboundMessage,
)
from nerve.channels.router import ChannelRouter


class _StubChannel(BaseChannel):
    def __init__(self, *, automatic_responses: bool = True):
        self._automatic_responses = automatic_responses
        self.sent = []

    @property
    def name(self) -> str:
        return "buzz"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT

    @property
    def automatic_responses(self) -> bool:
        return self._automatic_responses

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message) -> None:
        self.sent.append(message)


@pytest.mark.asyncio
async def test_explicit_text_delivery_uses_matching_session_context():
    engine = MagicMock()
    router = ChannelRouter(engine)
    channel = _StubChannel()
    router.register(channel)
    router._message_context["shared"] = {
        "channel_name": "buzz",
        "target": "channel-1",
        "message_id": "event-1",
    }

    assert await router.send_text(
        "shared", "deliberate reply", channel="buzz",
    ) is True
    assert len(channel.sent) == 1
    assert channel.sent[0].target == "channel-1"
    assert channel.sent[0].text == "deliberate reply"
    assert channel.sent[0].session_id == "shared"


@pytest.mark.asyncio
async def test_explicit_text_delivery_refuses_stale_or_mismatched_context():
    engine = MagicMock()
    router = ChannelRouter(engine)
    channel = _StubChannel()
    router.register(channel)
    router._message_context["shared"] = {
        "channel_name": "web",
        "target": "client-1",
        "message_id": "event-1",
    }

    assert await router.send_text(
        "shared", "must not leak", channel="buzz",
    ) is False
    assert await router.send_text(
        "missing", "must not leak", channel="buzz",
    ) is False
    assert channel.sent == []


@pytest.mark.asyncio
async def test_run_without_automatic_responses_does_not_register_adapter():
    engine = MagicMock()
    engine.run = AsyncMock(return_value="internal answer")
    engine.register_task = MagicMock()
    router = ChannelRouter(engine)
    router._setup_streaming = AsyncMock()
    router._teardown_streaming = AsyncMock()
    channel = _StubChannel(automatic_responses=False)

    response = await router._run_single(
        channel,
        InboundMessage(
            channel_name="buzz",
            channel_key="buzz:channel-1",
            sender_id="channel-1",
            text="hello",
        ),
        "shared",
    )

    assert response == "internal answer"
    router._setup_streaming.assert_not_awaited()
    router._teardown_streaming.assert_not_awaited()


@pytest.mark.asyncio
async def test_busy_session_steers_opted_in_message():
    engine = MagicMock()
    engine.sessions.get_active_session = AsyncMock(return_value="shared")
    engine.sessions.is_running.return_value = True
    engine.steer = AsyncMock(return_value=True)
    router = ChannelRouter(engine)
    router.register(_StubChannel())

    lock = asyncio.Lock()
    await lock.acquire()
    router._session_locks["shared"] = lock
    try:
        result = await router.handle_message(InboundMessage(
            channel_name="buzz",
            channel_key="buzz:channel-1",
            sender_id="channel-1",
            text="second participant",
            steer_if_busy=True,
        ))
    finally:
        lock.release()

    assert result == ""
    engine.steer.assert_awaited_once_with(
        session_id="shared",
        user_message="second participant",
        channel="buzz",
        images=None,
    )
    assert "shared" not in router._pending_batches


@pytest.mark.asyncio
async def test_failed_steer_falls_back_to_pending_queue():
    engine = MagicMock()
    engine.sessions.get_active_session = AsyncMock(return_value="shared")
    engine.sessions.is_running.return_value = True
    engine.steer = AsyncMock(return_value=False)
    router = ChannelRouter(engine)
    router.register(_StubChannel())

    lock = asyncio.Lock()
    await lock.acquire()
    router._session_locks["shared"] = lock
    task = asyncio.create_task(router.handle_message(InboundMessage(
        channel_name="buzz",
        channel_key="buzz:channel-1",
        sender_id="channel-1",
        text="queue this safely",
        steer_if_busy=True,
    )))
    await asyncio.sleep(0)
    pending = router._pending_batches["shared"]
    pending[0][1].set_result("queued")
    lock.release()

    assert await task == "queued"
    assert pending[0][0].text == "queue this safely"
