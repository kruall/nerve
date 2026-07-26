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
    @property
    def name(self) -> str:
        return "buzz"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send(self, message) -> None:
        pass


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
