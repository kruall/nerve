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
        return "manual"

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


class _TypingChannel(_StubChannel):
    def __init__(self, *, automatic_responses: bool = False):
        super().__init__(automatic_responses=automatic_responses)
        self.typing_targets: list[str] = []
        self.typing_refreshed = asyncio.Event()

    @property
    def name(self) -> str:
        return "discord"

    @property
    def capabilities(self) -> ChannelCapability:
        return (
            ChannelCapability.SEND_TEXT
            | ChannelCapability.TYPING_INDICATOR
        )

    async def send_typing(self, target: str) -> None:
        self.typing_targets.append(target)
        if len(self.typing_targets) >= 2:
            self.typing_refreshed.set()


class _DiscordStubChannel(_StubChannel):
    @property
    def name(self) -> str:
        return "discord"


def test_unregister_removes_failed_channel():
    engine = MagicMock()
    router = ChannelRouter(engine)
    channel = _StubChannel()
    router.register(channel)

    assert router.unregister("manual") is channel
    assert router.get_channel("manual") is None
    assert router.unregister("manual") is None


@pytest.mark.asyncio
async def test_implicit_session_resolution_forwards_channel_title():
    engine = MagicMock()
    engine.sessions.get_active_session = AsyncMock(return_value="shared")
    engine.run = AsyncMock(return_value="")
    engine.register_task = MagicMock()
    router = ChannelRouter(engine)
    router.BATCH_DEBOUNCE = 0
    router.register(_StubChannel(automatic_responses=False))

    await router.handle_message(InboundMessage(
        channel_name="manual",
        channel_key="manual:channel-1",
        sender_id="channel-1",
        text="hello",
        session_title="Manual · General",
    ))

    engine.sessions.get_active_session.assert_awaited_once_with(
        "manual:channel-1",
        source="manual",
        title="Manual · General",
    )


@pytest.mark.asyncio
async def test_explicit_text_delivery_uses_matching_session_context():
    engine = MagicMock()
    router = ChannelRouter(engine)
    channel = _StubChannel()
    router.register(channel)
    router._message_context["shared"] = {
        "channel_name": "manual",
        "target": "channel-1",
        "message_id": "event-1",
    }

    assert await router.send_text(
        "shared", "deliberate reply", channel="manual",
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
        "shared", "must not leak", channel="manual",
    ) is False
    assert await router.send_text(
        "missing", "must not leak", channel="manual",
    ) is False
    assert channel.sent == []


@pytest.mark.asyncio
async def test_discord_text_delivery_uses_durable_binding_after_router_restart(
    db,
):
    await db.create_session("discord-session", source="discord")
    await db.bind_discord_session(
        "discord-session", guild_id="100", thread_id="200",
    )
    engine = MagicMock()
    engine.db = db
    router = ChannelRouter(engine)
    channel = _DiscordStubChannel()
    router.register(channel)
    router._message_context["discord-session"] = {
        "channel_name": "web",
        "target": "stale-web-client",
        "message_id": "old-event",
    }

    assert await router.send_text(
        "discord-session", "wakeup complete", channel="discord",
    ) is True
    assert len(channel.sent) == 1
    assert channel.sent[0].target == "200"
    assert channel.sent[0].text == "wakeup complete"


@pytest.mark.asyncio
async def test_discord_text_delivery_refuses_session_without_binding(db):
    await db.create_session("web-session", source="web")
    engine = MagicMock()
    engine.db = db
    router = ChannelRouter(engine)
    channel = _DiscordStubChannel()
    router.register(channel)
    router._message_context["web-session"] = {
        "channel_name": "discord",
        "target": "stale-thread",
        "message_id": "old-event",
    }

    assert await router.send_text(
        "web-session", "must not leak", channel="discord",
    ) is False
    assert channel.sent == []


@pytest.mark.asyncio
async def test_discord_inbound_persists_binding_before_running():
    engine = MagicMock()
    engine.sessions.get_active_session = AsyncMock(
        return_value="discord-session",
    )
    engine.db.bind_discord_session = AsyncMock(return_value={
        "session_id": "discord-session",
        "guild_id": "100",
        "thread_id": "200",
    })
    engine.run = AsyncMock(return_value="")
    engine.register_task = MagicMock()
    router = ChannelRouter(engine)
    router.BATCH_DEBOUNCE = 0
    router.register(_DiscordStubChannel(automatic_responses=False))

    await router.handle_message(InboundMessage(
        channel_name="discord",
        channel_key="discord:100:200",
        sender_id="200",
        text="hello",
        metadata={
            "message_id": "message-1",
            "discord_guild_id": "100",
        },
    ))

    engine.db.bind_discord_session.assert_awaited_once_with(
        "discord-session",
        guild_id="100",
        thread_id="200",
    )
    engine.run.assert_awaited_once()


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
            channel_name="manual",
            channel_key="manual:channel-1",
            sender_id="channel-1",
            text="hello",
        ),
        "shared",
    )

    assert response == "internal answer"
    router._setup_streaming.assert_not_awaited()
    router._teardown_streaming.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("channel_name", [None, "discord"])
async def test_session_typing_uses_durable_discord_binding_until_stopped(
    channel_name: str | None,
):
    engine = MagicMock()
    engine.db.get_discord_session_binding = AsyncMock(return_value={
        "session_id": "shared",
        "guild_id": "guild-1",
        "thread_id": "thread-1",
    })
    router = ChannelRouter(engine)
    router.TYPING_REFRESH_INTERVAL = 0.01
    channel = _TypingChannel()
    router.register(channel)

    typing_task = await router.start_session_typing(
        "shared",
        channel_name=channel_name,
    )
    await asyncio.wait_for(channel.typing_refreshed.wait(), timeout=1)
    await router.stop_session_typing(typing_task)
    typing_count = len(channel.typing_targets)
    await asyncio.sleep(0.03)

    engine.db.get_discord_session_binding.assert_awaited_once_with("shared")
    assert typing_count >= 2
    assert len(channel.typing_targets) == typing_count
    assert set(channel.typing_targets) == {"thread-1"}


@pytest.mark.asyncio
async def test_session_typing_failure_is_non_fatal():
    engine = MagicMock()
    engine.db.get_discord_session_binding = AsyncMock(return_value={
        "session_id": "shared",
        "guild_id": "guild-1",
        "thread_id": "thread-1",
    })
    router = ChannelRouter(engine)
    channel = _TypingChannel()
    channel.send_typing = AsyncMock(side_effect=RuntimeError("unavailable"))
    router.register(channel)

    typing_task = await router.start_session_typing(
        "shared",
        channel_name="discord",
    )
    await router.stop_session_typing(typing_task)

    channel.send_typing.assert_awaited_once_with("thread-1")


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
            channel_name="manual",
            channel_key="manual:channel-1",
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
        channel="manual",
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
        channel_name="manual",
        channel_key="manual:channel-1",
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
