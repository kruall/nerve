"""Discord AUDIT forum system-lifecycle feed."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from nerve.agent.streaming import StreamBroadcaster
from nerve.channels.discord_system_audit import DiscordSystemAudit

GUILD_ID = 100
FORUM_ID = 200
THREAD_ID = 300


class _Tag:
    def __init__(self, tag_id: int, name: str):
        self.id = tag_id
        self.name = name


class _AsyncRows:
    def __init__(self, rows):
        self.rows = iter(rows)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.rows)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


def _thread(
    *,
    archived: bool = False,
    applied_tags=None,
) -> MagicMock:
    thread = MagicMock(spec=discord.Thread)
    thread.id = THREAD_ID
    thread.parent_id = FORUM_ID
    thread.name = "System"
    thread.archived = archived
    thread.applied_tags = list(applied_tags or [])

    async def edit(**kwargs):
        for key, value in kwargs.items():
            if key != "reason":
                setattr(thread, key, value)
        return thread

    thread.edit = AsyncMock(side_effect=edit)
    thread.send = AsyncMock()
    return thread


def _audit(*, db=None, stream=None):
    client = MagicMock(spec=discord.Client)
    return DiscordSystemAudit(
        client=client,
        db=db or MagicMock(),
        guild_id=GUILD_ID,
        forum_id=FORUM_ID,
        stream=stream or StreamBroadcaster(),
    )


@pytest.mark.asyncio
async def test_start_creates_unpinned_system_tagged_thread():
    system_tag = _Tag(250, "system")
    inbox_tag = _Tag(251, "user-inbox")
    thread = _thread()
    audit = _audit()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = [system_tag, inbox_tag]
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )

    await audit.start(guild)

    assert forum.create_thread.await_args.kwargs["name"] == "System"
    assert forum.create_thread.await_args.kwargs["applied_tags"] == [
        system_tag,
        inbox_tag,
    ]
    assert thread.edit.await_args.kwargs == {
        "archived": False,
        "pinned": False,
        "reason": "Prepare Nerve system audit",
        "applied_tags": [system_tag, inbox_tag],
    }
    await audit.stop()


@pytest.mark.asyncio
async def test_start_restores_existing_archived_thread_and_preserves_tags():
    unrelated_tag = _Tag(240, "operator")
    system_tag = _Tag(250, "system")
    inbox_tag = _Tag(251, "user-inbox")
    thread = _thread(archived=True, applied_tags=[unrelated_tag])
    audit = _audit()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = [system_tag, inbox_tag]
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([thread])
    forum.create_thread = AsyncMock()

    await audit.start(guild)

    forum.create_thread.assert_not_awaited()
    assert thread.edit.await_args.kwargs["archived"] is False
    assert thread.edit.await_args.kwargs["pinned"] is False
    assert thread.edit.await_args.kwargs["applied_tags"] == [
        unrelated_tag,
        system_tag,
        inbox_tag,
    ]
    await audit.stop()


@pytest.mark.asyncio
async def test_emit_appends_timestamped_event_without_mentions():
    thread = _thread()
    audit = _audit()
    audit._thread = thread

    await audit.emit(
        "Nerve started",
        details="Process ID: `123`",
        level="success",
        occurred_at=1_700_000_000,
    )

    thread.send.assert_awaited_once()
    card = thread.send.await_args.kwargs["embed"]
    assert card.title == "🟢 Nerve started"
    assert card.description == "<t:1700000000:F>\n\nProcess ID: `123`"
    assert card.colour == discord.Colour.green()
    assert isinstance(
        thread.send.await_args.kwargs["allowed_mentions"],
        discord.AllowedMentions,
    )
    assert thread.send.await_args.kwargs[
        "allowed_mentions"
    ].everyone is False


@pytest.mark.asyncio
async def test_emit_splits_long_system_event_at_discord_limit():
    thread = _thread()
    audit = _audit()
    audit._thread = thread

    await audit.emit("Large event", details="x" * 4000)

    assert thread.send.await_count == 3
    assert all(
        len(call.kwargs["embed"].description or "") <= 2000
        for call in thread.send.await_args_list
    )
    assert thread.send.await_args_list[1].kwargs["embed"].title.endswith(
        "(continued)",
    )


@pytest.mark.asyncio
async def test_missing_system_tag_does_not_block_thread_creation():
    thread = _thread()
    audit = _audit()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = []
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )

    await audit.start(guild)

    assert "applied_tags" not in forum.create_thread.await_args.kwargs
    assert "applied_tags" not in thread.edit.await_args.kwargs
    await audit.stop()


@pytest.mark.asyncio
async def test_terminal_session_error_creates_one_red_card(db):
    stream = StreamBroadcaster()
    thread = _thread()
    audit = _audit(db=db, stream=stream)
    audit._thread = thread
    await db.create_session(
        "session-error-123",
        title="Broken canary",
        source="discord",
        backend="codex",
    )

    try:
        await audit.start()
        await stream.broadcast("session-error-123", {
            "type": "error",
            "error": "Backend startup failed",
        })
        await audit._error_queue.join()

        thread.send.assert_awaited_once()
        card = thread.send.await_args.kwargs["embed"]
        assert card.title == "🔴 Session error"
        assert "Session: Broken canary" in card.description
        assert "ID: `session-`" in card.description
        assert "Source: `discord` · Backend: `codex`" in card.description
        assert "Error: Backend startup failed" in card.description
        assert card.colour == discord.Colour.red()
    finally:
        await audit.stop()


@pytest.mark.asyncio
async def test_error_listener_ignores_nonterminal_and_global_events(db):
    stream = StreamBroadcaster()
    thread = _thread()
    audit = _audit(db=db, stream=stream)
    audit._thread = thread

    try:
        await audit.start()
        await stream.broadcast("session-1", {"type": "token", "content": "x"})
        await stream.broadcast("__global__", {
            "type": "error",
            "error": "not a session event",
        })
        await audit._error_queue.join()

        thread.send.assert_not_awaited()
    finally:
        await audit.stop()


@pytest.mark.asyncio
async def test_error_listener_returns_before_database_or_discord_work():
    thread = _thread()
    db = MagicMock()
    db.get_session = AsyncMock()
    stream = StreamBroadcaster()
    audit = _audit(db=db, stream=stream)
    audit._thread = thread

    try:
        await audit.start()
        await audit._on_stream_event("session-1", {
            "type": "error",
            "error": "late failure",
        })

        db.get_session.assert_not_awaited()
        thread.send.assert_not_awaited()
    finally:
        await audit.stop()


@pytest.mark.asyncio
async def test_start_is_idempotent_and_stop_removes_listener_and_worker():
    stream = StreamBroadcaster()
    audit = _audit(stream=stream)
    audit._thread = _thread()

    await audit.start()
    worker = audit._worker_task
    await audit.start()

    assert worker is not None
    assert audit._worker_task is worker
    assert [item[0] for item in stream._global_listeners] == [
        audit._listener_id,
    ]

    await audit.stop()

    assert worker.done()
    assert audit._worker_task is None
    assert stream._global_listeners == []


@pytest.mark.asyncio
async def test_one_database_or_delivery_failure_does_not_block_next_error():
    stream = StreamBroadcaster()
    thread = _thread()
    thread.send = AsyncMock(side_effect=[RuntimeError("Discord unavailable"), None])
    db = MagicMock()
    db.get_session = AsyncMock(side_effect=[
        RuntimeError("database unavailable"),
        {"title": "First delivered", "source": "web", "backend": "codex"},
        {"title": "Second delivered", "source": "web", "backend": "codex"},
    ])
    audit = _audit(db=db, stream=stream)
    audit._thread = thread

    try:
        await audit.start()
        await stream.broadcast("session-1", {"type": "error", "error": "db"})
        await stream.broadcast("session-2", {"type": "error", "error": "send"})
        await stream.broadcast("session-3", {"type": "error", "error": "next"})
        await audit._error_queue.join()

        assert db.get_session.await_count == 3
        assert thread.send.await_count == 2
        assert "Second delivered" in thread.send.await_args.kwargs["embed"].description
    finally:
        await audit.stop()


@pytest.mark.asyncio
async def test_session_error_is_bounded_and_disables_mentions(db):
    stream = StreamBroadcaster()
    thread = _thread()
    audit = _audit(db=db, stream=stream)
    audit._thread = thread
    await db.create_session("session-long", title="Long error")

    try:
        await audit.start()
        await stream.broadcast("session-long", {
            "type": "error",
            "error": "@everyone " + "x" * 10_000,
        })
        await audit._error_queue.join()

        call = thread.send.await_args
        assert len(call.kwargs["embed"].description) <= 2000
        assert call.kwargs["embed"].description.endswith("…")
        assert call.kwargs["allowed_mentions"].everyone is False
        assert call.kwargs["allowed_mentions"].users is False
        assert call.kwargs["allowed_mentions"].roles is False
    finally:
        await audit.stop()
