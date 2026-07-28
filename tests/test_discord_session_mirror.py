from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.agent.streaming import StreamBroadcaster
from nerve.channels.discord_mirror import DiscordSessionMirror


class _Message:
    def __init__(self, message_id: int, content: str):
        self.id = message_id
        self.content = content
        self.deleted = False
        self.edit_count = 0

    async def edit(self, *, content, **kwargs):
        self.content = content
        self.edit_count += 1
        return self

    async def delete(self):
        self.deleted = True


class _Thread:
    def __init__(self, thread_id: int, starter_id: int, starter: str):
        self.id = thread_id
        self._next_id = starter_id + 1
        self.messages = {
            starter_id: _Message(starter_id, starter),
        }

    async def fetch_message(self, message_id: int):
        return self.messages[message_id]

    async def send(self, content: str, **kwargs):
        message = _Message(self._next_id, content)
        self.messages[message.id] = message
        self._next_id += 1
        return message


class _Forum:
    def __init__(self, client):
        self.id = 200
        self.guild = SimpleNamespace(id=100)
        self.client = client
        self.created = []

    async def create_thread(self, *, name, content, **kwargs):
        thread_id = 1000 + len(self.created)
        starter_id = 2000 + len(self.created)
        thread = _Thread(thread_id, starter_id, content)
        self.client.channels[thread_id] = thread
        result = SimpleNamespace(
            thread=thread,
            message=thread.messages[starter_id],
        )
        self.created.append((name, content, result))
        return result


class _Client:
    def __init__(self):
        self.channels = {}

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)

    async def fetch_channel(self, channel_id: int):
        return self.channels[channel_id]


def _contents(thread: _Thread) -> list[str]:
    return [
        message.content
        for message in thread.messages.values()
        if not message.deleted
    ]


@pytest.mark.asyncio
async def test_mirror_creates_one_thread_and_incrementally_appends(db):
    await db.create_session(
        "session-1",
        title="Investigate",
        source="cron",
        backend="codex",
    )
    await db.log_session_event("session-1", "created", {"source": "cron"})
    await db.add_message("session-1", "user", "check the system")

    client = _Client()
    forum = _Forum(client)
    client.channels[forum.id] = forum
    mirror = DiscordSessionMirror(
        client=client,
        db=db,
        guild_id=100,
        forum_id=forum.id,
        stream=StreamBroadcaster(),
    )
    mirror._forum = forum

    await mirror._sync_session("session-1")

    assert len(forum.created) == 1
    thread = forum.created[0][2].thread
    contents = _contents(thread)
    assert any("Nerve session mirror" in value for value in contents)
    assert any("event · created" in value for value in contents)
    assert any("check the system" in value for value in contents)
    assert any(
        "event · created" in value and "check the system" in value
        for value in contents
    )

    await db.add_message(
        "session-1",
        "assistant",
        "done",
        thinking="private reasoning must not be mirrored",
        blocks=[
            {"type": "thinking", "content": "hidden"},
            {"type": "text", "content": "done"},
            {
                "type": "tool_call",
                "tool": "Read",
                "input": {"path": "README.md"},
                "result": "contents",
            },
        ],
    )
    await mirror._sync_session("session-1")

    assert len(forum.created) == 1
    contents = _contents(thread)
    assert any("tool · `Read`" in value for value in contents)
    assert any("done" in value for value in contents)
    assert all("private reasoning" not in value for value in contents)
    assert all("hidden" not in value for value in contents)

    message_count = len(contents)
    await mirror._sync_session("session-1")
    assert len(_contents(thread)) == message_count


@pytest.mark.asyncio
async def test_batch_window_coalesces_updates_and_terminal_flushes(db):
    mirror = DiscordSessionMirror(
        client=_Client(),
        db=db,
        guild_id=100,
        forum_id=200,
        batch_window_seconds=0.03,
        stream=StreamBroadcaster(),
    )
    mirror._sync_session = AsyncMock()
    mirror._worker_task = asyncio.create_task(mirror._worker_loop())

    mirror._mark_dirty("batched")
    mirror._mark_dirty("batched")
    await asyncio.sleep(0.01)
    mirror._sync_session.assert_not_awaited()
    await asyncio.sleep(0.04)
    mirror._sync_session.assert_awaited_once_with("batched")

    mirror._sync_session.reset_mock()
    mirror._mark_dirty("terminal")
    await mirror._on_stream_event("terminal", {"type": "done"})
    await asyncio.sleep(0.01)
    mirror._sync_session.assert_awaited_once_with("terminal")

    await mirror.stop()


@pytest.mark.asyncio
async def test_mirror_edits_live_turn_then_replaces_it_with_persisted_message(db):
    await db.create_session("session-2", title="Live", source="discord")
    await db.add_message("session-2", "user", "start")

    client = _Client()
    forum = _Forum(client)
    client.channels[forum.id] = forum
    mirror = DiscordSessionMirror(
        client=client,
        db=db,
        guild_id=100,
        forum_id=forum.id,
        stream=StreamBroadcaster(),
    )
    mirror._forum = forum

    await mirror._on_stream_event(
        "session-2",
        {"type": "token", "content": "working"},
    )
    await mirror._on_stream_event("session-2", {
        "type": "tool_use",
        "tool": "Bash",
        "tool_use_id": "tool-1",
        "input": {"command": "true"},
    })
    await mirror._sync_session("session-2")

    thread = forum.created[0][2].thread
    assert any("live turn" in value for value in _contents(thread))
    assert any("tool · `Bash`" in value for value in _contents(thread))

    await db.add_message("session-2", "assistant", "finished")
    await mirror._on_stream_event("session-2", {"type": "done"})
    await mirror._sync_session("session-2")

    contents = _contents(thread)
    assert any("finished" in value for value in contents)
    assert all("live turn" not in value for value in contents)


@pytest.mark.asyncio
async def test_mirror_replaces_legacy_per_item_checkpoints_with_batches(db):
    await db.create_session("session-legacy", title="Legacy")
    await db.log_session_event("session-legacy", "started", {"source": "web"})
    await db.add_message("session-legacy", "user", "one batched update")

    client = _Client()
    forum = _Forum(client)
    client.channels[forum.id] = forum
    mirror = DiscordSessionMirror(
        client=client,
        db=db,
        guild_id=100,
        forum_id=forum.id,
        stream=StreamBroadcaster(),
    )
    mirror._forum = forum

    await mirror._sync_session("session-legacy")
    thread = forum.created[0][2].thread
    checkpoints = await db.get_discord_mirror_items("session-legacy")
    old_message_id = int(checkpoints[("batch", 1)]["discord_message_ids"][0])
    await db._write(
        """UPDATE discord_session_mirror_items
           SET item_kind = 'event', item_id = 999
           WHERE session_id = ? AND item_kind = 'batch' AND item_id = 1""",
        ("session-legacy",),
    )

    await mirror._sync_session("session-legacy")

    assert thread.messages[old_message_id].deleted is True
    checkpoints = await db.get_discord_mirror_items("session-legacy")
    assert set(checkpoints) == {("batch", 1)}
    assert any(
        "event · started" in value and "one batched update" in value
        for value in _contents(thread)
    )


@pytest.mark.asyncio
async def test_mirror_store_round_trips_restart_checkpoints(db):
    await db.create_session("session-3", title="Restart")
    await db.upsert_discord_session_mirror(
        "session-3",
        guild_id=100,
        forum_id=200,
        thread_id=300,
        starter_message_id=400,
        header_hash="header",
    )
    await db.set_discord_mirror_live_messages("session-3", [500, 501])
    await db.upsert_discord_mirror_item(
        "session-3",
        item_kind="message",
        item_id=1,
        discord_message_ids=[600, 601],
        content_hash="content",
    )

    mirror = await db.get_discord_session_mirror("session-3")
    items = await db.get_discord_mirror_items("session-3")

    assert mirror["live_message_ids"] == ["500", "501"]
    assert items[("message", 1)]["discord_message_ids"] == ["600", "601"]

    await db.delete_session("session-3")
    assert await db.get_discord_session_mirror("session-3") is None
    assert await db.get_discord_mirror_items("session-3") == {}


@pytest.mark.asyncio
async def test_reconcile_skips_old_unmapped_sessions_but_keeps_mapped_ones(db):
    await db.create_session("old-session", title="Old")
    await db.create_session("new-session", title="New")
    await db._write(
        "UPDATE sessions SET created_at = ?, updated_at = ? WHERE id = ?",
        ("2020-01-01 00:00:00", "2020-01-01 00:00:00", "old-session"),
    )
    await db._write(
        "UPDATE sessions SET created_at = ?, updated_at = ? WHERE id = ?",
        ("2030-01-01 00:00:00", "2030-01-01 00:00:00", "new-session"),
    )

    sessions = await db.list_discord_mirror_sessions(
        active_after="2025-01-01T00:00:00+00:00",
    )
    assert [session["id"] for session in sessions] == ["new-session"]

    await db.upsert_discord_session_mirror(
        "old-session",
        guild_id=100,
        forum_id=200,
        thread_id=300,
        starter_message_id=400,
        header_hash="header",
    )
    sessions = await db.list_discord_mirror_sessions(
        active_after="2025-01-01T00:00:00+00:00",
    )
    assert {session["id"] for session in sessions} == {
        "old-session",
        "new-session",
    }
