from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

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
        self.bulk_delete_calls = 0
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

    async def delete_messages(self, messages, **kwargs):
        self.bulk_delete_calls += 1
        for message in messages:
            self.messages[message.id].deleted = True


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
        source="web",
        backend="codex",
    )
    await db.log_session_event("session-1", "created", {"source": "cron"})
    await db.log_session_event(
        "session-1",
        "started",
        {"sdk_session_id": "private"},
    )
    await db.log_session_event(
        "session-1",
        "error",
        {"error": "Agent error: provider stopped"},
    )
    await db.log_session_event(
        "session-1",
        "codex_rate_limits",
        {"rateLimits": {"primary": {"usedPercent": 25}}},
    )
    await db.log_session_event(
        "session-1",
        "external_tool_call",
        {
            "tool": "notify",
            "args": "{\"body\":\"private details\"}",
            "result": "Notification sent",
            "is_error": False,
        },
    )
    await db.add_message(
        "session-1",
        "assistant",
        "",
        blocks=[
            {
                "type": "tool_call",
                "tool": "mcp__nerve__notify",
                "input": {"body": "private details"},
                "result": "Notification sent",
            },
        ],
    )
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
    assert any("`[created]`" in value for value in contents)
    assert any("`[started]`" in value for value in contents)
    assert any(
        "`[error]`" in value and "Agent error: provider stopped" in value
        for value in contents
    )
    assert all('"error":' not in value for value in contents)
    assert all("sdk_session_id" not in value for value in contents)
    assert any("check the system" in value for value in contents)
    assert all("codex_rate_limits" not in value for value in contents)
    assert all("usedPercent" not in value for value in contents)
    assert any("`[notify]`" in value for value in contents)
    assert sum(value.count("`[notify]`") for value in contents) == 1
    assert all("mcp__nerve__notify" not in value for value in contents)
    assert all("private details" not in value for value in contents)
    assert all("Notification sent" not in value for value in contents)
    assert any(
        "`[created]`" in value and "check the system" in value
        for value in contents
    )
    assert all("———" not in value for value in contents)

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
    assert any("`[Read]`" in value for value in contents)
    assert all("README.md" not in value for value in contents)
    assert all("contents" not in value for value in contents)
    assert any("done" in value for value in contents)
    assert all("private reasoning" not in value for value in contents)
    assert all("hidden" not in value for value in contents)

    message_count = len(contents)
    await mirror._sync_session("session-1")
    assert len(_contents(thread)) == message_count


def test_compacts_assistant_wakeup_and_adjacent_timeline_items():
    mirror = DiscordSessionMirror(
        client=_Client(),
        db=AsyncMock(),
        guild_id=100,
        forum_id=200,
        stream=StreamBroadcaster(),
    )
    created_at = "2026-07-28T15:57:09+00:00"
    assistant = mirror._render_item({
        "item_kind": "message",
        "item_type": "assistant",
        "created_at": created_at,
        "content": "",
        "details": [{"type": "wakeup"}],
    })

    assert assistant == (
        "`[assistant]` · `2026-07-28 15:57:09 UTC`\n"
        "`[wakeup]`"
    )
    custom_event = mirror._render_item({
        "item_kind": "event",
        "item_type": "custom",
        "created_at": created_at,
        "details": {"value": 1},
    })
    assert custom_event == (
        "`[custom]` · `2026-07-28 15:57:09 UTC`\n"
        "```json\n{\n  \"value\": 1\n}\n```"
    )
    assert "event ·" not in custom_event

    batches = mirror._render_persisted_batches([
        {
            "item_kind": "event",
            "item_type": "created",
            "created_at": created_at,
            "details": {"source": "discord"},
        },
        {
            "item_kind": "event",
            "item_type": "started",
            "created_at": created_at,
            "details": {"sdk_session_id": "private"},
        },
        {
            "item_kind": "event",
            "item_type": "stopped",
            "created_at": created_at,
            "details": {"resumable": True},
        },
        {
            "item_kind": "event",
            "item_type": "error",
            "created_at": created_at,
            "details": {"error": "Agent error: provider stopped"},
        },
    ])

    assert len(batches) == 1
    assert "———" not in batches[0]
    assert batches[0].splitlines() == [
        "`[created]` · `2026-07-28 15:57:09 UTC`",
        "`[started]` · `2026-07-28 15:57:09 UTC`",
        "`[stopped]` · `2026-07-28 15:57:09 UTC`",
        "`[error]` · `2026-07-28 15:57:09 UTC`",
        "Agent error: provider stopped",
    ]


@pytest.mark.asyncio
async def test_persisted_timeline_orders_mixed_timestamp_formats(db):
    await db.create_session("session-timeline", title="Timeline")
    event_id = await db.log_session_event(
        "session-timeline",
        "created",
        {"source": "discord"},
    )
    await db._write(
        "UPDATE session_events SET created_at = ? WHERE id = ?",
        ("2026-07-28T12:04:00.500000+00:00", event_id),
    )
    await db.add_message(
        "session-timeline",
        "user",
        "later message",
        created_at="2026-07-28 12:05:00",
    )

    items = await db.get_discord_mirror_content_items("session-timeline")

    assert [
        (item["item_kind"], item["item_type"], item["created_at"])
        for item in items
    ] == [
        ("event", "created", "2026-07-28T12:04:00.500000+00:00"),
        ("message", "user", "2026-07-28 12:05:00"),
    ]


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
    assert any("working" in value for value in _contents(thread))
    assert all("live turn" not in value for value in _contents(thread))
    assert all("`[live]`" not in value for value in _contents(thread))
    assert any("`[Bash]`" in value for value in _contents(thread))

    await db.add_message("session-2", "assistant", "finished")
    await mirror._on_stream_event("session-2", {"type": "done"})
    await mirror._sync_session("session-2")

    contents = _contents(thread)
    assert any("finished" in value for value in contents)
    assert all("`[live]`" not in value for value in contents)


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
    assert thread.bulk_delete_calls == 0
    checkpoints = await db.get_discord_mirror_items("session-legacy")
    assert set(checkpoints) == {("batch", 1)}
    assert any(
        "`[started]`" in value and "one batched update" in value
        for value in _contents(thread)
    )


@pytest.mark.asyncio
async def test_mirror_bulk_deletes_large_checkpoint_sets(db):
    client = _Client()
    thread = _Thread(1000, 2000, "starter")
    for index in range(250):
        await thread.send(f"legacy {index}")
    message_ids = [
        message_id
        for message_id in thread.messages
        if message_id != 2000
    ]
    mirror = DiscordSessionMirror(
        client=client,
        db=db,
        guild_id=100,
        forum_id=200,
        stream=StreamBroadcaster(),
    )

    await mirror._delete_messages(thread, message_ids)

    assert thread.bulk_delete_calls == 3
    assert all(thread.messages[value].deleted for value in message_ids)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("session_id", "source"),
    [
        ("cron:task-planner:run-1", "cron"),
        ("internal-run", "system"),
    ],
)
async def test_mirror_skips_system_sessions(db, session_id, source):
    await db.create_session(session_id, title="System run", source=source)
    await db.add_message(session_id, "user", "internal trigger")

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

    await mirror._sync_session(session_id)

    assert forum.created == []
    assert await db.get_discord_session_mirror(session_id) is None
    eligible = await db.list_discord_mirror_sessions(
        active_after="2000-01-01T00:00:00+00:00",
    )
    assert session_id not in {session["id"] for session in eligible}


def test_tool_calls_are_compact_and_grouped_like_telegram():
    mirror = DiscordSessionMirror(
        client=_Client(),
        db=AsyncMock(),
        guild_id=100,
        forum_id=200,
        stream=StreamBroadcaster(),
    )
    body = mirror._render_message_body({
        "content": "",
        "details": [
            {
                "type": "tool_call",
                "tool": "Read",
                "input": {"path": "secret.txt"},
                "result": "sensitive output",
            },
            {
                "type": "tool_call",
                "tool": "Read",
                "input": {"path": "other.txt"},
                "result": "more output",
            },
            {"type": "text", "content": "Done"},
        ],
    })

    assert body == "`[Read] x2`\n\nDone"
    assert "secret.txt" not in body
    assert "sensitive output" not in body


def test_live_lifecycle_events_are_compact():
    mirror = DiscordSessionMirror(
        client=_Client(),
        db=AsyncMock(),
        guild_id=100,
        forum_id=200,
        stream=StreamBroadcaster(),
    )

    rendered = mirror._render_live([
        {
            "kind": "system",
            "label": "started",
            "content": '{"sdk_session_id":"private"}',
        },
        {
            "kind": "system",
            "label": "idle",
            "content": '{"duration_ms":1234}',
        },
        {
            "kind": "system",
            "label": "error",
            "content": "Agent error: provider stopped",
        },
        {
            "kind": "system",
            "label": "wakeup",
            "content": "",
        },
        {
            "kind": "system",
            "label": "custom",
            "content": '{"value":1}',
        },
    ])

    assert rendered == (
        "`[started]`\n\n"
        "`[idle]`\n\n"
        "`[error]`\nAgent error: provider stopped\n\n"
        "`[wakeup]`\n\n"
        "`[custom]`\n```\n{\"value\":1}\n```"
    )
    assert "sdk_session_id" not in rendered
    assert "duration_ms" not in rendered
    assert '"error":' not in rendered


@pytest.mark.asyncio
async def test_live_rate_limit_backend_status_is_ignored():
    mirror = DiscordSessionMirror(
        client=_Client(),
        db=AsyncMock(),
        guild_id=100,
        forum_id=200,
        stream=StreamBroadcaster(),
    )
    mirror._mark_dirty = MagicMock()

    await mirror._on_stream_event("session-1", {
        "type": "backend_status",
        "subtype": "codex_rate_limits",
        "data": {"rateLimits": {"primary": {"usedPercent": 25}}},
    })

    assert "session-1" not in mirror._live_blocks
    mirror._mark_dirty.assert_not_called()
