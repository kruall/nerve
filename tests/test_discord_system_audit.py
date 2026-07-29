"""Discord AUDIT forum system-lifecycle feed."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

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


def _audit():
    client = MagicMock(spec=discord.Client)
    return DiscordSystemAudit(
        client=client,
        guild_id=GUILD_ID,
        forum_id=FORUM_ID,
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
    assert thread.send.await_args.args == (
        "🟢 **Nerve started** · <t:1700000000:F>\nProcess ID: `123`",
    )
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
        len(call.args[0]) <= 2000
        for call in thread.send.await_args_list
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
