"""Tests for editable pinned prompts in Discord project forums."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from nerve.channels.discord_project_prompts import DiscordProjectPrompts

GUILD = 100
FORUM = 300
PROMPT_THREAD = 301
USER = 400


def _thread(*, pinned: bool = False):
    thread = MagicMock(spec=discord.Thread)
    thread.id = PROMPT_THREAD
    thread.parent_id = FORUM
    thread.name = "Project prompt"
    thread.pinned = pinned
    thread.archived = False
    thread.edit = AsyncMock(return_value=thread)
    thread.history = MagicMock(return_value=_history())
    return thread


def _manager(db, client) -> DiscordProjectPrompts:
    return DiscordProjectPrompts(
        client=client,
        db=db,
        guild_id=GUILD,
        project_forums={FORUM: "NERVE"},
        allowed_author_ids={USER},
    )


@pytest.mark.asyncio
async def test_start_creates_and_pins_prompt_thread():
    db = MagicMock()
    db.get_discord_project_prompt = AsyncMock(return_value=None)
    db.upsert_discord_project_prompt = AsyncMock()
    client = MagicMock()
    thread = _thread()
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )
    forum.archived_threads = _empty_history
    guild = MagicMock()
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])

    prompts = _manager(db, client)
    await prompts.start(guild)

    forum.create_thread.assert_awaited_once()
    assert forum.create_thread.await_args.kwargs["name"] == "Project prompt"
    assert "Project prompt for **NERVE**" in (
        forum.create_thread.await_args.kwargs["content"]
    )
    thread.edit.assert_awaited_once_with(
        archived=False,
        pinned=True,
        reason="Pin Nerve project prompt for NERVE",
    )
    db.upsert_discord_project_prompt.assert_awaited_once_with(
        guild_id=GUILD,
        forum_id=FORUM,
        project="NERVE",
        thread_id=PROMPT_THREAD,
        message_id=None,
        content="",
    )
    assert prompts.is_prompt_thread(PROMPT_THREAD)


@pytest.mark.asyncio
async def test_allowed_human_messages_become_ordered_editable_prompt():
    db = MagicMock()
    db.get_discord_project_prompt = AsyncMock(return_value=None)
    db.upsert_discord_project_prompt = AsyncMock()
    client = MagicMock()
    thread = _thread()
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )
    forum.archived_threads = _empty_history
    guild = MagicMock()
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    prompts = _manager(db, client)
    await prompts.start(guild)

    second = SimpleNamespace(
        id=502,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="Describe the PR succinctly.",
    )
    first = SimpleNamespace(
        id=501,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="Use a dedicated worktree.",
    )
    assert await prompts.observe_message(second) is True
    assert await prompts.observe_message(first) is True
    rendered = prompts.prompt_for_thread(FORUM, 999)
    assert "NERVE" in rendered
    assert rendered.index(first.content) < rendered.index(second.content)

    second.content = "Keep the PR description concise."
    assert await prompts.observe_edit(second) is True
    rendered = prompts.prompt_for_thread(FORUM, 999)
    assert first.content in rendered
    assert second.content in rendered
    assert "Describe the PR succinctly." not in rendered

    assert await prompts.observe_delete(first) is True
    assert first.content not in prompts.prompt_for_thread(FORUM, 999)
    assert second.content in prompts.prompt_for_thread(FORUM, 999)

    assert await prompts.observe_delete(second) is True
    assert prompts.prompt_for_thread(FORUM, 999) == ""
    replacement = SimpleNamespace(
        id=503,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="Replacement prompt.",
    )
    assert await prompts.observe_message(replacement) is True
    assert replacement.content in prompts.prompt_for_thread(FORUM, 999)


@pytest.mark.asyncio
async def test_combined_prompt_can_exceed_one_discord_message():
    db = MagicMock()
    db.get_discord_project_prompt = AsyncMock(return_value=None)
    db.upsert_discord_project_prompt = AsyncMock()
    client = MagicMock()
    thread = _thread()
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )
    forum.archived_threads = _empty_history
    guild = MagicMock()
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    prompts = _manager(db, client)
    await prompts.start(guild)

    first = SimpleNamespace(
        id=501,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="A" * 3500,
    )
    second = SimpleNamespace(
        id=502,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="B" * 3500,
    )
    unauthorized = SimpleNamespace(
        id=503,
        channel=thread,
        author=SimpleNamespace(id=USER + 1),
        content="must not be included",
    )

    assert await prompts.observe_message(first) is True
    assert await prompts.observe_message(second) is True
    assert await prompts.observe_message(unauthorized) is True

    rendered = prompts.prompt_for_thread(FORUM, 999)
    assert first.content in rendered
    assert second.content in rendered
    assert unauthorized.content not in rendered
    assert len(rendered) > 7000
    persisted = db.upsert_discord_project_prompt.await_args.kwargs
    assert persisted["message_id"] == first.id
    assert persisted["content"] == f"{first.content}\n\n{second.content}"


@pytest.mark.asyncio
async def test_restart_refreshes_prompt_from_human_source_message():
    db = MagicMock()
    db.get_discord_project_prompt = AsyncMock(return_value={
        "guild_id": str(GUILD),
        "forum_id": str(FORUM),
        "project": "NERVE",
        "thread_id": str(PROMPT_THREAD),
        "message_id": "501",
        "content": "stale",
    })
    db.upsert_discord_project_prompt = AsyncMock()
    thread = _thread(pinned=True)
    thread.history.return_value = _history(
        SimpleNamespace(
            id=1,
            author=SimpleNamespace(id=999),
            content="Bot-owned starter.",
        ),
        SimpleNamespace(
            id=501,
            author=SimpleNamespace(id=USER),
            content="fresh first part",
        ),
        SimpleNamespace(
            id=502,
            author=SimpleNamespace(id=USER),
            content="fresh second part",
        ),
    )
    client = MagicMock()
    client.get_channel.return_value = thread
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    guild = MagicMock()
    guild.get_channel.return_value = forum

    prompts = _manager(db, client)
    await prompts.start(guild)

    thread.history.assert_called_once_with(limit=None, oldest_first=True)
    rendered = prompts.prompt_for_thread(FORUM, 999)
    assert "Bot-owned starter." not in rendered
    assert rendered.index("fresh first part") < rendered.index("fresh second part")


@pytest.mark.asyncio
async def test_restart_keeps_persisted_snapshot_when_history_is_unavailable():
    db = MagicMock()
    db.get_discord_project_prompt = AsyncMock(return_value={
        "guild_id": str(GUILD),
        "forum_id": str(FORUM),
        "project": "NERVE",
        "thread_id": str(PROMPT_THREAD),
        "message_id": "501",
        "content": "persisted prompt",
    })
    db.upsert_discord_project_prompt = AsyncMock()
    thread = _thread(pinned=True)
    thread.history.side_effect = discord.Forbidden(
        MagicMock(status=403, reason="Forbidden"),
        "Missing permissions",
    )
    client = MagicMock()
    client.get_channel.return_value = thread
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    guild = MagicMock()
    guild.get_channel.return_value = forum

    prompts = _manager(db, client)
    await prompts.start(guild)

    assert "persisted prompt" in prompts.prompt_for_thread(FORUM, 999)


async def _empty_history(**_kwargs):
    if False:
        yield None


async def _history(*messages):
    for message in messages:
        yield message
