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
    thread.fetch_message = AsyncMock()
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
async def test_allowed_human_message_becomes_editable_prompt():
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

    source = SimpleNamespace(
        id=501,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="Use a dedicated worktree and describe the PR succinctly.",
    )
    assert await prompts.observe_message(source) is True
    rendered = prompts.prompt_for_thread(FORUM, 999)
    assert "NERVE" in rendered
    assert source.content in rendered

    source.content = "Use a dedicated worktree; request push approval."
    assert await prompts.observe_edit(source) is True
    assert source.content in prompts.prompt_for_thread(FORUM, 999)

    assert await prompts.observe_delete(source) is True
    assert prompts.prompt_for_thread(FORUM, 999) == ""
    replacement = SimpleNamespace(
        id=502,
        channel=thread,
        author=SimpleNamespace(id=USER),
        content="Replacement prompt.",
    )
    assert await prompts.observe_message(replacement) is True
    assert replacement.content in prompts.prompt_for_thread(FORUM, 999)


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
    thread.fetch_message.return_value = SimpleNamespace(content="fresh prompt")
    client = MagicMock()
    client.get_channel.return_value = thread
    forum = MagicMock(spec=discord.ForumChannel)
    forum.id = FORUM
    guild = MagicMock()
    guild.get_channel.return_value = forum

    prompts = _manager(db, client)
    await prompts.start(guild)

    thread.fetch_message.assert_awaited_once_with(501)
    assert "fresh prompt" in prompts.prompt_for_thread(FORUM, 999)


async def _empty_history(**_kwargs):
    if False:
        yield None
