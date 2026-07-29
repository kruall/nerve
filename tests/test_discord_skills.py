"""Discord skill-forum projection and cross-agent discovery."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from nerve.channels.discord_skills import DiscordSkillForum, _digest

GUILD_ID = 100
FORUM_ID = 200
THREAD_ID = 300


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


def _skill(raw: str = "---\nname: Demo\ndescription: Test\n---\nBody\n"):
    return SimpleNamespace(
        id="demo-skill",
        name="Demo",
        description="Test skill",
        version="1.2.3",
        raw=raw,
    )


def _manager(skill=None):
    manager = MagicMock()
    manager.db.list_skills = AsyncMock(
        return_value=[] if skill is None else [{"id": skill.id}],
    )
    manager.get_skill = AsyncMock(return_value=skill)
    manager.add_change_listener = MagicMock()
    manager.remove_change_listener = MagicMock()
    return manager


def _thread(*, starter_content: str = "", archived: bool = False):
    thread = MagicMock(spec=discord.Thread)
    thread.id = THREAD_ID
    thread.parent_id = FORUM_ID
    thread.name = "demo-skill"
    thread.archived = archived
    starter = SimpleNamespace(
        id=THREAD_ID,
        content=starter_content,
    )
    thread.fetch_message = AsyncMock(return_value=starter)
    thread.history.return_value = _AsyncRows([starter])
    thread.edit = AsyncMock(return_value=thread)
    thread.send = AsyncMock()
    return thread


def _environment(*, manager, active=None, archived=None, created_thread=None):
    client = MagicMock(spec=discord.Client)
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    client.get_guild.return_value = guild
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=list(active or []))
    forum.archived_threads.return_value = _AsyncRows(list(archived or []))
    if created_thread is not None:
        forum.create_thread = AsyncMock(
            return_value=SimpleNamespace(thread=created_thread),
        )
    projection = DiscordSkillForum(
        client=client,
        skill_manager=manager,
        guild_id=GUILD_ID,
        forum_id=FORUM_ID,
    )
    return projection, guild, forum


@pytest.mark.asyncio
async def test_start_creates_one_thread_with_exact_skill_attachment():
    skill = _skill()
    manager = _manager(skill)
    created_thread = _thread()
    projection, guild, forum = _environment(
        manager=manager,
        created_thread=created_thread,
    )

    await projection.start(guild)

    manager.add_change_listener.assert_called_once_with(
        projection._on_skill_change,
    )
    forum.create_thread.assert_awaited_once()
    kwargs = forum.create_thread.await_args.kwargs
    assert kwargs["name"] == "demo-skill"
    assert f"SHA-256: `{_digest(skill.raw)}`" in kwargs["content"]
    assert kwargs["file"].filename == "demo-skill-SKILL.md"
    assert kwargs["auto_archive_duration"] == 10080
    assert projection.skill_id_for_thread(THREAD_ID) == "demo-skill"


@pytest.mark.asyncio
async def test_restart_discovers_matching_snapshot_without_duplicate_publish():
    skill = _skill()
    content = (
        f"**Nerve skill: `demo-skill`**\n"
        f"SHA-256: `{_digest(skill.raw)}`"
    )
    existing = _thread(starter_content=content)
    manager = _manager(skill)
    projection, guild, forum = _environment(
        manager=manager,
        active=[existing],
    )

    await projection.start(guild)

    forum.create_thread.assert_not_awaited()
    existing.send.assert_not_awaited()
    assert projection.skill_id_for_thread(THREAD_ID) == "demo-skill"


@pytest.mark.asyncio
async def test_skill_update_appends_changed_exact_snapshot():
    original = _skill()
    changed = _skill(
        "---\nname: Demo\ndescription: Test\n---\nChanged body\n",
    )
    content = (
        f"**Nerve skill: `demo-skill`**\n"
        f"SHA-256: `{_digest(original.raw)}`"
    )
    existing = _thread(starter_content=content)
    manager = _manager(original)
    projection, guild, _forum = _environment(
        manager=manager,
        active=[existing],
    )
    await projection.start(guild)
    manager.get_skill.return_value = changed

    await projection._on_skill_change("update", "demo-skill")

    existing.send.assert_awaited_once()
    kwargs = existing.send.await_args.kwargs
    assert f"SHA-256: `{_digest(changed.raw)}`" in (
        existing.send.await_args.args[0]
    )
    assert kwargs["file"].filename == "demo-skill-SKILL.md"


@pytest.mark.asyncio
async def test_remote_only_managed_thread_is_registered_for_discussion():
    existing = _thread(
        starter_content=(
            "**Nerve skill: `remote-skill`**\n"
            f"SHA-256: `{'a' * 64}`"
        ),
    )
    manager = _manager()
    projection, guild, forum = _environment(
        manager=manager,
        archived=[existing],
    )

    await projection.start(guild)

    forum.create_thread.assert_not_awaited()
    assert projection.skill_id_for_thread(THREAD_ID) == "remote-skill"


@pytest.mark.asyncio
async def test_unmanaged_manual_thread_is_ignored():
    existing = _thread(starter_content="A human-created discussion")
    manager = _manager()
    projection, guild, _forum = _environment(
        manager=manager,
        active=[existing],
    )

    await projection.start(guild)

    assert projection.skill_id_for_thread(THREAD_ID) == ""


@pytest.mark.asyncio
async def test_stop_unsubscribes_from_skill_changes():
    manager = _manager()
    projection, guild, _forum = _environment(manager=manager)
    await projection.start(guild)

    await projection.stop()

    manager.remove_change_listener.assert_called_once_with(
        projection._on_skill_change,
    )
