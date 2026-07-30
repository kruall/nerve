"""Discord project-task creation lifecycle tags."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.discord_project_tasks import (
    DiscordProjectTaskCreateError,
    DiscordProjectTaskCreator,
)


GUILD_ID = 1
FORUM_ID = 100


async def _empty_archived_threads(*, limit):
    del limit
    if False:
        yield None


def _creator(*, tags):
    forum = SimpleNamespace(
        available_tags=tags,
        archived_threads=_empty_archived_threads,
        create_thread=AsyncMock(
            return_value=SimpleNamespace(thread=SimpleNamespace(id=200)),
        ),
    )
    guild = SimpleNamespace(
        get_channel=MagicMock(return_value=forum),
        active_threads=AsyncMock(return_value=[]),
    )
    client = SimpleNamespace(get_guild=MagicMock(return_value=guild))
    creator = DiscordProjectTaskCreator(
        guild_id=GUILD_ID,
        task_forums={"NERVE": FORUM_ID},
        allowed_author_ids={10},
        client=lambda: client,
    )
    return creator, forum


@pytest.mark.asyncio
async def test_ready_for_agent_task_starts_with_lifecycle_tag():
    ready_tag = SimpleNamespace(id=301, name="ready-for-agent")
    creator, forum = _creator(tags=[ready_tag])

    task_id, thread_id = await creator.create_for_agent(
        project="NERVE",
        title="Record an actionable problem",
        description="Create a follow-up task when the agent finds it.",
        ready_for_agent=True,
    )

    assert (task_id, thread_id) == ("NERVE-1", 200)
    assert forum.create_thread.await_args.kwargs["applied_tags"] == [ready_tag]


@pytest.mark.asyncio
async def test_unmarked_task_remains_new_task():
    creator, forum = _creator(tags=[SimpleNamespace(id=301, name="ready-for-agent")])

    await creator.create_for_agent(
        project="NERVE",
        title="Human task",
        description="Keep the task in triage.",
        author_id=10,
    )

    assert "applied_tags" not in forum.create_thread.await_args.kwargs


@pytest.mark.asyncio
async def test_ready_for_agent_task_requires_one_lifecycle_tag():
    creator, forum = _creator(tags=[])

    with pytest.raises(
        DiscordProjectTaskCreateError,
        match="ровно один тег ready-for-agent",
    ):
        await creator.create_for_agent(
            project="NERVE",
            title="Record an actionable problem",
            description="Create a follow-up task when the agent finds it.",
            ready_for_agent=True,
        )

    forum.create_thread.assert_not_awaited()
