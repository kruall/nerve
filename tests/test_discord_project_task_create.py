"""Agent tool for creating Discord project-forum tasks."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.discord import discord_project_task_create_handler
from nerve.agent.tools.registry import ToolContext
from nerve.channels.discord_project_tasks import DiscordProjectTaskCreateError
from nerve.config import DiscordConfig, NerveConfig


def _config(*, enabled: bool = True) -> NerveConfig:
    config = NerveConfig()
    config.discord = DiscordConfig(
        enabled=enabled,
        guild_id=1,
        task_forums={"NERVE": 100},
    )
    return config


@pytest.mark.asyncio
async def test_project_task_create_uses_connected_discord_channel():
    channel = MagicMock()
    channel.create_project_task = AsyncMock(return_value=("NERVE-35", 200))
    engine = SimpleNamespace(
        router=SimpleNamespace(get_channel=MagicMock(return_value=channel)),
    )

    result = await discord_project_task_create_handler(
        ToolContext(session_id="s1", config=_config(), engine=engine),
        {
            "project": "NERVE",
            "title": "Record an actionable problem",
            "description": "Create a follow-up task when the agent finds it.",
        },
    )

    assert result.is_error is False
    assert result.content[0]["text"] == (
        "Created Discord project task NERVE-35 in its configured forum."
    )
    channel.create_project_task.assert_awaited_once_with(
        project="NERVE",
        title="Record an actionable problem",
        description="Create a follow-up task when the agent finds it.",
    )


@pytest.mark.asyncio
async def test_project_task_create_requires_a_running_discord_channel():
    engine = SimpleNamespace(
        router=SimpleNamespace(get_channel=MagicMock(return_value=None)),
    )

    result = await discord_project_task_create_handler(
        ToolContext(session_id="s1", config=_config(), engine=engine),
        {"project": "NERVE", "title": "Task", "description": "Details"},
    )

    assert result.is_error is True
    assert result.content[0]["text"] == (
        "discord_project_task_create: Discord channel is unavailable."
    )


@pytest.mark.asyncio
async def test_project_task_create_preserves_safe_discord_validation_error():
    channel = MagicMock()
    channel.create_project_task = AsyncMock(
        side_effect=DiscordProjectTaskCreateError(
            "Укажите непустой заголовок задачи.",
        ),
    )
    engine = SimpleNamespace(
        router=SimpleNamespace(get_channel=MagicMock(return_value=channel)),
    )

    result = await discord_project_task_create_handler(
        ToolContext(session_id="s1", config=_config(), engine=engine),
        {"project": "NERVE", "title": "", "description": "Details"},
    )

    assert result.is_error is True
    assert result.content[0]["text"] == (
        "discord_project_task_create: Укажите непустой заголовок задачи."
    )
