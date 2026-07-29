"""Discord project-task lifecycle tag transitions."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from nerve.agent.tools.handlers.discord import (
    discord_project_task_status_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import DiscordConfig, NerveConfig
from nerve.discord_tags import (
    DiscordProjectTaskStatusError,
    transition_project_task_status,
)


GUILD_ID = 1
FORUM_ID = 100
THREAD_ID = 200
OTHER_TAG_ID = "999"
STATUS_NAMES = [
    "backlog",
    "ready-for-agent",
    "in-progress",
    "ready-for-user",
    "completed",
    "blocked",
    "cancelled",
]


def _config() -> NerveConfig:
    config = NerveConfig()
    config.discord = DiscordConfig(
        enabled=True,
        bot_token="test-token",
        guild_id=GUILD_ID,
        task_forums={"NERVE": FORUM_ID},
    )
    return config


def _forum(tags: list[dict] | None = None) -> dict:
    return {
        "id": str(FORUM_ID),
        "guild_id": str(GUILD_ID),
        "type": 15,
        "name": "nerve",
        "available_tags": tags or [
            {
                "id": str(300 + index),
                "name": name,
                "moderated": False,
                "emoji_id": None,
                "emoji_name": None,
            }
            for index, name in enumerate(STATUS_NAMES)
        ] + [{
            "id": OTHER_TAG_ID,
            "name": "priority",
            "moderated": False,
            "emoji_id": None,
            "emoji_name": None,
        }],
    }


def _thread(applied: list[str]) -> dict:
    return {
        "id": str(THREAD_ID),
        "guild_id": str(GUILD_ID),
        "type": 11,
        "parent_id": str(FORUM_ID),
        "name": "NERVE-25",
        "applied_tags": applied,
    }


def _fake_api(monkeypatch, *, applied: list[str]) -> list[tuple]:
    calls: list[tuple] = []

    def request(config, method, channel_id, payload=None, *, audit_reason=""):
        calls.append((method, channel_id, payload, audit_reason))
        if method == "GET" and channel_id == THREAD_ID:
            return _thread(applied)
        if method == "GET" and channel_id == FORUM_ID:
            return _forum()
        if method == "PATCH" and channel_id == THREAD_ID:
            return _thread(payload["applied_tags"])
        raise AssertionError((method, channel_id, payload))

    monkeypatch.setattr("nerve.discord_tags._discord_request", request)
    return calls


def test_new_task_can_be_triaged_to_ready_for_agent(monkeypatch):
    calls = _fake_api(monkeypatch, applied=[OTHER_TAG_ID])

    result = transition_project_task_status(
        _config(),
        thread_id=THREAD_ID,
        target_status="ready-for-agent",
        audit_reason="test",
    )

    assert result["previous_status"] == "new-task"
    assert result["current_status"] == "ready-for-agent"
    assert calls[-1][2] == {"applied_tags": [OTHER_TAG_ID, "301"]}


def test_transition_replaces_only_the_managed_status_tag(monkeypatch):
    calls = _fake_api(monkeypatch, applied=[OTHER_TAG_ID, "301"])

    result = transition_project_task_status(
        _config(),
        thread_id=THREAD_ID,
        target_status="in-progress",
        audit_reason="test",
    )

    assert result["previous_status"] == "ready-for-agent"
    assert calls[-1][2] == {"applied_tags": [OTHER_TAG_ID, "302"]}


def test_transition_rejects_skipped_or_terminal_paths(monkeypatch):
    _fake_api(monkeypatch, applied=["301"])

    with pytest.raises(DiscordProjectTaskStatusError, match="Invalid project task transition"):
        transition_project_task_status(
            _config(),
            thread_id=THREAD_ID,
            target_status="completed",
            audit_reason="test",
        )


def test_transition_fails_closed_for_multiple_status_tags(monkeypatch):
    _fake_api(monkeypatch, applied=["301", "302"])

    with pytest.raises(DiscordProjectTaskStatusError, match="more than one"):
        transition_project_task_status(
            _config(),
            thread_id=THREAD_ID,
            target_status="in-progress",
            audit_reason="test",
        )


@pytest.mark.asyncio
async def test_tool_uses_the_bound_discord_project_thread(monkeypatch):
    _fake_api(monkeypatch, applied=["301"])
    engine = MagicMock()
    engine.get_active_channel.return_value = "discord"
    engine.router.get_message_context.return_value = {
        "channel_name": "discord",
        "target": str(THREAD_ID),
    }
    result = await discord_project_task_status_handler(
        ToolContext(session_id="s1", config=_config(), engine=engine),
        {"status": "in-progress"},
    )

    assert result.is_error is False
    assert json.loads(result.content[0]["text"])["current_status"] == "in-progress"


@pytest.mark.asyncio
async def test_tool_rejects_non_discord_sessions():
    engine = MagicMock()
    engine.get_active_channel.return_value = "web"
    result = await discord_project_task_status_handler(
        ToolContext(session_id="s1", config=_config(), engine=engine),
        {"status": "in-progress"},
    )

    assert result.is_error is True
    assert "requires a Discord project thread" in result.content[0]["text"]
