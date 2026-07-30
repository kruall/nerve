"""Discord project-task lifecycle tag transitions."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.discord import (
    discord_project_task_status_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import DiscordConfig, NerveConfig, NotificationsConfig
from nerve.discord_tags import (
    DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
    DiscordProjectTaskStatusError,
    dispatch_discord_project_task_completion,
    transition_project_task_status,
)
from nerve.notifications.service import NotificationService


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
            if "applied_tags" in payload:
                return _thread(payload["applied_tags"])
            assert payload == {"archived": True}
            return _thread(applied)
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


@pytest.mark.asyncio
async def test_completed_status_queues_confirmation_instead_of_mutating():
    service = MagicMock()
    service.propose_action = AsyncMock(
        return_value={"notification_id": "approval-complete"},
    )
    engine = MagicMock()
    engine.get_active_channel.return_value = "discord"
    engine.router.get_message_context.return_value = {
        "channel_name": "discord",
        "target": str(THREAD_ID),
    }

    result = await discord_project_task_status_handler(
        ToolContext(
            session_id="s1", config=_config(), engine=engine,
            notification_service=service,
        ),
        {"status": "completed"},
    )

    assert result.is_error is False
    assert "No Discord task state changed" in result.content[0]["text"]
    kwargs = service.propose_action.await_args.kwargs
    assert kwargs["target_kind"] == DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND
    assert kwargs["target_id"] == str(THREAD_ID)
    assert kwargs["options"] == [
        {"label": "Complete & archive", "value": "approve"},
        {"label": "Keep task open", "value": "decline"},
    ]


def test_approved_completion_changes_tag_then_archives(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["303"])

    result = dispatch_discord_project_task_completion(
        {"id": "approval-complete"}, str(THREAD_ID), "approve", _config(),
    )

    assert result.ok is True
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID),
        ("GET", FORUM_ID),
        ("PATCH", THREAD_ID),
        ("PATCH", THREAD_ID),
    ]
    assert calls[2][2] == {"applied_tags": ["304"]}
    assert calls[3][2] == {"archived": True}


def test_declined_completion_does_not_contact_discord(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["303"])

    result = dispatch_discord_project_task_completion(
        {"id": "approval-complete"}, str(THREAD_ID), "decline", _config(),
    )

    assert result.ok is True
    assert calls == []


@pytest.mark.asyncio
async def test_confirmation_click_completes_and_archives_without_model(
    db, monkeypatch,
):
    calls = _fake_api(monkeypatch, applied=["303"])
    config = _config()
    config.notifications = NotificationsConfig(channels=["web"])
    engine = MagicMock()
    service = NotificationService(config, db, engine)
    service._append_approval_audit = AsyncMock()
    await db.create_session("s1")
    await db.create_notification(
        notification_id="approval-complete",
        session_id="s1",
        type="approval",
        title="Complete and archive project task",
        options=["approve", "decline"],
        target_kind=DISCORD_PROJECT_TASK_COMPLETION_TARGET_KIND,
        target_id=str(THREAD_ID),
    )

    assert await service.handle_answer(
        "approval-complete", "approve", "discord:400",
    )

    notification = await db.get_notification("approval-complete")
    assert notification["status"] == "answered"
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID),
        ("GET", FORUM_ID),
        ("PATCH", THREAD_ID),
        ("PATCH", THREAD_ID),
    ]
    engine.run.assert_not_called()
