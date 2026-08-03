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
    DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
    DiscordProjectTaskStatusError,
    dispatch_discord_project_task_completion,
    dispatch_discord_project_task_recovery,
    resume_project_task,
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
        if method == "POST" and channel_id == THREAD_ID:
            return {"id": "900"}
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


def test_in_progress_task_can_be_returned_to_backlog(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["302"])

    result = transition_project_task_status(
        _config(),
        thread_id=THREAD_ID,
        target_status="backlog",
        audit_reason="recovery",
    )

    assert result["previous_status"] == "in-progress"
    assert result["current_status"] == "backlog"
    assert calls[-1][2] == {"applied_tags": ["300"]}


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
async def test_completed_status_is_rejected_for_agents():
    engine = MagicMock()
    engine.get_active_channel.return_value = "discord"
    engine.router.get_message_context.return_value = {
        "channel_name": "discord",
        "target": str(THREAD_ID),
    }

    result = await discord_project_task_status_handler(
        ToolContext(
            session_id="s1", config=_config(), engine=engine,
        ),
        {"status": "completed"},
    )

    assert result.is_error is True
    assert "/close_task" in result.content[0]["text"]


@pytest.mark.asyncio
async def test_ready_for_user_handoff_does_not_create_approval(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["302"])
    engine = MagicMock()
    engine.get_active_channel.return_value = "discord"
    engine.router.get_message_context.return_value = {
        "channel_name": "discord",
        "target": str(THREAD_ID),
    }

    result = await discord_project_task_status_handler(
        ToolContext(
            session_id="s1", config=_config(), engine=engine,
        ),
        {"status": "ready-for-user"},
    )

    assert result.is_error is False
    payload = json.loads(result.content[0]["text"])
    assert payload["current_status"] == "ready-for-user"
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID),
        ("GET", FORUM_ID),
        ("PATCH", THREAD_ID),
    ]


def test_resume_only_reclaims_ready_for_user(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["303"])

    result = resume_project_task(
        _config(), thread_id=THREAD_ID, audit_reason="reply",
    )

    assert result["previous_status"] == "ready-for-user"
    assert result["current_status"] == "in-progress"
    assert calls[-1][2] == {"applied_tags": ["302"]}


def test_resume_does_not_claim_terminal_task(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["304"])

    result = resume_project_task(
        _config(), thread_id=THREAD_ID, audit_reason="mention",
    )

    assert result["status"] == "no_op"
    assert result["current_status"] == "completed"
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID), ("GET", FORUM_ID),
    ]


def test_close_rejects_task_not_ready_for_user(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["301"])

    with pytest.raises(DiscordProjectTaskStatusError, match="only from ready-for-user"):
        from nerve.discord_tags import complete_project_task

        complete_project_task(
            _config(), thread_id=THREAD_ID, audit_reason="close",
        )

    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID), ("GET", FORUM_ID),
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


def test_recovery_action_changes_status_and_mentions_the_actor(monkeypatch):
    calls = _fake_api(monkeypatch, applied=["302"])

    result = dispatch_discord_project_task_recovery(
        {"id": "recovery-1"},
        str(THREAD_ID),
        "ready-for-user",
        _config(),
        answered_by="discord:400",
    )

    assert result.ok is True
    assert [call[:2] for call in calls] == [
        ("GET", THREAD_ID),
        ("GET", FORUM_ID),
        ("PATCH", THREAD_ID),
        ("POST", THREAD_ID),
    ]
    assert calls[-1][2] == {
        "content": "<@400> Task runner claim released: `ready-for-user`.",
        "allowed_mentions": {"users": ["400"]},
    }
    assert result.audit_event["user_mentioned"] is True


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


@pytest.mark.asyncio
async def test_recovery_choice_changes_status_mentions_user_without_model_turn(
    db, monkeypatch,
):
    calls = _fake_api(monkeypatch, applied=["302"])
    config = _config()
    config.notifications = NotificationsConfig(channels=["web"])
    engine = MagicMock()
    service = NotificationService(config, db, engine)
    service._append_approval_audit = AsyncMock()
    await db.create_session("s1")
    await db.create_notification(
        notification_id="recovery-approval",
        session_id="s1",
        type="approval",
        title="Release blocked task",
        options=["cancelled", "backlog", "ready-for-user"],
        target_kind=DISCORD_PROJECT_TASK_RECOVERY_TARGET_KIND,
        target_id=str(THREAD_ID),
    )

    assert await service.handle_answer(
        "recovery-approval", "ready-for-user", "discord:400",
    )

    notification = await db.get_notification("recovery-approval")
    assert notification["status"] == "answered"
    assert calls[-1][0:2] == ("POST", THREAD_ID)
    engine.run.assert_not_called()


@pytest.mark.asyncio
async def test_completion_retires_task_session_and_suppresses_continuation(
    db, monkeypatch,
):
    _fake_api(monkeypatch, applied=["303"])
    config = _config()
    config.notifications = NotificationsConfig(channels=["web"])
    engine = MagicMock()
    service = NotificationService(config, db, engine)
    service._append_approval_audit = AsyncMock()
    service._resume_approval_session = AsyncMock()
    await db.create_session(
        "s1", source="discord", metadata={"discord_task_runner": True},
    )
    await db.add_wakeup("s1", "check task", "2099-01-01T00:00:00+00:00")
    await db.set_session_run_recovery(
        "s1", source="discord", channel="discord", user_message="resume",
        channel_context=None,
    )
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

    session = await db.get_session("s1")
    metadata = json.loads(session["metadata"])
    assert metadata["discord_project_task_terminal"] == "completed"
    assert await db.list_pending_wakeups("s1") == []
    assert await db.get_session_run_recovery("s1") is None
    service._resume_approval_session.assert_not_awaited()
