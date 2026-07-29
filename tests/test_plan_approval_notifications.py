"""Plan proposals become actionable approval notifications."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.plans import (
    plan_propose_handler,
    plan_update_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService


async def _task_and_plan(db, tmp_path):
    task_file = tmp_path / "task.md"
    task_file.write_text("# Demo task\n\nBody.\n", encoding="utf-8")
    await db.upsert_task(
        task_id="task-1",
        file_path="task.md",
        title="Demo task",
        status="pending",
        content=task_file.read_text(encoding="utf-8"),
    )
    await db.create_session("planner-1", model="gpt-5.6-terra")
    await db.create_plan(
        plan_id="plan-1",
        task_id="task-1",
        content="1. Implement\n2. Test",
        session_id="planner-1",
        version=1,
        plan_type="generic",
    )


@pytest.mark.asyncio
async def test_plan_propose_creates_review_approval(db, tmp_path):
    task_file = tmp_path / "task.md"
    task_file.write_text("# Demo task\n\nBody.\n", encoding="utf-8")
    await db.upsert_task(
        task_id="task-1",
        file_path="task.md",
        title="Demo task",
        status="pending",
        content=task_file.read_text(encoding="utf-8"),
    )
    await db.create_session("planner-1", model="gpt-5.6-terra")
    notifications = MagicMock()
    notifications.propose_action = AsyncMock(return_value={
        "notification_id": "approval-plan-1",
        "status": "sent",
    })
    cfg = NerveConfig.from_dict({
        "discord": {
            "enabled": True,
            "bot_token": "synthetic",
            "guild_id": 100,
            "audit_forum_id": 200,
        },
    })
    ctx = ToolContext(
        session_id="planner-1",
        workspace=tmp_path,
        db=db,
        config=cfg,
        notification_service=notifications,
    )

    result = await plan_propose_handler(ctx, {
        "task_id": "task-1",
        "content": "1. Implement\n2. Test",
        "summary": "Implement the requested behavior and verify it with tests.",
    })

    kwargs = notifications.propose_action.await_args.kwargs
    assert kwargs["target_kind"] == "plan"
    assert kwargs["target_id"].startswith("plan-")
    assert kwargs["title"].startswith("Review plan v1: Demo task")
    assert kwargs["options"] == [
        {"label": "Approve & implement", "value": "approve"},
        {"label": "Request changes", "value": "revise"},
        {"label": "Decline", "value": "decline"},
    ]
    assert kwargs["channels"] == ["discord"]
    assert kwargs["metadata"]["plan_summary"] == (
        "Implement the requested behavior and verify it with tests."
    )
    plan = await db.get_plan(kwargs["target_id"])
    assert plan["model"] == "gpt-5.6-terra"
    assert "Approval requested: approval-plan-1" in result.content[0]["text"]


@pytest.mark.asyncio
async def test_revised_plan_creates_fresh_review_approval(db, tmp_path):
    await _task_and_plan(db, tmp_path)
    notifications = MagicMock()
    notifications.propose_action = AsyncMock(return_value={
        "notification_id": "approval-plan-2",
        "status": "sent",
    })
    cfg = NerveConfig.from_dict({
        "discord": {
            "enabled": True,
            "bot_token": "synthetic",
            "guild_id": 100,
            "audit_forum_id": 200,
        },
    })
    ctx = ToolContext(
        session_id="planner-1",
        workspace=tmp_path,
        db=db,
        config=cfg,
        notification_service=notifications,
    )

    result = await plan_update_handler(ctx, {
        "plan_id": "plan-1",
        "content": "1. Revised implementation\n2. Rollback tests",
        "summary": "Add rollback coverage to the revised implementation plan.",
        "feedback": "Add rollback coverage",
    })

    kwargs = notifications.propose_action.await_args.kwargs
    assert kwargs["target_kind"] == "plan"
    assert kwargs["target_id"] != "plan-1"
    assert kwargs["metadata"]["plan_version"] == 2
    assert kwargs["metadata"]["plan_summary"] == (
        "Add rollback coverage to the revised implementation plan."
    )
    plan = await db.get_plan(kwargs["target_id"])
    assert plan["model"] == "gpt-5.6-terra"
    assert "Approval requested: approval-plan-2" in result.content[0]["text"]


def _service(db, tmp_path):
    cfg = NerveConfig()
    cfg.workspace = tmp_path
    cfg.notifications = NotificationsConfig(channels=["web"])
    engine = MagicMock()
    engine.sessions.get_or_create = AsyncMock()
    engine.run = AsyncMock()
    service = NotificationService(cfg, db, engine)
    service._append_approval_audit = AsyncMock()
    return service, engine


async def _approval_row(db):
    await db.create_notification(
        notification_id="approval-plan-1",
        session_id="planner-1",
        type="approval",
        title="Review",
        options=["approve", "revise", "decline"],
        metadata={
            "option_labels": {
                "approve": "Approve & implement",
                "revise": "Request changes",
                "decline": "Decline",
            },
        },
        target_kind="plan",
        target_id="plan-1",
    )


@pytest.mark.asyncio
async def test_plan_approval_button_starts_implementation(db, tmp_path):
    await _task_and_plan(db, tmp_path)
    await _approval_row(db)
    service, engine = _service(db, tmp_path)

    assert await service.handle_answer(
        "approval-plan-1", "approve", "discord:400",
    )
    await asyncio.sleep(0)

    plan = await db.get_plan("plan-1")
    notification = await db.get_notification("approval-plan-1")
    assert plan["status"] == "implementing"
    assert plan["impl_session_id"].startswith("impl-")
    assert notification["status"] == "answered"
    engine.sessions.get_or_create.assert_awaited()


@pytest.mark.asyncio
async def test_plan_revision_button_persists_modal_feedback(db, tmp_path):
    await _task_and_plan(db, tmp_path)
    await _approval_row(db)
    service, engine = _service(db, tmp_path)

    assert await service.handle_answer(
        "approval-plan-1",
        "revise",
        "discord:400",
        feedback="Add rollback and restart tests",
    )
    await asyncio.sleep(0)

    plan = await db.get_plan("plan-1")
    notification = await db.get_notification("approval-plan-1")
    metadata = json.loads(notification["metadata"])
    assert plan["status"] == "pending"
    assert plan["feedback"] == "Add rollback and restart tests"
    assert metadata["decision_feedback"] == "Add rollback and restart tests"
    assert notification["status"] == "answered"
    engine.run.assert_awaited()
