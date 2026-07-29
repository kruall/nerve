"""Pinned Discord approval inbox, buttons, and feedback modals."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from nerve.channels.discord_approvals import (
    ApprovalFeedbackModal,
    ApprovalView,
    DiscordApprovalInbox,
)
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService
from nerve.notifications import handlers as notification_handlers

GUILD_ID = 100
FORUM_ID = 200
THREAD_ID = 201
MESSAGE_ID = 300
USER_ID = 400


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


def _thread(*, archived: bool = False):
    thread = MagicMock(spec=discord.Thread)
    thread.id = THREAD_ID
    thread.parent_id = FORUM_ID
    thread.name = "Approvals"
    thread.archived = archived
    thread.edit = AsyncMock(return_value=thread)
    thread.send = AsyncMock()
    return thread


def _inbox(*, thread=None):
    client = MagicMock(spec=discord.Client)
    db = MagicMock()
    db.list_notifications = AsyncMock(return_value=[])
    db.update_notification = AsyncMock()
    db.get_notification = AsyncMock()
    service = MagicMock()
    service.handle_answer = AsyncMock(return_value=True)
    inbox = DiscordApprovalInbox(
        client=client,
        db=db,
        notification_service=service,
        guild_id=GUILD_ID,
        forum_id=FORUM_ID,
        allowed_author_ids={USER_ID},
    )
    inbox._thread = thread
    return inbox


def _interaction(message=None, *, user_id: int = USER_ID):
    interaction = MagicMock(spec=discord.Interaction)
    interaction.guild = SimpleNamespace(id=GUILD_ID)
    interaction.user = SimpleNamespace(id=user_id)
    interaction.message = message
    interaction.response.defer = AsyncMock()
    interaction.response.send_modal = AsyncMock()
    interaction.response.send_message = AsyncMock()
    interaction.response.is_done.return_value = False
    interaction.followup.send = AsyncMock()
    return interaction


@pytest.mark.asyncio
async def test_start_reuses_and_pins_existing_approval_thread():
    thread = _thread()
    inbox = _inbox()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[thread])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock()

    await inbox.start(guild)

    forum.create_thread.assert_not_awaited()
    thread.edit.assert_awaited_once_with(
        archived=False,
        pinned=True,
        reason="Pin Nerve approval inbox",
    )
    assert inbox._thread is thread


@pytest.mark.asyncio
async def test_start_creates_and_pins_missing_approval_thread():
    thread = _thread()
    inbox = _inbox()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock(
        return_value=SimpleNamespace(thread=thread),
    )

    await inbox.start(guild)

    forum.create_thread.assert_awaited_once()
    assert forum.create_thread.await_args.kwargs["name"] == "Approvals"
    thread.edit.assert_awaited_once()
    assert thread.edit.await_args.kwargs["pinned"] is True


@pytest.mark.asyncio
async def test_delivery_posts_persistent_view_and_saves_coordinates():
    thread = _thread()
    message = MagicMock(spec=discord.Message)
    message.id = MESSAGE_ID
    thread.send.return_value = message
    inbox = _inbox(thread=thread)
    row = {
        "id": "approval-1",
        "title": "Review this",
        "body": "Details",
        "priority": "high",
        "target_kind": "plan",
        "target_id": "plan-1",
        "options": json.dumps(["approve", "revise", "decline"]),
        "metadata": json.dumps({
            "option_labels": {
                "approve": "Approve & implement",
                "revise": "Request changes",
                "decline": "Decline",
            },
        }),
    }

    result = await inbox.deliver(row)

    assert result == str(MESSAGE_ID)
    sent_view = thread.send.await_args.kwargs["view"]
    assert isinstance(sent_view, ApprovalView)
    assert sent_view.timeout is None
    assert [item.custom_id for item in sent_view.children] == [
        "nerve:approval:approval-1:approve",
        "nerve:approval:approval-1:revise",
        "nerve:approval:approval-1:decline",
    ]
    encoded = inbox.db.update_notification.await_args.kwargs["metadata"]
    assert json.loads(encoded)["discord_approval"] == {
        "thread_id": str(THREAD_ID),
        "message_id": str(MESSAGE_ID),
    }


@pytest.mark.asyncio
async def test_long_approval_posts_full_details_before_action_card():
    thread = _thread()
    action_message = MagicMock(spec=discord.Message)
    action_message.id = MESSAGE_ID
    thread.send.return_value = action_message
    inbox = _inbox(thread=thread)
    row = {
        "id": "approval-long",
        "title": "Review long plan",
        "body": "step\n" * 900,
        "priority": "high",
        "target_kind": "plan",
        "target_id": "plan-long",
        "options": json.dumps(["approve", "revise", "decline"]),
        "metadata": json.dumps({"option_labels": {}}),
    }

    await inbox.deliver(row)

    calls = thread.send.await_args_list
    assert len(calls) >= 3
    assert all("view" not in call.kwargs for call in calls[:-1])
    assert isinstance(calls[-1].kwargs["view"], ApprovalView)
    assert "Full details are in the messages immediately above" in (
        calls[-1].args[0]
    )


@pytest.mark.asyncio
async def test_restart_restores_pending_view_by_message_id():
    inbox = _inbox(thread=_thread())
    inbox.db.list_notifications.return_value = [{
        "id": "approval-1",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "option_labels": {"approve": "Approve", "decline": "Decline"},
            "discord_approval": {
                "thread_id": str(THREAD_ID),
                "message_id": str(MESSAGE_ID),
            },
        }),
    }]

    await inbox._restore_pending_views()

    view = inbox.client.add_view.call_args.args[0]
    assert isinstance(view, ApprovalView)
    assert inbox.client.add_view.call_args.kwargs["message_id"] == MESSAGE_ID


@pytest.mark.asyncio
async def test_decline_button_opens_feedback_modal_without_dispatching():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(thread=_thread())
    view = ApprovalView(
        inbox,
        "approval-1",
        ["decline"],
        {"decline": "Decline"},
    )
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    modal = interaction.response.send_modal.await_args.args[0]
    assert isinstance(modal, ApprovalFeedbackModal)
    assert modal.decision == "decline"
    inbox.notification_service.handle_answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_revision_modal_submits_required_feedback():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(thread=_thread())
    inbox.answer = AsyncMock(return_value=True)
    modal = ApprovalFeedbackModal(
        inbox, "approval-1", "revise", source,
    )
    modal.feedback._value = "Add rollback tests"
    interaction = _interaction(source)

    await modal.on_submit(interaction)

    inbox.answer.assert_awaited_once_with(
        interaction=interaction,
        notification_id="approval-1",
        decision="revise",
        feedback="Add rollback tests",
        source_message=source,
    )
    interaction.followup.send.assert_awaited_once_with(
        "Decision recorded.", ephemeral=True,
    )


@pytest.mark.asyncio
async def test_unauthorized_approval_click_is_rejected():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(thread=_thread())
    view = ApprovalView(
        inbox,
        "approval-1",
        ["approve"],
        {"approve": "Approve"},
    )
    interaction = _interaction(source, user_id=999)

    await view.children[0].callback(interaction)

    interaction.response.send_message.assert_awaited_once()
    inbox.notification_service.handle_answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_approval_fanout_always_includes_discord_inbox(db, tmp_path):
    cfg = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "notifications": {"channels": ["web"]},
        "discord": {
            "enabled": True,
            "bot_token": "synthetic",
            "guild_id": GUILD_ID,
            "audit_forum_id": FORUM_ID,
        },
    })
    cfg.notifications = NotificationsConfig(channels=["web"])
    engine = MagicMock()
    discord_channel = MagicMock()
    discord_channel.deliver_approval = AsyncMock(return_value="300")
    engine.router.get_channel.return_value = discord_channel
    service = NotificationService(cfg, db, engine)
    service._deliver_web = AsyncMock()
    await db.create_session("s1")

    result = await service.propose_action(
        session_id="s1",
        target_kind="plan",
        target_id="plan-1",
        title="Review",
        options=[
            {"label": "Approve", "value": "approve"},
            {"label": "Request changes", "value": "revise"},
        ],
    )

    discord_channel.deliver_approval.assert_awaited_once()
    row = await db.get_notification(result["notification_id"])
    assert set(json.loads(row["channels_delivered"])) == {"web", "discord"}


def test_mechanical_decline_uses_modal_feedback_as_reason(
    tmp_path, monkeypatch,
):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "mechanical-action.sh"
    script.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    script.chmod(0o755)
    monkeypatch.setenv("NERVE_WORKSPACE_PATH", str(tmp_path))
    completed = SimpleNamespace(returncode=0, stdout="", stderr="")
    run = MagicMock(return_value=completed)
    monkeypatch.setattr(
        notification_handlers.subprocess, "run", run,
    )

    result = notification_handlers._dispatch_mechanical_action(
        {
            "id": "approval-1",
            "metadata": json.dumps({
                "decision_feedback": "The target is obsolete",
            }),
        },
        "action-1",
        "decline",
        None,
    )

    assert result.ok is True
    command = run.call_args.args[0]
    assert command[-2:] == ["--reason", "The target is obsolete"]
