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


class _Tag:
    def __init__(self, tag_id: int, name: str):
        self.id = tag_id
        self.name = name


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


def _thread(*, archived: bool = False, applied_tags=None, thread_id=THREAD_ID):
    thread = MagicMock(spec=discord.Thread)
    thread.id = thread_id
    thread.parent_id = FORUM_ID
    thread.name = "Approvals"
    thread.archived = archived
    thread.applied_tags = list(applied_tags or [])
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
    interaction.response.edit_message = AsyncMock()
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
async def test_start_tags_existing_approval_thread_as_user_inbox():
    inbox_tag = _Tag(250, "user-inbox")
    thread = _thread()
    inbox = _inbox()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = [inbox_tag]
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[thread])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock()

    await inbox.start(guild)

    forum.create_thread.assert_not_awaited()
    assert thread.edit.await_args.kwargs == {
        "archived": False,
        "pinned": True,
        "reason": "Pin Nerve approval inbox",
        "applied_tags": [inbox_tag],
    }


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
            "plan_summary": "Implement the requested behavior and cover it with tests.",
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
        "nerve:approval:approval-1:show_plan",
        "nerve:approval:approval-1:describe",
    ]
    card = thread.send.await_args.kwargs["embed"]
    assert card.title == "⚠️ Review this"
    assert "Details" not in (card.description or "")
    assert "Show plan" in (card.description or "")
    assert "Describe" in (card.description or "")
    assert card.footer.text == "plan:plan-1"
    encoded = inbox.db.update_notification.await_args.kwargs["metadata"]
    assert json.loads(encoded)["discord_approval"] == {
        "thread_id": str(THREAD_ID),
        "message_id": str(MESSAGE_ID),
    }


@pytest.mark.asyncio
async def test_task_thread_copy_persists_separate_coordinates():
    thread = _thread()
    message = MagicMock(spec=discord.Message)
    message.id = MESSAGE_ID
    thread.send.return_value = message
    inbox = _inbox(thread=thread)
    row = {
        "id": "approval-task-complete",
        "title": "Complete and archive project task",
        "body": "Confirm task closure.",
        "target_kind": "discord-project-task-completion",
        "target_id": "200",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "option_labels": {
                "approve": "Complete & archive",
                "decline": "Keep task open",
            },
        }),
    }

    await inbox.deliver_to_thread(
        row, thread, metadata_key="discord_project_task_completion",
    )

    metadata = json.loads(inbox.db.update_notification.await_args.kwargs["metadata"])
    assert metadata["discord_project_task_completion"] == {
        "thread_id": str(THREAD_ID),
        "message_id": str(MESSAGE_ID),
    }
    footer = thread.send.await_args.kwargs["embed"].footer.text or ""
    assert "discord-project-task-completion:200" not in footer


@pytest.mark.asyncio
async def test_long_plan_posts_only_compact_action_card():
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
        "metadata": json.dumps({
            "option_labels": {},
            "plan_summary": "Implement the requested behavior and verify it.",
        }),
    }

    await inbox.deliver(row)

    calls = thread.send.await_args_list
    assert len(calls) == 1
    assert isinstance(calls[0].kwargs["view"], ApprovalView)
    card = calls[0].kwargs["embed"]
    assert "step" not in (card.description or "")
    assert "Show plan" in (card.description or "")
    assert "Describe" in (card.description or "")


@pytest.mark.asyncio
async def test_restart_restores_pending_view_by_message_id():
    inbox = _inbox(thread=_thread())
    inbox.db.list_notifications.return_value = [{
        "id": "approval-1",
        "body": "Plan body",
        "target_kind": "plan",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "option_labels": {"approve": "Approve", "decline": "Decline"},
            "plan_summary": "A concise plan overview.",
            "discord_approval": {
                "thread_id": str(THREAD_ID),
                "message_id": str(MESSAGE_ID),
            },
        }),
    }]

    await inbox._restore_pending_views()

    view = inbox.client.add_view.call_args.args[0]
    assert isinstance(view, ApprovalView)
    assert view.children[-2].custom_id == (
        "nerve:approval:approval-1:show_plan"
    )
    assert view.children[-1].custom_id == (
        "nerve:approval:approval-1:describe"
    )
    assert inbox.client.add_view.call_args.kwargs["message_id"] == MESSAGE_ID


@pytest.mark.asyncio
async def test_restart_restores_views_for_audit_and_task_thread_copies():
    inbox = _inbox(thread=_thread())
    inbox.db.list_notifications.return_value = [{
        "id": "approval-1",
        "body": "Close task",
        "target_kind": "discord-project-task-completion",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "option_labels": {"approve": "Complete", "decline": "Keep open"},
            "discord_approval": {"thread_id": "201", "message_id": "300"},
            "discord_project_task_completion": {
                "thread_id": "202", "message_id": "301",
            },
        }),
    }]

    await inbox._restore_pending_views()

    assert inbox.client.add_view.call_count == 2
    assert [call.kwargs["message_id"] for call in inbox.client.add_view.call_args_list] == [
        300, 301,
    ]


@pytest.mark.asyncio
async def test_show_plan_button_sends_ephemeral_chunks():
    thread = _thread()
    inbox = _inbox(thread=thread)
    inbox.db.get_notification.return_value = {
        "title": "Review long plan",
        "body": "step\n" * 900,
    }
    view = ApprovalView(
        inbox,
        "approval-1",
        ["approve"],
        {"approve": "Approve"},
        show_plan=True,
    )
    interaction = _interaction()

    await view.children[-1].callback(interaction)

    interaction.response.defer.assert_awaited_once_with(ephemeral=True)
    calls = interaction.followup.send.await_args_list
    assert len(calls) >= 3
    assert all(call.kwargs["ephemeral"] is True for call in calls)
    assert all(len(call.args[0]) <= 2000 for call in calls)
    assert "Review long plan" in calls[0].args[0]
    thread.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_describe_button_sends_ephemeral_summary():
    inbox = _inbox(thread=_thread())
    inbox.db.get_notification.return_value = {
        "metadata": json.dumps({
            "plan_summary": "This adds a concise plan overview to approvals.",
        }),
    }
    view = ApprovalView(
        inbox,
        "approval-1",
        ["approve"],
        {"approve": "Approve"},
        show_describe=True,
    )
    interaction = _interaction()

    await view.children[-1].callback(interaction)

    interaction.response.send_message.assert_awaited_once()
    args = interaction.response.send_message.await_args
    assert args.args == (
        "**Plan overview**\n\nThis adds a concise plan overview to approvals.",
    )
    assert args.kwargs["ephemeral"] is True
    assert args.kwargs["allowed_mentions"].everyone is False
    assert args.kwargs["allowed_mentions"].users is False
    assert args.kwargs["allowed_mentions"].roles is False


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
async def test_task_completion_decline_requires_a_reason():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(thread=_thread())
    view = ApprovalView(
        inbox,
        "approval-1",
        ["decline"],
        {"decline": "Keep task open"},
        target_kind="discord-project-task-completion",
    )
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    modal = interaction.response.send_modal.await_args.args[0]
    assert isinstance(modal, ApprovalFeedbackModal)
    assert modal.feedback.required is True
    assert "remain open" in modal.feedback.placeholder
    assert modal.suppress_ephemeral_outcome is True


@pytest.mark.asyncio
async def test_task_completion_approve_edits_card_without_ephemeral_reply():
    source = MagicMock(spec=discord.Message)
    source.id = MESSAGE_ID
    source.content = ""
    source.embeds = [discord.Embed(title="Complete task")]
    source.embeds[0].add_field(
        name="Status", value="⏳ Completing", inline=False,
    )
    source.edit = AsyncMock()
    inbox = _inbox(thread=_thread())
    inbox.db.get_notification.return_value = {
        "id": "approval-1",
        "target_kind": "discord-project-task-completion",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "approval_dispatch": {"ok": True, "error": ""},
            "discord_project_task_completion": {
                "thread_id": str(THREAD_ID),
                "message_id": str(MESSAGE_ID),
            },
        }),
    }
    view = ApprovalView(
        inbox,
        "approval-1",
        ["approve"],
        {"approve": "Complete & archive"},
        target_kind="discord-project-task-completion",
    )
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    interaction.response.edit_message.assert_awaited_once()
    edited = interaction.response.edit_message.await_args.kwargs
    assert edited["view"] is None
    assert [(field.name, field.value) for field in edited["embed"].fields] == [
        ("Status", "✅ Completed"),
    ]
    interaction.response.defer.assert_not_awaited()
    interaction.followup.send.assert_not_awaited()
    source.edit.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_completion_decline_modal_has_no_ephemeral_outcome():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(thread=_thread())
    inbox.answer = AsyncMock(return_value=True)
    modal = ApprovalFeedbackModal(
        inbox,
        "approval-1",
        "decline",
        source,
        feedback_required=True,
        suppress_ephemeral_outcome=True,
    )
    modal.feedback._value = "Keep it open"
    interaction = _interaction(source)

    await modal.on_submit(interaction)

    interaction.response.defer.assert_awaited_once_with()
    interaction.followup.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_completion_outcome_updates_audit_copy_and_leaves_archived_task_copy(
):
    task_thread = _thread(archived=True)
    audit_thread = _thread(thread_id=THREAD_ID + 1)
    source = MagicMock(spec=discord.Message)
    source.id = MESSAGE_ID + 1
    source.content = ""
    source.embeds = [discord.Embed(title="Complete task")]
    source.channel = audit_thread
    source.edit = AsyncMock()
    task_card = MagicMock(spec=discord.Message)
    task_card.id = MESSAGE_ID
    task_card.content = ""
    task_card.embeds = [discord.Embed(title="Complete task")]
    response = SimpleNamespace(status=400, reason="Bad Request", headers={})
    task_card.edit = AsyncMock(side_effect=discord.HTTPException(
        response,
        {"code": 50083, "message": "Thread is archived"},
    ))
    task_card.channel = task_thread
    task_thread.fetch_message = AsyncMock(return_value=task_card)
    inbox = _inbox(thread=audit_thread)
    inbox.client.get_channel.side_effect = {
        task_thread.id: task_thread,
        audit_thread.id: audit_thread,
    }.get
    inbox.db.get_notification.return_value = {
        "id": "approval-1",
        "target_kind": "discord-project-task-completion",
        "target_id": str(THREAD_ID),
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "approval_dispatch": {"ok": True, "error": ""},
            "discord_project_task_completion": {
                "thread_id": str(THREAD_ID),
                "message_id": str(MESSAGE_ID),
            },
            "discord_approval": {
                "thread_id": str(audit_thread.id),
                "message_id": str(source.id),
            },
        }),
    }

    assert await inbox.answer(
        interaction=_interaction(source),
        notification_id="approval-1",
        decision="approve",
        feedback="",
        source_message=source,
    )

    edited = source.edit.await_args.kwargs
    assert [(field.name, field.value) for field in edited["embed"].fields] == [
        ("Status", "✅ Completed"),
    ]
    assert edited["view"] is None
    task_card.edit.assert_awaited_once()
    task_thread.edit.assert_not_awaited()


@pytest.mark.asyncio
async def test_task_completion_failure_records_reason_on_both_embed_cards():
    source = MagicMock(spec=discord.Message)
    source.id = MESSAGE_ID
    source.content = ""
    source.embeds = [discord.Embed(title="Complete task")]
    source.edit = AsyncMock()
    approval_copy = MagicMock(spec=discord.Message)
    approval_copy.id = MESSAGE_ID + 1
    approval_copy.content = ""
    approval_copy.embeds = [discord.Embed(title="Complete task")]
    approval_copy.edit = AsyncMock()
    approval_thread = _thread()
    approval_thread.fetch_message = AsyncMock(return_value=approval_copy)
    inbox = _inbox(thread=approval_thread)
    inbox.client.get_channel.return_value = approval_thread
    inbox.db.get_notification.return_value = {
        "id": "approval-1",
        "target_kind": "discord-project-task-completion",
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "approval_dispatch": {
                "ok": False,
                "error": "task thread was already archived",
            },
            "discord_project_task_completion": {
                "thread_id": str(THREAD_ID),
                "message_id": str(MESSAGE_ID),
            },
            "discord_approval": {
                "thread_id": str(THREAD_ID + 1),
                "message_id": str(MESSAGE_ID + 1),
            },
        }),
    }

    assert await inbox.answer(
        interaction=_interaction(source),
        notification_id="approval-1",
        decision="approve",
        feedback="",
        source_message=source,
    )

    for message in (source, approval_copy):
        status = message.edit.await_args.kwargs["embed"].fields[0]
        assert status.name == "Status"
        assert "Not completed: task thread was already archived" in status.value
        assert message.edit.await_args.kwargs["view"] is None


@pytest.mark.asyncio
async def test_successful_archived_task_completion_card_is_not_reopened():
    source = MagicMock(spec=discord.Message)
    source.id = MESSAGE_ID
    source.content = ""
    source.embeds = [discord.Embed(title="Complete task")]
    source.channel = _thread(archived=True)
    response = SimpleNamespace(
        status=400,
        reason="Bad Request",
        headers={},
    )
    archived_error = discord.HTTPException(
        response,
        {"code": 50083, "message": "Thread is archived"},
    )
    source.edit = AsyncMock(side_effect=archived_error)
    inbox = _inbox(thread=source.channel)
    inbox.db.get_notification.return_value = {
        "id": "approval-1",
        "target_kind": "discord-project-task-completion",
        "target_id": str(THREAD_ID),
        "options": json.dumps(["approve", "decline"]),
        "metadata": json.dumps({
            "approval_dispatch": {"ok": True, "error": ""},
        }),
    }

    assert await inbox.answer(
        interaction=_interaction(source),
        notification_id="approval-1",
        decision="approve",
        feedback="",
        source_message=source,
    )

    assert source.edit.await_count == 1
    source.channel.edit.assert_not_awaited()


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
async def test_answer_updates_embed_card_with_decision_and_feedback():
    source = MagicMock(spec=discord.Message)
    source.content = ""
    source.embeds = [discord.Embed(title="Review this")]
    source.edit = AsyncMock()
    inbox = _inbox(thread=_thread())
    inbox.db.get_notification.return_value = {
        "target_kind": "plan",
        "body": "Details",
        "options": json.dumps(["approve", "revise"]),
        "metadata": json.dumps({
            "option_labels": {"revise": "Request changes"},
        }),
    }
    interaction = _interaction(source)

    assert await inbox.answer(
        interaction=interaction,
        notification_id="approval-1",
        decision="revise",
        feedback="Add rollback coverage",
        source_message=source,
    )

    edited = source.edit.await_args.kwargs
    assert "content" not in edited
    assert [(field.name, field.value) for field in edited["embed"].fields] == [
        ("Decision", f"Request changes by <@{USER_ID}>"),
        ("Feedback", "Add rollback coverage"),
    ]
    assert edited["view"] is None


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
    discord_channel.deliver_notification = AsyncMock(return_value="300")
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

    discord_channel.deliver_notification.assert_awaited_once()
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
