"""Discord notification and question inboxes."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from nerve.channels.discord_notifications import (
    DiscordNotificationInbox,
    NotificationView,
    QuestionAnswerModal,
    QuestionView,
)
from nerve.config import NerveConfig, NotificationsConfig
from nerve.notifications.service import NotificationService

GUILD_ID = 100
FORUM_ID = 200
NOTIFICATION_THREAD_ID = 201
QUESTION_THREAD_ID = 202
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


def _thread(
    name: str,
    thread_id: int,
    *,
    archived: bool = False,
    applied_tags=None,
):
    thread = MagicMock(spec=discord.Thread)
    thread.id = thread_id
    thread.parent_id = FORUM_ID
    thread.name = name
    thread.archived = archived
    thread.applied_tags = list(applied_tags or [])
    thread.edit = AsyncMock(return_value=thread)
    thread.send = AsyncMock()
    return thread


def _inbox(*, notification_thread=None, question_thread=None):
    client = MagicMock(spec=discord.Client)
    db = MagicMock()
    db.list_notifications = AsyncMock(return_value=[])
    db.update_notification = AsyncMock()
    db.get_notification = AsyncMock()
    service = MagicMock()
    service.handle_answer = AsyncMock(return_value=True)
    service.handle_dismiss = AsyncMock(return_value=True)
    inbox = DiscordNotificationInbox(
        client=client,
        db=db,
        notification_service=service,
        guild_id=GUILD_ID,
        forum_id=FORUM_ID,
        allowed_author_ids={USER_ID},
    )
    if notification_thread is not None:
        inbox._threads["notify"] = notification_thread
    if question_thread is not None:
        inbox._threads["question"] = question_thread
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
async def test_start_creates_unpinned_tagged_notification_and_question_threads():
    notification_thread = _thread(
        "Notifications", NOTIFICATION_THREAD_ID,
    )
    question_thread = _thread("Questions", QUESTION_THREAD_ID)
    inbox_tag = _Tag(250, "user-inbox")
    inbox = _inbox()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = [inbox_tag]
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock(side_effect=[
        SimpleNamespace(thread=notification_thread),
        SimpleNamespace(thread=question_thread),
    ])

    await inbox.start(guild)

    assert [
        call.kwargs["name"]
        for call in forum.create_thread.await_args_list
    ] == ["Notifications", "Questions"]
    assert all(
        call.kwargs["applied_tags"] == [inbox_tag]
        for call in forum.create_thread.await_args_list
    )
    notification_thread.edit.assert_awaited_once()
    question_thread.edit.assert_awaited_once()
    assert notification_thread.edit.await_args.kwargs == {
        "archived": False,
        "pinned": False,
        "reason": "Prepare Nerve notify inbox",
        "applied_tags": [inbox_tag],
    }
    assert question_thread.edit.await_args.kwargs == {
        "archived": False,
        "pinned": False,
        "reason": "Prepare Nerve question inbox",
        "applied_tags": [inbox_tag],
    }
    assert set(inbox._threads) == {"notify", "question"}


@pytest.mark.asyncio
async def test_missing_inbox_tag_does_not_block_thread_creation():
    notification_thread = _thread(
        "Notifications", NOTIFICATION_THREAD_ID,
    )
    question_thread = _thread("Questions", QUESTION_THREAD_ID)
    inbox = _inbox()
    guild = MagicMock(spec=discord.Guild)
    forum = MagicMock(spec=discord.ForumChannel)
    forum.available_tags = []
    guild.get_channel.return_value = forum
    guild.active_threads = AsyncMock(return_value=[])
    forum.archived_threads.return_value = _AsyncRows([])
    forum.create_thread = AsyncMock(side_effect=[
        SimpleNamespace(thread=notification_thread),
        SimpleNamespace(thread=question_thread),
    ])

    await inbox.start(guild)

    assert forum.create_thread.await_count == 2
    assert all(
        "applied_tags" not in call.kwargs
        for call in forum.create_thread.await_args_list
    )
    assert notification_thread.edit.await_args.kwargs["pinned"] is False
    assert question_thread.edit.await_args.kwargs["pinned"] is False


@pytest.mark.asyncio
async def test_one_inbox_failure_does_not_block_the_other():
    inbox = _inbox()
    question_thread = _thread("Questions", QUESTION_THREAD_ID)
    inbox._ensure_thread = AsyncMock(side_effect=[
        RuntimeError("notification thread failed"),
        question_thread,
    ])
    inbox._restore_pending_views = AsyncMock()
    guild = MagicMock(spec=discord.Guild)

    await inbox.start(guild)

    assert [
        call.args[0] for call in inbox._ensure_thread.await_args_list
    ] == ["notify", "question"]
    inbox._restore_pending_views.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_fails_only_when_no_inbox_can_start():
    inbox = _inbox()
    inbox._ensure_thread = AsyncMock(
        side_effect=RuntimeError("thread failed"),
    )
    inbox._restore_pending_views = AsyncMock()
    guild = MagicMock(spec=discord.Guild)

    with pytest.raises(RuntimeError, match="No Discord notification inbox"):
        await inbox.start(guild)

    inbox._restore_pending_views.assert_not_awaited()


@pytest.mark.asyncio
async def test_notification_delivery_has_dismiss_view_and_coordinates():
    thread = _thread("Notifications", NOTIFICATION_THREAD_ID)
    message = MagicMock(spec=discord.Message)
    message.id = MESSAGE_ID
    thread.send.return_value = message
    inbox = _inbox(notification_thread=thread)
    row = {
        "id": "notif-1",
        "session_id": "session-1",
        "type": "notify",
        "title": "Build complete",
        "body": "All checks passed",
        "priority": "normal",
        "metadata": json.dumps({}),
    }

    result = await inbox.deliver(row)

    assert result == str(MESSAGE_ID)
    sent = thread.send.await_args
    assert "Build complete" in sent.args[0]
    assert isinstance(sent.kwargs["view"], NotificationView)
    assert sent.kwargs["view"].children[0].custom_id == (
        "nerve:notification:notif-1:dismiss"
    )
    assert sent.kwargs["view"].children[0].emoji.name == "✅"
    encoded = inbox.db.update_notification.await_args.kwargs["metadata"]
    assert json.loads(encoded)["discord_notification"] == {
        "thread_id": str(NOTIFICATION_THREAD_ID),
        "message_id": str(MESSAGE_ID),
    }


@pytest.mark.asyncio
async def test_question_delivery_has_options_and_free_form_answer():
    thread = _thread("Questions", QUESTION_THREAD_ID)
    message = MagicMock(spec=discord.Message)
    message.id = MESSAGE_ID
    thread.send.return_value = message
    inbox = _inbox(question_thread=thread)
    row = {
        "id": "ask-1",
        "session_id": "session-1",
        "type": "question",
        "title": "Deploy now?",
        "body": "Choose a rollout window",
        "priority": "high",
        "options": json.dumps(["Now", "Tonight"]),
        "metadata": json.dumps({}),
    }

    await inbox.deliver(row)

    view = thread.send.await_args.kwargs["view"]
    assert isinstance(view, QuestionView)
    assert [item.label for item in view.children] == [
        "Now", "Tonight", "Write answer",
    ]
    assert [item.custom_id for item in view.children] == [
        "nerve:question:ask-1:option:0",
        "nerve:question:ask-1:option:1",
        "nerve:question:ask-1:write",
    ]


@pytest.mark.asyncio
async def test_question_option_answers_and_disables_card():
    thread = _thread("Questions", QUESTION_THREAD_ID)
    source = MagicMock(spec=discord.Message)
    source.content = "**Deploy now?**"
    source.edit = AsyncMock()
    inbox = _inbox(question_thread=thread)
    inbox.db.get_notification.return_value = {
        "options": json.dumps(["Now", "Tonight"]),
    }
    view = QuestionView(inbox, "ask-1", ["Now", "Tonight"])
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    inbox.notification_service.handle_answer.assert_awaited_once_with(
        notification_id="ask-1",
        answer="Now",
        answered_by=f"discord:{USER_ID}",
    )
    edited_view = source.edit.await_args.kwargs["view"]
    assert all(item.disabled for item in edited_view.children)
    assert "**Answer:** Now" in source.edit.await_args.kwargs["content"]


@pytest.mark.asyncio
async def test_write_answer_button_opens_and_submits_modal():
    thread = _thread("Questions", QUESTION_THREAD_ID)
    source = MagicMock(spec=discord.Message)
    source.content = "**Question**"
    source.edit = AsyncMock()
    inbox = _inbox(question_thread=thread)
    inbox.db.get_notification.return_value = {"options": None}
    view = QuestionView(inbox, "ask-1", [])
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    modal = interaction.response.send_modal.await_args.args[0]
    assert isinstance(modal, QuestionAnswerModal)
    modal.answer._value = "Use the canary environment"
    submit_interaction = _interaction(source)
    await modal.on_submit(submit_interaction)

    inbox.notification_service.handle_answer.assert_awaited_once_with(
        notification_id="ask-1",
        answer="Use the canary environment",
        answered_by=f"discord:{USER_ID}",
    )
    submit_interaction.followup.send.assert_awaited_once_with(
        "Answer recorded.", ephemeral=True,
    )


@pytest.mark.asyncio
async def test_notification_dismiss_button_closes_card():
    thread = _thread("Notifications", NOTIFICATION_THREAD_ID)
    source = MagicMock(spec=discord.Message)
    source.content = "**Build complete**"
    source.edit = AsyncMock()
    inbox = _inbox(notification_thread=thread)
    view = NotificationView(inbox, "notif-1")
    interaction = _interaction(source)

    await view.children[0].callback(interaction)

    inbox.notification_service.handle_dismiss.assert_awaited_once_with(
        "notif-1",
    )
    edited = source.edit.await_args.kwargs
    assert edited["view"].children[0].disabled is True
    assert "**Dismissed by:**" in edited["content"]


@pytest.mark.asyncio
async def test_unauthorized_question_answer_is_rejected():
    source = MagicMock(spec=discord.Message)
    inbox = _inbox(question_thread=_thread(
        "Questions", QUESTION_THREAD_ID,
    ))
    view = QuestionView(inbox, "ask-1", ["Yes"])
    interaction = _interaction(source, user_id=999)

    await view.children[0].callback(interaction)

    interaction.response.send_message.assert_awaited_once()
    inbox.notification_service.handle_answer.assert_not_awaited()


@pytest.mark.asyncio
async def test_restart_restores_pending_views_by_message_id():
    inbox = _inbox(
        notification_thread=_thread(
            "Notifications", NOTIFICATION_THREAD_ID,
        ),
        question_thread=_thread("Questions", QUESTION_THREAD_ID),
    )

    async def list_notifications(*, type, **kwargs):
        if type == "notify":
            return [{
                "id": "notif-1",
                "metadata": json.dumps({
                    "discord_notification": {
                        "message_id": "301",
                    },
                }),
            }]
        return [{
            "id": "ask-1",
            "options": json.dumps(["Yes"]),
            "metadata": json.dumps({
                "discord_notification": {
                    "message_id": "302",
                },
            }),
        }]

    inbox.db.list_notifications.side_effect = list_notifications

    await inbox._restore_pending_views()

    calls = inbox.client.add_view.call_args_list
    assert isinstance(calls[0].args[0], NotificationView)
    assert calls[0].kwargs["message_id"] == 301
    assert isinstance(calls[1].args[0], QuestionView)
    assert calls[1].kwargs["message_id"] == 302


@pytest.mark.asyncio
async def test_all_notification_kinds_fan_out_to_discord(db, tmp_path):
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

    notification_id = await service.send_notification(
        session_id="s1",
        title="FYI",
    )
    question = await service.ask_question(
        session_id="s1",
        title="Choose",
        options=["A", "B"],
    )
    approval = await service.propose_action(
        session_id="s1",
        target_kind="plan",
        target_id="plan-1",
        title="Review",
        options=[
            {"label": "Approve", "value": "approve"},
            {"label": "Decline", "value": "decline"},
        ],
    )

    assert discord_channel.deliver_notification.await_count == 3
    for row_id in (
        notification_id,
        question["notification_id"],
        approval["notification_id"],
    ):
        row = await db.get_notification(row_id)
        assert set(json.loads(row["channels_delivered"])) == {
            "web", "discord",
        }
