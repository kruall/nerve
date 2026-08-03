"""Tests for single-flight autonomous Discord project-task execution."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.channels.discord_project_task_runner import DiscordProjectTaskRunner
from nerve.config import NerveConfig

GUILD_ID = 1
FORUM_ID = 10
USER_ID = 20
READY_TAG_ID = 30
OTHER_TAG_ID = 31
READY_USER_TAG_ID = 32


class _Thread:
    def __init__(self, thread_id: int, *, tag_id: int = READY_TAG_ID) -> None:
        self.id = thread_id
        self.parent_id = FORUM_ID
        self.name = f"task-{thread_id}"
        self.applied_tags = [SimpleNamespace(id=tag_id), SimpleNamespace(id=99)]

    async def history(self, **_kwargs):
        yield SimpleNamespace(
            id=self.id,
            content="Implement the selected task.",
            author=SimpleNamespace(id=USER_ID, display_name="kruall"),
        )


def _runner(threads: list[_Thread]) -> tuple[DiscordProjectTaskRunner, MagicMock]:
    config = NerveConfig.from_dict({
        "agent": {"backend": "claude"},
        "discord": {
            "enabled": True,
            "bot_token": "synthetic-token",
            "guild_id": GUILD_ID,
            "task_forums": {"NERVE": FORUM_ID},
            "allowed_author_ids": [USER_ID],
            "project_task_runner_enabled": True,
        },
    })
    forum = SimpleNamespace(available_tags=[
        SimpleNamespace(id=READY_TAG_ID, name="ready-for-agent"),
        SimpleNamespace(id=OTHER_TAG_ID, name="in-progress"),
        SimpleNamespace(id=99, name="priority"),
    ])
    guild = MagicMock()
    guild.id = GUILD_ID
    guild.active_threads = AsyncMock(return_value=threads)
    guild.get_channel.return_value = forum

    db = MagicMock()
    db.get_channel_session = AsyncMock(return_value=None)
    db.bind_discord_session = AsyncMock()
    sessions = MagicMock()
    sessions.is_running.return_value = False
    sessions.get_or_create = AsyncMock()
    sessions.set_active_session = AsyncMock()
    router = MagicMock()
    router.engine.sessions = sessions
    router.handle_message = AsyncMock()
    runner = DiscordProjectTaskRunner(
        config=config,
        router=router,
        db=db,
        project_forums={FORUM_ID: "NERVE"},
        project_prompt=lambda _forum, _thread: "Project instructions.",
    )
    router.handle_message.side_effect = ["1. Inspect the code.\n2. Implement it.", "Done."]
    return runner, guild


@pytest.mark.asyncio
async def test_scans_oldest_ready_task_claims_it_then_dispatches(monkeypatch):
    newer = _Thread(200)
    older = _Thread(100)
    runner, guild = _runner([newer, older])
    transition = MagicMock(return_value={"current_status": "in-progress"})
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        transition,
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    transition.assert_called_once_with(
        runner.nerve_config,
        thread_id=older.id,
        target_status="in-progress",
        audit_reason="Nerve autonomous project-task runner",
    )
    planning_binding, execution_binding = (
        runner.db.bind_discord_session.await_args_list
    )
    assert planning_binding.args == ("discord-task-plan:1:100",)
    assert planning_binding.kwargs == {
        "guild_id": GUILD_ID, "thread_id": older.id,
    }
    assert execution_binding.args == ("discord-task:1:100",)
    assert execution_binding.kwargs == {
        "guild_id": GUILD_ID, "thread_id": older.id,
    }
    planning_message, execution_message = [
        call.args[0] for call in runner.router.handle_message.await_args_list
    ]
    assert planning_message.session_id == "discord-task-plan:1:100"
    assert "Project instructions." in planning_message.text
    assert "Implement the selected task." in planning_message.text
    assert (
        "Produce a concrete, self-contained implementation plan"
        in planning_message.text
    )
    assert execution_message.session_id == "discord-task:1:100"
    assert "[Planner handoff]" in execution_message.text
    assert "1. Inspect the code." in execution_message.text
    assert "already been moved to `in-progress`" in execution_message.text


@pytest.mark.asyncio
async def test_planning_uses_project_tier_but_execution_uses_global_default(
    monkeypatch,
):
    runner, guild = _runner([_Thread(100)])
    runner.nerve_config.agent.backend = "codex"
    runner.nerve_config.discord.project_model_tiers = {"NERVE": "terra-high"}
    runner.nerve_config.discord.project_planner_model_tiers = {
        "NERVE": "sol-xhigh",
    }
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    planning_call, execution_call = (
        runner.router.engine.sessions.get_or_create.await_args_list
    )
    assert planning_call.args[0] == "discord-task-plan:1:100"
    assert planning_call.kwargs["model"] == "gpt-5.6-sol"
    assert planning_call.kwargs["model_tier"] == "sol-xhigh"
    assert planning_call.kwargs["reasoning_effort"] == "xhigh"
    assert (
        planning_call.kwargs["metadata"]["discord_task_stage"] == "planning"
    )
    assert execution_call.args[0] == "discord-task:1:100"
    assert "model" not in execution_call.kwargs
    assert (
        execution_call.kwargs["metadata"]["discord_task_stage"]
        == "implementation"
    )


def test_planner_uses_legacy_project_tier_without_override():
    runner, _guild = _runner([])
    runner.nerve_config.agent.backend = "codex"
    runner.nerve_config.discord.project_model_tiers = {"NERVE": "terra-high"}

    assert runner._planning_model_args("NERVE") == {
        "model": "gpt-5.6-terra",
        "model_tier": "terra-high",
        "reasoning_effort": "high",
    }


@pytest.mark.asyncio
async def test_project_task_policy_reaches_planner_and_implementation(monkeypatch):
    runner, guild = _runner([_Thread(100)])
    runner.nerve_config.discord.project_task_runner_instructions = {
        "NERVE": "Integrate into the local live branch before ready-for-user.",
    }
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    planner_message, implementation_message = [
        call.args[0] for call in runner.router.handle_message.await_args_list
    ]
    policy = "[Trusted autonomous task policy for NERVE]"
    requirement = "Integrate into the local live branch before ready-for-user."
    assert policy in planner_message.text
    assert requirement in planner_message.text
    assert policy in implementation_message.text
    assert requirement in implementation_message.text


@pytest.mark.asyncio
async def test_project_task_policy_is_omitted_when_not_configured(monkeypatch):
    runner, guild = _runner([_Thread(100)])
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    for call in runner.router.handle_message.await_args_list:
        assert "[Trusted autonomous task policy for NERVE]" not in call.args[0].text


@pytest.mark.asyncio
async def test_empty_planner_response_does_not_start_execution(monkeypatch):
    runner, guild = _runner([_Thread(100)])
    runner.router.handle_message.side_effect = [""]
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    assert runner.router.handle_message.await_count == 1
    assert runner.router.engine.sessions.get_or_create.await_count == 1


@pytest.mark.asyncio
async def test_only_one_task_session_is_active_across_all_project_forums(
    monkeypatch,
):
    runner, guild = _runner([_Thread(100), _Thread(200)])
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def hold(_message):
        started.set()
        await release.wait()

    runner.router.handle_message.side_effect = hold
    assert await runner.scan_once(guild) is True
    await started.wait()
    assert await runner.scan_once(guild) is False
    assert runner.router.handle_message.await_count == 1

    release.set()
    active = runner._active_task
    assert active is not None
    await active


@pytest.mark.asyncio
async def test_failed_claim_does_not_dispatch_a_task(monkeypatch):
    runner, guild = _runner([_Thread(100)])

    def fail(*_args, **_kwargs):
        raise RuntimeError("Discord unavailable")

    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        fail,
    )
    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    runner.router.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_stop_cancels_the_current_task_dispatch(monkeypatch):
    runner, guild = _runner([_Thread(100)])
    monkeypatch.setattr(
        "nerve.channels.discord_project_task_runner.transition_project_task_status",
        lambda *_args, **_kwargs: {"current_status": "in-progress"},
    )
    started = asyncio.Event()

    async def hold(_message):
        started.set()
        await asyncio.Event().wait()

    runner.router.handle_message.side_effect = hold
    assert await runner.scan_once(guild) is True
    await started.wait()
    await runner.stop()

    assert runner.active_session_id is None
    assert runner._active_task is None


def _recovery_runner(
    *,
    stage: str = "implementation",
    mapped_session_id: str | None = None,
    plan: str | None = None,
    fetched_thread: _Thread | None = None,
) -> tuple[DiscordProjectTaskRunner, MagicMock, _Thread, MagicMock]:
    runner, guild = _runner([_Thread(100, tag_id=OTHER_TAG_ID), _Thread(200)])
    thread = guild.active_threads.return_value[0]
    guild.fetch_channel = AsyncMock(return_value=fetched_thread)
    guild.get_channel.return_value.available_tags.append(
        SimpleNamespace(id=READY_USER_TAG_ID, name="ready-for-user"),
    )
    session_id = mapped_session_id or f"discord-task:{GUILD_ID}:{thread.id}"
    metadata = {"discord_task_stage": stage}
    session = {
        "id": session_id,
        "source": "discord",
        "status": "idle",
        "metadata": metadata,
    }
    runner.db.get_channel_session = AsyncMock(
        return_value=(
            {"session_id": session_id} if mapped_session_id is not None else None
        ),
    )
    sessions = runner.router.engine.sessions
    sessions.is_running.return_value = False
    sessions._running_tasks = {}
    runner.router.engine._restart_recovery_sessions = set()
    runner.router.engine._pending_model_tier_continuations = {}
    runner.router.engine.run = AsyncMock(return_value="resumed")
    runner.router.engine.sessions.register_task = MagicMock()
    runner.db.get_session = AsyncMock(
        side_effect=lambda candidate: session if candidate == session_id else None,
    )
    runner.db.get_discord_session_binding = AsyncMock(return_value={
        "session_id": session_id,
        "guild_id": str(GUILD_ID),
        "thread_id": str(thread.id),
    })
    runner.db.get_session_run_recovery = AsyncMock(return_value=None)
    runner.db.list_pending_wakeups = AsyncMock(return_value=[])
    runner.db.list_pending_long_command_resumes = AsyncMock(return_value=[])
    runner.db.list_running_long_commands = AsyncMock(return_value=[])
    planning_id = f"discord-task-plan:{GUILD_ID}:{thread.id}"
    runner.db.get_messages = AsyncMock(return_value=(
        [{"role": "assistant", "content": plan}] if plan else []
    ))
    return runner, guild, thread, sessions


@pytest.mark.asyncio
async def test_in_progress_wakes_the_same_idle_implementation_session():
    runner, guild, _thread, sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    runner.router.engine.run.assert_awaited_once()
    call = runner.router.engine.run.await_args.kwargs
    assert call["session_id"] == "discord-task:1:100"
    assert call["source"] == "wakeup"
    assert call["internal"] is True
    sessions.register_task.assert_called_once()


@pytest.mark.asyncio
async def test_running_or_pending_continuation_is_not_duplicated():
    runner, guild, _thread, sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    sessions.is_running.return_value = True

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task
    runner.router.engine.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_in_progress_blocks_ready_task_when_recovery_is_impossible():
    runner, guild, _thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    runner.db.get_discord_session_binding = AsyncMock(return_value=None)

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task
    runner.router.handle_message.assert_not_awaited()
    assert runner.router.engine.run.await_count == 0


@pytest.mark.asyncio
async def test_missing_or_conflicting_binding_never_creates_parallel_session():
    runner, guild, _thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    runner.db.get_discord_session_binding = AsyncMock(return_value={
        "session_id": "discord-task:1:100",
        "guild_id": "999",
        "thread_id": "999",
    })

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task
    runner.router.engine.sessions.get_or_create.assert_not_awaited()
    runner.router.engine.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_unrecoverable_in_progress_task_offers_one_release_action():
    runner, guild, thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    runner.db.get_discord_session_binding = AsyncMock(return_value=None)
    runner.db.get_notification = AsyncMock(return_value=None)
    service = MagicMock()
    service.propose_action = AsyncMock(return_value={
        "notification_id": "discord-task-recovery:1:100",
        "status": "sent",
    })
    runner.notification_service = service

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    service.propose_action.assert_awaited_once()
    call = service.propose_action.await_args.kwargs
    assert call["notification_id"] == "discord-task-recovery:1:100"
    assert call["target_id"] == str(thread.id)
    assert [option["value"] for option in call["options"]] == [
        "cancelled", "backlog", "ready-for-user",
    ]


@pytest.mark.asyncio
async def test_pending_release_action_is_not_duplicated_on_next_poll():
    runner, guild, _thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    runner.db.get_discord_session_binding = AsyncMock(return_value=None)
    runner.db.get_notification = AsyncMock(return_value={
        "id": "discord-task-recovery:1:100",
        "status": "pending",
    })
    service = MagicMock()
    service.propose_action = AsyncMock()
    runner.notification_service = service

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    service.propose_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_saved_planning_output_hands_off_without_new_planner():
    runner, guild, thread, _sessions = _recovery_runner(
        stage="planning",
        mapped_session_id="discord-task-plan:1:100",
        plan="1. Inspect the repository.\n2. Implement the fix.",
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    runner.router.engine.run.assert_awaited_once()
    call = runner.router.engine.run.await_args.kwargs
    assert call["session_id"] == f"discord-task:{GUILD_ID}:{thread.id}"
    assert "Implement the fix." in call["user_message"]
    assert "source" in call and call["source"] == "wakeup"
    assert runner.router.handle_message.await_count == 0


@pytest.mark.asyncio
async def test_incomplete_planning_session_is_woken_in_place():
    runner, guild, _thread, _sessions = _recovery_runner(
        stage="planning",
        mapped_session_id="discord-task-plan:1:100",
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task

    runner.router.engine.run.assert_awaited_once()
    call = runner.router.engine.run.await_args.kwargs
    assert call["session_id"] == "discord-task-plan:1:100"
    assert "planning recovery" in call["user_message"]
    assert "create another planner" in call["user_message"]


@pytest.mark.asyncio
async def test_tag_change_to_ready_for_user_stops_repeated_wakeups():
    changed = _Thread(100, tag_id=READY_USER_TAG_ID)
    runner, guild, _thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
        fetched_thread=changed,
    )

    assert await runner.scan_once(guild) is True
    task = runner._active_task
    assert task is not None
    await task
    guild.active_threads.return_value = [changed]
    assert await runner.scan_once(guild) is False
    assert runner.router.engine.run.await_count == 1


@pytest.mark.asyncio
async def test_stop_cancels_internal_recovery_continuation():
    runner, guild, _thread, _sessions = _recovery_runner(
        mapped_session_id="discord-task:1:100",
    )
    entered = asyncio.Event()

    async def hold(**_kwargs):
        entered.set()
        await asyncio.Event().wait()

    runner.router.engine.run.side_effect = hold
    assert await runner.scan_once(guild) is True
    await entered.wait()
    await runner.stop()

    assert runner.active_session_id is None
    assert runner._active_task is None
