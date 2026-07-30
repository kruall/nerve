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
