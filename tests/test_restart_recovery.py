"""Restart recovery for agent turns interrupted with the daemon."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nerve.agent.engine import AgentEngine
from nerve.channels.base import ChannelCapability
from nerve.config import NerveConfig


def _engine(tmp_path, db) -> AgentEngine:
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path / "workspace"),
        "codex": {"home_dir": str(tmp_path / "codex-home")},
    })
    return AgentEngine(config, db)


def _stub_broadcaster(bc) -> None:
    bc.start_buffering.return_value = None
    bc.stop_buffering.return_value = []
    bc.mark_turn_open.return_value = None
    bc.is_turn_open.return_value = False
    bc.broadcast = AsyncMock()


@pytest.mark.asyncio
async def test_recovery_checkpoint_round_trip_and_clear(db):
    await db.create_session("recover-db", source="discord")
    context = {
        "channel_name": "discord",
        "target": "thread-123",
        "message_id": "message-456",
    }

    await db.set_session_run_recovery(
        "recover-db",
        source="discord",
        channel="discord",
        user_message="finish the task",
        channel_context=context,
    )
    await db.mark_session_run_recovering("recover-db")

    row = await db.get_session_run_recovery("recover-db")
    assert row is not None
    assert row["source"] == "discord"
    assert row["channel"] == "discord"
    assert row["user_message"] == "finish the task"
    assert row["channel_context"] == context
    assert row["recovery_attempts"] == 1
    assert row["last_recovery_at"]

    await db.clear_session_run_recovery("recover-db")
    assert await db.get_session_run_recovery("recover-db") is None


@pytest.mark.asyncio
async def test_active_session_without_native_id_stays_resumable_when_checkpointed(
    db,
):
    engine = AgentEngine(NerveConfig(), db)
    await db.create_session("recover-no-native", source="discord", status="active")
    await db.set_session_run_recovery(
        "recover-no-native",
        source="discord",
        channel="discord",
        user_message="continue me",
        channel_context=None,
    )

    recovered = await engine.sessions.recover_orphaned_sessions()

    assert recovered == 1
    session = await db.get_session("recover-no-native")
    assert session["status"] == "idle"
    events = await db.get_session_events("recover-no-native")
    assert events[0]["details"]["reason"] == "orphan_recovery_pending_run"


@pytest.mark.asyncio
async def test_startup_recovery_restores_discord_target_and_dispatches_continuation(
    tmp_path, db,
):
    engine = _engine(tmp_path, db)
    await db.create_session(
        "recover-discord", source="discord", backend="codex",
    )
    await db.set_session_run_recovery(
        "recover-discord",
        source="discord",
        channel="discord",
        user_message="implement NERVE-5",
        channel_context={
            "channel_name": "discord",
            "target": "thread-123",
            "message_id": "message-456",
        },
    )
    engine.router.register(SimpleNamespace(
        name="discord",
        capabilities=ChannelCapability.SEND_TEXT,
    ))
    engine.run = AsyncMock(return_value="resumed")

    scheduled = await engine.recover_interrupted_runs()
    await asyncio.gather(*engine._restart_recovery_tasks)

    assert scheduled == 1
    context = engine.router.get_message_context("recover-discord")
    assert context == {
        "channel_name": "discord",
        "target": "thread-123",
        "message_id": "message-456",
    }
    kwargs = engine.run.await_args.kwargs
    assert kwargs["session_id"] == "recover-discord"
    assert kwargs["source"] == "discord"
    assert kwargs["channel"] == "discord"
    assert kwargs["internal"] is True
    assert kwargs["_restart_recovery"] is True
    assert "implement NERVE-5" in kwargs["user_message"]
    row = await db.get_session_run_recovery("recover-discord")
    assert row["recovery_attempts"] == 1


@pytest.mark.asyncio
async def test_successful_run_clears_restart_checkpoint(tmp_path, db):
    engine = _engine(tmp_path, db)
    engine._run_inner = AsyncMock(return_value="done")

    with patch("nerve.agent.engine.broadcaster") as bc:
        _stub_broadcaster(bc)
        result = await engine.run(
            "recover-success",
            "do the work",
            source="discord",
            channel="discord",
        )

    assert result == "done"
    assert await db.get_session_run_recovery("recover-success") is None


@pytest.mark.asyncio
async def test_shutdown_cancellation_retains_restart_checkpoint(tmp_path, db):
    engine = _engine(tmp_path, db)
    entered = asyncio.Event()

    async def _wait_forever(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    engine._run_inner = _wait_forever

    with patch("nerve.agent.engine.broadcaster") as bc:
        _stub_broadcaster(bc)
        task = asyncio.create_task(engine.run(
            "recover-cancel",
            "keep going after restart",
            source="discord",
            channel="discord",
        ))
        engine.register_task("recover-cancel", task)
        await entered.wait()
        await engine.shutdown()

    assert task.cancelled()
    row = await db.get_session_run_recovery("recover-cancel")
    assert row is not None
    assert row["user_message"] == "keep going after restart"
