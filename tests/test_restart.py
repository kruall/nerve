"""Focused coverage for graceful scheduled restarts."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers import build_default_registry
from nerve.config import NerveConfig
from nerve.restart import (
    RestartCoordinator,
    RestartScheduledError,
    _RestartSchedule,
)


class _Sessions:
    def __init__(self, running: set[str]) -> None:
        self.running = set(running)

    def get_running_ids(self) -> set[str]:
        return set(self.running)


class _Engine:
    def __init__(self, running: set[str]) -> None:
        self.sessions = _Sessions(running)
        self.steers: list[tuple[str, str, str | None]] = []
        self.stops: list[str] = []

    async def steer(self, *, session_id: str, user_message: str, channel: str | None) -> bool:
        self.steers.append((session_id, user_message, channel))
        return True

    async def stop_session(self, session_id: str) -> bool:
        self.stops.append(session_id)
        self.sessions.running.discard(session_id)
        return True

    def get_active_channel(self, session_id: str) -> str | None:
        return "discord"


async def _until(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition did not become true")
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_restart_waits_for_active_sessions_then_launches() -> None:
    engine = _Engine({"active"})
    launched = asyncio.Event()

    async def launch() -> None:
        launched.set()

    coordinator = RestartCoordinator(
        engine, SimpleNamespace(config_dir="/tmp"),
        check_interval_seconds=0.01, launcher=launch,
    )
    result = await coordinator.schedule("requester")

    assert result["already_pending"] is False
    assert coordinator.pending is True
    await asyncio.sleep(0.03)
    assert not launched.is_set()

    engine.sessions.running.clear()
    await _until(launched.is_set)
    assert coordinator.pending is True
    assert coordinator._schedule is not None
    assert coordinator._schedule.restart_started is True


@pytest.mark.asyncio
async def test_long_wait_steers_and_honors_wait_then_ready() -> None:
    engine = _Engine({"active"})
    launched = asyncio.Event()

    async def launch() -> None:
        launched.set()

    coordinator = RestartCoordinator(
        engine, SimpleNamespace(config_dir="/tmp"),
        check_interval_seconds=0.01, launcher=launch,
    )
    await coordinator.schedule("requester", prompt_after_seconds=60)
    assert coordinator._schedule is not None
    coordinator._schedule.requested_at -= 61

    await _until(lambda: len(engine.steers) == 1)
    assert "restart_ready" in engine.steers[0][1]
    assert await coordinator.request_more_time("active", 60) == 60
    await asyncio.sleep(0.03)
    assert len(engine.steers) == 1

    coordinator._schedule.next_prompt_at["active"] = time.monotonic() - 1
    await _until(lambda: len(engine.steers) == 2)
    assert await coordinator.mark_ready("active") is True
    await _until(lambda: engine.stops == ["active"])
    await _until(launched.is_set)


def test_restart_tools_are_registered() -> None:
    registry = build_default_registry()
    assert registry.get("schedule_restart") is not None
    assert registry.get("restart_ready") is not None
    assert registry.get("restart_wait") is not None


@pytest.mark.asyncio
async def test_engine_rejects_new_turn_while_restart_is_pending(db) -> None:
    engine = AgentEngine(NerveConfig(), db)
    engine.restart_coordinator._schedule = _RestartSchedule(
        requester_session_id="requester",
        requested_at=time.monotonic(),
        prompt_after_seconds=300,
    )

    with pytest.raises(RestartScheduledError):
        await engine.run("new-session", "do not start")
