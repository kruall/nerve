"""Durability, exclusivity, expiry, and FIFO handoff tests for tool leases."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from nerve.agent.tools.handlers.tool_leases import (
    tool_lease_acquire_handler,
    tool_lease_subscribe_handler,
)
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig
from nerve.cron.service import CronService


def _at(seconds: int = 0) -> datetime:
    return datetime(2026, 7, 30, tzinfo=timezone.utc) + timedelta(seconds=seconds)


@pytest.mark.asyncio
class TestToolLeaseStore:
    async def _sessions(self, db):
        await db.create_session("s1")
        await db.create_session("s2")
        await db.create_session("s3")

    async def test_only_one_concurrent_acquire_wins(self, db):
        await self._sessions(db)
        first, second = await asyncio.gather(
            db.acquire_tool_lease("mcp__github__publish", "s1", 300),
            db.acquire_tool_lease("mcp__github__publish", "s2", 300),
        )
        assert sorted([first[0], second[0]]) == ["acquired", "busy"]

    async def test_expired_lease_is_reclaimed_by_next_acquire(self, db):
        await self._sessions(db)
        outcome, _ = await db.acquire_tool_lease("deploy", "s1", 300, now=_at())
        assert outcome == "acquired"
        outcome, lease = await db.acquire_tool_lease("deploy", "s2", 300, now=_at(301))
        assert outcome == "acquired"
        assert lease["session_id"] == "s2"

    async def test_only_owner_can_renew_or_release(self, db):
        await self._sessions(db)
        await db.acquire_tool_lease("deploy", "s1", 300, now=_at())
        assert await db.renew_tool_lease("deploy", "s2", 300, now=_at(1)) is None
        assert await db.release_tool_lease("deploy", "s2") is False
        assert await db.renew_tool_lease("deploy", "s1", 300, now=_at(1)) is not None
        assert await db.release_tool_lease("deploy", "s1") is True

    async def test_subscription_handoff_reserves_lease_fifo(self, db):
        await self._sessions(db)
        await db.acquire_tool_lease("deploy", "s1", 300, now=_at())
        first = await db.subscribe_tool_lease("deploy", "s2", "resume s2", 120, 600, now=_at(1))
        await db.subscribe_tool_lease("deploy", "s3", "resume s3", 120, 600, now=_at(2))
        assert await db.list_ready_tool_lease_subscriptions(now=_at(3)) == []
        assert await db.release_tool_lease("deploy", "s1")
        ready = await db.list_ready_tool_lease_subscriptions(now=_at(3))
        assert [item["id"] for item in ready] == [first["id"]]
        handoff = await db.claim_tool_lease_subscription(first["id"], now=_at(3))
        assert handoff is not None
        assert handoff["session_id"] == "s2"
        lease = await db.get_tool_lease("deploy", now=_at(4))
        assert lease is not None and lease["session_id"] == "s2"

    async def test_subscription_expiry_does_not_handoff(self, db):
        await self._sessions(db)
        await db.subscribe_tool_lease("deploy", "s2", "resume", 120, 60, now=_at())
        assert await db.list_ready_tool_lease_subscriptions(now=_at(61)) == []


@pytest.mark.asyncio
class TestToolLeaseHandlers:
    async def test_acquire_then_subscribe_reports_current_ownership(self, db):
        await db.create_session("s1")
        ctx = ToolContext(session_id="s1", db=db)
        acquired = await tool_lease_acquire_handler(ctx, {"tool_name": "deploy"})
        assert "acquired" in acquired.content[0]["text"]
        subscribed = await tool_lease_subscribe_handler(ctx, {"tool_name": "deploy"})
        assert "no subscription is needed" in subscribed.content[0]["text"]

    async def test_invalid_tool_name_is_rejected(self, db):
        await db.create_session("s1")
        result = await tool_lease_acquire_handler(
            ToolContext(session_id="s1", db=db), {"tool_name": "not a tool"},
        )
        assert result.is_error is True

    async def test_external_session_cannot_subscribe_for_wakeup(self, db):
        await db.create_session("external", source="external")
        result = await tool_lease_subscribe_handler(
            ToolContext(session_id="external", db=db), {"tool_name": "deploy"},
        )
        assert result.is_error is True


@pytest.mark.asyncio
class TestToolLeaseCronHandoff:
    @pytest_asyncio.fixture
    async def svc(self, db):
        await db.create_session("s1")
        await db.create_session("s2")
        engine = AsyncMock()
        engine.sessions = MagicMock()
        engine.sessions.is_running = MagicMock(return_value=False)
        engine.run = AsyncMock(return_value="ok")
        return CronService(NerveConfig(timezone="UTC"), engine, db)

    async def test_sweep_reserves_then_wakes_waiter(self, svc):
        await svc.db.acquire_tool_lease("deploy", "s1", 300)
        await svc.db.subscribe_tool_lease("deploy", "s2", "continue deploy", 120, 600)
        await svc.db.release_tool_lease("deploy", "s1")
        await svc._sweep_tool_lease_subscriptions()
        await asyncio.sleep(0.05)
        svc.engine.run.assert_awaited_once_with(
            session_id="s2", user_message="continue deploy", source="wakeup", internal=True,
        )
        lease = await svc.db.get_tool_lease("deploy")
        assert lease is not None and lease["session_id"] == "s2"

    async def test_busy_waiter_keeps_fifo_position(self, svc):
        await svc.db.subscribe_tool_lease("deploy", "s1", "s1", 120, 600)
        await svc.db.subscribe_tool_lease("deploy", "s2", "s2", 120, 600)
        svc.engine.sessions.is_running = MagicMock(side_effect=lambda sid: sid == "s1")
        await svc._sweep_tool_lease_subscriptions()
        svc.engine.run.assert_not_awaited()
        ready = await svc.db.list_ready_tool_lease_subscriptions()
        assert ready[0]["session_id"] == "s1"
