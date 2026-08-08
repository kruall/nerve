"""Tests for approved-plan implementation session dispatch."""

from __future__ import annotations

import asyncio

import pytest

from nerve.agent.tools.handlers.plans import plan_approve_handler
from nerve.agent.tools.registry import ToolContext
from nerve.config import NerveConfig


class _Sessions:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_or_create(self, session_id: str, **kwargs) -> dict:
        self.calls.append({"session_id": session_id, **kwargs})
        return {"id": session_id}


class _Engine:
    def __init__(self) -> None:
        self.sessions = _Sessions()
        self.calls: list[dict] = []
        self.started = asyncio.Event()

    async def run(self, **kwargs) -> None:
        self.calls.append(kwargs)
        self.started.set()


@pytest.mark.asyncio
async def test_codex_implementation_session_uses_plan_model(db, tmp_path):
    task_file = tmp_path / "task.md"
    task_file.write_text("# Demo task\n", encoding="utf-8")
    await db.upsert_task(
        task_id="task-plan-model",
        file_path="task.md",
        title="Demo task",
        status="pending",
        content=task_file.read_text(encoding="utf-8"),
    )
    await db.create_plan(
        plan_id="plan-model",
        task_id="task-plan-model",
        content="Implement it.",
        session_id="planner",
        version=1,
        plan_type="generic",
    )
    engine = _Engine()
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "agent": {"backend": "codex"},
        "codex": {"plan_model": "gpt-5.6-terra"},
    })
    ctx = ToolContext(
        session_id="reviewer",
        workspace=tmp_path,
        db=db,
        config=config,
        engine=engine,
    )

    result = await plan_approve_handler(ctx, {"plan_id": "plan-model"})
    await asyncio.wait_for(engine.started.wait(), timeout=1.0)

    assert not result.is_error
    assert engine.sessions.calls[0]["model"] == "gpt-5.6-terra"
    assert engine.calls[0]["model"] == "gpt-5.6-terra"
