"""Focused behavior tests for the durable long-command tool."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.long_commands import long_command_handler
from nerve.agent.tools.registry import ToolContext


@pytest.mark.asyncio
async def test_tool_starts_engine_owned_command(tmp_path):
    engine = SimpleNamespace(start_long_command=AsyncMock(return_value={
        "id": "job-1", "output_path": str(tmp_path / "job.log"),
    }))
    result = await long_command_handler(ToolContext(
        session_id="s1", workspace=tmp_path, db=object(), engine=engine,
        runtime_metadata={"runtime": "codex"},
    ), {
        "command": ["pytest", "tests/test_one.py"],
        "cwd": ".", "timeoutSeconds": 90, "prompt": "continue",
    })
    assert not result.is_error
    engine.start_long_command.assert_awaited_once_with(
        session_id="s1", command=["pytest", "tests/test_one.py"],
        cwd=".", timeout_seconds=90, prompt="continue",
    )


@pytest.mark.asyncio
async def test_tool_rejects_satellite_session(tmp_path):
    engine = SimpleNamespace(start_long_command=AsyncMock())
    result = await long_command_handler(ToolContext(
        session_id="s1", workspace=tmp_path, db=object(), engine=engine,
        runtime_metadata={"runtime": "external"},
    ), {"command": ["pytest"]})
    assert result.is_error
    engine.start_long_command.assert_not_awaited()


class _Db:
    def __init__(self, session):
        self.session = session
        self.finished: list[tuple] = []
        self.completed: list[str] = []

    async def finish_long_command(self, *args):
        self.finished.append(args)
        return True

    async def claim_long_command_resume(self, command_id):
        return True

    async def complete_long_command_resume(self, command_id):
        self.completed.append(command_id)

    async def get_session(self, session_id):
        return self.session


def _engine(db):
    engine = AgentEngine.__new__(AgentEngine)
    engine.db = db
    engine._long_command_monitors = {}
    engine.run = AsyncMock(return_value="continued")
    return engine


@pytest.mark.asyncio
async def test_completion_resumes_same_session_with_output(tmp_path):
    output = tmp_path / "job.log"
    output.write_text("all tests passed\n", encoding="utf-8")
    status = tmp_path / "job.json"
    status.write_text('{"exit_code": 0}', encoding="utf-8")
    db = _Db({"status": "idle"})
    engine = _engine(db)
    job = {
        "id": "job-1", "session_id": "s1", "status_path": str(status),
        "output_path": str(output), "timeout_at": (datetime.now(timezone.utc)
        + timedelta(minutes=1)).isoformat(), "process_pid": None,
        "prompt": "inspect output",
    }
    await engine._monitor_long_command(job)
    assert db.finished[0][:3] == ("job-1", "completed", 0)
    assert db.completed == ["job-1"]
    assert engine.run.await_count == 1
    assert engine.run.await_args.kwargs["session_id"] == "s1"
    assert "all tests passed" in engine.run.await_args.kwargs["user_message"]


@pytest.mark.asyncio
async def test_timeout_terminates_and_resumes(tmp_path):
    db = _Db({"status": "idle"})
    engine = _engine(db)
    job = {
        "id": "job-2", "session_id": "s1", "status_path": str(tmp_path / "none"),
        "output_path": str(tmp_path / "none.log"),
        "timeout_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
        "process_pid": None, "prompt": "inspect timeout",
    }
    await engine._monitor_long_command(job)
    assert db.finished[0][:3] == ("job-2", "timed_out", None)
    assert "timed out" in engine.run.await_args.kwargs["user_message"]


@pytest.mark.asyncio
async def test_command_state_and_resume_claim_are_durable(db, tmp_path):
    await db.create_session("s1")
    await db.add_long_command({
        "id": "job-db", "session_id": "s1", "command_json": '["pytest"]',
        "cwd": str(tmp_path), "output_path": str(tmp_path / "job.log"),
        "status_path": str(tmp_path / "job.json"), "process_pid": 123,
        "timeout_at": (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(),
        "prompt": "continue",
    })
    assert [job["id"] for job in await db.list_running_long_commands()] == ["job-db"]
    assert await db.finish_long_command("job-db", "completed", 0, "ok")
    pending = await db.list_pending_long_command_resumes()
    assert pending[0]["details"] == "ok"
    assert await db.claim_long_command_resume("job-db")
    assert not await db.claim_long_command_resume("job-db")
    await db.complete_long_command_resume("job-db")
    assert await db.list_pending_long_command_resumes() == []
