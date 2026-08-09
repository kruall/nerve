"""Durable detached lifecycle, cancellation races, and restart recovery."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from nerve.executions.backend import BackendRecovery, BackendResult, LocalExecutionBackend
from nerve.executions.public import public_execution
from nerve.executions.service import ExecutionService


class StubProfile:
    source = "test.yaml"

    def describe(self):
        return {"kind": "test.wait", "version": "1", "profile_hash": "hash"}


class StubPlan:
    kind = "test.wait"
    profile_version = "1"
    profile_hash = "hash"
    profile = StubProfile()

    def as_dict(self, *, redact_secrets=True):
        return {
            "kind": self.kind,
            "profile_version": self.profile_version,
            "profile_hash": self.profile_hash,
            "arguments": {},
            "resources": {},
            "artifacts": {},
            "steps": [{
                "id": "wait", "type": "command", "transport": "local",
                "executable": "/bin/true", "argv": [], "cwd": "execution_dir",
                "resource_slot": None, "capture_stdout": None, "capture_stderr": None,
            }],
            "result": {
                "success_exit_codes": [0], "required_artifacts": [],
                "output_capture": None,
            },
            "timeout_seconds": 30,
            "cleanup": {"when": "always", "timeout_seconds": 5, "steps": []},
            "cancellation": {"mode": "terminate", "grace_seconds": 0, "run_cleanup": True},
        }


class ControlledBackend:
    name = "controlled"

    def __init__(self, *, result=None, recovery=None):
        self.started_event = asyncio.Event()
        self.release_event = asyncio.Event()
        self.cancelled = False
        self.result = result or BackendResult(0, summary="ok")
        self.recovery = recovery or BackendRecovery("orphaned")

    async def run(self, *, execution_id, plan, workspace, execution_dir, emit, started):
        await started({"job": execution_id})
        await emit("stdout", "started\n")
        self.started_event.set()
        await self.release_event.wait()
        return self.result

    async def cancel(self, *, execution_id, grace_seconds, mode):
        self.cancelled = True
        self.release_event.set()
        return True

    async def recover(self, execution):
        return self.recovery

    async def reattach(self, *, execution, emit):
        return self.result


async def _eventually(predicate, timeout=2.0):
    async with asyncio.timeout(timeout):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.01)


@pytest_asyncio.fixture
async def owner(db):
    await db.create_session(
        "owner", source="web", backend="codex", status="idle",
    )
    await db.update_session_fields("owner", {"sdk_session_id": "native-codex-thread"})
    return "owner"


@pytest.fixture
def broadcast_stub(monkeypatch):
    stub = SimpleNamespace(broadcast=AsyncMock())
    monkeypatch.setattr("nerve.executions.service.broadcaster", stub)
    return stub


def _engine():
    return SimpleNamespace(run=AsyncMock(return_value="continued"), is_session_running=lambda _sid: False)


@pytest.mark.asyncio
async def test_start_completes_and_resumes_same_session_once(db, owner, tmp_path, broadcast_stub):
    backend = ControlledBackend()
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    started = await service.start(session_id=owner, plan=StubPlan())
    execution_id = started["id"]
    await backend.started_event.wait()
    running = await db.get_execution(execution_id)
    assert running["status"] == "running"

    backend.release_event.set()

    async def completed():
        row = await db.get_execution(execution_id)
        return row if row["continuation_state"] == "completed" else None

    row = await _eventually(completed)
    assert row["status"] == "succeeded"
    assert engine.run.await_count == 1
    call = engine.run.await_args.kwargs
    assert call["session_id"] == owner
    assert call["source"] == "execution"
    assert call["internal"] is True
    assert (await db.get_session(owner))["sdk_session_id"] == "native-codex-thread"
    tail = await service.tail_logs(execution_id=execution_id, limit=20, before=None)
    assert tail["entries"][-1]["text"] == "started\n"
    await service.shutdown()


@pytest.mark.asyncio
async def test_join_consumes_completion_without_autonomous_resume(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    started = await service.start(
        session_id=owner, plan=StubPlan(), auto_continue=False,
    )
    await backend.started_event.wait()
    joined = asyncio.create_task(service.join_execution(
        execution_id=started["id"], session_id=owner,
    ))
    backend.release_event.set()
    row = await joined
    assert row["status"] == "succeeded"
    assert row["continuation_state"] == "suppressed"
    engine.run.assert_not_awaited()
    await service.shutdown()


@pytest.mark.asyncio
async def test_forget_keeps_execution_running_and_suppresses_resume(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    started = await service.start(session_id=owner, plan=StubPlan())
    await backend.started_event.wait()
    forgotten = await service.forget_execution(
        execution_id=started["id"], session_id=owner,
    )
    assert forgotten["status"] == "running"
    assert backend.cancelled is False
    backend.release_event.set()

    async def finished():
        row = await db.get_execution(started["id"])
        return row if row["status"] == "succeeded" else None

    row = await _eventually(finished)
    assert row["continuation_state"] == "suppressed"
    engine.run.assert_not_awaited()
    await service.shutdown()


@pytest.mark.asyncio
async def test_failure_continues_with_terminal_metadata(db, owner, tmp_path, broadcast_stub):
    backend = ControlledBackend(result=BackendResult(7, summary="bad"))
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=StubPlan()))["id"]
    await backend.started_event.wait()
    backend.release_event.set()

    async def failed():
        row = await db.get_execution(execution_id)
        return row if row["continuation_state"] == "completed" else None

    row = await _eventually(failed)
    assert row["status"] == "failed"
    assert row["result"]["exit_code"] == 7
    assert "status: failed" in engine.run.await_args.kwargs["user_message"]
    await service.shutdown()


@pytest.mark.asyncio
async def test_required_artifact_controls_terminal_success(
    db, owner, tmp_path, broadcast_stub,
):
    class ArtifactPlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["artifacts"] = {
                "report": {"root": "execution_dir", "path": "report.json", "required": True},
            }
            data["result"]["required_artifacts"] = ["report"]
            return data

    backend = ControlledBackend(result=BackendResult(0, summary="command ok"))
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=ArtifactPlan()))["id"]
    await backend.started_event.wait()
    backend.release_event.set()

    async def failed():
        row = await db.get_execution(execution_id)
        return row if row["continuation_state"] == "completed" else None

    row = await _eventually(failed)
    assert row["status"] == "failed"
    assert row["result"]["missing_artifacts"] == ["report"]
    await service.shutdown()


@pytest.mark.asyncio
async def test_textual_result_rules_override_success_exit_code(db, owner, tmp_path, broadcast_stub):
    class TextPlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["result"].update({"required_output": ["GOOD", "Ok"], "forbidden_output": ["FAIL"]})
            return data
    backend = ControlledBackend(result=BackendResult(0, summary="pipeline exited zero"))
    service = ExecutionService(db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(), backend=backend, execution_root=tmp_path / "runs")
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=TextPlan()))["id"]
    await backend.started_event.wait(); backend.release_event.set()
    row = await _eventually(lambda: _textual_done(db, execution_id))
    assert row["status"] == "failed"
    assert row["result"]["missing_output"] == ["GOOD", "Ok"]
    await service.shutdown()


async def _textual_done(db, execution_id):
    row = await db.get_execution(execution_id)
    return row if row["continuation_state"] == "completed" else None


@pytest.mark.asyncio
async def test_local_backend_executes_without_shell_and_drains_output(
    db, owner, tmp_path, broadcast_stub,
):
    class EchoPlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["steps"][0]["executable"] = "/bin/echo"
            data["steps"][0]["argv"] = [{"type": "literal", "value": "hello"}]
            return data

    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=EchoPlan()))["id"]

    async def completed():
        row = await db.get_execution(execution_id)
        return row if row["continuation_state"] == "completed" else None

    row = await _eventually(completed)
    assert row["status"] == "succeeded"
    tail = await service.tail_logs(execution_id=execution_id, limit=20, before=None)
    assert "hello" in "".join(entry["text"] for entry in tail["entries"])
    await service.shutdown()


@pytest.mark.asyncio
async def test_local_backend_cancellation_reaps_process_group(tmp_path):
    backend = LocalExecutionBackend()
    started = asyncio.Event()
    handle = {}
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = "owner"
    plan["steps"][0]["executable"] = "/bin/sleep"
    plan["steps"][0]["argv"] = [{"type": "literal", "value": "10"}]

    async def on_started(value):
        handle.update(value)
        started.set()

    task = asyncio.create_task(backend.run(
        execution_id="exec-cancelled",
        plan=plan,
        workspace=tmp_path,
        execution_dir=tmp_path / "run",
        emit=AsyncMock(),
        started=on_started,
    ))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert handle["pid"] > 0
    assert "exec-cancelled" not in backend._processes


@pytest.mark.asyncio
async def test_service_acquires_persists_and_releases_resource_leases(
    db, owner, tmp_path, broadcast_stub,
):
    class ResourcePlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["resources"] = {"builder": "builders"}
            return data

    class LeaseManager:
        def __init__(self):
            self.acquire = AsyncMock(return_value=[{
                "id": "lease-1", "host_id": "host-1", "state": "acquired",
            }])
            self.release = AsyncMock()
            self.quarantine = AsyncMock()

    backend = ControlledBackend()
    leases = LeaseManager()
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, resource_manager=leases,
        execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=ResourcePlan()))["id"]
    await backend.started_event.wait()
    row = await db.get_execution(execution_id)
    assert row["selected_leases"][0]["id"] == "lease-1"
    leases.acquire.assert_awaited_once()
    backend.release_event.set()

    async def released():
        return leases.release.await_count == 1

    await _eventually(released)
    leases.release.assert_awaited_once_with(
        execution_id=execution_id,
        leases=[{"id": "lease-1", "host_id": "host-1", "state": "acquired"}],
    )
    await service.shutdown()


@pytest.mark.asyncio
async def test_stop_without_agent_turn_cancels_and_suppresses_continuation(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=StubPlan()))["id"]
    await backend.started_event.wait()

    assert await service.cancel_session(owner) is True

    async def cancelled():
        row = await db.get_execution(execution_id)
        return row if row["status"] == "cancelled" else None

    row = await _eventually(cancelled)
    assert row["continuation_state"] == "suppressed"
    assert backend.cancelled is True
    engine.run.assert_not_awaited()
    await service.shutdown()


@pytest.mark.asyncio
async def test_completion_cancel_race_has_no_post_cancel_continuation(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=StubPlan()))["id"]
    await backend.started_event.wait()

    # cancel_session atomically stamps cancellation before it releases the
    # backend. The natural callback may still arrive, but its completion CAS
    # cannot publish the continuation outbox item.
    await service.cancel_session(owner)

    async def terminal():
        row = await db.get_execution(execution_id)
        return row if row["status"] == "cancelled" else None

    row = await _eventually(terminal)
    assert row["continuation_state"] == "suppressed"
    engine.run.assert_not_awaited()
    await service.shutdown()


@pytest.mark.asyncio
async def test_stop_cancels_a_claimed_continuation_turn(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    continuation_started = asyncio.Event()

    async def blocking_run(**kwargs):
        continuation_started.set()
        await asyncio.Event().wait()

    engine = SimpleNamespace(
        run=AsyncMock(side_effect=blocking_run),
        is_session_running=lambda _sid: True,
    )
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=StubPlan()))["id"]
    await backend.started_event.wait()
    backend.release_event.set()
    await continuation_started.wait()
    assert (await db.get_execution(execution_id))["continuation_state"] == "claimed"

    assert await service.cancel_session(owner) is True

    async def suppressed():
        row = await db.get_execution(execution_id)
        return row if row["continuation_state"] == "suppressed" else None

    await _eventually(suppressed)
    await _eventually(lambda: asyncio.sleep(0, result=execution_id not in service._continuations))
    assert engine.run.await_count == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_duplicate_terminal_callbacks_claim_only_one_continuation(
    db, owner, tmp_path, broadcast_stub,
):
    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=ControlledBackend(), execution_root=tmp_path / "runs",
    )
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-duplicate", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan=plan, resource_requests=[],
    )
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",))
    assert await db.transition_execution(row["id"], to_status="running", expect=("starting",))
    assert await db.finish_execution(row["id"], status="succeeded", result={"exit_code": 0})
    assert not await db.finish_execution(row["id"], status="succeeded", result={"exit_code": 0})
    await service.start_continuations()
    service._schedule_continuation(row["id"])
    service._schedule_continuation(row["id"])

    async def continued():
        current = await db.get_execution(row["id"])
        return current if current["continuation_state"] == "completed" else None

    await _eventually(continued)
    assert engine.run.await_count == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_restart_marks_unreattachable_work_lost_and_recovers_outbox(
    db, owner, tmp_path, broadcast_stub,
):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-restart", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan=plan, resource_requests=[],
    )
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",))
    assert await db.transition_execution(row["id"], to_status="running", expect=("starting",))

    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=ControlledBackend(recovery=BackendRecovery("orphaned")),
        execution_root=tmp_path / "runs",
    )
    await service.initialize()

    async def recovered():
        current = await db.get_execution(row["id"])
        return current if current["continuation_state"] == "completed" else None

    current = await _eventually(recovered)
    assert current["status"] == "lost"
    assert engine.run.await_count == 1
    await service.shutdown()


@pytest.mark.asyncio
async def test_restart_does_not_redispatch_an_uncertain_claim(
    db, owner, tmp_path, broadcast_stub,
):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-claimed", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan=plan, resource_requests=[],
    )
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",))
    assert await db.finish_execution(
        row["id"], status="failed", result={"exit_code": 2}, expect=("starting",),
    )
    claimed, _ = await db.claim_execution_continuation(row["id"])
    assert claimed

    engine = _engine()
    service = ExecutionService(
        db=db, engine=engine, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=ControlledBackend(), execution_root=tmp_path / "runs",
    )
    await service.initialize()
    current = await db.get_execution(row["id"])
    assert current["continuation_state"] == "failed"
    assert "restarted after continuation claim" in current["continuation_error"]
    engine.run.assert_not_awaited()
    await service.shutdown()


@pytest.mark.asyncio
async def test_engine_stop_counts_execution_listener_without_live_agent():
    from nerve.agent.engine import AgentEngine

    listener = AsyncMock(return_value=True)
    sessions = SimpleNamespace(stop_session=AsyncMock(return_value=False))
    engine = SimpleNamespace(_stop_listeners=[listener], sessions=sessions)
    assert await AgentEngine.stop_session(engine, "owner") is True
    listener.assert_awaited_once_with("owner")


@pytest.mark.asyncio
async def test_session_archive_invokes_execution_cancellation_hook(db, owner):
    from nerve.agent.sessions import SessionManager

    manager = SessionManager(db)
    manager._on_archive = AsyncMock(return_value=True)
    await manager.archive_session(owner)
    manager._on_archive.assert_awaited_once_with(owner)
    assert (await db.get_session(owner))["status"] == "archived"


@pytest.mark.asyncio
async def test_bounded_log_storage_and_tail(db, owner):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    await db.create_execution(
        "exec-logs", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan=plan, resource_requests=[],
    )
    for index in range(12):
        await db.append_execution_log(
            "exec-logs", stream="stdout", text=f"line-{index}",
            max_lines=5, max_chars=1000,
        )
    tail = await db.tail_execution_logs("exec-logs", limit=20)
    assert [entry["text"] for entry in tail["entries"]] == [
        "line-7", "line-8", "line-9", "line-10", "line-11",
    ]


def test_public_continuation_states_match_frontend_contract():
    assert public_execution({"continuation": {"state": "none"}})["continuation"]["state"] == "not_requested"
    assert public_execution({"continuation": {"state": "claimed"}})["continuation"]["state"] == "running"
    assert public_execution({"continuation": {"state": "completed"}})["continuation"]["state"] == "succeeded"
