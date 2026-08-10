"""Durable detached lifecycle, cancellation races, and restart recovery."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from nerve.executions.backend import BackendRecovery, BackendResult, ExecutionBackendUncertain, LocalExecutionBackend
from nerve.executions.public import public_execution
from nerve.executions.service import ExecutionService, _bind_session_reservation_slot
from nerve.resources import LeaseService, ResourceInventory


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


async def _lease_service(db):
    inventory = ResourceInventory(
        db,
        {
            "connections": ["lab"],
            "hosts": [{"id": "builder-1", "connection_ref": "lab"}],
            "pools": [{"id": "builders", "members": ["builder-1"]}],
        },
    )
    await inventory.initialize()
    return LeaseService(db=db, inventory=inventory)


class _SynchronousBackend:
    name = "synchronous-backend"

    async def run(self, *, execution_id, plan, workspace, execution_dir, emit, started):
        await emit("stdout", "stage=backend_once\n")
        return BackendResult(0, summary="ok")

    async def cancel(self, *, execution_id, grace_seconds, mode):
        return True

    async def recover(self, execution):
        return BackendRecovery("orphaned")


def test_session_reservation_lease_is_bound_to_compiled_resource_slot():
    lease = _bind_session_reservation_slot(
        {"resources": {"session": "ydb-builders"}},
        {"pool": "ydb-builders"},
        {"id": "lease-1", "fencing_token": 7},
    )
    assert lease == {"id": "lease-1", "fencing_token": 7, "slot": "session"}

    with pytest.raises(ValueError, match="exactly one"):
        _bind_session_reservation_slot(
            {"resources": {"first": "ydb-builders", "second": "ydb-builders"}},
            {"pool": "ydb-builders"},
            {"id": "lease-1"},
        )


@pytest.mark.asyncio
async def test_ydb_test_plan_does_not_reject_test_owned_error_text(
    db, owner, tmp_path, monkeypatch,
):
    worktree = tmp_path / "ydb"
    monkeypatch.setattr(
        "nerve.executions.service.validate_worktree", lambda *_args: worktree,
    )
    monkeypatch.setattr(
        "nerve.executions.service.ydb_snapshot",
        lambda _top: {
            "snapshot_id": "a" * 40,
            "head": "b" * 40,
            "pack": b"pack",
        },
    )
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        execution_root=tmp_path / "runs",
    )
    service._start_serialized = AsyncMock(return_value={"id": "exec-ydb"})

    await service.start_ydb(
        session_id=owner, kind="ydb_test", worktree=str(worktree), args=["target"],
    )

    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["result"]["required_output"] == ["GOOD", "Ok"]
    assert plan["result"]["forbidden_output"] == []


@pytest.mark.asyncio
async def test_ydb_make_publishes_one_confined_output_with_explicit_build_type(
    db, owner, tmp_path, monkeypatch,
):
    worktree = tmp_path / "ydb"
    monkeypatch.setattr("nerve.executions.service.validate_worktree", lambda *_args: worktree)
    monkeypatch.setattr("nerve.executions.service.ydb_snapshot", lambda _top: {
        "snapshot_id": "a" * 40, "head": "b" * 40, "pack": b"pack",
    })
    service = ExecutionService(db=db, engine=_engine(), workspace=tmp_path,
                               catalog=SimpleNamespace(), execution_root=tmp_path / "runs")
    service._start_serialized = AsyncMock(return_value={"id": "exec-ydb"})

    await service.start_ydb(session_id=owner, kind="ydb_make", worktree=str(worktree),
                            args=["ydb/tools/ydb_bench"], build_type="profile",
                            publish={"output_path": "ydb/tools/ydb_bench/ydb_bench"})

    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["steps"][0]["argv"][:5] == [
        {"type": "literal", "value": "make"},
        {"type": "literal", "value": "--build"},
        {"type": "literal", "value": "profile"},
        {"type": "literal", "value": "--output"},
        {"type": "literal", "value": ".nerve-ydb-output"},
    ]
    assert plan["ydb_publish"]["output_path"] == ".nerve-ydb-output/ydb/tools/ydb_bench/ydb_bench"
    assert plan["ydb_publish"]["artifact_root"] == "artifacts"
    assert plan["ydb_publish"]["path"].endswith("/ydb_bench")

    with pytest.raises(ValueError, match="publish output path"):
        await service.start_ydb(session_id=owner, kind="ydb_make", worktree=str(worktree),
                                args=[], publish={"output_path": "../secret"})


@pytest.mark.asyncio
async def test_artifact_transfer_builds_one_or_two_resource_slots(db, owner, tmp_path, monkeypatch):
    inventory = SimpleNamespace(local_artifact_roots={"control": tmp_path})
    inventory.members = lambda pool: [pool + "-host"]
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        resource_manager=SimpleNamespace(inventory=inventory),
    )
    service._start_serialized = AsyncMock(return_value={"id": "exec-transfer"})

    await service.start_artifact_transfer(
        session_id=owner,
        source={"host": "localhost", "artifact_root": "control", "path": "a.bin"},
        destination={"pool": "workers", "artifact_root": "artifacts", "path": "b.bin"},
        auto_continue=False,
    )
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["resources"] == {"destination": "workers"}
    assert plan["artifact_transfer"]["source_local"] is True

    await service.start_artifact_transfer(
        session_id=owner,
        source={"pool": "builders", "artifact_root": "artifacts", "path": "a.bin"},
        destination={"pool": "workers", "artifact_root": "artifacts", "path": "b.bin"},
    )
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["resources"] == {"source": "builders", "destination": "workers"}

    await service.start_artifact_transfer(
        session_id=owner,
        source={"pool": "builders", "host": "builders-host", "artifact_root": "artifacts", "path": "a.bin"},
        destination={"pool": "workers", "host": "workers-host", "artifact_root": "artifacts", "path": "b.bin"},
    )
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["resource_hosts"] == {"source": "builders-host", "destination": "workers-host"}
    assert plan["source_session_reservation"] is False

    monkeypatch.setattr(db, "get_session_resource_reservation", AsyncMock(return_value={
        "state": "active", "pool": "builders", "lease_id": "reservation-lease",
    }))
    monkeypatch.setattr(db, "get_resource_lease", AsyncMock(return_value={
        "state": "active", "host_id": "builders-host",
    }))
    await service.start_artifact_transfer(
        session_id=owner,
        source={"pool": "builders", "host": "builders-host", "artifact_root": "artifacts", "path": "a.bin"},
        destination={"pool": "workers", "artifact_root": "artifacts", "path": "b.bin"},
    )
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["source_session_reservation"] is True

    with pytest.raises(ValueError, match="not a member"):
        await service.start_artifact_transfer(
            session_id=owner,
            source={"pool": "builders", "host": "other-host", "artifact_root": "artifacts", "path": "a.bin"},
            destination={"pool": "workers", "artifact_root": "artifacts", "path": "b.bin"},
        )

    with pytest.raises(ValueError, match="localhost-to-localhost"):
        await service.start_artifact_transfer(
            session_id=owner,
            source={"host": "localhost", "artifact_root": "control", "path": "a.bin"},
            destination={"host": "localhost", "artifact_root": "control", "path": "b.bin"},
        )


@pytest.mark.asyncio
async def test_artifact_transfer_validates_remote_endpoint_before_start(db, owner, tmp_path):
    inventory = SimpleNamespace(local_artifact_roots={"control": tmp_path})
    inventory.members = lambda _pool: ["host"]
    backend = _engine()
    backend.validate_artifact_endpoint = Mock(
        side_effect=ValueError("artifact root is not configured"),
    )
    service = ExecutionService(
        db=db, engine=backend, workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend,
        resource_manager=SimpleNamespace(inventory=inventory),
    )
    service._start_serialized = AsyncMock(return_value={"id": "must-not-start"})

    with pytest.raises(ValueError, match="artifact root is not configured"):
        await service.start_artifact_transfer(
            session_id=owner,
            source={"host": "localhost", "artifact_root": "control", "path": "a.bin"},
            destination={"pool": "workers", "artifact_root": "missing", "path": "b.bin"},
        )

    service._start_serialized.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_command_builds_shell_free_leased_plan(db, owner, tmp_path):
    inventory = SimpleNamespace(members=Mock(return_value=["worker-1"]))
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        resource_manager=SimpleNamespace(inventory=inventory),
    )
    service._start_serialized = AsyncMock(return_value={"id": "exec-command"})

    await service.start_resource_command(
        session_id=owner,
        pool="test-machines",
        executable="/opt/tests/run",
        args=["--case", "value with spaces; $(still-data)"],
        timeout_seconds=90,
        auto_continue=False,
    )

    inventory.members.assert_called_once_with("test-machines")
    call = service._start_serialized.await_args.kwargs
    assert call["auto_continue"] is False
    assert call["plan"] == {
        "kind": "resource_command",
        "profile_version": "1",
        "profile_hash": "built-in-resource-command-v1",
        "arguments": {
            "pool": "test-machines",
            "executable": "/opt/tests/run",
            "args": ["--case", "value with spaces; $(still-data)"],
        },
        "resources": {"worker": "test-machines"},
        "steps": [{
            "id": "command", "transport": "resource",
            "resource_slot": "worker", "executable": "/opt/tests/run",
            "argv": [
                {"type": "literal", "value": "--case"},
                {"type": "literal", "value": "value with spaces; $(still-data)"},
            ],
            "cwd": "execution_dir",
        }],
        "result": {"success_exit_codes": [0]},
        "timeout_seconds": 90,
        "cancellation": {
            "mode": "terminate", "grace_seconds": 10,
            "run_cleanup": False,
        },
    }


@pytest.mark.asyncio
async def test_resource_command_rejects_unknown_pool_and_malformed_argv(db, owner, tmp_path):
    inventory = SimpleNamespace(members=Mock(side_effect=KeyError("missing")))
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        resource_manager=SimpleNamespace(inventory=inventory),
    )
    service._start_serialized = AsyncMock()

    with pytest.raises(KeyError):
        await service.start_resource_command(
            session_id=owner, pool="missing", executable="/bin/true", args=[],
        )
    with pytest.raises(ValueError, match="literal strings"):
        await service.start_resource_command(
            session_id=owner, pool="workers", executable="/bin/true", args=[1],
        )
    service._start_serialized.assert_not_awaited()


@pytest.mark.asyncio
async def test_ydb_host_release_refuses_active_execution(db, owner, tmp_path):
    await db.create_execution(
        "exec-active",
        session_id=owner,
        kind="ydb_make",
        profile_version="1",
        profile_hash="hash",
        profile_snapshot={},
        plan={},
        resource_requests=[],
    )
    manager = SimpleNamespace(release_session_reservation=AsyncMock(return_value=True))
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        resource_manager=manager,
    )

    with pytest.raises(ValueError, match="execution is active"):
        await service.release_ydb_host(session_id=owner)
    manager.release_session_reservation.assert_not_awaited()


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
async def test_dismiss_hides_only_settled_owner_execution_and_preserves_evidence(
    db, owner, tmp_path, broadcast_stub,
):
    backend = ControlledBackend()
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=backend, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    started = await service.start(session_id=owner, plan=StubPlan(), auto_continue=False)
    await backend.started_event.wait()

    with pytest.raises(ValueError, match="not eligible"):
        await service.dismiss_execution(
            execution_id=started["id"], session_id=owner, requested_by="operator",
        )
    with pytest.raises(KeyError):
        await service.dismiss_execution(
            execution_id=started["id"], session_id="other-session", requested_by="operator",
        )

    backend.release_event.set()
    row = await _eventually(lambda: _settled(db, started["id"]))
    await db.append_execution_log(started["id"], stream="stdout", text="durable\n")
    dismissed = await service.dismiss_execution(
        execution_id=started["id"], session_id=owner, requested_by="operator",
    )
    again = await service.dismiss_execution(
        execution_id=started["id"], session_id=owner, requested_by="operator",
    )
    assert dismissed["dismissed_at"] == again["dismissed_at"]
    assert await service.list_executions(session_id=owner, include_terminal=True, limit=20) == []
    assert (await service.get_execution(execution_id=started["id"]))["dismissed_at"]
    assert (await service.tail_logs(execution_id=started["id"], limit=20, before=None))["entries"][-1]["text"] == "durable\n"
    assert row["continuation_state"] == "suppressed"
    await service.shutdown()


@pytest.mark.asyncio
async def test_db_dismiss_rejects_pending_continuation(db, owner):
    await db.create_execution(
        "exec-pending-dismiss", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan={},
        resource_requests=[], auto_continue=True,
    )
    assert await db.transition_execution(
        "exec-pending-dismiss", to_status="running", expect=("queued",),
    )
    assert await db.finish_execution(
        "exec-pending-dismiss", status="succeeded", result={"outcome": "ok"},
    )
    assert not await db.dismiss_session_execution("exec-pending-dismiss", session_id=owner)
    assert (await db.get_execution("exec-pending-dismiss"))["continuation_state"] == "pending"


async def _settled(db, execution_id):
    row = await db.get_execution(execution_id)
    return row if row and row["status"] == "succeeded" else None


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


@pytest.mark.asyncio
async def test_textual_result_rules_allow_error_text_in_passing_test_output(
    db, owner, tmp_path, broadcast_stub,
):
    class TextPlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["result"].update({"required_output": ["GOOD", "Ok"]})
            return data

    class PassingTestBackend(ControlledBackend):
        async def run(self, *, execution_id, plan, workspace, execution_dir, emit, started):
            await started({"job": execution_id})
            await emit("stderr", 'priority: ERROR\nTotal 1 suite:\n\t1 - GOOD\nOk\n')
            return BackendResult(0, summary="pipeline exited zero")

    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=PassingTestBackend(), execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=TextPlan()))["id"]
    row = await _eventually(lambda: _textual_done(db, execution_id))
    assert row["status"] == "succeeded"
    assert row["result"]["outcome"] == "succeeded"
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
async def test_uncertain_remote_cleanup_quarantines_instead_of_releasing(
    db, owner, tmp_path, broadcast_stub,
):
    class ResourcePlan(StubPlan):
        def as_dict(self, *, redact_secrets=True):
            data = super().as_dict(redact_secrets=redact_secrets)
            data["resources"] = {"source": "sources", "destination": "destinations"}
            return data

    class Backend(ControlledBackend):
        async def run(self, *, execution_id, plan, workspace, execution_dir, emit, started):
            await started({"transfer_id": "transfer-a", "reattachable": False})
            raise ExecutionBackendUncertain("cleanup is ambiguous")

    leases = SimpleNamespace(
        acquire=AsyncMock(return_value=[
            {"id": "lease-source", "host_id": "source", "slot": "source"},
            {"id": "lease-destination", "host_id": "destination", "slot": "destination"},
        ]),
        release=AsyncMock(),
        quarantine=AsyncMock(),
    )
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=Backend(), resource_manager=leases, execution_root=tmp_path / "runs",
    )
    await service.initialize()
    execution_id = (await service.start(session_id=owner, plan=ResourcePlan()))["id"]

    async def terminal():
        row = await db.get_execution(execution_id)
        return row if row["status"] == "failed" else None

    row = await _eventually(terminal)
    assert row["result"]["error"] == "remote_quiescence_unknown"
    leases.quarantine.assert_awaited_once()
    leases.release.assert_not_awaited()
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
async def test_execution_stop_listener_releases_session_reservation():
    from nerve.agent.engine import AgentEngine

    execution_service = SimpleNamespace(cancel_session=AsyncMock(return_value=True))
    resource_service = SimpleNamespace(
        cleanup_session_reservation=AsyncMock(return_value=True),
    )
    engine = SimpleNamespace(
        _execution_stop_listener=None,
        _stop_listeners=[],
        resource_service=resource_service,
        sessions=SimpleNamespace(_on_archive=None),
    )
    engine.add_stop_listener = engine._stop_listeners.append

    AgentEngine.set_execution_service(engine, execution_service)

    assert await engine._execution_stop_listener("owner") is True
    execution_service.cancel_session.assert_awaited_once_with("owner")
    resource_service.cleanup_session_reservation.assert_awaited_once_with("owner")


@pytest.mark.asyncio
async def test_operator_cancels_only_unleased_queued_execution(db, owner, tmp_path):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    await db.create_execution(
        "exec-orphaned", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan=plan,
        resource_requests=[{"slot": "worker", "pool": "builders", "state": "requested"}],
    )
    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=ControlledBackend(), execution_root=tmp_path / "runs",
    )

    cancelled = await service.cancel_queued_execution(
        execution_id="exec-orphaned", requested_by="operator", reason="owner confirmed stale",
    )

    assert cancelled["status"] == "cancelled"
    assert (await db.get_execution("exec-orphaned"))["status"] == "cancelled"


@pytest.mark.asyncio
async def test_confirmed_remote_cancellation_releases_selected_lease(db, owner, tmp_path):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-confirmed-cancel", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan=plan,
        resource_requests=[],
    )
    lease = {"id": "lease-confirmed", "host_id": "host-1", "fencing_token": 9}
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",), fields={"selected_leases": [lease]})
    assert await db.transition_execution(row["id"], to_status="running", expect=("starting",))
    leases = SimpleNamespace(release=AsyncMock(), quarantine=AsyncMock())
    service = ExecutionService(db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
                               backend=ControlledBackend(), resource_manager=leases)

    cancelled = await service.cancel_execution(execution_id=row["id"], requested_by="owner", reason="stop")

    assert cancelled["status"] == "cancelled"
    leases.release.assert_awaited_once_with(execution_id=row["id"], leases=[lease])
    leases.quarantine.assert_not_awaited()
    tail = await db.tail_execution_logs(row["id"], limit=20)
    assert "stage=remote_quiescence_confirmed" in "".join(x["text"] for x in tail["entries"])


@pytest.mark.asyncio
async def test_reconnect_terminal_status_releases_without_new_cancel_rpc(db, owner, tmp_path):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-terminal-recovery", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan=plan,
        resource_requests=[],
    )
    lease = {"id": "lease-recovery", "host_id": "host-1", "fencing_token": 10}
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",), fields={"selected_leases": [lease]})
    assert await db.transition_execution(row["id"], to_status="running", expect=("starting",))
    assert await db.request_execution_cancel(row["id"], reason="restart")

    backend = ControlledBackend(recovery=BackendRecovery("finished", BackendResult(1, summary="remote terminal")))
    backend.name = "ssh-supervisor"
    leases = SimpleNamespace(release=AsyncMock(), quarantine=AsyncMock())
    service = ExecutionService(db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
                               backend=backend, resource_manager=leases)
    await service._recover_and_cancel(await db.get_execution(row["id"]))

    leases.release.assert_awaited_once_with(execution_id=row["id"], leases=[lease])
    assert (await db.get_execution(row["id"]))["status"] == "cancelled"
    assert backend.cancelled is False


@pytest.mark.asyncio
async def test_ambiguous_remote_reconnect_quarantines_selected_lease(db, owner, tmp_path):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-ambiguous-cancel", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan=plan,
        resource_requests=[],
    )
    lease = {"id": "lease-ambiguous", "host_id": "host-1", "fencing_token": 11}
    assert await db.transition_execution(row["id"], to_status="starting", expect=("queued",), fields={"selected_leases": [lease]})
    assert await db.transition_execution(row["id"], to_status="running", expect=("starting",))
    backend = ControlledBackend(recovery=BackendRecovery("orphaned")); backend.name = "ssh-supervisor"
    leases = SimpleNamespace(release=AsyncMock(), quarantine=AsyncMock())
    service = ExecutionService(db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
                               backend=backend, resource_manager=leases)

    cancelled = await service.cancel_execution(execution_id=row["id"], requested_by="owner", reason="stop")

    assert cancelled["status"] == "lost"
    leases.release.assert_not_awaited()
    leases.quarantine.assert_awaited_once()


@pytest.mark.asyncio
async def test_terminal_unleased_cancellation_settles_stale_queue_atomically(db, owner):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-stale-queue", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan=plan,
        resource_requests=[{"slot": "worker", "pool": "builders", "state": "requested"}],
    )
    await db.enqueue_resource_request(
        request_id="request-stale-queue", execution_id=row["id"],
        session_id=owner, slot="worker", pool="builders",
    )
    assert await db.request_execution_cancel(row["id"], reason="recovery")
    assert await db.finalize_execution_cancelled(row["id"])

    requests = await db.list_resource_requests()
    assert not [request for request in requests if request["execution_id"] == row["id"]]


@pytest.mark.asyncio
async def test_startup_reconciles_orphaned_terminal_resource_queue(db, owner, tmp_path, broadcast_stub):
    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    row = await db.create_execution(
        "exec-terminal-recovery-startup", session_id=owner, kind="test.wait",
        profile_version="1", profile_hash="hash", profile_snapshot={}, plan=plan,
        resource_requests=[],
    )
    assert await db.transition_execution(row["id"], to_status="succeeded", expect=("queued",))
    await db.enqueue_resource_bundle(
        bundle_id="bundle-startup-queue", execution_id=row["id"],
        session_id=owner, requests=[{"id": "request-startup-queue", "slot": "worker", "pool": "builders"}],
    )

    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=ControlledBackend(), execution_root=tmp_path / "runs",
    )
    await service.initialize()

    requests = await db.list_resource_requests()
    assert not [request for request in requests if request["execution_id"] == row["id"]]
    async with db.db.execute(
        "SELECT state FROM resource_lease_bundles WHERE id=?",
        ("bundle-startup-queue",),
    ) as cursor:
        bundle = await cursor.fetchone()
    assert bundle is not None and bundle["state"] == "cancelled"


@pytest.mark.asyncio
async def test_startup_retries_stale_session_reservation_lease(db, owner, tmp_path):
    resource_manager = await _lease_service(db)
    stale = await resource_manager.reserve_for_session(
        session_id=owner, pool="builders", worktree=f"spin:{owner}",
    )
    await resource_manager.release(
        execution_id=stale["lease"]["execution_id"],
        leases=[stale["lease"]],
    )

    plan = StubPlan().as_dict(redact_secrets=False)
    plan["session_id"] = owner
    plan["resources"] = {"session": "builders"}
    plan["session_reservation"] = {"pool": "builders", "worktree": f"spin:{owner}"}
    await db.create_execution(
        "exec-stale-session-reservation-startup", session_id=owner,
        kind="test.wait", profile_version="1", profile_hash="hash",
        profile_snapshot={}, plan=plan, resource_requests=[],
    )

    service = ExecutionService(
        db=db, engine=_engine(), workspace=tmp_path, catalog=SimpleNamespace(),
        backend=_SynchronousBackend(), resource_manager=resource_manager,
        execution_root=tmp_path / "runs",
    )
    await service.initialize()

    async def _succeeded():
        row = await db.get_execution("exec-stale-session-reservation-startup")
        return row if row is not None and row["status"] == "succeeded" else None

    current = await _eventually(_succeeded)
    logs = await db.tail_execution_logs("exec-stale-session-reservation-startup", limit=20)
    assert current["status"] == "succeeded"
    assert any("stage=reservation_acquired" in entry["text"] for entry in logs["entries"])
    await service.shutdown()


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
