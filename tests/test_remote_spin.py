from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock

from nerve.executions.service import ExecutionService
from nerve.executions.spin import validate_request
from nerve.executions import remote_supervisor


def test_spin_validation_rejects_unsafe_source_and_invalid_limits():
    with pytest.raises(ValueError, match="includes"): validate_request(model="#include <x>", profile="exhaustive", timeout_seconds=60, memory_mb=512, max_depth=1000, hash_bits=24)
    with pytest.raises(ValueError, match="embedded C"): validate_request(model="c_code { x; }", profile="exhaustive", timeout_seconds=60, memory_mb=512, max_depth=1000, hash_bits=24)
    with pytest.raises(ValueError, match="hash_bits"): validate_request(model="init { skip }", profile="bitstate", timeout_seconds=60, memory_mb=512, max_depth=1000, hash_bits=9)


@pytest.mark.asyncio
async def test_spin_plan_pins_replay_to_session_retained_handle(db, tmp_path):
    resources = SimpleNamespace(
        acquire_ydb_handle=AsyncMock(return_value={"id": "ydb-handle"}),
        _resolve_handle_lease=AsyncMock(return_value={
            "id": "ydb-handle", "host_id": "builder-1",
            "lease": {"id": "lease-1", "fencing_token": 7},
        }),
        release_handle=AsyncMock(),
    )
    service = ExecutionService(db=db, engine=SimpleNamespace(), workspace=tmp_path,
                               catalog=SimpleNamespace(), execution_root=tmp_path / "runs",
                               resource_manager=resources)
    service._start_serialized = AsyncMock(return_value={"id": "e"})
    await service.start_spin_verify(session_id="s-1", model="init { skip }")
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["retained_handle_ids"] == ["ydb-handle"]
    assert plan["resource_hosts"] == {"session": "builder-1"}
    assert "session_reservation" not in plan
    assert plan["steps"][0]["executable"] == "/usr/bin/spin"
    await service.start_spin_replay(session_id="s-1", run_id=plan["spin"]["run_id"])
    replay = service._start_serialized.await_args.kwargs["plan"]
    assert replay["retained_handle_ids"] == plan["retained_handle_ids"]
    assert replay["resource_hosts"] == plan["resource_hosts"]
    assert resources.acquire_ydb_handle.await_count == 2
    assert resources.release_handle.await_count == 0


@pytest.mark.asyncio
async def test_spin_failed_start_releases_only_a_new_session_handle(db, tmp_path):
    resources = SimpleNamespace(
        acquire_ydb_handle=AsyncMock(return_value={"id": "new-handle"}),
        _resolve_handle_lease=AsyncMock(return_value={
            "id": "new-handle", "host_id": "builder-1",
            "lease": {"id": "lease-1", "fencing_token": 7},
        }),
        release_handle=AsyncMock(),
    )
    service = ExecutionService(db=db, engine=SimpleNamespace(), workspace=tmp_path,
                               catalog=SimpleNamespace(), execution_root=tmp_path / "runs",
                               resource_manager=resources)
    service._start_serialized = AsyncMock(side_effect=RuntimeError("no row"))
    with pytest.raises(RuntimeError, match="no row"):
        await service.start_spin_verify(session_id="s-1", model="init { skip }")
    resources.release_handle.assert_awaited_once_with("s-1", "new-handle")


def test_remote_spin_prepare_is_fenced_and_retained(tmp_path, monkeypatch):
    monkeypatch.setattr(remote_supervisor.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="Spin Version 6"))
    request = {"root": str(tmp_path), "session_id": "s-1", "run_id": "spin-abc", "lease_id": "lease-1", "fencing_token": 7, "model": "init { skip }"}
    prepared = remote_supervisor._spin_prepare(request)
    assert prepared["spin_version"] == "Spin Version 6"
    assert prepared["expires_at"] > 0
    assert (tmp_path / ".nerve-spin-runs").exists()
    with pytest.raises(PermissionError): remote_supervisor._spin_prepare({**request, "fencing_token": 6})


def test_remote_spin_prepare_expires_retained_source(tmp_path, monkeypatch):
    monkeypatch.setattr(remote_supervisor.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="Spin Version 6"))
    now = [1000.0]
    monkeypatch.setattr(remote_supervisor.time, "time", lambda: now[0])
    request = {"root": str(tmp_path), "session_id": "s-1", "run_id": "spin-old", "lease_id": "lease-1", "fencing_token": 7, "model": "init { skip }", "retention_seconds": 60}
    remote_supervisor._spin_prepare(request)
    now[0] += 61
    with pytest.raises(FileNotFoundError):
        remote_supervisor._spin_prepare({**request, "model": None, "run_id": "spin-old"})
