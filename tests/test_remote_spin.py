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
async def test_spin_plan_pins_replay_to_session_reservation(db, tmp_path):
    service = ExecutionService(db=db, engine=SimpleNamespace(), workspace=tmp_path, catalog=SimpleNamespace(), execution_root=tmp_path / "runs")
    service._start_serialized = AsyncMock(return_value={"id": "e"})
    await service.start_spin_verify(session_id="s-1", model="init { skip }")
    plan = service._start_serialized.await_args.kwargs["plan"]
    assert plan["session_reservation"] == {"pool": "ydb-builders", "worktree": "spin:s-1"}
    assert plan["steps"][0]["executable"] == "/usr/bin/spin"
    await service.start_spin_replay(session_id="s-1", run_id=plan["spin"]["run_id"])
    assert service._start_serialized.await_args.kwargs["plan"]["session_reservation"] == plan["session_reservation"]


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
