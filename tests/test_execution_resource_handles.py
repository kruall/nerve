"""Acceptance coverage for execution use of session-owned lease handles."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.executions.backend import BackendRecovery, BackendResult
from nerve.executions.service import ExecutionService
from nerve.resources import LeaseService, ResourceHandleOwnershipError, ResourceInventory


class _Plan:
    kind = "test.remote"
    profile_version = "1"
    profile_hash = "test-hash"
    profile = SimpleNamespace(source="test.yaml", describe=lambda: {"kind": "test.remote"})

    def __init__(self, resources: dict[str, str]):
        self.resources = resources

    def as_dict(self, *, redact_secrets=True):
        return {
            "kind": self.kind, "profile_version": self.profile_version,
            "profile_hash": self.profile_hash, "arguments": {},
            "resources": self.resources, "artifacts": {}, "steps": [],
            "result": {"success_exit_codes": [0], "required_artifacts": []},
            "timeout_seconds": 10, "cleanup": {"steps": []},
            "cancellation": {"mode": "terminate", "grace_seconds": 0},
        }


class _Backend:
    name = "recording"

    def __init__(self):
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.plans = []

    async def run(self, *, execution_id, plan, workspace, execution_dir, emit, started):
        self.plans.append(plan)
        await started({"job_id": execution_id})
        self.started.set()
        await self.release.wait()
        return BackendResult(0, summary="ok")

    async def cancel(self, **_kwargs):
        self.release.set()
        return True

    async def recover(self, _execution):
        return BackendRecovery("orphaned")

    async def reattach(self, **_kwargs):
        return BackendResult(0)


async def _service(db, tmp_path):
    inventory = ResourceInventory(db, {
        "connections": ["test"],
        "hosts": [
            {"id": "a", "connection_ref": "test"},
            {"id": "b", "connection_ref": "test"},
        ],
        "pools": [{"id": "a", "members": ["a"]}, {"id": "b", "members": ["b"]}],
    })
    await inventory.initialize()
    await db.create_session("owner")
    await db.create_session("foreign")
    resources = LeaseService(db=db, inventory=inventory)
    backend = _Backend()
    service = ExecutionService(
        db=db, engine=SimpleNamespace(run=AsyncMock(), is_session_running=lambda _id: False),
        workspace=Path(tmp_path), catalog=SimpleNamespace(), backend=backend,
        resource_manager=resources, execution_root=Path(tmp_path) / "executions",
    )
    return service, resources, backend


async def _wait_for(event):
    async with asyncio.timeout(2):
        await event.wait()


@pytest.mark.asyncio
async def test_retained_handles_keep_order_across_reopen_and_backend_slots(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    handles = await resources.acquire_handles("owner", [{"pool": "b"}, {"pool": "a"}])
    await db.close(); await db.connect()

    execution = await service.start(session_id="owner", plan=_Plan({"first": "b", "second": "a"}),
                                    handle_ids=[handle["id"] for handle in handles], auto_continue=False)
    await _wait_for(backend.started)
    refs = await db.list_operation_resource_refs(execution["id"])
    assert [(ref["handle_id"], ref["position"]) for ref in refs] == [
        (handles[0]["id"], 0), (handles[1]["id"], 1),
    ]
    assert [(lease["slot"], lease["host_id"]) for lease in backend.plans[0]["selected_leases"]] == [
        ("first", "b"), ("second", "a"),
    ]
    task = service._tasks[execution["id"]]
    backend.release.set()
    await task
    assert await db.list_operation_resource_refs(execution["id"]) == []


@pytest.mark.asyncio
async def test_handle_reuse_and_invalid_handles_never_dispatch_backend(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    handle = await db.get_session_resource_handle(handle["id"])

    first = await service.start(session_id="owner", plan=_Plan({"slot": "a"}), handle_ids=[handle["id"]], auto_continue=False)
    await _wait_for(backend.started); task = service._tasks[first["id"]]; backend.release.set(); await task
    backend.release = asyncio.Event(); backend.started = asyncio.Event()
    second = await service.start(session_id="owner", plan=_Plan({"slot": "a"}), handle_ids=[handle["id"]], auto_continue=False)
    await _wait_for(backend.started); task = service._tasks[second["id"]]; backend.release.set(); await task

    checks = [
        ("foreign", handle["id"]),
        ("owner", "missing"),
    ]
    for session_id, handle_id in checks:
        with pytest.raises(ResourceHandleOwnershipError):
            await service.start(session_id=session_id, plan=_Plan({"slot": "a"}), handle_ids=[handle_id])
    await db.update_session_resource_handle(handle["id"], expected_state="active", state="quarantined")
    with pytest.raises(ResourceHandleOwnershipError):
        await service.start(session_id="owner", plan=_Plan({"slot": "a"}), handle_ids=[handle["id"]])
    assert len(backend.plans) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("table", "assignment"),
    [
        ("session_resource_handles", "state='released'"),
        ("resource_leases", "state='released'"),
        ("resource_leases", "fencing_token=fencing_token+1"),
        ("resource_hosts", "enabled=0"),
        ("resource_hosts", "draining=1"),
        ("resource_hosts", "offline=1"),
        ("resource_hosts", "quarantined=1"),
        ("resource_hosts", "permanently_unavailable=1"),
    ],
)
async def test_invalid_handle_lineage_is_rejected_before_backend_start(
    db, tmp_path, table, assignment,
):
    service, resources, backend = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    handle = await db.get_session_resource_handle(handle["id"])
    key = handle["id"] if table == "session_resource_handles" else (
        handle["lease_id"] if table == "resource_leases" else handle["host_id"]
    )
    await db.db.execute(f"UPDATE {table} SET {assignment} WHERE id=?", (key,))
    await db.db.commit()

    with pytest.raises(ResourceHandleOwnershipError):
        await service.start(
            session_id="owner", plan=_Plan({"slot": "a"}),
            handle_ids=[handle["id"]],
        )
    assert backend.plans == []


async def _create_handle_execution(db, execution_id, handle_id, *, auto_continue=False):
    return await db.create_execution(
        execution_id, session_id="owner", kind="test.remote",
        profile_version="1", profile_hash="hash", profile_snapshot={},
        plan={"retained_handle_ids": [handle_id]}, resource_requests=[],
        handle_ids=[handle_id], auto_continue=auto_continue,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["succeeded", "failed", "lost"])
async def test_continuable_terminal_transitions_detach_refs_once(
    db, tmp_path, terminal_status,
):
    _service_instance, resources, _backend = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    execution_id = f"terminal-{terminal_status}"
    await _create_handle_execution(db, execution_id, handle["id"], auto_continue=True)
    assert await db.transition_execution(
        execution_id, to_status="running", expect=("queued",),
    )

    assert len(await db.list_operation_resource_refs(execution_id)) == 1
    assert await db.finish_execution(execution_id, status=terminal_status, result={})
    assert await db.list_operation_resource_refs(execution_id) == []
    assert not await db.finish_execution(execution_id, status=terminal_status, result={})
    claimed, _row = await db.claim_execution_continuation(execution_id)
    assert claimed
    claimed_again, _row = await db.claim_execution_continuation(execution_id)
    assert not claimed_again


@pytest.mark.asyncio
async def test_cancel_dismiss_and_suppress_keep_terminal_refs_detached(db, tmp_path):
    _service_instance, resources, _backend = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    await _create_handle_execution(db, "cancelled", handle["id"])
    assert await db.request_execution_cancel("cancelled", reason="test")
    assert await db.finalize_execution_cancelled("cancelled")
    assert await db.list_operation_resource_refs("cancelled") == []
    assert await db.suppress_execution_continuation("cancelled")
    assert await db.dismiss_session_execution("cancelled", session_id="owner")
    assert await db.list_operation_resource_refs("cancelled") == []


@pytest.mark.asyncio
async def test_legacy_start_retains_auto_release_handle_until_idle_cleanup(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    response = await service.start(
        session_id="owner", plan=_Plan({"slot": "a"}), auto_continue=False,
    )
    await _wait_for(backend.started)
    running = await db.get_execution(response["id"])
    handle_id = running["plan"]["retained_handle_ids"][0]
    handle = await db.get_session_resource_handle(handle_id)
    assert handle["auto_release_when_session_idle"] == 1
    assert response["id"] == running["id"]

    task = service._tasks[response["id"]]
    backend.release.set()
    await task
    assert (await db.get_session_resource_handle(handle_id))["state"] == "active"
    assert await resources.release_all_session_handles("owner") == [handle_id]
    assert (await db.get_session_resource_handle(handle_id))["state"] == "released"


@pytest.mark.asyncio
async def test_terminal_detachment_and_auto_release_policy_are_idempotent(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    explicit = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    legacy = (await resources.acquire_handles("owner", [{"pool": "b"}], auto_release_when_session_idle=True))[0]
    execution = await service.start(session_id="owner", plan=_Plan({"slot": "a"}), handle_ids=[explicit["id"]], auto_continue=False)
    await _wait_for(backend.started); task = service._tasks[execution["id"]]; backend.release.set(); await task
    assert await db.list_operation_resource_refs(execution["id"]) == []
    # A repeated terminal transition cannot recreate a reference or continuation.
    assert not await db.finish_execution(execution["id"], status="succeeded", result={})
    assert await resources.release_all_session_handles("owner") == [legacy["id"]]
    assert (await db.get_session_resource_handle(explicit["id"]))["state"] == "active"
    assert (await db.get_session_resource_handle(legacy["id"]))["state"] == "released"
