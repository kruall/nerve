"""Final stop is a durable no-resume boundary for retained resources."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from nerve.executions.service import ExecutionService
from nerve.resources import LeaseService, ResourceInventory


class _Backend:
    name = "recording"

    async def cancel(self, **_kwargs):
        return True


async def _service(db, tmp_path):
    inventory = ResourceInventory(db, {
        "connections": ["test"],
        "hosts": [{"id": "host", "connection_ref": "test"}],
        "pools": [{"id": "pool", "members": ["host"]}],
    })
    await inventory.initialize()
    await db.create_session("owner")
    resources = LeaseService(db=db, inventory=inventory)
    service = ExecutionService(
        db=db, engine=SimpleNamespace(is_session_running=lambda _session_id: False), workspace=Path(tmp_path),
        catalog=SimpleNamespace(), backend=_Backend(), resource_manager=resources,
        execution_root=Path(tmp_path) / "executions",
    )
    return service, resources


@pytest.mark.asyncio
async def test_final_stop_suppresses_wait_and_continuation_then_releases_handles(db, tmp_path):
    service, resources = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "pool"}]))[0]
    operation = await db.create_execution(
        "operation", session_id="owner", kind="remote", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
        handle_ids=[handle["id"]], auto_continue=True,
    )
    wait = await db.create_resource_wait_operation({
        "id": "wait", "session_id": "owner", "operation_id": operation["id"],
        "request_kind": "pool", "pool": "pool", "queue_ticket": 1,
    })

    assert await service.final_stop_session("owner")
    assert (await db.get_session("owner"))["status"] == "stopped"
    assert (await db.get_resource_wait_operation(wait["id"]))["outcome"] == "REQUEST_CANCELLED"
    final = await db.get_execution(operation["id"])
    assert final["status"] == "cancelled"
    assert final["continuation_state"] == "suppressed"
    assert not (await db.claim_execution_continuation(operation["id"]))[0]
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "released"
    handle_row = await db.get_session_resource_handle(handle["id"])
    assert handle_row is not None
    assert (await db.get_resource_lease(handle_row["lease_id"]))["state"] == "released"


@pytest.mark.asyncio
async def test_final_stop_is_idempotent_after_completion_race(db, tmp_path):
    service, resources = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "pool"}]))[0]
    operation = await db.create_execution(
        "completed", session_id="owner", kind="remote", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
        handle_ids=[handle["id"]], auto_continue=True,
    )
    assert await db.commit_operation_terminal(
        operation_id=operation["id"], status="succeeded", result={},
    )

    await service.final_stop_session("owner")
    await service.final_stop_session("owner")
    final = await db.get_execution(operation["id"])
    assert final["continuation_state"] == "suppressed"
    assert not (await db.claim_execution_continuation(operation["id"]))[0]
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "released"
