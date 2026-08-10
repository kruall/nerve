"""R10: one session may run disjoint explicit-handle Operations together."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from nerve.executions.backend import BackendRecovery, BackendResult
from nerve.executions.service import ExecutionService
from nerve.resources import ResourceHandleConflictError
from tests.test_execution_resource_handles import _Plan, _service, _wait_for


async def _wait_for_starts(backend, count: int) -> None:
    async with asyncio.timeout(2):
        while len(backend.plans) < count:
            await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_disjoint_explicit_handles_run_concurrently_and_activity_aggregates(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    first_handle, second_handle = await resources.acquire_handles(
        "owner", [{"pool": "a"}, {"pool": "b"}],
    )

    first, second = await asyncio.gather(
        service.start(session_id="owner", plan=_Plan({"slot": "a"}),
                      handle_ids=[first_handle["id"]], auto_continue=False),
        service.start(session_id="owner", plan=_Plan({"slot": "b"}),
                      handle_ids=[second_handle["id"]], auto_continue=False),
    )
    await _wait_for_starts(backend, 2)
    activity = await db.session_execution_activity(["owner"])
    assert activity["owner"]["active_execution_count"] == 2
    assert {first["id"], second["id"]} == set(service._tasks)

    backend.release.set()
    await asyncio.gather(*(service._tasks[execution_id] for execution_id in (first["id"], second["id"])))
    assert await resources.release_all_session_handles("owner") == []


@pytest.mark.asyncio
async def test_shared_handle_conflict_is_stable_before_second_backend_start(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    handle = (await resources.acquire_handles("owner", [{"pool": "a"}]))[0]
    first = await service.start(session_id="owner", plan=_Plan({"slot": "a"}),
                                handle_ids=[handle["id"]], auto_continue=False)
    await _wait_for(backend.started)

    with pytest.raises(ResourceHandleConflictError) as conflict:
        await service.start(session_id="owner", plan=_Plan({"slot": "a"}),
                            handle_ids=[handle["id"]], auto_continue=False)
    assert conflict.value.operation_ids == (first["id"],)
    assert len(backend.plans) == 1

    backend.release.set()
    await service._tasks[first["id"]]


@pytest.mark.asyncio
async def test_legacy_starts_remain_session_serialized_during_race(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    second_service = ExecutionService(
        db=db, engine=service.engine, workspace=service.workspace,
        catalog=service.catalog, backend=backend,
        resource_manager=resources, execution_root=service.execution_root,
    )
    await resources.acquire_handles("owner", [{"pool": "a"}])
    barrier = asyncio.Barrier(2)
    acquire_handles = resources.acquire_handles

    async def synchronized_acquire(*args, **kwargs):
        await barrier.wait()
        return await acquire_handles(*args, **kwargs)

    resources.acquire_handles = synchronized_acquire
    results = await asyncio.gather(
        service.start(session_id="owner", plan=_Plan({"slot": "a"}), auto_continue=False),
        second_service.start(session_id="owner", plan=_Plan({"slot": "a"}), auto_continue=False),
        return_exceptions=True,
    )
    winners = [result for result in results if isinstance(result, dict)]
    losers = [result for result in results if isinstance(result, ValueError)]
    assert len(winners) == 1 and len(losers) == 1
    assert str(losers[0]) == "session already owns an active execution"
    winner = winners[0]
    await _wait_for(backend.started)
    assert len(backend.plans) == 1
    assert await db.list_operation_resource_refs(winner["id"])
    handle_id = (await db.list_session_resource_handles("owner", states=("active",)))[0]["id"]
    assert (await db.get_session_resource_handle(handle_id))["state"] == "active"

    backend.release.set()
    await asyncio.gather(
        *[instance._tasks[winner["id"]]
          for instance in (service, second_service)
          if winner["id"] in instance._tasks]
    )


@pytest.mark.asyncio
async def test_session_stop_settles_every_concurrent_operation(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    handles = await resources.acquire_handles("owner", [{"pool": "a"}, {"pool": "b"}])
    executions = await asyncio.gather(
        service.start(session_id="owner", plan=_Plan({"slot": "a"}),
                      handle_ids=[handles[0]["id"]], auto_continue=False),
        service.start(session_id="owner", plan=_Plan({"slot": "b"}),
                      handle_ids=[handles[1]["id"]], auto_continue=False),
    )
    await _wait_for_starts(backend, 2)
    tasks = [service._tasks[execution["id"]] for execution in executions]

    assert await service.cancel_session("owner")
    await asyncio.gather(*tasks)
    assert (await db.session_execution_activity(["owner"]))["owner"]["active_execution_count"] == 0
    for execution in executions:
        assert await db.list_operation_resource_refs(execution["id"]) == []


@pytest.mark.asyncio
async def test_restart_reattaches_each_operation_without_a_second_backend_start(db, tmp_path):
    service, resources, backend = await _service(db, tmp_path)
    handles = await resources.acquire_handles("owner", [{"pool": "a"}, {"pool": "b"}])
    execution_ids = ("restart-a", "restart-b")
    for execution_id, handle, pool in zip(execution_ids, handles, ("a", "b"), strict=True):
        lease = await resources._resolve_handle_lease("owner", handle["id"])
        await db.create_execution(
            execution_id, session_id="owner", kind="test.remote", profile_version="1",
            profile_hash="test-hash", profile_snapshot={},
            plan={"retained_handle_ids": [handle["id"]], "resources": {"slot": pool}},
            resource_requests=[], handle_ids=[handle["id"]], auto_continue=False,
        )
        assert await db.transition_execution(
            execution_id, to_status="starting", expect=("queued",),
            fields={"selected_leases": [lease["lease"]]},
        )
        assert await db.transition_execution(execution_id, to_status="running", expect=("starting",))

    backend.recover = AsyncMock(return_value=BackendRecovery("reattachable"))
    backend.reattach = AsyncMock(return_value=BackendResult(0, summary="recovered"))
    await service.initialize(dispatch_continuations=False)
    await asyncio.gather(*(service._tasks[execution_id] for execution_id in execution_ids))

    assert backend.plans == []
    assert backend.recover.await_count == 2 and backend.reattach.await_count == 2
    for execution_id in execution_ids:
        assert (await db.get_execution(execution_id))["status"] == "succeeded"
        assert await db.list_operation_resource_refs(execution_id) == []
