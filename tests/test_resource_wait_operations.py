"""R6 durable retained-handle wait operation contracts."""
import asyncio
import sqlite3
from types import SimpleNamespace
import pytest
from nerve.resources import LeaseService, ResourceInventory, ResourceRecoveryGate
CONFIG = {"connections": ["lab"], "hosts": [{"id": "a", "connection_ref": "lab"}, {"id": "b", "connection_ref": "lab"}], "pools": [{"id": "workers", "members": ["a", "b"]}]}
async def _service(db, recovery_gate=None):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    return LeaseService(db=db, inventory=inventory, recovery_gate=recovery_gate)
async def _operation(db, ident: str):
    await db.create_session(ident)
    return await db.create_execution(f"op-{ident}", session_id=ident, kind="remote", profile_version="1", profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[])
async def _settled(db, wait_id: str):
    for _ in range(100):
        wait = await db.get_resource_wait_operation(wait_id)
        if wait and wait["state"] != "pending":
            return wait
        await asyncio.sleep(.01)
    raise AssertionError("wait did not settle")
@pytest.mark.asyncio
async def test_grant_persists_handles_before_one_continuation_claim(db):
    service = await _service(db)
    operation = await _operation(db, "one")
    wait = await service.start_or_reattach_wait(
        session_id="one", operation_id=operation["id"], spec=[{"pool": "workers", "host": "a"}],
    )

    settled = await _settled(db, wait["id"])
    assert settled["outcome"] == "LEASE_GRANTED" and settled["wakeup_generation"] == 1
    handles = await db.list_session_resource_handles("one", states=("active",))
    assert len(handles) == 1 and handles[0]["host_id"] == "a"
    claimed, resumed = await db.claim_execution_continuation(operation["id"])
    assert claimed and resumed is not None
    assert await db.get_session_resource_handle(handles[0]["id"]) is not None
    assert not (await db.claim_execution_continuation(operation["id"]))[0]
    assert await service.complete_wait_grant(wait["id"], handles)  # duplicate callback
    assert (await db.get_resource_wait_operation(wait["id"]))["wakeup_generation"] == 1
    assert not await service.cancel_wait(wait["id"])  # a later different outcome loses
    assert (await db.get_execution(operation["id"]))["result"]["outcome"] == "LEASE_GRANTED"
@pytest.mark.asyncio
async def test_cancel_removes_queue_edge_keeps_retained_liveness_and_wakes_once(db):
    service = await _service(db)
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}])
    operation = await _operation(db, "cancel")
    wait = await service.start_or_reattach_wait(
        session_id="cancel", operation_id=operation["id"], spec=[{"pool": "workers", "host": "a"}],
    )
    await asyncio.sleep(.03)
    assert await db.session_resource_is_live("cancel")
    assert await service.cancel_wait(wait["id"])
    assert await service.cancel_wait(wait["id"])
    settled = await db.get_resource_wait_operation(wait["id"])
    assert settled["outcome"] == "REQUEST_CANCELLED" and settled["wakeup_generation"] == 1
    operation = await db.get_execution("op-cancel")
    assert operation["status"] == "cancelled"
    assert operation["result"] == {"outcome": "REQUEST_CANCELLED", "wait_id": wait["id"], "generation": 1}
    assert await db.session_resource_is_live("cancel")
    assert not [row for row in await db.list_resource_requests() if row["execution_id"] == operation["id"]]
    assert (await db.claim_execution_continuation(operation["id"]))[0]
    assert not (await db.claim_execution_continuation(operation["id"]))[0]
    await db.settle_execution_continuation(operation["id"], success=True)
    assert not await db.session_resource_is_live("cancel")
    await service.release(execution_id="holder", leases=held)
@pytest.mark.asyncio
async def test_restart_reattaches_same_durable_queue_without_duplicate_edge_or_wakeup(db):
    first = await _service(db)
    held = await first.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}])
    operation = await _operation(db, "restart")
    wait = await first.start_or_reattach_wait(
        session_id="restart", operation_id=operation["id"], spec=[{"pool": "workers", "host": "a"}],
    )
    await asyncio.sleep(.03)
    await first.shutdown()
    queued_before = await db.list_resource_requests()
    gate = ResourceRecoveryGate(ready=False)
    second = LeaseService(db=db, inventory=first.inventory, recovery_gate=gate)
    calls = 0
    original_acquire = second.acquire
    async def tracked_acquire(*args, **kwargs):
        nonlocal calls; calls += 1
        return await original_acquire(*args, **kwargs)
    second.acquire = tracked_acquire
    started, proceed = asyncio.Event(), asyncio.Event()
    original_recovery = second._recover_startup
    async def blocked_recovery():
        started.set(); await proceed.wait()
        await original_recovery()
    second._recover_startup = blocked_recovery
    opening = asyncio.create_task(gate.open(second, SimpleNamespace(_recover_startup=lambda **_: asyncio.sleep(0)), dispatch_continuations=False))
    await started.wait(); assert not gate.ready and calls == 0 and not second._wait_tasks
    proceed.set()
    await opening
    assert len(await db.list_resource_requests()) == len(queued_before) == 1
    await second.release(execution_id="holder", leases=held)
    settled = await _settled(db, wait["id"])
    assert settled["outcome"] == "LEASE_GRANTED" and settled["wakeup_generation"] == 1
    await second.shutdown()
@pytest.mark.asyncio
async def test_start_or_reattach_returns_one_pending_generation_and_retains_handles(db):
    service = await _service(db)
    await db.create_session("retained")
    retained = await service.acquire_handles("retained", [{"pool": "workers", "host": "a"}])
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "b"}])
    operation = await _operation(db, "reattach")
    first = await service.start_or_reattach_wait(session_id="reattach", operation_id=operation["id"], spec=[{"pool": "workers", "host": "b"}])
    second = await service.start_or_reattach_wait(session_id="reattach", operation_id=operation["id"], spec=[{"pool": "workers", "host": "b"}])
    assert first["id"] == second["id"]
    assert len(await db.list_resource_wait_operations(state="pending")) == 1
    assert (await db.list_session_resource_handles("retained", states=("active",)))[0]["id"] == retained[0]["id"]
    assert await db.session_resource_is_live("reattach")
    await service.cancel_wait(first["id"])
    await service.release_all_session_handles("retained")
    await service.release(execution_id="holder", leases=held)
@pytest.mark.asyncio
async def test_concurrent_start_or_reattach_serializes_one_wait_and_poller(db):
    service = await _service(db)
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"slot": "held", "pool": "workers", "host": "a"}])
    operation = await _operation(db, "concurrent")
    waits = await asyncio.gather(*[
        service.start_or_reattach_wait(
            session_id="concurrent", operation_id=operation["id"],
            spec=[{"pool": "workers", "host": "a"}],
        )
        for _ in range(2)
    ])
    assert waits[0]["id"] == waits[1]["id"]
    assert len(await db.list_resource_wait_operations(state="pending")) == 1
    assert len(service._wait_tasks) == 1
    await service.cancel_wait(waits[0]["id"])
    await service.release(execution_id="holder", leases=held)


@pytest.mark.asyncio
async def test_wait_poller_does_not_swallow_deadlock_detector_errors(db):
    service = await _service(db)
    operation = await _operation(db, "deadlock-error")
    wait = await service.start_or_reattach_wait(
        session_id="deadlock-error", operation_id=operation["id"],
        spec=[{"pool": "workers", "host": "a"}],
    )

    async def broken_deadlock_detector(_wait):
        raise RuntimeError("injected deadlock detector failure")

    service._deadlock_victim = broken_deadlock_detector
    task = service._wait_tasks[wait["id"]]
    with pytest.raises(RuntimeError, match="injected deadlock detector failure"):
        await task
    assert wait["id"] not in service._wait_tasks


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ValueError("Connection closed"), sqlite3.ProgrammingError("closed database")],
)
async def test_wait_poller_ignores_closed_database_teardown_errors(error):
    class ClosedDatabase:
        async def get_resource_wait_operation(self, _wait_id):
            raise error

    database = ClosedDatabase()
    service = LeaseService(db=database, inventory=ResourceInventory(database, CONFIG))
    await service._run_wait("closed-database")


@pytest.mark.asyncio
async def test_permanent_loss_only_settles_exact_host_and_deadlock_hook_is_terminal(db):
    service = await _service(db)
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"slot": "a", "pool": "workers", "host": "a"}, {"slot": "b", "pool": "workers", "host": "b"}])
    exact_op, pool_op = await _operation(db, "exact"), await _operation(db, "pool")
    exact = await service.start_or_reattach_wait(session_id="exact", operation_id=exact_op["id"], spec=[{"pool": "workers", "host": "a"}])
    pool = await service.start_or_reattach_wait(session_id="pool", operation_id=pool_op["id"], spec=[{"pool": "workers"}])
    await asyncio.sleep(.03)
    assert await service.terminalize_exact_host_waits("a") == [exact["id"]]
    assert (await db.get_resource_wait_operation(exact["id"]))["outcome"] == "HOST_PERMANENTLY_UNAVAILABLE"
    assert (await db.get_execution(exact_op["id"]))["status"] == "failed"
    assert (await db.get_resource_wait_operation(pool["id"]))["state"] == "pending"
    assert await service.mark_wait_deadlock_replan_required(pool["id"])
    assert await service.mark_wait_deadlock_replan_required(pool["id"])
    assert (await db.get_resource_wait_operation(pool["id"]))["outcome"] == "DEADLOCK_REPLAN_REQUIRED"
    assert (await db.get_execution(pool_op["id"]))["status"] == "failed"
    assert not await service.mark_wait_deadlock_replan_required("missing-wait")
    await service.release(execution_id="holder", leases=held)
