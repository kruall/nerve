from __future__ import annotations

import asyncio

import pytest

from nerve.resources import LeaseService, ResourceInventory


async def _service(db):
    for host in ("a", "b"):
        await db.seed_resource_host({"id": host, "connection_ref": f"ssh://{host}"})
    return LeaseService(db=db, inventory=ResourceInventory(db, {"connections": ["ssh://a", "ssh://b"], "hosts": [
        {"id": "a", "connection_ref": "ssh://a"}, {"id": "b", "connection_ref": "ssh://b"},
    ], "pools": [{"id": "workers", "members": ["a", "b"]}], "queue_poll_seconds": .01}))


async def _operation(db, session):
    await db.create_session(session)
    await db.create_execution(f"op-{session}", session_id=session, kind="remote", profile_version="1",
                              profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[])
    return f"op-{session}"


async def _settled(db, wait_id):
    for _ in range(40):
        wait = await db.get_resource_wait_operation(wait_id)
        if wait and wait["state"] != "pending":
            return wait
        await asyncio.sleep(.01)
    return await db.get_resource_wait_operation(wait_id)


@pytest.mark.asyncio
async def test_wait_tickets_are_monotonic_and_exact_host_does_not_block_other_host(db):
    service = await _service(db)
    first, second = await _operation(db, "first"), await _operation(db, "second")
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}])
    a = await service.start_or_reattach_wait(session_id="first", operation_id=first, spec=[{"pool": "workers", "host": "a"}])
    b = await service.start_or_reattach_wait(session_id="second", operation_id=second, spec=[{"pool": "workers", "host": "b"}])
    await asyncio.sleep(.05)
    assert a["queue_ticket"] < b["queue_ticket"]
    assert (await db.get_resource_wait_operation(b["id"]))["outcome"] == "LEASE_GRANTED"
    assert (await db.get_resource_wait_operation(a["id"]))["state"] == "pending"
    await service.release(execution_id="holder", leases=held)
    await asyncio.sleep(.05)
    assert (await db.get_resource_wait_operation(a["id"]))["outcome"] == "LEASE_GRANTED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_retained_handle_cycle_terminalizes_highest_ticket_victim(db):
    service = await _service(db)
    one, two = await _operation(db, "one"), await _operation(db, "two")
    await service.acquire_handles("one", [{"pool": "workers", "host": "a"}])
    await service.acquire_handles("two", [{"pool": "workers", "host": "b"}])
    first = await service.start_or_reattach_wait(session_id="one", operation_id=one, spec=[{"pool": "workers", "host": "b"}])
    second = await service.start_or_reattach_wait(session_id="two", operation_id=two, spec=[{"pool": "workers", "host": "a"}])
    await asyncio.sleep(.1)
    loser = max((first, second), key=lambda row: (row["queue_ticket"], row["session_id"]))
    assert (await db.get_resource_wait_operation(loser["id"]))["outcome"] == "DEADLOCK_REPLAN_REQUIRED"
    assert not await db.list_session_resource_handles(loser["session_id"], states=("active",))
    survivor = first if loser["id"] == second["id"] else second
    assert (await _settled(db, survivor["id"]))["outcome"] == "LEASE_GRANTED"
    assert (await db.get_execution(loser["operation_id"]))["status"] == "failed"
    assert (await db.get_execution(survivor["operation_id"]))["status"] == "succeeded"
    await service.shutdown()


@pytest.mark.asyncio
async def test_same_host_waiter_never_overtakes_older_ticket_after_release(db):
    service = await _service(db)
    old_op, new_op = await _operation(db, "old"), await _operation(db, "new")
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}])
    old = await service.start_or_reattach_wait(session_id="old", operation_id=old_op, spec=[{"pool": "workers", "host": "a"}])
    new = await service.start_or_reattach_wait(session_id="new", operation_id=new_op, spec=[{"pool": "workers", "host": "a"}])
    assert old["queue_ticket"] < new["queue_ticket"]
    await service.release(execution_id="holder", leases=held)
    assert (await _settled(db, old["id"]))["outcome"] == "LEASE_GRANTED"
    assert (await db.get_resource_wait_operation(new["id"]))["state"] == "pending"
    handles = await db.list_session_resource_handles("old", states=("active",))
    await service.release_all_session_handles("old")
    # The completed wait still has a continuation, so explicit handle release
    # is the quiescent transition the model assumes before the next grant.
    for handle in handles:
        await service.release_handle("old", handle["id"])
    assert (await _settled(db, new["id"]))["outcome"] == "LEASE_GRANTED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_retained_wait_yields_to_older_exact_host_queue_request(db):
    service = await _service(db)
    old_op, retained_op = await _operation(db, "ordinary"), await _operation(db, "retained")
    held = await service.acquire(
        execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}],
    )
    ordinary = asyncio.create_task(service.acquire(
        execution_id=old_op, session_id="ordinary", requests=[{"pool": "workers", "host": "a"}],
    ))
    for _ in range(40):
        if await db.list_resource_requests():
            break
        await asyncio.sleep(.01)
    queued = await db.list_resource_requests()
    assert queued[0]["requested_host"] == "a"
    retained = await service.start_or_reattach_wait(
        session_id="retained", operation_id=retained_op, spec=[{"pool": "workers", "host": "a"}],
    )
    await service.release(execution_id="holder", leases=held)
    ordinary_leases = await asyncio.wait_for(ordinary, 1)
    await asyncio.sleep(.03)
    assert (await db.get_resource_wait_operation(retained["id"]))["state"] == "pending"
    await service.release(execution_id=old_op, leases=ordinary_leases)
    assert (await _settled(db, retained["id"]))["outcome"] == "LEASE_GRANTED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_older_retained_wait_blocks_newer_ordinary_request_only_on_same_host(db):
    service = await _service(db)
    retained_op = await _operation(db, "retained-first")
    ordinary_op = await _operation(db, "ordinary-next")
    other_op = await _operation(db, "ordinary-other")
    held = await service.acquire(execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}])
    retained = await service.start_or_reattach_wait(session_id="retained-first", operation_id=retained_op, spec=[{"pool": "workers", "host": "a"}])
    ordinary = asyncio.create_task(service.acquire(execution_id=ordinary_op, session_id="ordinary-next", requests=[{"pool": "workers", "host": "a"}]))
    other = await asyncio.wait_for(service.acquire(execution_id=other_op, session_id="ordinary-other", requests=[{"pool": "workers", "host": "b"}]), 1)
    queued = [row for row in await db.list_resource_requests() if row["execution_id"] == ordinary_op]
    assert queued and retained["queue_ticket"] < queued[0]["queue_ticket"]
    await service.release(execution_id="holder", leases=held)
    assert (await _settled(db, retained["id"]))["outcome"] == "LEASE_GRANTED"
    assert not ordinary.done()
    for handle in await db.list_session_resource_handles("retained-first", states=("active",)):
        await service.release_handle("retained-first", handle["id"])
    await asyncio.wait_for(ordinary, 1)
    await service.release(execution_id=other_op, leases=other)
    await service.shutdown()


@pytest.mark.asyncio
async def test_deadlock_tie_breaker_uses_largest_session_id(db):
    service = await _service(db)
    left, right = await _operation(db, "a-session"), await _operation(db, "z-session")
    await service.acquire_handles("a-session", [{"pool": "workers", "host": "a"}])
    await service.acquire_handles("z-session", [{"pool": "workers", "host": "b"}])
    # Equal persisted tickets model a recovered replan bundle.  The SQL
    # allocator accepts this only as an explicit old ticket, never normally.
    a = await db.create_resource_wait_operation({
        "id": "wait-a", "session_id": "a-session", "operation_id": left,
        "request_kind": "host", "requested_hosts": [{"pool": "workers", "host": "b"}],
        "pool": "workers", "queue_ticket": 900,
    })
    z = await db.create_resource_wait_operation({
        "id": "wait-z", "session_id": "z-session", "operation_id": right,
        "request_kind": "host", "requested_hosts": [{"pool": "workers", "host": "a"}],
        "pool": "workers", "queue_ticket": 900,
    })
    victim = await service._deadlock_victim(z)
    assert victim is not None and victim["id"] == z["id"] and a["queue_ticket"] == z["queue_ticket"]
    await service.mark_wait_deadlock_replan_required(z["id"])
    assert (await db.get_resource_wait_operation(z["id"]))["outcome"] == "DEADLOCK_REPLAN_REQUIRED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_deadlock_graph_keeps_multiple_waits_for_one_session(db):
    service = await _service(db)
    one, two = await _operation(db, "multi-one"), await _operation(db, "multi-two")
    assert await db.commit_operation_terminal(operation_id=one, status="succeeded", result={})
    await db.create_execution(
        "op-multi-one-second", session_id="multi-one", kind="remote", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
    )
    await service.acquire_handles("multi-one", [{"pool": "workers", "host": "a"}])
    await service.acquire_handles("multi-two", [{"pool": "workers", "host": "b"}])
    # The cycle-causing wait is inserted first.  A session-only map would
    # overwrite it with the later, non-cycle wait and lose the edge.
    await db.create_resource_wait_operation({
        "id": "multi-one-cycle", "session_id": "multi-one", "operation_id": one,
        "request_kind": "host", "requested_hosts": [{"pool": "workers", "host": "b"}],
        "pool": "workers", "queue_ticket": 1,
    })
    await db.create_resource_wait_operation({
        "id": "multi-one-own", "session_id": "multi-one", "operation_id": "op-multi-one-second",
        "request_kind": "host", "requested_hosts": [{"pool": "workers", "host": "a"}],
        "pool": "workers", "queue_ticket": 2,
    })
    other = await db.create_resource_wait_operation({
        "id": "multi-two-cycle", "session_id": "multi-two", "operation_id": two,
        "request_kind": "host", "requested_hosts": [{"pool": "workers", "host": "a"}],
        "pool": "workers", "queue_ticket": 3,
    })
    victim = await service._deadlock_victim(other)
    assert victim is not None and victim["id"] == other["id"]
    await service.shutdown()


@pytest.mark.asyncio
async def test_reopened_allocator_keeps_ticket_monotonic_for_new_waits(db):
    service = await _service(db)
    first = await _operation(db, "persist-one")
    wait = await service.start_or_reattach_wait(session_id="persist-one", operation_id=first, spec=[{"pool": "workers", "host": "a"}])
    await _settled(db, wait["id"])
    await db.close()
    await db.connect()
    second = await _operation(db, "persist-two")
    later = await service.start_or_reattach_wait(session_id="persist-two", operation_id=second, spec=[{"pool": "workers", "host": "b"}])
    assert later["queue_ticket"] > wait["queue_ticket"]
    await service.shutdown()


@pytest.mark.asyncio
async def test_quiescence_ref_prevents_deadlock_victim_handle_release(db):
    service = await _service(db)
    operation = await _operation(db, "referenced")
    handles = await service.acquire_handles("referenced", [{"pool": "workers", "host": "a"}])
    handle = await db.get_session_resource_handle(handles[0]["id"])
    assert handle is not None and await db.attach_operation_resource_ref(operation, handle["id"])
    await service._release_deadlock_victim_handles("referenced")
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "active"
    assert await db.detach_operation_resource_refs(operation) == 1
    await service._release_deadlock_victim_handles("referenced")
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "released"
    await service.shutdown()


@pytest.mark.asyncio
async def test_bundle_ticket_remains_stable_when_a_wait_poller_is_reattached(db):
    service = await _service(db)
    operation = await _operation(db, "bundle")
    held = await service.acquire(
        execution_id="holder", session_id="holder",
        requests=[{"slot": "a", "pool": "workers", "host": "a"}, {"slot": "b", "pool": "workers", "host": "b"}],
    )
    wait = await service.start_or_reattach_wait(
        session_id="bundle", operation_id=operation,
        spec=[{"pool": "workers", "host": "a"}, {"pool": "workers", "host": "b"}],
    )
    task = service._wait_tasks[wait["id"]]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    reattached = await service.start_or_reattach_wait(
        session_id="bundle", operation_id=operation,
        spec=[{"pool": "workers", "host": "a"}, {"pool": "workers", "host": "b"}],
    )
    assert reattached["id"] == wait["id"]
    assert reattached["queue_ticket"] == wait["queue_ticket"]
    assert len(await db.list_resource_wait_operations(state="pending")) == 1
    await service.release(execution_id="holder", leases=held)
    assert (await _settled(db, wait["id"]))["outcome"] == "LEASE_GRANTED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_nonconflicting_waiter_progresses_while_older_exact_host_waits(db):
    service = await _service(db)
    blocked_op = await _operation(db, "blocked")
    free_op = await _operation(db, "free")
    held = await service.acquire(
        execution_id="holder", session_id="holder", requests=[{"pool": "workers", "host": "a"}],
    )
    blocked = await service.start_or_reattach_wait(
        session_id="blocked", operation_id=blocked_op, spec=[{"pool": "workers", "host": "a"}],
    )
    free = await service.start_or_reattach_wait(
        session_id="free", operation_id=free_op, spec=[{"pool": "workers", "host": "b"}],
    )
    free_terminal = await _settled(db, free["id"])
    assert free_terminal is not None and free_terminal["outcome"] == "LEASE_GRANTED"
    assert (await db.get_resource_wait_operation(blocked["id"]))["state"] == "pending"
    await service.release(execution_id="holder", leases=held)
    blocked_terminal = await _settled(db, blocked["id"])
    assert blocked_terminal is not None and blocked_terminal["outcome"] == "LEASE_GRANTED"
    await service.shutdown()


@pytest.mark.asyncio
async def test_pool_wait_conflicts_with_each_member_but_not_a_disjoint_pool(db):
    service = await _service(db)
    service.inventory.pools["other"] = {"id": "other", "members": []}
    # The inventory API intentionally derives members from its static pool
    # specification; add a distinct host to exercise its host-set comparison.
    await db.seed_resource_host({"id": "c", "connection_ref": "ssh://c"})
    service.inventory.connections.add("ssh://c")
    service.inventory.hosts["c"] = {"id": "c", "connection_ref": "ssh://c"}
    service.inventory.pools["other"] = {"id": "other", "members": ["c"]}
    broad_op = await _operation(db, "broad")
    other_op = await _operation(db, "other")
    broad = await service.start_or_reattach_wait(
        session_id="broad", operation_id=broad_op, spec=[{"pool": "workers"}],
    )
    other = await service.start_or_reattach_wait(
        session_id="other", operation_id=other_op, spec=[{"pool": "other", "host": "c"}],
    )
    assert service._wait_hosts(broad) == {"a", "b"}
    assert service._wait_hosts(other) == {"c"}
    assert await service._wait_is_fair(other)
    await service.cancel_wait(broad["id"])
    await service.cancel_wait(other["id"])
    await service.shutdown()


@pytest.mark.asyncio
async def test_quiescent_replan_resubscribes_complete_bundle_with_old_ticket(db):
    service = await _service(db)
    victim_op = await _operation(db, "victim")
    other_op = await _operation(db, "other")
    await service.acquire_handles("victim", [{"pool": "workers", "host": "a"}])
    await service.acquire_handles("other", [{"pool": "workers", "host": "b"}])
    await service.start_or_reattach_wait(
        session_id="other", operation_id=other_op, spec=[{"pool": "workers", "host": "a"}],
    )
    old = await service.start_or_reattach_wait(
        session_id="victim", operation_id=victim_op,
        spec=[{"pool": "workers", "host": "a"}, {"pool": "workers", "host": "b"}],
    )
    assert (await _settled(db, old["id"]))["outcome"] == "DEADLOCK_REPLAN_REQUIRED"
    assert not await db.list_session_resource_handles("victim", states=("active",))
    assert (await _settled(db, (await db.list_resource_wait_operations(state="pending"))[0]["id"]))["outcome"] == "LEASE_GRANTED"
    for handle in await db.list_session_resource_handles("other", states=("active",)):
        await service.release_handle("other", handle["id"])
    await db.create_execution("op-victim-replan", session_id="victim", kind="remote", profile_version="1",
                              profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[])
    replan = await service.replan_deadlock_wait(
        session_id="victim", operation_id="op-victim-replan", deadlocked_wait_id=old["id"],
    )
    assert replan["queue_ticket"] == old["queue_ticket"]
    assert replan["requested_hosts"] == old["requested_hosts"]
    assert len(await db.list_resource_wait_operations(state="pending")) == 1
    assert (await _settled(db, replan["id"]))["outcome"] == "LEASE_GRANTED"
    await service.shutdown()
