from __future__ import annotations
import asyncio
import json
import pytest
from types import SimpleNamespace
from nerve.agent.tools import ToolContext, build_default_registry
from nerve.resources import (LeaseService, ResourceHandleConflictError, ResourceHandleOwnershipError,
                             ResourceInventory, ResourceInventoryError,
                             ResourceRecoveryGate, ResourceRecoveryUnavailable)

CONFIG={"connections":["lab-ssh"],"hosts":[{"id":"host-a","connection_ref":"lab-ssh","labels":{"rack":"a"}},{"id":"host-b","connection_ref":"lab-ssh","labels":{"rack":"b"}}],"pools":[{"id":"build","members":["host-a"]},{"id":"test","selector":{"rack":"a"}}]}

@pytest.mark.asyncio
async def test_overlapping_pools_are_exclusive_and_fenced(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    first=await service.acquire(execution_id="one",session_id="s1",requests=[{"pool":"build"}])
    waiting=asyncio.create_task(service.acquire(execution_id="two",session_id="s2",requests=[{"pool":"test"}]))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    await service.release(execution_id="one",leases=first)
    second=await asyncio.wait_for(waiting, 1)
    assert second[0]["host_id"] == "host-a" and second[0]["fencing_token"] > first[0]["fencing_token"]
    assert not await service.heartbeat(execution_id="one", lease=first[0])

@pytest.mark.asyncio
async def test_explicit_host_is_selected_from_its_pool(db):
    config = {"connections": ["lab-ssh"], "hosts": [
        {"id": "host-a", "connection_ref": "lab-ssh"}, {"id": "host-b", "connection_ref": "lab-ssh"}],
        "pools": [{"id": "workers", "members": ["host-a", "host-b"]}]}
    inventory = ResourceInventory(db, config); await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory)
    lease = await service.acquire(execution_id="pinned", session_id="s", requests=[{"pool": "workers", "host": "host-b"}])
    assert lease[0]["host_id"] == "host-b"
    with pytest.raises(ResourceInventoryError, match="not a member"):
        await service.acquire(execution_id="invalid", session_id="s", requests=[{"pool": "workers", "host": "missing"}])

@pytest.mark.asyncio
async def test_quarantine_never_becomes_available_without_confirmed_recovery(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    lease=(await service.acquire(execution_id="one",session_id="s",requests=[{"pool":"build"}]))[0]
    await service.quarantine(execution_id="one",leases=[lease],reason="unknown remote state")
    waiting=asyncio.create_task(service.acquire(execution_id="two",session_id="s",requests=[{"pool":"test"}]))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    with pytest.raises(ResourceInventoryError): await service.recover_host(host_id="host-a", requested_by="u")
    async def proved(*_): return True
    service._recovery_probe = proved
    await db.db.execute("UPDATE resource_hosts SET recovery_retry_at='2000-01-01T00:00:00+00:00' WHERE id='host-a'")
    await db.db.commit()
    await service.recover_host(host_id="host-a", requested_by="u")
    assert (await asyncio.wait_for(waiting, 1))[0]["host_id"] == "host-a"

@pytest.mark.asyncio
async def test_fifo_queue_survives_service_recreation(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize()
    first_service=LeaseService(db=db, inventory=inventory)
    held=await first_service.acquire(execution_id="held",session_id="s",requests=[{"slot":"worker","pool":"build"}])
    one=asyncio.create_task(first_service.acquire(execution_id="one",session_id="s",requests=[{"slot":"worker","pool":"build"}]))
    await asyncio.sleep(0.03)
    two=asyncio.create_task(first_service.acquire(execution_id="two",session_id="s",requests=[{"slot":"worker","pool":"build"}]))
    await asyncio.sleep(0.05)
    queue=await db.list_resource_requests()
    assert [(row["execution_id"], row["position"]) for row in queue] == [("one",1),("two",2)]
    await first_service.release(execution_id="held",leases=held)
    first_lease=await asyncio.wait_for(one, 1)
    assert not two.done()
    await first_service.release(execution_id="one",leases=first_lease)
    await asyncio.wait_for(two, 1)

@pytest.mark.asyncio
async def test_multi_slot_bundle_is_all_or_none_and_cannot_deadlock(db):
    config = {"connections":["lab-ssh"], "hosts":[
        {"id":"a", "connection_ref":"lab-ssh"}, {"id":"b", "connection_ref":"lab-ssh"}],
        "pools":[{"id":"a", "members":["a"]}, {"id":"b", "members":["b"]}]}
    inventory = ResourceInventory(db, config); await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory)
    # Reverse slot order was the old sequential-acquire deadlock shape.
    one = asyncio.create_task(service.acquire(execution_id="one", session_id="s1", requests=[{"slot":"left", "pool":"a"}, {"slot":"right", "pool":"b"}]))
    two = asyncio.create_task(service.acquire(execution_id="two", session_id="s2", requests=[{"slot":"right", "pool":"b"}, {"slot":"left", "pool":"a"}]))
    first = await asyncio.wait_for(one, 1)
    assert {lease["host_id"] for lease in first} == {"a", "b"}
    assert not two.done()
    await service.release(execution_id="one", leases=first)
    second = await asyncio.wait_for(two, 1)
    assert {lease["host_id"] for lease in second} == {"a", "b"}


@pytest.mark.asyncio
async def test_queued_bundle_is_reused_after_waiter_restart(db):
    inventory = ResourceInventory(db, CONFIG); await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory)
    held = await service.acquire(
        execution_id="blocker", session_id="blocker-session",
        requests=[{"slot": "worker", "pool": "build"}],
    )
    first = asyncio.create_task(service.acquire(
        execution_id="restarted", session_id="session-a",
        requests=[{"slot": "worker", "pool": "build"}],
    ))
    await asyncio.sleep(.02)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    resumed = asyncio.create_task(service.acquire(
        execution_id="restarted", session_id="session-a",
        requests=[{"slot": "worker", "pool": "build"}],
    ))
    await service.release(execution_id="blocker", leases=held)
    acquired = await asyncio.wait_for(resumed, 1)
    assert acquired[0]["slot"] == "worker"

@pytest.mark.asyncio
async def test_expiry_moves_to_revoking_and_never_frees_host(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    lease=(await service.acquire(execution_id="old",session_id="s",requests=[{"pool":"build"}]))[0]
    await db._write("UPDATE resource_leases SET expires_at='2000-01-01T00:00:00+00:00' WHERE id=?", (lease["id"],))
    expired=await service.reconcile_expired()
    assert expired[0]["state"] == "revoking"
    waiting=asyncio.create_task(service.acquire(execution_id="new",session_id="s",requests=[{"pool":"test"}]))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    waiting.cancel()
    await asyncio.gather(waiting, return_exceptions=True)

def test_inventory_rejects_raw_ssh_coordinates(db):
    bad={**CONFIG, "hosts":[{"id":"bad","connection_ref":"lab-ssh","hostname":"secret.example"}]}
    with pytest.raises(ResourceInventoryError): ResourceInventory(db, bad)

@pytest.mark.asyncio
async def test_resource_mcp_surface_is_secret_free_and_guarded(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    registry=build_default_registry(); ctx=ToolContext(session_id="admin",engine=SimpleNamespace(resource_service=service))
    listed=await registry.invoke("resource_inventory",ctx,{})
    assert "host-a" in listed.content[0]["text"] and "lab-ssh" not in listed.content[0]["text"]
    rejected=await registry.invoke("resource_host_quarantine",ctx,{"host_id":"host-a","confirm_host_id":"wrong","reason":"maintenance"})
    assert rejected.is_error
    accepted=await registry.invoke("resource_host_quarantine",ctx,{"host_id":"host-a","confirm_host_id":"host-a","reason":"maintenance"})
    assert not accepted.is_error
    assert (await db.get_resource_host("host-a"))["quarantined"] == 1


HANDLE_CONFIG = {
    "connections": ["lab-ssh"],
    "hosts": [
        {"id": "host-a", "connection_ref": "lab-ssh"},
        {"id": "host-b", "connection_ref": "lab-ssh"},
        {"id": "host-c", "connection_ref": "lab-ssh"},
    ],
    "pools": [
        {"id": "workers", "members": ["host-a", "host-b", "host-c"]},
        {"id": "only-a", "members": ["host-a"]},
        {"id": "only-b", "members": ["host-b"]},
    ],
}


async def _handle_service(db, *, ready=True):
    inventory = ResourceInventory(db, HANDLE_CONFIG)
    await inventory.initialize()
    for session_id in ("session-a", "session-b", "blocker"):
        await db.create_session(session_id)
    async def proved(*_): return True
    return LeaseService(db=db, inventory=inventory, recovery_gate=ResourceRecoveryGate(ready=ready), recovery_probe=proved)


@pytest.mark.asyncio
async def test_session_handles_are_opaque_and_one_session_can_hold_two_hosts(db):
    service = await _handle_service(db)
    handles = await service.acquire_handles("session-a", {
        "requests": [{"pool": "only-a"}, {"pool": "only-b"}],
    })

    assert len(handles) == 2
    assert all(set(handle) == {"id"} and handle["id"].startswith("handle-") for handle in handles)
    assert all("fencing_token" not in handle for handle in handles)
    assert all("lease_id" not in handle for handle in handles)
    assert all("host_id" not in handle for handle in handles)
    assert {item["id"] for item in await service.list_session_handles("session-a")} == {
        item["id"] for item in handles
    }
    resolved = await service.resolve_handle("session-a", handles[0]["id"])
    assert resolved == handles[0]
    internal = await service._resolve_handle_lease("session-a", handles[0]["id"])
    assert {"lease", "lease_id", "fencing_token", "host_id", "pool"} <= set(internal)
    assert "connection_ref" not in internal


@pytest.mark.asyncio
async def test_session_handle_resolution_rejects_foreign_and_unknown_handles_stably(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]

    for handle_id in (handle["id"], "handle-not-found"):
        with pytest.raises(ResourceHandleOwnershipError, match="does not belong to this session"):
            await service.resolve_handle("session-b", handle_id)
    assert await service.list_session_handles("session-b") == []


@pytest.mark.asyncio
async def test_handle_bundle_waits_without_creating_partial_new_handles_or_leases(db):
    service = await _handle_service(db)
    held = await service.acquire(execution_id="blocker", session_id="blocker", requests=[
        {"pool": "only-b"},
    ])
    request = asyncio.create_task(service.acquire_handles("session-a", {
        "requests": [{"pool": "only-a"}, {"pool": "only-b"}],
    }))
    await asyncio.sleep(.05)

    assert not request.done()
    assert await service.list_session_handles("session-a") == []
    assert [lease["host_id"] for lease in await db.list_resource_leases()
            if lease["state"] == "active"] == ["host-b"]
    await service.release(execution_id="blocker", leases=held)
    handles = await asyncio.wait_for(request, 1)
    assert len(handles) == 2
    assert {row["host_id"] for row in await service._active_session_handles("session-a")} == {
        "host-a", "host-b",
    }


@pytest.mark.asyncio
async def test_recovery_reacquires_only_missing_member_of_retained_bundle(db):
    service = await _handle_service(db)
    original = await service.acquire_handles("session-a", {
        "requests": [{"pool": "only-a"}, {"pool": "only-b"}],
    })
    before = await service._active_session_handles("session-a")
    failed = next(handle for handle in before if handle["host_id"] == "host-b")
    healthy = next(handle for handle in before if handle["host_id"] == "host-a")
    await service.quarantine(execution_id=failed["_lease"]["execution_id"],
                             leases=[failed["_lease"]], reason="transport lost")
    await service.recover_host(host_id="host-b", requested_by="operator")

    reacquired = await service.acquire_handles("session-a", {
        "requests": [{"pool": "only-a"}, {"pool": "only-b"}],
    })
    current = await service._active_session_handles("session-a")
    assert len(reacquired) == 2 and len(current) == 2
    assert next(handle for handle in current if handle["host_id"] == "host-a")["id"] == healthy["id"]
    assert {handle["id"] for handle in current} != {handle["id"] for handle in before}
    all_leases = await db.list_resource_leases()
    assert len([lease for lease in all_leases if lease["session_id"] == "session-a"]) == 3
    assert len([lease for lease in all_leases if lease["session_id"] == "session-a" and lease["state"] == "active"]) == 2
    assert original[0]["id"] in {handle["id"] for handle in await service.list_session_handles("session-a")}


@pytest.mark.asyncio
async def test_handle_acquisition_is_closed_by_recovery_gate_without_writes(db):
    service = await _handle_service(db, ready=False)
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await service.acquire_handles("session-a", [{"pool": "only-a"}])
    assert await db.list_session_resource_handles("session-a") == []
    assert await db.list_resource_requests() == []


@pytest.mark.asyncio
async def test_handle_spec_accepts_any_and_exact_hosts_without_transport_details(db):
    service = await _handle_service(db)
    handles = await service.acquire_handles("session-a", [
        {"pool": "workers", "host": "host-c"},
        {"pool": "only-a"},
    ])

    internal = [await service._resolve_handle_lease("session-a", handle["id"])
                for handle in handles]
    assert {handle["host_id"] for handle in internal} == {"host-a", "host-c"}
    assert all("connection_ref" not in handle for handle in internal)
    assert await service.acquire_handles("session-a", [
        {"pool": "workers", "host": "host-c"},
        {"pool": "only-a"},
    ]) == handles


@pytest.mark.asyncio
async def test_retained_handle_matching_reserves_exact_hosts_before_any_slots(db):
    service = await _handle_service(db)
    original = await service.acquire_handles("session-a", [
        {"pool": "workers"}, {"pool": "workers"},
    ])
    original_internal = [await service._resolve_handle_lease("session-a", handle["id"])
                         for handle in original]
    assert [item["host_id"] for item in original_internal] == ["host-a", "host-b"]
    lease_count = len(await db.list_resource_leases())

    reacquired = await asyncio.wait_for(service.acquire_handles("session-a", [
        {"pool": "workers"}, {"pool": "workers", "host": "host-a"},
    ]), 1)

    assert [item["id"] for item in reacquired] == [original[1]["id"], original[0]["id"]]
    assert len(await db.list_resource_leases()) == lease_count


@pytest.mark.asyncio
async def test_invalid_handle_specs_and_unknown_sessions_do_not_enqueue_requests(db):
    service = await _handle_service(db)
    invalid_specs = [
        {},
        {"requests": []},
        {"requests": [{"pool": "workers", "host": "missing"}]},
        {"requests": ["workers"]},
    ]
    for spec in invalid_specs:
        with pytest.raises(ResourceInventoryError):
            await service.acquire_handles("session-a", spec)
    with pytest.raises(ResourceInventoryError, match="unknown session"):
        await service.acquire_handles("missing-session", [{"pool": "only-a"}])
    assert await db.list_resource_requests() == []
    assert await db.list_session_resource_handles("session-a") == []


@pytest.mark.asyncio
async def test_handle_retention_failure_releases_the_entire_new_lease_bundle(db, monkeypatch):
    service = await _handle_service(db)

    async def fail_retention(handles):
        assert len(handles) == 2
        raise RuntimeError("durable handle write failed")

    monkeypatch.setattr(db, "create_session_resource_handles", fail_retention)
    with pytest.raises(RuntimeError, match="durable handle write failed"):
        await service.acquire_handles("session-a", [
            {"pool": "only-a"}, {"pool": "only-b"},
        ])
    assert await service.list_session_handles("session-a") == []
    session_leases = [lease for lease in await db.list_resource_leases()
                      if lease["session_id"] == "session-a"]
    assert len(session_leases) == 2
    assert {lease["state"] for lease in session_leases} == {"released"}


@pytest.mark.asyncio
async def test_handle_acquire_intent_covers_grant_before_handle_persistence(db, monkeypatch):
    service = await _handle_service(db)
    observed = {}

    async def fail_retention(handles):
        observed["intent"] = (await db.list_resource_recovery_intents())[0]
        observed["active_leases"] = [lease for lease in await db.list_resource_leases()
                                      if lease["state"] == "active"]
        raise RuntimeError("durable handle write failed")

    monkeypatch.setattr(db, "create_session_resource_handles", fail_retention)
    with pytest.raises(RuntimeError, match="durable handle write failed"):
        await service.acquire_handles("session-a", [{"pool": "only-a"}])

    assert observed["intent"]["state"] == "prepared"
    assert observed["intent"]["operation_id"] is None
    payload = json.loads(observed["intent"]["payload_json"])
    assert payload["allocator_execution_id"] == observed["active_leases"][0]["execution_id"]
    assert await db.get_execution(observed["active_leases"][0]["execution_id"]) is None
    intents = await db.list_resource_recovery_intents()
    assert len(intents) == 1 and intents[0]["state"] == "failed"
    assert all(lease["state"] == "released" for lease in await db.list_resource_leases())


@pytest.mark.asyncio
async def test_handle_list_and_resolution_hide_stale_recovered_lease(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    private = await service._resolve_handle_lease("session-a", handle["id"])
    await service.quarantine(execution_id=private["lease"]["execution_id"],
                             leases=[private["lease"]], reason="lost")
    await service.recover_host(host_id="host-a", requested_by="operator")

    assert await service.list_session_handles("session-a") == []
    with pytest.raises(ResourceHandleOwnershipError, match="does not belong"):
        await service.resolve_handle("session-a", handle["id"])


async def _handle_operation(db, operation_id: str, *, status: str = "queued") -> None:
    await db.create_execution(
        operation_id, session_id="session-a", kind="remote", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
    )
    if status != "queued":
        assert await db.commit_operation_terminal(
            operation_id=operation_id, status=status, result={"outcome": status},
        )


@pytest.mark.asyncio
async def test_operation_completion_detaches_refs_but_retains_handle_until_final_cleanup(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    private = await service._resolve_handle_lease("session-a", handle["id"])
    await _handle_operation(db, "completed-operation")
    assert await db.attach_operation_resource_ref("completed-operation", handle["id"])

    assert await db.commit_operation_terminal(
        operation_id="completed-operation", status="succeeded", result={"ok": True},
    )
    assert await db.list_operation_resource_refs("completed-operation") == []
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "active"
    assert (await db.get_resource_lease(private["lease_id"]))["state"] == "active"


@pytest.mark.asyncio
async def test_stopped_session_with_nonterminal_operation_retains_all_handles(db):
    service = await _handle_service(db)
    handles = await service.acquire_handles("session-a", [
        {"pool": "only-a"}, {"pool": "only-b"},
    ])
    await _handle_operation(db, "still-running")
    await db.update_session_fields("session-a", {"status": "stopped"})

    assert await service.release_all_session_handles("session-a") == []
    assert {item["id"] for item in await service.list_session_handles("session-a")} == {
        item["id"] for item in handles
    }
    assert await db.session_resource_is_live("session-a") is True


@pytest.mark.asyncio
async def test_active_session_durable_state_retains_handles_without_caller_flag(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    await db.update_session_fields("session-a", {"status": "active"})

    assert await service.release_all_session_handles("session-a") == []
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "active"
    assert await db.session_resource_is_live("session-a") is True


@pytest.mark.asyncio
async def test_stopped_resource_idle_session_releases_every_handle_idempotently(db):
    service = await _handle_service(db)
    handles = await service.acquire_handles("session-a", [
        {"pool": "only-a"}, {"pool": "only-b"},
    ], auto_release_when_session_idle=True)
    await db.update_session_fields("session-a", {"status": "stopped"})

    assert set(await service.release_all_session_handles("session-a")) == {
        item["id"] for item in handles
    }
    assert await service.release_all_session_handles("session-a") == []
    rows = await db.list_session_resource_handles("session-a")
    assert {row["state"] for row in rows} == {"released"}
    assert {lease["state"] for lease in await db.list_resource_leases()
            if lease["session_id"] == "session-a"} == {"released"}


@pytest.mark.asyncio
async def test_manual_release_rejects_referencing_operations_with_stable_ids(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    # Terminal Operations must detach refs before becoming terminal. The
    # active Operation below is the sole valid ref for this non-shareable
    # handle, so this test does not construct an invalid post-R10 state.
    await _handle_operation(db, "operation-z", status="failed")
    assert await db.list_operation_resource_refs("operation-z") == []
    await _handle_operation(db, "operation-a")
    assert await db.attach_operation_resource_ref("operation-a", handle["id"])

    with pytest.raises(ResourceHandleConflictError,
                       match="referenced by operations: operation-a") as exc:
        await service.release_handle("session-a", handle["id"])
    assert exc.value.operation_ids == ("operation-a",)
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "active"
    assert await db.list_resource_recovery_intents(state="processing") == []


@pytest.mark.asyncio
async def test_release_conflict_and_nonactive_retry_create_no_intent(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    before = await db.list_resource_recovery_intents()
    await _handle_operation(db, "conflict-operation")
    assert await db.attach_operation_resource_ref("conflict-operation", handle["id"])

    with pytest.raises(ResourceHandleConflictError):
        await service.release_handle("session-a", handle["id"])
    assert await db.list_resource_recovery_intents() == before

    await db.update_session_resource_handle(
        handle["id"], expected_state="active", state="released",
    )
    assert not await service.release_handle("session-a", handle["id"])
    assert await db.list_resource_recovery_intents() == before


@pytest.mark.asyncio
async def test_attach_after_atomic_begin_release_cannot_reference_handle(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    intent_id, conflicts = await db.begin_release_handle(
        "session-a", handle["id"], {
            "id": "intent-order-release", "payload": {"lease_id": "hidden"},
        },
    )
    assert intent_id == "intent-order-release"
    assert conflicts == []
    await _handle_operation(db, "late-operation")
    assert not await db.attach_operation_resource_ref("late-operation", handle["id"])
    assert await db.list_operation_resource_refs("late-operation") == []
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "releasing"


@pytest.mark.asyncio
async def test_unknown_handle_quiescence_quarantines_host_lease_and_handle(db, monkeypatch):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    private = await service._resolve_handle_lease("session-a", handle["id"])

    async def unknown_quiescence(_handle):
        return False

    monkeypatch.setattr(service, "_reconcile_handle_quiescence", unknown_quiescence)
    assert await service.release_handle("session-a", handle["id"])
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "quarantined"
    assert (await db.get_resource_lease(private["lease_id"]))["state"] == "quarantined"
    assert (await db.get_resource_host("host-a"))["quarantined"] == 1

    waiter = asyncio.create_task(service.acquire_handles("session-b", [{"pool": "only-a"}]))
    await asyncio.sleep(.05)
    assert not waiter.done(), "a quarantined host must not become allocatable"
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter


@pytest.mark.asyncio
async def test_resource_live_predicate_covers_resume_pending_and_resuming(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles(
        "session-a", [{"pool": "only-a"}], auto_release_when_session_idle=True,
    ))[0]
    await _handle_operation(db, "resume-operation", status="succeeded")

    pending = await db.get_execution("resume-operation")
    assert pending is not None and pending["continuation_state"] == "pending"
    assert await db.session_resource_is_live("session-a") is True
    assert await service.release_all_session_handles("session-a") == []

    claimed, resuming = await db.claim_execution_continuation("resume-operation")
    assert claimed and resuming is not None and resuming["continuation_state"] == "claimed"
    assert await db.session_resource_is_live("session-a") is True
    assert await service.release_all_session_handles("session-a") == []

    assert await db.settle_execution_continuation("resume-operation", success=True)
    assert await db.session_resource_is_live("session-a") is False
    assert await service.release_all_session_handles("session-a") == [handle["id"]]


@pytest.mark.asyncio
async def test_resource_live_predicate_covers_durable_lease_wait(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles(
        "session-a", [{"pool": "only-a"}], auto_release_when_session_idle=True,
    ))[0]
    await _handle_operation(db, "wait-operation", status="succeeded")
    claimed, _ = await db.claim_execution_continuation("wait-operation")
    assert claimed
    assert await db.settle_execution_continuation("wait-operation", success=True)
    await db.create_resource_wait_operation({
        "id": "durable-wait", "session_id": "session-a", "operation_id": "wait-operation",
        "request_kind": "pool", "pool": "only-a", "queue_ticket": 1,
    })

    assert await db.session_resource_is_live("session-a") is True
    assert await service.release_all_session_handles("session-a") == []
    assert await db.update_resource_wait_operation(
        "durable-wait", expected_state="pending", state="cancelled", outcome="REQUEST_CANCELLED",
    )
    assert await db.session_resource_is_live("session-a") is False
    assert await service.release_all_session_handles("session-a") == [handle["id"]]


@pytest.mark.asyncio
async def test_handle_release_lifecycle_mutations_are_closed_by_recovery_gate(db):
    service = await _handle_service(db, ready=False)
    active = LeaseService(
        db=db, inventory=service.inventory, recovery_gate=ResourceRecoveryGate(),
    )
    handle = (await active.acquire_handles("session-a", [{"pool": "only-a"}]))[0]

    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await service.release_handle("session-a", handle["id"])
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await service.release_all_session_handles("session-a")
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "active"


@pytest.mark.asyncio
async def test_manual_handle_release_is_fenced_terminal_and_idempotent(db):
    service = await _handle_service(db)
    handle = (await service.acquire_handles("session-a", [{"pool": "only-a"}]))[0]
    private = await service._resolve_handle_lease("session-a", handle["id"])

    assert await service.release_handle("session-a", handle["id"])
    assert not await service.release_handle("session-a", handle["id"])
    released = await db.get_session_resource_handle(handle["id"])
    assert released is not None
    assert released["state"] == "released"
    assert released["released_at"] is not None
    lease = await db.get_resource_lease(private["lease_id"])
    assert lease is not None and lease["state"] == "released"

    intents = await db.list_resource_recovery_intents()
    release_intents = [intent for intent in intents if intent["kind"] == "release"]
    assert len(release_intents) == 1
    assert release_intents[0]["state"] == "completed"
    assert release_intents[0]["handle_id"] == handle["id"]
