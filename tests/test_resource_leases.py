from __future__ import annotations
import asyncio
import pytest
from types import SimpleNamespace
from nerve.agent.tools import ToolContext, build_default_registry
from nerve.resources import LeaseService, ResourceInventory, ResourceInventoryError

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
async def test_quarantine_never_becomes_available_without_confirmed_recovery(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    lease=(await service.acquire(execution_id="one",session_id="s",requests=[{"pool":"build"}]))[0]
    await service.quarantine(execution_id="one",leases=[lease],reason="unknown remote state")
    waiting=asyncio.create_task(service.acquire(execution_id="two",session_id="s",requests=[{"pool":"test"}]))
    await asyncio.sleep(0.05)
    assert not waiting.done()
    with pytest.raises(ResourceInventoryError): await service.recover_host(host_id="host-a",requested_by="u",remote_quiescence_confirmed=False)
    await service.recover_host(host_id="host-a",requested_by="u",remote_quiescence_confirmed=True)
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
