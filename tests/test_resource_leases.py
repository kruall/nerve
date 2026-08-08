from __future__ import annotations
import asyncio
import pytest
from nerve.resources import LeaseService, ResourceInventory, ResourceInventoryError

CONFIG={"connections":["lab-ssh"],"hosts":[{"id":"host-a","connection_ref":"lab-ssh","labels":{"rack":"a"}},{"id":"host-b","connection_ref":"lab-ssh","labels":{"rack":"b"}}],"pools":[{"id":"build","members":["host-a"]},{"id":"test","selector":{"rack":"a"}}]}

@pytest.mark.asyncio
async def test_overlapping_pools_are_exclusive_and_fenced(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    first=await service.acquire(execution_id="one",session_id="s1",requests=[{"pool":"build"}])
    with pytest.raises(ResourceInventoryError): await service.acquire(execution_id="two",session_id="s2",requests=[{"pool":"test"}])
    await service.release(execution_id="one",leases=first)
    second=await service.acquire(execution_id="two",session_id="s2",requests=[{"pool":"test"}])
    assert second[0]["host_id"] == "host-a" and second[0]["fencing_token"] > first[0]["fencing_token"]
    assert not await service.heartbeat(execution_id="one", lease=first[0])

@pytest.mark.asyncio
async def test_quarantine_never_becomes_available_without_confirmed_recovery(db):
    inventory=ResourceInventory(db, CONFIG); await inventory.initialize(); service=LeaseService(db=db, inventory=inventory)
    lease=(await service.acquire(execution_id="one",session_id="s",requests=[{"pool":"build"}]))[0]
    await service.quarantine(execution_id="one",leases=[lease],reason="unknown remote state")
    with pytest.raises(ResourceInventoryError): await service.acquire(execution_id="two",session_id="s",requests=[{"pool":"test"}])
    with pytest.raises(ResourceInventoryError): await service.recover_host(host_id="host-a",requested_by="u",remote_quiescence_confirmed=False)
    await service.recover_host(host_id="host-a",requested_by="u",remote_quiescence_confirmed=True)
    assert (await service.acquire(execution_id="two",session_id="s",requests=[{"pool":"test"}]))[0]["host_id"] == "host-a"

def test_inventory_rejects_raw_ssh_coordinates(db):
    bad={**CONFIG, "hosts":[{"id":"bad","connection_ref":"lab-ssh","hostname":"secret.example"}]}
    with pytest.raises(ResourceInventoryError): ResourceInventory(db, bad)
