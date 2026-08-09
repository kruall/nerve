from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nerve.resources import LeaseService, ResourceInventory, ResourceInventoryError

CONFIG = {
    "connections": ["lab"],
    "hosts": [{"id": "builder-1", "connection_ref": "lab"}],
    "pools": [{"id": "builders", "members": ["builder-1"]}],
}


async def _service(db):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    return LeaseService(db=db, inventory=inventory)


@pytest.mark.asyncio
async def test_session_reservation_is_durable_and_pins_canonical_worktree(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    first = await service.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path / "worktree" / ".." / "worktree",
    )
    resumed = await (await _service(db)).reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path / "worktree",
    )
    assert resumed["host_id"] == first["host_id"]
    with pytest.raises(ResourceInventoryError, match="different worktree"):
        await service.reserve_for_session(session_id="session-a", pool="builders", worktree=tmp_path / "other")


@pytest.mark.asyncio
async def test_session_resource_reservation_cleanup_releases_when_idle(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")

    reservation = await service.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path / "worktree",
    )
    assert reservation["state"] == "active"

    assert await service.cleanup_session_reservation("session-a")
    assert (await db.get_resource_host("builder-1"))["quarantined"] == 0
    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None
    assert settled["state"] == "released"
    lease = await db.get_resource_lease(settled["lease_id"])
    assert lease is not None and lease["state"] == "released"


@pytest.mark.asyncio
async def test_session_commands_serialize_and_uncertain_cleanup_quarantines(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first():
        async with service.use_session_reservation(session_id="session-a", pool="builders", worktree=tmp_path):
            entered.set()
            await release.wait()

    second_entered = asyncio.Event()
    async def second():
        async with service.use_session_reservation(session_id="session-a", pool="builders", worktree=tmp_path):
            second_entered.set()

    task = asyncio.create_task(first())
    await entered.wait()
    queued = asyncio.create_task(second())
    await asyncio.sleep(0)
    assert not second_entered.is_set()
    assert await service.cleanup_session_reservation("session-a")
    assert (await db.get_resource_host("builder-1"))["quarantined"] == 1
    release.set()
    await task
    with pytest.raises(ResourceInventoryError, match="settled"):
        await queued


@pytest.mark.asyncio
async def test_session_resource_reservation_cleanup_after_recovery_quarantines(db, tmp_path):
    first = await _service(db)
    await db.create_session("session-a")
    await first.reserve_for_session(session_id="session-a", pool="builders", worktree=tmp_path)
    second = await _service(db)
    await asyncio.sleep(0)

    assert await second.cleanup_session_reservation("session-a")
    assert (await db.get_resource_host("builder-1"))["quarantined"] == 1
    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None
    assert settled["state"] == "quarantined"


@pytest.mark.asyncio
async def test_session_reservation_can_reacquire_after_explicit_release(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    first = await service.reserve_for_session(session_id="session-a", pool="builders", worktree=tmp_path)
    assert first["state"] == "active"

    assert await service.release_session_reservation(
        session_id="session-a", remote_quiescence_confirmed=True,
        reason="test explicit release",
    )
    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None
    assert settled["state"] == "released"

    second = await service.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path,
    )
    assert second["state"] == "active"
    assert second["lease"]["state"] == "active"
    assert second["lease_id"] != first["lease_id"]


@pytest.mark.asyncio
async def test_session_reservation_can_reacquire_after_operator_recovery(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    await service.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path,
    )
    assert await service.release_session_reservation(
        session_id="session-a",
        remote_quiescence_confirmed=False,
        reason="uncertain remote state",
    )
    assert (await db.get_session_resource_reservation("session-a"))["state"] == "quarantined"

    await service.recover_host(
        host_id="builder-1",
        requested_by="operator",
        remote_quiescence_confirmed=True,
    )
    recovered = await db.get_session_resource_reservation("session-a")
    assert recovered["state"] == "released"
    assert recovered["quarantine_reason"] is None

    reacquired = await service.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path,
    )
    assert reacquired["state"] == "active"
    assert reacquired["lease"]["state"] == "active"
