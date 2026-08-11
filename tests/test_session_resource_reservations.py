from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from nerve.resources import LeaseService, ResourceInventory, ResourceInventoryError

CONFIG = {
    "connections": ["lab"],
    "hosts": [{"id": "builder-1", "connection_ref": "lab"}],
    "pools": [
        {"id": "builders", "members": ["builder-1"]},
        {"id": "ydb-builders", "members": ["builder-1"]},
    ],
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
async def test_session_reservation_cleanup_cancels_queued_request_without_lease(db):
    service = await _service(db)
    await db.create_session("session-a")
    execution_id = service._reservation_execution_id("session-a")
    await db.enqueue_resource_bundle(
        bundle_id="bundle-session-a",
        execution_id=execution_id,
        session_id="session-a",
        requests=[{"id": "request-session-a", "slot": "session", "pool": "builders"}],
    )

    assert await service.cleanup_session_reservation("session-a")
    assert await db.list_resource_requests() == []


@pytest.mark.asyncio
async def test_startup_cancels_orphaned_queued_session_reservation(db):
    first = await _service(db)
    await db.create_session("session-a")
    await db.enqueue_resource_bundle(
        bundle_id="bundle-session-a",
        execution_id=first._reservation_execution_id("session-a"),
        session_id="session-a",
        requests=[{"id": "request-session-a", "slot": "session", "pool": "builders"}],
    )

    recovered = await _service(db)
    await recovered.initialize()

    assert await db.list_resource_requests() == []
    await recovered.shutdown()


@pytest.mark.asyncio
async def test_startup_cancels_queued_session_reservation_for_active_execution(db):
    first = await _service(db)
    await db.create_session("session-a")
    await db.create_execution(
        "exec-starting", session_id="session-a", kind="test", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
    )
    assert await db.transition_execution(
        "exec-starting", to_status="starting", expect=("queued",),
    )
    await db.enqueue_resource_bundle(
        bundle_id="bundle-session-a",
        execution_id=first._reservation_execution_id("session-a"),
        session_id="session-a",
        requests=[{"id": "request-session-a", "slot": "session", "pool": "builders"}],
    )

    recovered = await _service(db)
    await recovered.initialize()

    assert await db.list_resource_requests() == []
    await recovered.shutdown()


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
async def test_startup_migrates_idle_session_reservation_to_the_same_retained_lineage(db, tmp_path):
    first = await _service(db)
    await db.create_session("session-a")
    reservation = await first.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )

    recovered = await _service(db)
    await recovered.initialize()

    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None and settled["state"] == "released"
    lease = await db.get_resource_lease(reservation["lease_id"])
    assert lease is not None and lease["state"] == "active"
    handles = await db.list_session_resource_handles("session-a", states=("active",))
    assert [(handle["lease_id"], handle["worktree_identity"]) for handle in handles] == [
        (reservation["lease_id"], str(tmp_path.resolve())),
    ]


@pytest.mark.asyncio
async def test_startup_quarantines_legacy_reservation_when_host_fence_is_stale(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    reservation = await service.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )
    await db.db.execute(
        "UPDATE resource_hosts SET fencing_token=fencing_token+1 WHERE id=?",
        (reservation["host_id"],),
    )
    await db.db.commit()

    restarted = await _service(db)
    await restarted.initialize()

    settled = await db.get_session_resource_reservation("session-a")
    lease = await db.get_resource_lease(reservation["lease_id"])
    assert settled is not None and settled["state"] == "quarantined"
    assert lease is not None and lease["state"] == "released"
    assert (await db.get_resource_host(reservation["host_id"]))["quarantined"] == 1
    assert await db.list_session_resource_handles("session-a", states=("active",)) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host_state", "permanent_loss"),
    [
        ({"enabled": 0}, False),
        ({"draining": 1}, False),
        ({"offline": 1}, False),
        ({"quarantined": 1}, False),
        ({}, True),
    ],
    ids=["disabled", "draining", "offline", "quarantined", "permanent-loss"],
)
async def test_startup_quarantines_legacy_reservation_for_unavailable_host(
    db, tmp_path, host_state, permanent_loss,
):
    service = await _service(db)
    await db.create_session("session-a")
    reservation = await service.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )
    restarted = await _service(db)
    if permanent_loss:
        await restarted.permanently_lose_host(
            host_id=reservation["host_id"],
            confirm_host_id=reservation["host_id"],
            requested_by="test",
        )
    else:
        columns = ", ".join(f"{column}=?" for column in host_state)
        await db.db.execute(
            f"UPDATE resource_hosts SET {columns} WHERE id=?",
            (*host_state.values(), reservation["host_id"]),
        )
        await db.db.commit()

    await restarted.migrate_legacy_session_reservations()

    settled = await db.get_session_resource_reservation("session-a")
    lease = await db.get_resource_lease(reservation["lease_id"])
    host = await db.get_resource_host(reservation["host_id"])
    assert settled is not None and settled["state"] == "quarantined"
    assert lease is not None and lease["state"] == "released"
    assert host is not None and host["quarantined"] == 1
    if permanent_loss:
        assert host["enabled"] == 0 and host["permanently_unavailable"] == 1
    assert await db.list_session_resource_handles("session-a", states=("active",)) == []


@pytest.mark.asyncio
async def test_startup_quarantines_legacy_reservation_when_existing_handle_worktree_differs(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    reservation = await service.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )
    await db.create_session_resource_handle({
        "id": "handle-mismatched-worktree",
        "session_id": "session-a",
        "pool": "ydb-builders",
        "host_id": reservation["host_id"],
        "lease_id": reservation["lease_id"],
        "fencing_token": reservation["lease"]["fencing_token"],
        "worktree_identity": str((tmp_path / "other").resolve()),
    })

    restarted = await _service(db)
    await restarted.initialize()

    settled = await db.get_session_resource_reservation("session-a")
    lease = await db.get_resource_lease(reservation["lease_id"])
    handle = await db.get_session_resource_handle("handle-mismatched-worktree")
    assert settled is not None and settled["state"] == "quarantined"
    assert lease is not None and lease["state"] == "released"
    assert handle is not None and handle["state"] == "quarantined"
    assert await db.list_session_resource_handles("session-a", states=("active",)) == []


@pytest.mark.asyncio
async def test_startup_quarantines_running_ydb_reservation(db, tmp_path):
    first = await _service(db)
    await db.create_session("session-a")
    reservation = await first.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )
    await db.create_execution(
        "exec-active", session_id="session-a", kind="test", profile_version="1",
        profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[],
    )

    recovered = await _service(db)
    await recovered.initialize()

    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None and settled["state"] == "quarantined"
    lease = await db.get_resource_lease(reservation["lease_id"])
    assert lease is not None and lease["state"] == "released"
    handles = await db.list_session_resource_handles("session-a", states=("active",))
    assert await db.list_session_resource_handles("session-a", states=("active",)) == []


@pytest.mark.asyncio
async def test_ydb_handle_reuses_pinned_lineage_across_restart(db, tmp_path):
    first = await _service(db)
    await db.create_session("session-a")
    handle = await first.acquire_ydb_handle(session_id="session-a", worktree=tmp_path)
    before = await db.get_session_resource_handle(handle["id"])

    restarted = await _service(db)
    await restarted.initialize()
    reused = await restarted.acquire_ydb_handle(session_id="session-a", worktree=tmp_path)
    after = await db.get_session_resource_handle(reused["id"])

    assert reused["id"] == handle["id"]
    assert (after["host_id"], after["lease_id"], after["fencing_token"]) == (
        before["host_id"], before["lease_id"], before["fencing_token"],
    )


@pytest.mark.asyncio
async def test_concurrent_ydb_handle_acquisition_is_serialized_without_deadlock(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")

    first, second = await asyncio.wait_for(asyncio.gather(
        service.acquire_ydb_handle(session_id="session-a", worktree=tmp_path),
        service.acquire_ydb_handle(session_id="session-a", worktree=tmp_path),
    ), timeout=1)

    assert first["id"] == second["id"]
    handles = await db.list_session_resource_handles("session-a", states=("active",))
    assert len(handles) == 1 and handles[0]["pool"] == "ydb-builders"


@pytest.mark.asyncio
async def test_startup_quarantines_legacy_reservation_without_active_lineage(db, tmp_path):
    service = await _service(db)
    await db.create_session("session-a")
    reservation = await service.reserve_for_session(
        session_id="session-a", pool="ydb-builders", worktree=tmp_path,
    )
    await db.release_resource_lease(
        lease_id=reservation["lease_id"], execution_id=reservation["lease"]["execution_id"],
        fencing_token=reservation["lease"]["fencing_token"],
    )

    restarted = await _service(db)
    await restarted.initialize()

    settled = await db.get_session_resource_reservation("session-a")
    assert settled is not None and settled["state"] == "quarantined"
    assert (await db.get_resource_host("builder-1"))["quarantined"] == 1
    assert await db.list_session_resource_handles("session-a", states=("active",)) == []


@pytest.mark.asyncio
async def test_explicit_confirmed_release_after_recovery_does_not_quarantine(db, tmp_path):
    first = await _service(db)
    await db.create_session("session-a")
    await first.reserve_for_session(
        session_id="session-a", pool="builders", worktree=tmp_path,
    )
    recovered = await _service(db)
    await asyncio.sleep(0)

    assert await recovered.release_session_reservation(
        session_id="session-a",
        remote_quiescence_confirmed=True,
        reason="owner explicitly released idle host",
    )
    assert (await db.get_resource_host("builder-1"))["quarantined"] == 0
    reservation = await db.get_session_resource_reservation("session-a")
    assert reservation is not None and reservation["state"] == "released"
    lease = await db.get_resource_lease(reservation["lease_id"])
    assert lease is not None and lease["state"] == "released"


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
