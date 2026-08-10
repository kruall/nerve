"""Durable quarantine recovery and permanent-loss races."""
from __future__ import annotations

import asyncio

import pytest

from nerve.resources import LeaseService, ResourceInventory


CONFIG = {"connections": ["lab"], "hosts": [{"id": "host-1", "connection_ref": "lab"}], "pools": [{"id": "builders", "members": ["host-1"]}]}


async def _service(db, probe):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory, recovery_probe=probe)
    await db.set_resource_host_state("host-1", quarantined=True, reason="uncertain")
    return service


@pytest.mark.asyncio
async def test_concurrent_claims_have_one_winner_and_stale_result_cannot_open(db):
    service = await _service(db, lambda *_: True)
    one, two = await asyncio.gather(db.claim_host_recovery("host-1"), db.claim_host_recovery("host-1"))
    winner = one or two
    assert winner is not None and (one is None) != (two is None)
    assert await db.complete_host_recovery("host-1", int(winner["recovery_generation"]) + 1) is None
    assert (await db.get_resource_host("host-1"))["quarantined"]


@pytest.mark.asyncio
async def test_failed_claim_is_only_reclaimed_after_durable_retry(db):
    service = await _service(db, lambda *_: False)
    claim = await db.claim_host_recovery("host-1")
    assert claim is not None
    assert await db.fail_host_recovery("host-1", int(claim["recovery_generation"]), retry_seconds=60)
    assert await db.claim_host_recovery("host-1") is None


@pytest.mark.asyncio
async def test_non_quiescent_probe_keeps_quarantine_and_success_opens(db):
    async def blocked(*_): return False
    service = await _service(db, blocked)
    assert await service._recover_host_once("host-1") is None
    assert (await db.get_resource_host("host-1"))["quarantined"]
    # This represents a later durable retry, rather than an in-flight claimant.
    await db.db.execute("UPDATE resource_hosts SET recovery_retry_at='2000-01-01T00:00:00+00:00' WHERE id='host-1'")
    await db.db.commit()
    async def clear(*_): return True
    service._recovery_probe = clear
    assert await service._recover_host_once("host-1") is not None
    host = await db.get_resource_host("host-1")
    assert host is not None and not host["quarantined"]
    assert host["recovery_claim_state"] == "idle"
    assert host["recovery_claimed_at"] is None
    assert host["recovery_claim_expires_at"] is None
    assert host["recovery_retry_at"] is None


@pytest.mark.asyncio
async def test_successful_recovery_releases_claim_for_immediate_new_generation(db):
    await _service(db, lambda *_: True)
    first = await db.claim_host_recovery("host-1", lease_seconds=60)
    assert first is not None
    assert await db.complete_host_recovery("host-1", int(first["recovery_generation"])) is not None

    await db.set_resource_host_state("host-1", quarantined=True, reason="uncertain again")
    second = await db.claim_host_recovery("host-1", lease_seconds=60)
    assert second is not None
    assert int(second["recovery_generation"]) > int(first["recovery_generation"])
    assert await db.complete_host_recovery("host-1", int(second["recovery_generation"])) is not None


@pytest.mark.asyncio
async def test_successful_recovery_releases_quarantined_handle_with_its_lease(db):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory, recovery_probe=lambda *_: True)
    await db.create_session("session-1")
    handle = (await service.acquire_handles("session-1", [{"pool": "builders"}]))[0]
    private = await service._resolve_handle_lease("session-1", handle["id"])
    await service.quarantine(
        execution_id=private["lease"]["execution_id"], leases=[private["lease"]], reason="uncertain",
    )
    claim = await db.claim_host_recovery("host-1")
    assert claim is not None
    assert await db.complete_host_recovery("host-1", int(claim["recovery_generation"])) is not None
    assert (await db.get_resource_lease(private["lease_id"]))["state"] == "released"
    assert (await db.get_session_resource_handle(handle["id"]))["state"] == "released"


@pytest.mark.asyncio
async def test_successful_recovery_releases_quarantined_session_reservation(db, tmp_path):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    service = LeaseService(db=db, inventory=inventory, recovery_probe=lambda *_: True)
    await db.create_session("session-1")
    reservation = await service.reserve_for_session(
        session_id="session-1", pool="builders", worktree=tmp_path / "worktree",
    )
    assert await service.release_session_reservation(
        session_id="session-1", remote_quiescence_confirmed=False, reason="uncertain",
    )
    claim = await db.claim_host_recovery("host-1")
    assert claim is not None
    assert await db.complete_host_recovery("host-1", int(claim["recovery_generation"])) is not None
    assert (await db.get_session_resource_reservation("session-1"))["state"] == "released"
    assert (await db.get_resource_lease(reservation["lease_id"]))["state"] == "released"


@pytest.mark.asyncio
async def test_claim_survives_restart_until_expiry_and_stale_result_is_fenced(db):
    first_service = await _service(db, lambda *_: True)
    old = await db.claim_host_recovery("host-1", lease_seconds=60)
    assert old is not None

    # A newly constructed service sees the durable, unexpired claim.
    second_service = LeaseService(db=db, inventory=first_service.inventory, recovery_probe=lambda *_: True)
    assert await db.claim_host_recovery("host-1", lease_seconds=60) is None

    await db.db.execute(
        "UPDATE resource_hosts SET recovery_claim_expires_at='2000-01-01T00:00:00+00:00' WHERE id='host-1'"
    )
    await db.db.commit()
    one, two = await asyncio.gather(
        db.claim_host_recovery("host-1", lease_seconds=60),
        db.claim_host_recovery("host-1", lease_seconds=60),
    )
    winner = one or two
    assert winner is not None and (one is None) != (two is None)
    assert int(winner["recovery_generation"]) > int(old["recovery_generation"])
    assert await db.complete_host_recovery("host-1", int(old["recovery_generation"])) is None
    assert (await db.get_resource_host("host-1"))["quarantined"]
    assert await db.complete_host_recovery("host-1", int(winner["recovery_generation"])) is not None
    await second_service.shutdown()


@pytest.mark.asyncio
async def test_permanent_loss_is_atomic_and_leaves_pool_wait_pending(db):
    service = await _service(db, lambda *_: False)
    for session in ("exact", "pool"):
        await db.create_session(session)
        await db.create_execution("op-" + session, session_id=session, kind="remote", profile_version="1", profile_hash="h", profile_snapshot={}, plan={}, resource_requests=[])
    exact = await service.start_or_reattach_wait(session_id="exact", operation_id="op-exact", spec=[{"pool": "builders", "host": "host-1"}])
    pool = await service.start_or_reattach_wait(session_id="pool", operation_id="op-pool", spec=[{"pool": "builders"}])
    await service.permanently_lose_host(host_id="host-1", confirm_host_id="host-1", requested_by="operator")
    assert (await db.get_resource_wait_operation(exact["id"]))["outcome"] == "HOST_PERMANENTLY_UNAVAILABLE"
    assert (await db.get_resource_wait_operation(pool["id"]))["state"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_first", [False, True])
async def test_temporary_recovery_loses_race_to_permanent_loss_without_duplicate_wakeup(db, lose_first):
    probe_started = asyncio.Event()
    probe_release = asyncio.Event()

    async def probe(*_):
        probe_started.set()
        await probe_release.wait()
        return True

    service = await _service(db, probe)
    for session in ("exact-race", "pool-race"):
        await db.create_session(session)
        await db.create_execution(
            "op-" + session, session_id=session, kind="remote", profile_version="1",
            profile_hash="h", profile_snapshot={}, plan={}, resource_requests=[],
        )
    exact = await service.start_or_reattach_wait(
        session_id="exact-race", operation_id="op-exact-race",
        spec=[{"pool": "builders", "host": "host-1"}],
    )
    pool = await service.start_or_reattach_wait(
        session_id="pool-race", operation_id="op-pool-race", spec=[{"pool": "builders"}],
    )

    recovery = None
    if lose_first:
        await service.permanently_lose_host(
            host_id="host-1", confirm_host_id="host-1", requested_by="operator",
        )
    else:
        recovery = asyncio.create_task(service._recover_host_once("host-1"))
        await probe_started.wait()
        await service.permanently_lose_host(
            host_id="host-1", confirm_host_id="host-1", requested_by="operator",
        )
        probe_release.set()
        assert await recovery is None

    if lose_first:
        probe_release.set()
        assert await service._recover_host_once("host-1") is None

    await service.permanently_lose_host(
        host_id="host-1", confirm_host_id="host-1", requested_by="operator",
    )
    host = await db.get_resource_host("host-1")
    assert host is not None and host["permanently_unavailable"] and host["quarantined"]
    settled = await db.get_resource_wait_operation(exact["id"])
    assert settled is not None
    assert (settled["state"], settled["outcome"], settled["wakeup_generation"]) == (
        "failed", "HOST_PERMANENTLY_UNAVAILABLE", 1,
    )
    pool_wait = await db.get_resource_wait_operation(pool["id"])
    assert pool_wait is not None and pool_wait["state"] == "pending"
    exact_operation = await db.get_execution("op-exact-race")
    assert exact_operation is not None
    assert exact_operation["continuation_state"] == "pending"
    assert exact_operation["result"]["outcome"] == "HOST_PERMANENTLY_UNAVAILABLE"
    assert not [row for row in await db.list_resource_requests()
                if row["execution_id"] == "op-exact-race"]
    await service.shutdown()
