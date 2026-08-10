"""Startup recovery is exclusive with resource and execution lifecycle writes."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from nerve.executions.service import ExecutionService
from nerve.resources import (
    LeaseService,
    ResourceInventory,
    ResourceRecoveryGate,
    ResourceRecoveryUnavailable,
)


CONFIG = {
    "connections": ["lab"],
    "hosts": [{"id": "host-1", "connection_ref": "lab"}],
    "pools": [{"id": "builders", "members": ["host-1"]}],
}


class RecoveryProbe:
    """Records the point at which durable execution recovery becomes eligible."""

    def __init__(self, db):
        self.db = db
        self.calls = 0

    async def _recover_startup(self, *, dispatch_continuations):
        self.calls += 1
        pending = [
            row for row in await self.db.list_resource_recovery_intents()
            if row["state"] in {"prepared", "processing"}
        ]
        assert pending == []


async def _resources(db, *, ready=False):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    gate = ResourceRecoveryGate(ready=ready)
    return LeaseService(db=db, inventory=inventory, recovery_gate=gate), gate


async def _execution(db, execution_id="operation-1"):
    if await db.get_session("session-1") is None:
        await db.create_session("session-1", source="web", backend="codex", status="idle")
    return await db.create_execution(
        execution_id,
        session_id="session-1",
        kind="test.remote",
        profile_version="1",
        profile_hash="test",
        profile_snapshot={},
        plan={"kind": "test.remote", "profile_version": "1", "profile_hash": "test"},
        resource_requests=[],
    )


@pytest.mark.asyncio
async def test_gate_replays_every_pending_intent_before_execution_recovery(db):
    resources, gate = await _resources(db)
    await _execution(db)
    lease = await db.acquire_resource_lease(
        lease_id="lease-1", execution_id="operation-1", session_id="session-1",
        pool="builders", host_id="host-1",
    )
    assert lease is not None
    await db.create_resource_recovery_intent({
        "id": "prepared", "kind": "release", "operation_id": "operation-1",
        "payload": {"lease_id": "lease-1"},
    })
    await db.create_resource_recovery_intent({
        "id": "processing", "kind": "reconcile", "operation_id": "operation-1",
        "payload": {"lease_ids": ["lease-1"]}, "state": "processing",
    })
    probe = RecoveryProbe(db)

    await gate.open(resources, probe, dispatch_continuations=False)

    intents = await db.list_resource_recovery_intents()
    assert {row["state"] for row in intents} == {"failed"}
    recovered_lease = await db.get_resource_lease("lease-1")
    assert recovered_lease is not None and recovered_lease["state"] == "quarantined"
    assert probe.calls == 1 and gate.ready


@pytest.mark.asyncio
async def test_reconciler_starts_only_after_replay_finishes(db):
    resources, gate = await _resources(db, ready=False)
    await _execution(db)
    lease = await db.acquire_resource_lease(
        lease_id="lease-1", execution_id="operation-1", session_id="session-1",
        pool="builders", host_id="host-1",
    )
    assert lease is not None
    await db.create_resource_recovery_intent({
        "id": "prepared", "kind": "release", "operation_id": "operation-1",
        "payload": {"lease_id": "lease-1"},
    })

    started = asyncio.Event()
    proceed = asyncio.Event()
    original_replay = resources._replay_recovery_intent

    async def blocked_replay(intent):
        started.set()
        assert resources._reconciler is None
        await proceed.wait()
        await original_replay(intent)

    async def noop_execution_recovery(*, dispatch_continuations: bool = False):
        return None

    resources._replay_recovery_intent = blocked_replay

    open_task = asyncio.create_task(
        gate.open(resources, SimpleNamespace(_recover_startup=noop_execution_recovery), dispatch_continuations=False),
    )
    await started.wait()
    assert not gate.ready
    assert resources._reconciler is None
    assert not open_task.done()

    proceed.set()
    await open_task
    assert gate.ready
    assert resources._reconciler is not None


@pytest.mark.asyncio
async def test_gate_is_idempotent_and_unknown_intent_is_terminal(db):
    resources, gate = await _resources(db)
    await db.create_resource_recovery_intent({
        "id": "no-subject", "kind": "acquire", "payload": {"unexpected": True},
    })
    probe = RecoveryProbe(db)

    await gate.open(resources, probe, dispatch_continuations=False)
    await gate.open(resources, probe, dispatch_continuations=False)

    intent = await db.get_resource_recovery_intent("no-subject")
    assert intent is not None and intent["state"] == "completed"
    assert probe.calls == 1


@pytest.mark.asyncio
async def test_gate_rejects_acquire_release_start_and_cancel_without_writes(db, tmp_path):
    resources, gate = await _resources(db)
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await resources.acquire(
            execution_id="blocked", session_id="session-1", requests=[{"pool": "builders"}],
        )
    assert await db.list_resource_requests() == []

    # This is deliberately a direct durable setup: the closed gate must stop
    # even a release that would otherwise mutate a known active lease.
    await db.create_session("session-1", source="web", backend="codex", status="idle")
    direct_lease = await db.acquire_resource_lease(
        lease_id="release-blocked", execution_id="release-op", session_id="session-1",
        pool="builders", host_id="host-1",
    )
    assert direct_lease is not None
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await resources.release(execution_id="release-op", leases=[direct_lease])
    still_active = await db.get_resource_lease("release-blocked")
    assert still_active is not None and still_active["state"] == "active"

    service = ExecutionService(
        db=db, engine=SimpleNamespace(), workspace=tmp_path, catalog=SimpleNamespace(),
        resource_manager=resources, recovery_gate=gate,
    )
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await service._start_serialized(
            session_id="session-1",
            plan={"kind": "test", "profile_version": "1", "profile_hash": "test"},
            profile_snapshot={},
        )
    assert await db.list_active_executions() == []

    row = await _execution(db, execution_id="cancel-blocked")
    with pytest.raises(ResourceRecoveryUnavailable, match="retry shortly"):
        await service.cancel_execution(
            execution_id=row["id"], requested_by="operator", reason="stop",
        )
    unchanged = await db.get_execution(row["id"])
    assert unchanged is not None and unchanged["status"] == "queued"


@pytest.mark.asyncio
async def test_processing_intent_with_handle_quarantines_its_host(db):
    resources, gate = await _resources(db)
    await _execution(db)
    lease = await db.acquire_resource_lease(
        lease_id="lease-handle", execution_id="operation-1", session_id="session-1",
        pool="builders", host_id="host-1",
    )
    assert lease is not None
    handle = await db.create_session_resource_handle({
        "id": "handle-1", "session_id": "session-1", "pool": "builders",
        "host_id": "host-1", "lease_id": lease["id"],
        "fencing_token": lease["fencing_token"],
    })
    await db.create_resource_recovery_intent({
        "id": "handle-intent", "kind": "reconcile", "handle_id": handle["id"],
        "operation_id": "operation-1", "payload": {}, "state": "processing",
    })

    await gate.open(resources, RecoveryProbe(db), dispatch_continuations=False)

    intent = await db.get_resource_recovery_intent("handle-intent")
    recovered = await db.get_resource_lease(lease["id"])
    assert intent is not None and intent["state"] == "failed"
    assert recovered is not None and recovered["state"] == "quarantined"


@pytest.mark.asyncio
async def test_normal_acquire_and_release_resume_only_after_gate_opens(db):
    resources, gate = await _resources(db)
    probe = RecoveryProbe(db)
    await gate.open(resources, probe, dispatch_continuations=False)

    leases = await resources.acquire(
        execution_id="normal", session_id="session-1", requests=[{"pool": "builders"}],
    )
    assert len(leases) == 1
    await resources.release(execution_id="normal", leases=leases)
    released = await db.get_resource_lease(leases[0]["id"])
    assert released is not None and released["state"] == "released"
    await resources.shutdown()
