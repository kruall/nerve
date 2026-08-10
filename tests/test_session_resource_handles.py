from __future__ import annotations

import asyncio
import sqlite3

import pytest


async def _operation(db, session_id: str = "session-a", operation_id: str = "operation-a"):
    await db.create_session(session_id)
    await db.create_execution(operation_id, session_id=session_id, kind="remote", profile_version="1",
                              profile_hash="hash", profile_snapshot={}, plan={}, resource_requests=[])
    await db.seed_resource_host({"id": "host-a", "connection_ref": "ssh://host-a"})
    lease = await db.acquire_resource_lease(lease_id="lease-a", execution_id=operation_id,
                                            session_id=session_id, pool="pool-a", host_id="host-a")
    assert lease is not None


def _handle(handle_id: str = "handle-a") -> dict:
    return {"id": handle_id, "session_id": "session-a", "pool": "pool-a",
            "host_id": "host-a", "lease_id": "lease-a", "fencing_token": 1}


@pytest.mark.asyncio
async def test_handle_refs_waits_and_recovery_intents_are_durable(db):
    await _operation(db)
    handle = await db.create_session_resource_handle(_handle())
    assert await db.update_session_resource_handle(handle["id"], expected_state="active", state="releasing")
    assert not await db.update_session_resource_handle(handle["id"], expected_state="active", state="released")
    assert await db.attach_operation_resource_ref("operation-a", handle["id"])
    assert not await db.attach_operation_resource_ref("operation-a", handle["id"])
    assert [ref["handle_id"] for ref in await db.list_operation_resource_refs("operation-a")] == [handle["id"]]
    assert await db.detach_operation_resource_refs("operation-a") == 1

    wait = await db.create_resource_wait_operation({"id": "wait-a", "session_id": "session-a", "operation_id": "operation-a", "request_kind": "pool", "pool": "pool-a", "queue_ticket": 1})
    assert await db.update_resource_wait_operation(wait["id"], expected_state="pending", state="cancelled", outcome="cancelled")
    intent = await db.create_resource_recovery_intent({"id": "intent-a", "kind": "reconcile", "session_id": "session-a", "handle_id": handle["id"], "operation_id": "operation-a", "payload": {"lease": "lease-a"}})
    await db.close()
    await db.connect()
    recovered = await db.get_resource_recovery_intent(intent["id"])
    assert recovered is not None and recovered["state"] == "prepared"
    assert await db.update_resource_recovery_intent(intent["id"], expected_state="prepared", state="completed")
    assert await db.delete_resource_wait_operation(wait["id"])
    assert await db.delete_resource_recovery_intent(intent["id"])
    assert await db.delete_session_resource_handle(handle["id"])


@pytest.mark.asyncio
async def test_composite_commits_are_idempotent_and_terminal_keeps_handle(db):
    await _operation(db)
    await db.create_resource_wait_operation({"id": "wait-a", "session_id": "session-a", "operation_id": "operation-a", "request_kind": "pool", "pool": "pool-a", "queue_ticket": 1})
    outcomes = await asyncio.gather(*[
        db.commit_handle_grant(wait_id="wait-a", handle=_handle("handle-a")),
        db.commit_handle_grant(wait_id="wait-a", handle=_handle("handle-b")),
    ])
    assert outcomes == [True, True]
    assert len(await db.list_session_resource_handles("session-a")) == 1
    assert (await db.get_resource_wait_operation("wait-a"))["state"] == "granted"
    assert (await db.get_execution("operation-a"))["continuation_state"] == "pending"
    assert len(await db.list_pending_wakeups("session-a")) == 0
    assert len(await db.list_pending_execution_continuations()) == 1

    assert await db.attach_operation_resource_ref("operation-a", "handle-a")
    assert await db.commit_operation_terminal(operation_id="operation-a", status="cancelled", result={"outcome": "cancelled"})
    assert await db.commit_operation_terminal(operation_id="operation-a", status="cancelled", result={"outcome": "cancelled"})
    assert await db.list_operation_resource_refs("operation-a") == []
    assert (await db.get_session_resource_handle("handle-a"))["state"] == "active"
    assert (await db.get_execution("operation-a"))["continuation_state"] == "pending"
    assert len(await db.list_pending_wakeups("session-a")) == 0


@pytest.mark.asyncio
async def test_concurrent_grant_retries_same_wait_keeps_one_handle_and_continuation(db):
    await _operation(db)
    await db.create_resource_wait_operation({"id": "wait-b", "session_id": "session-a", "operation_id": "operation-a", "request_kind": "pool", "pool": "pool-a", "queue_ticket": 2})
    outcomes = await asyncio.gather(*[
        db.commit_handle_grant(wait_id="wait-b", handle=_handle("handle-a")),
        db.commit_handle_grant(wait_id="wait-b", handle=_handle("handle-b")),
    ])
    assert outcomes == [True, True]
    assert len(await db.list_session_resource_handles("session-a")) == 1
    assert (await db.get_execution("operation-a"))["continuation_state"] == "pending"


@pytest.mark.asyncio
async def test_conflicting_grant_for_lease_keeps_wait_pending_without_handle(db):
    await _operation(db)
    await db.create_resource_wait_operation({"id": "wait-a", "session_id": "session-a", "operation_id": "operation-a", "request_kind": "pool", "pool": "pool-a", "queue_ticket": 3})
    await db.create_resource_wait_operation({"id": "wait-c", "session_id": "session-a", "operation_id": "operation-a", "request_kind": "pool", "pool": "pool-a", "queue_ticket": 4})
    assert await db.commit_handle_grant(wait_id="wait-a", handle=_handle("handle-a"))
    assert not await db.commit_handle_grant(wait_id="wait-c", handle=_handle("handle-b"))
    assert (await db.get_resource_wait_operation("wait-c"))["state"] == "pending"
    assert len(await db.list_session_resource_handles("session-a")) == 1
    assert (await db.get_execution("operation-a"))["continuation_state"] == "pending"


@pytest.mark.asyncio
async def test_wait_and_intent_crud_preserve_order_and_compare_state(db):
    await _operation(db)
    first = await db.create_resource_wait_operation({
        "id": "wait-first", "session_id": "session-a", "operation_id": "operation-a",
        "request_kind": "host", "requested_hosts": ["host-a"], "pool": "pool-a",
        "queue_ticket": 10,
    })
    second = await db.create_resource_wait_operation({
        "id": "wait-second", "session_id": "session-a", "operation_id": "operation-a",
        "request_kind": "bundle", "requested_hosts": ["host-a", "host-b"], "pool": "pool-a",
        "queue_ticket": 20,
    })
    waits = await db.list_resource_wait_operations(state="pending")
    assert [wait["id"] for wait in waits] == [first["id"], second["id"]]
    assert first["requested_hosts_json"] == '["host-a"]'
    assert await db.update_resource_wait_operation(first["id"], expected_state="pending", state="failed", outcome="no capacity")
    assert not await db.update_resource_wait_operation(first["id"], expected_state="pending", state="granted")
    failed = await db.get_resource_wait_operation(first["id"])
    assert failed is not None and failed["outcome"] == "no capacity" and failed["settled_at"]

    prepared = await db.create_resource_recovery_intent({
        "id": "intent-prepared", "kind": "acquire", "session_id": "session-a",
        "operation_id": "operation-a", "payload": {"wait_id": second["id"]},
    })
    processing = await db.create_resource_recovery_intent({
        "id": "intent-processing", "kind": "release", "session_id": "session-a",
        "operation_id": "operation-a", "payload": {"lease_id": "lease-a"}, "state": "processing",
    })
    assert [row["id"] for row in await db.list_resource_recovery_intents()] == [prepared["id"], processing["id"]]
    assert await db.update_resource_recovery_intent(processing["id"], expected_state="processing", state="failed")
    assert not await db.update_resource_recovery_intent(processing["id"], expected_state="processing", state="completed")
    failed_intent = await db.get_resource_recovery_intent(processing["id"])
    assert failed_intent is not None and failed_intent["completed_at"]
    assert await db.delete_resource_wait_operation(first["id"])
    assert await db.delete_resource_wait_operation(second["id"])
    assert await db.delete_resource_recovery_intent(prepared["id"])
    assert await db.delete_resource_recovery_intent(processing["id"])


@pytest.mark.asyncio
async def test_handle_state_cas_and_operation_ref_foreign_ownership(db):
    await _operation(db)
    await db.create_session_resource_handle(_handle())
    assert [row["id"] for row in await db.list_session_resource_handles("session-a", states=("active",))] == ["handle-a"]
    assert await db.update_session_resource_handle("handle-a", expected_state="active", state="quarantined", release_reason="lost transport")
    quarantined = await db.get_session_resource_handle("handle-a")
    assert quarantined is not None
    assert quarantined["release_reason"] == "lost transport"
    assert quarantined["released_at"] is not None
    assert not await db.update_session_resource_handle("handle-a", expected_state="active", state="released")
    assert await db.update_session_resource_handle("handle-a", expected_state="quarantined", state="released")
    released = await db.get_session_resource_handle("handle-a")
    assert released is not None and released["state"] == "released"
    assert await db.attach_operation_resource_ref("operation-a", "handle-a")
    refs = await db.list_operation_resource_refs("operation-a")
    assert refs[0]["operation_id"] == "operation-a" and refs[0]["handle_id"] == "handle-a"
    assert await db.detach_operation_resource_refs("operation-a") == 1
    assert await db.detach_operation_resource_refs("operation-a") == 0


@pytest.mark.asyncio
async def test_composite_commits_leave_no_partial_rows_on_losing_transition(db):
    await _operation(db)
    await db.create_resource_wait_operation({
        "id": "cancelled-wait", "session_id": "session-a", "operation_id": "operation-a",
        "request_kind": "pool", "pool": "pool-a", "queue_ticket": 1, "state": "cancelled",
    })
    assert not await db.commit_handle_grant(wait_id="cancelled-wait", handle=_handle())
    assert await db.get_session_resource_handle("handle-a") is None
    assert await db.list_pending_wakeups("session-a") == []

    assert await db.commit_operation_terminal(operation_id="operation-a", status="succeeded", result={"ok": True})
    terminal = await db.get_execution("operation-a")
    assert terminal is not None and terminal["status"] == "succeeded" and terminal["result"] == {"ok": True}
    assert not await db.commit_operation_terminal(operation_id="missing", status="failed", result={})
    assert len(await db.list_pending_wakeups("session-a")) == 0


@pytest.mark.asyncio
async def test_grant_and_terminal_commits_remain_safe_after_reopen(db):
    """A restart after either commit observes its completed durable outcome."""
    await _operation(db)
    await db.create_resource_wait_operation({
        "id": "wait-reopen", "session_id": "session-a", "operation_id": "operation-a",
        "request_kind": "pool", "pool": "pool-a", "queue_ticket": 11,
    })
    assert await db.commit_handle_grant(wait_id="wait-reopen", handle=_handle())
    await db.close()
    await db.connect()

    # The retried grant cannot create a second handle or wakeup after a crash
    # between the caller receiving its result and recording that receipt.
    assert await db.commit_handle_grant(wait_id="wait-reopen", handle=_handle())
    handles = await db.list_session_resource_handles("session-a")
    assert len(handles) == 1 and handles[0]["lease_id"] == "lease-a"
    assert len(await db.list_pending_wakeups("session-a")) == 0

    assert await db.attach_operation_resource_ref("operation-a", "handle-a")
    assert await db.commit_operation_terminal(
        operation_id="operation-a", status="failed", result={"error": "transport lost"},
    )
    await db.close()
    await db.connect()
    assert await db.commit_operation_terminal(
        operation_id="operation-a", status="failed", result={"error": "transport lost"},
    )
    operation = await db.get_execution("operation-a")
    assert operation is not None
    assert operation["status"] == "failed"
    assert operation["result"] == {"error": "transport lost"}
    assert await db.list_operation_resource_refs("operation-a") == []
    retained = await db.get_session_resource_handle("handle-a")
    assert retained is not None
    assert retained["state"] == "active"
    lease = await db.get_resource_lease("lease-a")
    assert lease is not None
    assert lease["state"] == "active"
    assert len(await db.list_pending_wakeups("session-a")) == 0


@pytest.mark.asyncio
async def test_handle_update_rejects_unknown_state(db):
    await _operation(db)
    await db.create_session_resource_handle(_handle())
    with pytest.raises(ValueError, match="invalid handle state"):
        await db.update_session_resource_handle("handle-a", expected_state="active", state="unknown")


@pytest.mark.asyncio
async def test_wait_and_intent_updates_reject_unknown_states(db):
    await _operation(db)
    await db.create_resource_wait_operation({
        "id": "wait-state", "session_id": "session-a", "operation_id": "operation-a",
        "request_kind": "pool", "pool": "pool-a", "queue_ticket": 1,
    })
    await db.create_resource_recovery_intent({
        "id": "intent-state", "kind": "acquire", "session_id": "session-a",
        "operation_id": "operation-a", "payload": {},
    })
    with pytest.raises(ValueError, match="invalid wait state"):
        await db.update_resource_wait_operation("wait-state", expected_state="pending", state="unknown")
    with pytest.raises(ValueError, match="invalid recovery intent state"):
        await db.update_resource_recovery_intent("intent-state", expected_state="prepared", state="unknown")


@pytest.mark.asyncio
async def test_batch_handle_retention_rolls_back_every_row_on_conflicting_lease(db):
    """The R4 service can safely compensate leases after a failed batch write."""
    await _operation(db)
    first = _handle("batch-first")
    conflicting = {**_handle("batch-conflicting")}

    with pytest.raises(sqlite3.IntegrityError):
        await db.create_session_resource_handles([first, conflicting])

    assert await db.get_session_resource_handle("batch-first") is None
    assert await db.get_session_resource_handle("batch-conflicting") is None
    lease = await db.get_resource_lease("lease-a")
    assert lease is not None and lease["state"] == "active"


@pytest.mark.asyncio
async def test_batch_handle_retention_persists_every_member_with_independent_leases(db):
    await _operation(db)
    await db.seed_resource_host({"id": "host-b", "connection_ref": "ssh://host-b"})
    second_lease = await db.acquire_resource_lease(
        lease_id="lease-b", execution_id="operation-b", session_id="session-a",
        pool="pool-b", host_id="host-b",
    )
    assert second_lease is not None
    second = {
        "id": "batch-second", "session_id": "session-a", "pool": "pool-b",
        "host_id": "host-b", "lease_id": "lease-b",
        "fencing_token": second_lease["fencing_token"],
    }

    await db.create_session_resource_handles([_handle("batch-first"), second])

    rows = await db.list_session_resource_handles("session-a", states=("active",))
    assert [(row["id"], row["lease_id"]) for row in rows] == [
        ("batch-first", "lease-a"), ("batch-second", "lease-b"),
    ]
    assert all(row["created_at"] and row["updated_at"] for row in rows)
