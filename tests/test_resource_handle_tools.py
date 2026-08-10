"""Public retained-handle tool contract: session binding and safe payloads."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from nerve.agent.tools import ToolContext, build_default_registry
from nerve.resources import LeaseService, ResourceInventory


CONFIG = {
    "connections": ["lab"],
    "hosts": [{"id": "a", "connection_ref": "lab"}],
    "pools": [{"id": "workers", "members": ["a"]}],
}


async def _service(db):
    inventory = ResourceInventory(db, CONFIG)
    await inventory.initialize()
    for session in ("owner", "other", "holder"):
        await db.create_session(session)
    return LeaseService(db=db, inventory=inventory)


def _payload(result):
    return json.loads(result.content[0]["text"])


@pytest.mark.asyncio
async def test_public_handle_tools_are_opaque_owner_bound_and_sessionless(db):
    service = await _service(db)
    registry = build_default_registry()
    owner = ToolContext(session_id="owner", engine=SimpleNamespace(resource_service=service))
    other = ToolContext(session_id="other", engine=SimpleNamespace(resource_service=service))

    for name in ("resource_handle_acquire", "resource_handle_list", "resource_handle_release",
                 "resource_handle_wait_cancel", "resource_handle_check"):
        assert "session_id" not in registry.get(name).input_schema["properties"]
    acquired = await registry.invoke("resource_handle_acquire", owner, {"requests": [{"pool": "workers"}]})
    handle = _payload(acquired)["handles"][0]
    assert set(handle) == {"id"}
    assert all(secret not in acquired.content[0]["text"] for secret in ("lease-", "fencing_token", "connection_ref"))

    checked = await registry.invoke("resource_handle_check", owner, {"handle_id": handle["id"]})
    assert _payload(checked) == {"outcome": "ACTIVE", "handle": handle}
    rejected = await registry.invoke("resource_handle_release", other, {"handle_id": handle["id"]})
    assert rejected.is_error and _payload(rejected)["outcome"] == "HANDLE_NOT_OWNED"
    assert _payload(await registry.invoke("resource_handle_release", owner, {"handle_id": handle["id"]}))["outcome"] == "RELEASED"


@pytest.mark.asyncio
async def test_detached_public_wait_has_one_durable_continuation_and_owner_cancel(db):
    service = await _service(db)
    notifications: list[str] = []
    service.set_wait_continuation_publisher(notifications.append)
    registry = build_default_registry()
    owner = ToolContext(session_id="owner", engine=SimpleNamespace(resource_service=service))
    other = ToolContext(session_id="other", engine=SimpleNamespace(resource_service=service))
    held = await service.acquire(execution_id="hold", session_id="holder", requests=[{"pool": "workers", "host": "a"}])

    started = await registry.invoke("resource_handle_acquire", owner, {"requests": [{"pool": "workers", "host": "a"}], "detached": True})
    wait_id = _payload(started)["wait_id"]
    assert _payload(started)["outcome"] == "WAITING"
    denied = await registry.invoke("resource_handle_wait_cancel", other, {"wait_id": wait_id})
    assert denied.is_error and _payload(denied)["outcome"] == "HANDLE_NOT_OWNED"
    await service.release(execution_id="hold", leases=held)
    for _ in range(100):
        wait = await service.public_handle_wait("owner", wait_id)
        if wait["outcome"]:
            break
        await asyncio.sleep(.01)
    assert wait["outcome"] == "LEASE_GRANTED"
    assert len(notifications) == 1
    assert (await service.db.get_execution(notifications[0]))["continuation_state"] == "pending"
    assert _payload(await registry.invoke("resource_handle_wait_cancel", owner, {"wait_id": wait_id}))["outcome"] == "ALREADY_SETTLED"


@pytest.mark.asyncio
async def test_detached_acquire_reuses_owner_handle_without_creating_wait(db):
    service = await _service(db)
    registry = build_default_registry()
    owner = ToolContext(session_id="owner", engine=SimpleNamespace(resource_service=service))
    handle = _payload(await registry.invoke(
        "resource_handle_acquire", owner, {"requests": [{"pool": "workers", "host": "a"}]},
    ))["handles"][0]

    result = await registry.invoke(
        "resource_handle_acquire", owner,
        {"requests": [{"pool": "workers", "host": "a"}], "detached": True},
    )
    assert _payload(result) == {"outcome": "ACQUIRED", "handles": [handle]}
    assert await db.list_resource_wait_operations(state="pending") == []
