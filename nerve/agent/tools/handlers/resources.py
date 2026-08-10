"""Secret-free MCP facade for trusted resource inventory and leases."""
from __future__ import annotations
import json

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    RESOURCE_DIAGNOSTICS_SCHEMA, RESOURCE_DRAIN_SCHEMA, RESOURCE_EMPTY_SCHEMA,
    RESOURCE_PERMANENT_LOSS_SCHEMA, RESOURCE_QUARANTINE_SCHEMA, RESOURCE_RECOVER_SCHEMA,
    RESOURCE_HANDLE_ACQUIRE_SCHEMA, RESOURCE_HANDLE_ID_SCHEMA, RESOURCE_HANDLE_LIST_SCHEMA,
    RESOURCE_HANDLE_WAIT_CANCEL_SCHEMA, RESOURCE_HANDLE_CHECK_SCHEMA,
)
from nerve.resources import (ResourceHandleConflictError, ResourceHandleOwnershipError,
                             ResourceInventoryError, ResourceRecoveryUnavailable)
from nerve.executions.public import public_resource_snapshot


def _service(ctx: ToolContext):
    service = getattr(ctx.engine, "resource_service", None) if ctx.engine else None
    if service is None:
        raise RuntimeError("resource service is unavailable")
    return service


def _json(value: object) -> ToolResult:
    return ToolResult.text(json.dumps(value, indent=2, sort_keys=True, default=str))


async def inventory(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        snapshot = public_resource_snapshot(await _service(ctx).resource_snapshot())
    except Exception:
        return ToolResult.text("Resource inventory is unavailable.", is_error=True)
    return _json({"pools": snapshot["pools"], "hosts": snapshot["hosts"]})


async def availability(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        snapshot = public_resource_snapshot(await _service(ctx).resource_snapshot())
    except Exception:
        return ToolResult.text("Resource availability is unavailable.", is_error=True)
    return _json({"pools": snapshot["pools"], "queue": snapshot["queue"]})


async def leases(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        snapshot = public_resource_snapshot(await _service(ctx).resource_snapshot())
    except Exception:
        return ToolResult.text("Resource leases are unavailable.", is_error=True)
    return _json({"leases": snapshot["leases"], "queue": snapshot["queue"]})


async def drain(ctx: ToolContext, args: dict) -> ToolResult:
    host_id = str(args.get("host_id") or "")
    if host_id != args.get("confirm_host_id"):
        return ToolResult.text("Host confirmation does not match.", is_error=True)
    try:
        host = await _service(ctx).set_host_draining(
            host_id=host_id, draining=bool(args.get("draining")),
            requested_by=f"session:{ctx.session_id}",
        )
    except Exception:
        return ToolResult.text("Host drain action was rejected.", is_error=True)
    return _json({"host": public_resource_snapshot({"hosts": [host]})["hosts"][0]})


async def recover(ctx: ToolContext, args: dict) -> ToolResult:
    host_id = str(args.get("host_id") or "")
    if host_id != args.get("confirm_host_id"):
        return ToolResult.text("Recovery requires matching host confirmation.", is_error=True)
    try:
        host = await _service(ctx).recover_host(
            host_id=host_id, requested_by=f"session:{ctx.session_id}",
        )
    except Exception:
        return ToolResult.text("Host recovery was rejected.", is_error=True)
    return _json({"host": public_resource_snapshot({"hosts": [host]})["hosts"][0]})


async def quarantine(ctx: ToolContext, args: dict) -> ToolResult:
    host_id = str(args.get("host_id") or "")
    if host_id != args.get("confirm_host_id"):
        return ToolResult.text("Host confirmation does not match.", is_error=True)
    try:
        host = await _service(ctx).quarantine_host(
            host_id=host_id, reason=str(args.get("reason") or ""),
            requested_by=f"session:{ctx.session_id}",
        )
    except Exception:
        return ToolResult.text("Host quarantine was rejected.", is_error=True)
    return _json({"host": public_resource_snapshot({"hosts": [host]})["hosts"][0]})


async def permanently_lose(ctx: ToolContext, args: dict) -> ToolResult:
    host_id = str(args.get("host_id") or "")
    if host_id != args.get("confirm_host_id"):
        return ToolResult.text("Permanent loss requires matching host confirmation.", is_error=True)
    try:
        host = await _service(ctx).permanently_lose_host(
            host_id=host_id, confirm_host_id=host_id, requested_by=f"session:{ctx.session_id}",
        )
    except Exception:
        return ToolResult.text("Permanent host-loss action was rejected.", is_error=True)
    return _json({"host": public_resource_snapshot({"hosts": [host]})["hosts"][0]})


async def diagnostics(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        raw = await _service(ctx).diagnostics(limit=int(args.get("limit", 100)))
        return _json({"resources": public_resource_snapshot(raw["snapshot"]), "events": raw["events"]})
    except Exception:
        return ToolResult.text("Resource diagnostics are unavailable.", is_error=True)


def _handle_error(exc: Exception) -> ToolResult:
    if isinstance(exc, ResourceHandleOwnershipError):
        code = "HANDLE_NOT_OWNED"
    elif isinstance(exc, ResourceHandleConflictError):
        code = "HANDLE_CONFLICT"
    elif isinstance(exc, ResourceRecoveryUnavailable):
        code = "RESOURCE_UNAVAILABLE"
    elif isinstance(exc, ResourceInventoryError):
        code = "INVALID_RESOURCE_REQUEST"
    else:
        code = "RESOURCE_UNAVAILABLE"
    return ToolResult.text(json.dumps({"outcome": code}), is_error=True)


async def _request_can_complete_now(service, session_id: str, requests: list[dict]) -> bool:
    """Conservative availability preflight used only to choose detached mode.

    The allocator remains authoritative.  Returning false merely creates the
    durable FIFO wait rather than holding an agent turn open.
    """
    snapshot = await service.resource_snapshot()
    free = {str(host["id"]) for host in snapshot["hosts"] if host.get("state") == "healthy"}
    retained = list(await service._active_session_handles(session_id))
    # Mirror acquire_handles: exact requests reserve matching retained handles
    # first, then pooled requests consume the remaining retained/free hosts.
    available_retained = retained[:]
    ordered = sorted(enumerate(requests), key=lambda item: item[1].get("host") is None)
    for _index, request in ordered:
        pool, exact = request["pool"], request.get("host")
        match = next(
            (handle for handle in available_retained
             if handle["pool"] == pool and (exact is None or handle["host_id"] == exact)),
            None,
        )
        if match is not None:
            available_retained.remove(match)
            continue
        candidates = {exact} if exact else set(service.inventory.members(pool))
        selected = next(iter(free & candidates), None)
        if selected is None:
            return False
        free.remove(selected)
    return True


async def handle_acquire(ctx: ToolContext, args: dict) -> ToolResult:
    service = _service(ctx)
    try:
        if args.get("detached") and not await _request_can_complete_now(service, ctx.session_id, args["requests"]):
            wait = await service.start_public_handle_wait(ctx.session_id, args["requests"])
            public = await service.public_handle_wait(ctx.session_id, str(wait["id"]))
            return _json({**public, "outcome": public["outcome"] or "WAITING"})
        handles = await service.acquire_handles(ctx.session_id, args["requests"])
        return _json({"outcome": "ACQUIRED", "handles": list(handles)})
    except Exception as exc:
        return _handle_error(exc)


async def handle_list(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        return _json({"handles": list(await _service(ctx).list_session_handles(ctx.session_id))})
    except Exception as exc:
        return _handle_error(exc)


async def handle_release(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        released = await _service(ctx).release_handle(ctx.session_id, args["handle_id"])
        return _json({"outcome": "RELEASED" if released else "ALREADY_SETTLED"})
    except Exception as exc:
        return _handle_error(exc)


async def handle_check(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        if args.get("wait_id"):
            public = await _service(ctx).public_handle_wait(ctx.session_id, args["wait_id"])
            return _json({**public, "outcome": public["outcome"] or "WAITING"})
        handle = await _service(ctx).resolve_handle(ctx.session_id, args["handle_id"])
        return _json({"outcome": "ACTIVE", "handle": handle})
    except Exception as exc:
        return _handle_error(exc)


async def handle_wait_cancel(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        cancelled = await _service(ctx).cancel_public_handle_wait(ctx.session_id, args["wait_id"])
        return _json({"outcome": "REQUEST_CANCELLED" if cancelled else "ALREADY_SETTLED"})
    except Exception as exc:
        return _handle_error(exc)


RESOURCE_SPECS = [
    ToolSpec("resource_inventory", "List trusted hosts and pool membership without connection details.", RESOURCE_EMPTY_SCHEMA, inventory),
    ToolSpec("resource_availability", "List pool availability and durable FIFO queue positions.", RESOURCE_EMPTY_SCHEMA, availability),
    ToolSpec("resource_leases", "List current and historical fenced leases and queued requests.", RESOURCE_EMPTY_SCHEMA, leases),
    ToolSpec("resource_host_drain", "Drain or un-drain a host after confirming its id.", RESOURCE_DRAIN_SCHEMA, drain),
    ToolSpec("resource_host_quarantine", "Quarantine a host globally across every pool after confirming its id.", RESOURCE_QUARANTINE_SCHEMA, quarantine),
    ToolSpec("resource_host_recover", "Recover a quarantined host only after supervisor proof of quiescence.", RESOURCE_RECOVER_SCHEMA, recover),
    ToolSpec("resource_host_permanently_unavailable", "Authoritatively permanently lose an exact confirmed host.", RESOURCE_PERMANENT_LOSS_SCHEMA, permanently_lose),
    ToolSpec("resource_diagnostics", "Inspect resource state and durable lease audit events.", RESOURCE_DIAGNOSTICS_SCHEMA, diagnostics),
    ToolSpec("resource_handle_acquire", "Retain one atomic bundle of pooled or exact-host handles; detached waits resume durably.", RESOURCE_HANDLE_ACQUIRE_SCHEMA, handle_acquire),
    ToolSpec("resource_handle_list", "List active opaque resource handles owned by this session.", RESOURCE_HANDLE_LIST_SCHEMA, handle_list),
    ToolSpec("resource_handle_release", "Release one idle resource handle owned by this session.", RESOURCE_HANDLE_ID_SCHEMA, handle_release),
    ToolSpec("resource_handle_wait_cancel", "Cancel one durable resource-handle wait owned by this session.", RESOURCE_HANDLE_WAIT_CANCEL_SCHEMA, handle_wait_cancel),
    ToolSpec("resource_handle_check", "Check one active handle or durable wait owned by this session.", RESOURCE_HANDLE_CHECK_SCHEMA, handle_check),
]
