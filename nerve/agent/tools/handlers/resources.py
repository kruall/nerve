"""Secret-free MCP facade for trusted resource inventory and leases."""
from __future__ import annotations
import json

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    RESOURCE_DIAGNOSTICS_SCHEMA, RESOURCE_DRAIN_SCHEMA, RESOURCE_EMPTY_SCHEMA,
    RESOURCE_QUARANTINE_SCHEMA, RESOURCE_RECOVER_SCHEMA,
)
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
    if host_id != args.get("confirm_host_id") or args.get("remote_quiescence_confirmed") is not True:
        return ToolResult.text("Recovery requires matching host confirmation and confirmed remote quiescence.", is_error=True)
    try:
        host = await _service(ctx).recover_host(
            host_id=host_id, requested_by=f"session:{ctx.session_id}",
            remote_quiescence_confirmed=True,
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


async def diagnostics(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        raw = await _service(ctx).diagnostics(limit=int(args.get("limit", 100)))
        return _json({"resources": public_resource_snapshot(raw["snapshot"]), "events": raw["events"]})
    except Exception:
        return ToolResult.text("Resource diagnostics are unavailable.", is_error=True)


RESOURCE_SPECS = [
    ToolSpec("resource_inventory", "List trusted hosts and pool membership without connection details.", RESOURCE_EMPTY_SCHEMA, inventory),
    ToolSpec("resource_availability", "List pool availability and durable FIFO queue positions.", RESOURCE_EMPTY_SCHEMA, availability),
    ToolSpec("resource_leases", "List current and historical fenced leases and queued requests.", RESOURCE_EMPTY_SCHEMA, leases),
    ToolSpec("resource_host_drain", "Drain or un-drain a host after confirming its id.", RESOURCE_DRAIN_SCHEMA, drain),
    ToolSpec("resource_host_quarantine", "Quarantine a host globally across every pool after confirming its id.", RESOURCE_QUARANTINE_SCHEMA, quarantine),
    ToolSpec("resource_host_recover", "Recover a quarantined host only after remote quiescence is confirmed.", RESOURCE_RECOVER_SCHEMA, recover),
    ToolSpec("resource_diagnostics", "Inspect resource state and durable lease audit events.", RESOURCE_DIAGNOSTICS_SCHEMA, diagnostics),
]
