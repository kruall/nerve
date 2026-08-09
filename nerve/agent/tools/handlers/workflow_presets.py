"""Progressive-disclosure tools for static workflow presets."""
from __future__ import annotations
import json
from collections.abc import Mapping
from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.workflows.presets import WorkflowPresetValidationError

_NAME = {"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}
_START = {"type":"object","properties":{"name":{"type":"string"},"inputs":{"type":"object"}},"required":["name"]}

def _catalog(ctx): return getattr(ctx.engine, "workflow_preset_catalog", None)
def _out(v): return ToolResult.text(json.dumps(v, indent=2, sort_keys=True, default=str))

async def list_handler(ctx, args):
    catalog = _catalog(ctx)
    if catalog is None: return ToolResult.text("Workflow preset catalog is unavailable.", is_error=True)
    return _out({"generation":catalog.snapshot.generation, "presets":catalog.snapshot.summaries()})

async def describe_handler(ctx, args):
    catalog = _catalog(ctx)
    if catalog is None: return ToolResult.text("Workflow preset catalog is unavailable.", is_error=True)
    try: return _out(catalog.describe(str(args.get("name") or "")))
    except WorkflowPresetValidationError as e: return ToolResult.text(str(e), is_error=True)

async def validate_handler(ctx, args):
    catalog = _catalog(ctx)
    if catalog is None: return ToolResult.text("Workflow preset catalog is unavailable.", is_error=True)
    try: return _out({"valid":True,"plan":catalog.compile(str(args.get("name") or ""), args.get("inputs", {})).as_dict()})
    except WorkflowPresetValidationError as e: return ToolResult.text(str(e), is_error=True)

async def start_handler(ctx, args):
    catalog = _catalog(ctx)
    service = getattr(ctx.engine, "workflow_preset_service", None)
    if catalog is None: return ToolResult.text("Workflow preset catalog is unavailable.", is_error=True)
    try: plan = catalog.compile(str(args.get("name") or ""), args.get("inputs", {}))
    except WorkflowPresetValidationError as e: return ToolResult.text(str(e), is_error=True)
    if service is None: return ToolResult.text("Workflow preset controller is not installed; the preset can be validated but not started.", is_error=True)
    started = await service.start(session_id=ctx.session_id, plan=plan)
    if not isinstance(started, Mapping): return ToolResult.text("Workflow preset controller returned an invalid result.", is_error=True)
    # Keep start responses stable and safe for chat clients.  The durable row
    # carries pinned inputs and stage specs; those must never be echoed into a
    # tool-result block.
    preset = plan.preset
    return _out({"workflow_id": started.get("id"), "preset": {"name": preset.name, "title": preset.title}, "status": started.get("status"), "links": {"workflow": f"/api/preset-workflows/{started.get('id')}", "workflows": "/workflows"}})

WORKFLOW_PRESET_SPECS = [
    ToolSpec("workflow_preset_list", "List compact summaries of reviewed workflow presets.", {"type":"object","properties":{},"required":[]}, list_handler),
    ToolSpec("workflow_preset_describe", "Describe one workflow preset and its static stages.", _NAME, describe_handler),
    ToolSpec("workflow_preset_validate", "Resolve and pin a workflow preset without starting it.", _START, validate_handler),
    ToolSpec("workflow_preset_start", "Start a pinned preset through the installed workflow controller.", _START, start_handler),
]
