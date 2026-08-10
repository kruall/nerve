"""Progressive-disclosure tools for static workflow presets."""
from __future__ import annotations
import json
from collections.abc import Mapping
from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.workflows.presets import WorkflowPresetValidationError

_NAME = {"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}
_START = {"type":"object","properties":{"name":{"type":"string"},"inputs":{"type":"object"}},"required":["name"]}
_JOIN = {"type":"object","properties":{"workflow_id":{"type":"string","pattern":"^wfp-"}},"required":["workflow_id"]}

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

async def join_handler(ctx, args):
    service = getattr(ctx.engine, "workflow_preset_service", None)
    if service is None: return ToolResult.text("Workflow preset controller is not installed.", is_error=True)
    workflow_id = str(args.get("workflow_id") or "")
    try:
        await service.join(workflow_id, session_id=ctx.session_id)
        return ToolResult.text(f"Join applied for preset workflow {workflow_id}. This session may stop now; Nerve will restore it when the workflow finishes.")
    except Exception as error:
        # Import lazily: controller imports stage resolution, which imports the
        # tool registry while this handler module is still being initialized.
        from nerve.workflows.controller import WorkflowActionError
        if isinstance(error, WorkflowActionError): return ToolResult.text(str(error), is_error=True)
        raise

WORKFLOW_PRESET_SPECS = [
    ToolSpec("workflow_preset_list", "List compact, safe summaries of reviewed workflow presets; use describe before choosing one.", {"type":"object","properties":{},"required":[]}, list_handler),
    ToolSpec("workflow_preset_describe", "Describe one workflow preset's static inputs and stages after discovery.", _NAME, describe_handler),
    ToolSpec("workflow_preset_validate", "Validate inputs and resolve a workflow preset without starting it.", _START, validate_handler),
    ToolSpec("workflow_preset_start", "Start a validated workflow preset through the installed workflow controller.", _START, start_handler),
    ToolSpec("workflow_preset_join", "Request automatic restoration of this session when a preset workflow finishes. Returns immediately after the durable join is applied.", _JOIN, join_handler),
]
