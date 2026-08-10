"""REST discovery/validation facade for reviewed workflow presets."""
from typing import Any
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps
from nerve.workflows.presets import WorkflowPresetValidationError
router = APIRouter()

def _catalog(): return get_deps().engine.workflow_preset_catalog
@router.get("/api/workflow-presets")
async def list_workflow_presets(user: dict = Depends(require_auth)):
    c=_catalog(); return {"generation":c.snapshot.generation,"presets":c.snapshot.summaries()}
@router.get("/api/workflow-presets/{name}")
async def describe_workflow_preset(name: str, user: dict = Depends(require_auth)):
    try: return _catalog().describe(name)
    except WorkflowPresetValidationError as e: raise HTTPException(404, str(e)) from e
def _compile(name: str, body: Any):
    if not isinstance(body, dict) or set(body)-{"inputs", "session_id"}: raise HTTPException(422, "request body must contain inputs and optional session_id")
    try: return _catalog().compile(name, body.get("inputs", {}))
    except WorkflowPresetValidationError as e: raise HTTPException(422, str(e)) from e
@router.post("/api/workflow-presets/{name}/validate")
async def validate_workflow_preset(name: str, body: Any=Body(default={}), user: dict=Depends(require_auth)):
    return {"valid":True,"plan":_compile(name,body).as_dict()}
@router.post("/api/workflow-presets/{name}/start")
async def start_workflow_preset(name: str, body: Any=Body(default={}), user: dict=Depends(require_auth)):
    plan=_compile(name,body); service=getattr(get_deps().engine,"workflow_preset_service",None)
    if service is None: raise HTTPException(503,"workflow preset controller is not installed")
    session_id = body.get("session_id")
    if not isinstance(session_id, str) or not session_id: raise HTTPException(422, "session_id is required")
    if not await get_deps().db.get_session(session_id): raise HTTPException(404, "owner session not found")
    # The persisted plan contains pinned prompts and raw specs.  The public
    # workflow projection is the only start response the web client needs.
    return {"workflow":await _public(await service.start(session_id=session_id,plan=plan))}

def _service():
    service = getattr(get_deps().engine, "workflow_preset_service", None)
    if service is None: raise HTTPException(503, "workflow preset controller is not installed")
    return service

async def _public(row: dict) -> dict:
    """Deliberately omit input prompts, raw stage specs and artifacts from list views."""
    service = _service(); stages = await service.db.list_stage_runs(row["id"])
    plan = row.get("plan") or {}; preset = plan.get("preset") or {}
    completion = await service.db.get_preset_workflow_completion(row["id"])
    return {"id": row["id"], "owner_session_id": row["observer_session_id"],
      "preset": {k: preset.get(k) for k in ("name", "version", "preset_hash", "title", "description", "budget_usd")},
      "preset_hash": row.get("preset_hash"), "status": row["status"], "result": row.get("result"), "available_actions": await service.available_actions(row), "spent_usd": None,
      "created_at": row["created_at"], "started_at": row.get("started_at"), "finished_at": row.get("finished_at"), "updated_at": row["updated_at"], "completion": completion,
      "stages": [{"id": s["id"], "stage_id": s["stage_id"], "runner": s["runner"], "status": s["status"], "child_type": s.get("child_type"), "child_id": s.get("child_id"), "created_at": s["created_at"], "started_at": s.get("started_at"), "finished_at": s.get("finished_at"),
        "runtime": ({"model": (s.get("spec") or {}).get("spec", {}).get("model"), "effort": (s.get("spec") or {}).get("spec", {}).get("reasoning_effort"), "sandbox": (s.get("spec") or {}).get("spec", {}).get("sandbox"), "capabilities": len((s.get("spec") or {}).get("spec", {}).get("mcp", {}).get("allow", []))} if s["runner"] == "agent" else {"kind": (s.get("spec") or {}).get("spec", {}).get("kind")}),
        "summary": (s.get("result") or {}).get("summary") or (s.get("result") or {}).get("outcome")} for s in stages]}

@router.get("/api/preset-workflows")
async def list_preset_workflows(user: dict = Depends(require_auth)):
    service = _service(); rows = await service.db.list_preset_workflows()
    return {"workflows": [await _public(row) for row in rows], "total": await service.db.count_preset_workflows()}

@router.get("/api/preset-workflows/{workflow_id}")
async def get_preset_workflow(workflow_id: str, user: dict = Depends(require_auth)):
    row = await _service().db.get_preset_workflow(workflow_id)
    if row is None: raise HTTPException(404, "workflow not found")
    return await _public(row)

@router.post("/api/preset-workflows/{workflow_id}/cancel")
async def cancel_preset_workflow(workflow_id: str, user: dict = Depends(require_auth)):
    service = _service(); row = await service.db.get_preset_workflow(workflow_id)
    if row is None: raise HTTPException(404, "workflow not found")
    await service.cancel(workflow_id, reason="Cancelled from web UI")
    row = await service.db.get_preset_workflow(workflow_id)
    return await _public(row)

class WorkflowActionRequest(BaseModel):
    revision: int = Field(ge=0)
    idempotency_key: str = Field(min_length=8, max_length=200)
    reason: str | None = Field(default=None, max_length=500)
    confirmed: bool = False

@router.post("/api/preset-workflows/{workflow_id}/actions/{action}")
async def execute_preset_workflow_action(workflow_id: str, action: str, body: WorkflowActionRequest,
                                         user: dict = Depends(require_auth)):
    if action != "abandon": raise HTTPException(404, "workflow action not found")
    if not body.confirmed: raise HTTPException(422, "confirmation is required")
    service = _service()
    try:
        row = await service.execute_action(workflow_id, action=action, revision=body.revision,
                                           actor=str(user.get("sub") or "unknown"), reason=body.reason,
                                           idempotency_key=body.idempotency_key)
    except WorkflowActionError as error:
        codes = {"not_found": 404, "stale": 409, "not_available": 409}
        raise HTTPException(codes.get(str(error), 403), str(error)) from error
    return await _public(row)
