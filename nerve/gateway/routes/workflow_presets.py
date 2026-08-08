"""REST discovery/validation facade for reviewed workflow presets."""
from typing import Any
from fastapi import APIRouter, Body, Depends, HTTPException
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
    if not isinstance(body, dict) or set(body)-{"inputs"}: raise HTTPException(422, "request body must contain only inputs")
    try: return _catalog().compile(name, body.get("inputs", {}))
    except WorkflowPresetValidationError as e: raise HTTPException(422, str(e)) from e
@router.post("/api/workflow-presets/{name}/validate")
async def validate_workflow_preset(name: str, body: Any=Body(default={}), user: dict=Depends(require_auth)):
    return {"valid":True,"plan":_compile(name,body).as_dict()}
@router.post("/api/workflow-presets/{name}/start")
async def start_workflow_preset(name: str, body: Any=Body(default={}), user: dict=Depends(require_auth)):
    plan=_compile(name,body); service=getattr(get_deps().engine,"workflow_preset_service",None)
    if service is None: raise HTTPException(503,"workflow preset controller is not installed")
    return {"plan":plan.as_dict(),"workflow":await service.start(session_id="system",plan=plan)}
