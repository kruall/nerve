"""HTTP discovery/validation facade for declarative execution kinds."""

from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException

from nerve.executions import OperationValidationError
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/api/execution-kinds")
async def list_execution_kinds(user: dict = Depends(require_auth)):
    snapshot = get_deps().engine.execution_catalog.snapshot
    return {"generation": snapshot.generation, "kinds": snapshot.summaries()}


@router.get("/api/execution-kinds/{kind}")
async def describe_execution_kind(kind: str, user: dict = Depends(require_auth)):
    try:
        return get_deps().engine.execution_catalog.describe(kind)
    except OperationValidationError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


def _compile(kind: str, body: Any):
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="request body must be an object")
    unknown = sorted(set(body) - {"arguments", "resources"})
    if unknown:
        raise HTTPException(status_code=422, detail=f"unknown field(s): {', '.join(unknown)}")
    try:
        return get_deps().engine.execution_catalog.compile(
            kind, body.get("arguments", {}), body.get("resources", {}),
        )
    except OperationValidationError as e:
        status = 404 if str(e).startswith("unknown execution kind:") else 422
        raise HTTPException(status_code=status, detail=str(e)) from e


@router.post("/api/execution-kinds/{kind}/validate")
async def validate_execution_kind(
    kind: str,
    body: Any = Body(default={}),
    user: dict = Depends(require_auth),
):
    plan = _compile(kind, body)
    return {"valid": True, "plan": plan.as_dict(redact_secrets=True)}


@router.post("/api/execution-kinds/{kind}/start")
async def start_execution_kind(
    kind: str,
    body: Any = Body(default={}),
    user: dict = Depends(require_auth),
):
    engine = get_deps().engine
    plan = _compile(kind, body)
    if engine.execution_service is None:
        raise HTTPException(
            status_code=503,
            detail="execution lifecycle service is not installed",
        )
    try:
        execution = await engine.execution_service.start(
            session_id="system", plan=plan,
        )
        if not isinstance(execution, Mapping):
            raise TypeError("ExecutionService.start must return a mapping")
    except Exception as e:
        # Lifecycle exceptions may embed argv values. Those can include secret
        # arguments, so keep the wire and log diagnostics value-free.
        logger.error(
            "Execution lifecycle service failed to start kind %s (%s)",
            kind, type(e).__name__,
        )
        raise HTTPException(
            status_code=503,
            detail="execution lifecycle service rejected the compiled plan",
        ) from e
    return {
        "kind": plan.kind,
        "profile_version": plan.profile_version,
        "profile_hash": plan.profile_hash,
        "execution": dict(execution),
    }
