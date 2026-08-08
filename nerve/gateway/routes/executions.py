"""HTTP discovery/validation facade for declarative execution kinds."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import logging
from typing import Any, Literal

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, Field

from nerve.executions import OperationValidationError
from nerve.executions.public import (
    DEFAULT_LOG_TAIL_LINES,
    MAX_LOG_TAIL_LINES,
    public_execution,
    public_log_tail,
    public_resource_snapshot,
)
from nerve.gateway.auth import require_auth
from nerve.gateway.routes._deps import get_deps

router = APIRouter()
logger = logging.getLogger(__name__)


class CancelExecutionRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=500)


class RetryExecutionRequest(BaseModel):
    profile_mode: Literal["pinned", "current"] = "pinned"


class DrainHostRequest(BaseModel):
    draining: bool
    confirm_host_id: str


class RecoverHostRequest(BaseModel):
    confirm_host_id: str
    remote_quiescence_confirmed: bool


def _execution_service():
    service = getattr(get_deps().engine, "execution_service", None)
    if service is None:
        raise HTTPException(
            status_code=503,
            detail="execution lifecycle service is not installed",
        )
    return service


def _resource_service():
    engine = get_deps().engine
    service = getattr(engine, "resource_service", None)
    # The lifecycle service may deliberately aggregate resource operations;
    # accept that deployment shape while keeping a separate formal contract.
    service = service or getattr(engine, "execution_service", None)
    if service is None:
        raise HTTPException(
            status_code=503,
            detail="resource inventory service is not installed",
        )
    return service


async def _service_call(method: str, **kwargs):
    service = _execution_service()
    fn = getattr(service, method, None)
    if not callable(fn):
        raise HTTPException(
            status_code=503,
            detail=f"execution lifecycle service does not provide {method}",
        )
    try:
        return await fn(**kwargs)
    except HTTPException:
        raise
    except (KeyError, LookupError) as e:
        raise HTTPException(status_code=404, detail="execution or host not found") from e
    except ValueError as e:
        # Service validation messages are required to be value-free.  Keep a
        # generic fallback here rather than reflecting arbitrary backend text.
        logger.info("Execution UI action %s was rejected (%s)", method, type(e).__name__)
        raise HTTPException(status_code=409, detail="execution action was rejected") from e
    except Exception as e:
        logger.error("Execution UI action %s failed (%s)", method, type(e).__name__)
        raise HTTPException(status_code=503, detail="execution service action failed") from e


async def _resource_call(method: str, **kwargs):
    service = _resource_service()
    fn = getattr(service, method, None)
    if not callable(fn):
        raise HTTPException(
            status_code=503,
            detail=f"resource inventory service does not provide {method}",
        )
    try:
        return await fn(**kwargs)
    except (KeyError, LookupError) as e:
        raise HTTPException(status_code=404, detail="host not found") from e
    except ValueError as e:
        logger.info("Resource UI action %s was rejected (%s)", method, type(e).__name__)
        raise HTTPException(status_code=409, detail="resource action was rejected") from e
    except Exception as e:
        logger.error("Resource UI action %s failed (%s)", method, type(e).__name__)
        raise HTTPException(status_code=503, detail="resource service action failed") from e


async def session_execution_activity(
    session_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """Best-effort busy-state enrichment used by the sessions routes.

    The execution lifecycle service is optional during staged rollout.  A
    missing service leaves agent activity untouched; a broken service must not
    make the conversation list unavailable.
    """
    service = getattr(get_deps().engine, "execution_service", None)
    fn = getattr(service, "session_activity", None)
    if not callable(fn) or not session_ids:
        return {}
    try:
        raw = await fn(session_ids=session_ids)
    except Exception as e:  # noqa: BLE001 - optional enrichment is fail-open
        logger.warning("Could not enrich session execution activity (%s)", type(e).__name__)
        return {}
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for session_id in session_ids:
        activity = raw.get(session_id)
        if not isinstance(activity, Mapping):
            continue
        count = activity.get("active_execution_count", 0)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            count = 0
        statuses = activity.get("execution_statuses", [])
        if not isinstance(statuses, list):
            statuses = []
        result[session_id] = {
            "active_execution_count": count,
            "execution_statuses": [
                status for status in statuses
                if isinstance(status, str)
            ][:20],
        }
    return result


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
        "execution": public_execution(execution),
    }


# --- Detached execution UI facade ---


@router.get("/api/sessions/{session_id}/executions")
async def list_session_executions(
    session_id: str,
    include_terminal: bool = True,
    limit: int = 20,
    user: dict = Depends(require_auth),
):
    rows = await _service_call(
        "list_executions",
        session_id=session_id,
        include_terminal=include_terminal,
        limit=max(1, min(limit, 100)),
    )
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise HTTPException(status_code=503, detail="execution service returned an invalid list")
    return {"executions": [public_execution(row) for row in rows if isinstance(row, Mapping)]}


@router.get("/api/executions/{execution_id}")
async def get_execution(execution_id: str, user: dict = Depends(require_auth)):
    row = await _service_call("get_execution", execution_id=execution_id)
    if row is None:
        raise HTTPException(status_code=404, detail="execution not found")
    if not isinstance(row, Mapping):
        raise HTTPException(status_code=503, detail="execution service returned an invalid record")
    return {"execution": public_execution(row)}


@router.get("/api/executions/{execution_id}/logs")
async def get_execution_logs(
    execution_id: str,
    limit: int = DEFAULT_LOG_TAIL_LINES,
    before: int | None = None,
    user: dict = Depends(require_auth),
):
    bounded_limit = max(1, min(limit, MAX_LOG_TAIL_LINES))
    raw = await _service_call(
        "tail_logs", execution_id=execution_id, limit=bounded_limit, before=before,
    )
    if not isinstance(raw, Mapping):
        raise HTTPException(status_code=503, detail="execution service returned invalid logs")
    return public_log_tail(raw, requested_limit=bounded_limit)


@router.post("/api/executions/{execution_id}/cancel")
async def cancel_execution(
    execution_id: str,
    request: CancelExecutionRequest,
    user: dict = Depends(require_auth),
):
    row = await _service_call(
        "cancel_execution",
        execution_id=execution_id,
        requested_by=str(user.get("sub", "user")),
        reason=request.reason,
    )
    if not isinstance(row, Mapping):
        raise HTTPException(status_code=503, detail="execution service returned an invalid record")
    return {"execution": public_execution(row)}


@router.post("/api/executions/{execution_id}/retry")
async def retry_execution(
    execution_id: str,
    request: RetryExecutionRequest,
    user: dict = Depends(require_auth),
):
    row = await _service_call(
        "retry_execution",
        execution_id=execution_id,
        profile_mode=request.profile_mode,
        requested_by=str(user.get("sub", "user")),
    )
    if not isinstance(row, Mapping):
        raise HTTPException(status_code=503, detail="execution service returned an invalid record")
    return {"execution": public_execution(row)}


@router.get("/api/resources")
async def list_resources(user: dict = Depends(require_auth)):
    raw = await _resource_call("resource_snapshot")
    if not isinstance(raw, Mapping):
        raise HTTPException(status_code=503, detail="resource service returned invalid resources")
    return {"resources": public_resource_snapshot(raw)}


@router.post("/api/resources/hosts/{host_id}/drain")
async def set_host_draining(
    host_id: str,
    request: DrainHostRequest,
    user: dict = Depends(require_auth),
):
    if request.confirm_host_id != host_id:
        raise HTTPException(status_code=409, detail="host confirmation does not match")
    raw = await _resource_call(
        "set_host_draining",
        host_id=host_id,
        draining=request.draining,
        requested_by=str(user.get("sub", "user")),
    )
    snapshot = public_resource_snapshot({"hosts": [raw]}) if isinstance(raw, Mapping) else None
    if snapshot is None or not snapshot["hosts"]:
        raise HTTPException(status_code=503, detail="resource service returned an invalid host")
    return {"host": snapshot["hosts"][0]}


@router.post("/api/resources/hosts/{host_id}/recover")
async def recover_host(
    host_id: str,
    request: RecoverHostRequest,
    user: dict = Depends(require_auth),
):
    if request.confirm_host_id != host_id:
        raise HTTPException(status_code=409, detail="host confirmation does not match")
    if not request.remote_quiescence_confirmed:
        raise HTTPException(
            status_code=409,
            detail="remote quiescence must be confirmed before quarantine recovery",
        )
    raw = await _resource_call(
        "recover_host",
        host_id=host_id,
        requested_by=str(user.get("sub", "user")),
        remote_quiescence_confirmed=True,
    )
    snapshot = public_resource_snapshot({"hosts": [raw]}) if isinstance(raw, Mapping) else None
    if snapshot is None or not snapshot["hosts"]:
        raise HTTPException(status_code=503, detail="resource service returned an invalid host")
    return {"host": snapshot["hosts"][0]}
