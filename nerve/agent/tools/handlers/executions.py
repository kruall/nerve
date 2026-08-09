"""Progressive-disclosure MCP facade for declarative execution profiles."""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    EXECUTION_CANCEL_SCHEMA,
    EXECUTION_KIND_DESCRIBE_SCHEMA,
    EXECUTION_KIND_LIST_SCHEMA,
    EXECUTION_KIND_START_SCHEMA,
    EXECUTION_KIND_VALIDATE_SCHEMA,
    EXECUTION_JOIN_SCHEMA,
    EXECUTION_FORGET_SCHEMA,
    EXECUTION_LIST_SCHEMA,
    EXECUTION_STATUS_SCHEMA,
    EXECUTION_TAIL_SCHEMA,
)
from nerve.executions import ExecutionCatalog, OperationValidationError
from nerve.executions.public import DEFAULT_LOG_TAIL_LINES, public_execution, public_log_tail

logger = logging.getLogger(__name__)


def _catalog(ctx: ToolContext) -> ExecutionCatalog | None:
    return ctx.execution_catalog or getattr(ctx.engine, "execution_catalog", None)


def _json(value: object) -> ToolResult:
    return ToolResult.text(json.dumps(value, indent=2, sort_keys=True, default=str))


async def execution_kind_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    catalog = _catalog(ctx)
    if catalog is None:
        return ToolResult.text("Execution catalog is unavailable.", is_error=True)
    snapshot = catalog.snapshot
    return _json({"generation": snapshot.generation, "kinds": snapshot.summaries()})


async def execution_kind_describe_handler(ctx: ToolContext, args: dict) -> ToolResult:
    catalog = _catalog(ctx)
    if catalog is None:
        return ToolResult.text("Execution catalog is unavailable.", is_error=True)
    try:
        return _json(catalog.describe(str(args.get("kind") or "")))
    except OperationValidationError as e:
        return ToolResult.text(str(e), is_error=True)


async def execution_kind_validate_handler(ctx: ToolContext, args: dict) -> ToolResult:
    catalog = _catalog(ctx)
    if catalog is None:
        return ToolResult.text("Execution catalog is unavailable.", is_error=True)
    try:
        plan = catalog.compile(
            str(args.get("kind") or ""),
            args.get("arguments", {}),
            args.get("resources", {}),
        )
    except OperationValidationError as e:
        return ToolResult.text(str(e), is_error=True)
    return _json({"valid": True, "plan": plan.as_dict(redact_secrets=True)})


async def execution_kind_start_handler(ctx: ToolContext, args: dict) -> ToolResult:
    catalog = _catalog(ctx)
    if catalog is None:
        return ToolResult.text("Execution catalog is unavailable.", is_error=True)
    requested_kind = str(args.get("kind") or "")
    try:
        plan = catalog.compile(
            requested_kind,
            args.get("arguments", {}),
            args.get("resources", {}),
        )
    except OperationValidationError as e:
        return ToolResult.text(str(e), is_error=True)
    service = ctx.execution_service or getattr(ctx.engine, "execution_service", None)
    if service is None:
        return ToolResult.text(
            "Execution lifecycle service is not installed; the profile can be "
            "validated but cannot be started by this Nerve build.",
            is_error=True,
        )
    try:
        detached = bool(args.get("detached", False))
        started = await service.start(
            session_id=ctx.session_id, plan=plan, auto_continue=detached,
        )
        if not isinstance(started, Mapping):
            raise TypeError("ExecutionService.start must return a mapping")
        if not detached:
            started = await service.join_execution(
                execution_id=str(started["id"]), session_id=ctx.session_id,
            )
    except Exception as e:  # lifecycle errors may contain secret argv; never echo them
        logger.error(
            "Execution lifecycle service failed to start kind %s (%s)",
            requested_kind, type(e).__name__,
        )
        return ToolResult.text(
            "Could not start execution: lifecycle service rejected the compiled plan.",
            is_error=True,
        )
    return _json({
        "kind": plan.kind,
        "profile_version": plan.profile_version,
        "profile_hash": plan.profile_hash,
        "execution": public_execution(started),
    })


def _service(ctx: ToolContext):
    return ctx.execution_service or getattr(ctx.engine, "execution_service", None)


async def _owned_execution(ctx: ToolContext, execution_id: str):
    service = _service(ctx)
    if service is None:
        raise RuntimeError("Execution lifecycle service is unavailable.")
    row = await service.get_execution(execution_id=execution_id)
    if row is None or row.get("session_id") != ctx.session_id:
        raise LookupError("Execution not found in this session.")
    return service, row


async def execution_status_handler(ctx: ToolContext, args: dict) -> ToolResult:
    try:
        _, row = await _owned_execution(ctx, str(args.get("execution_id") or ""))
    except (RuntimeError, LookupError) as e:
        return ToolResult.text(str(e), is_error=True)
    return _json({"execution": public_execution(row)})


async def execution_join_handler(ctx: ToolContext, args: dict) -> ToolResult:
    execution_id = str(args.get("execution_id") or "")
    try:
        service, _ = await _owned_execution(ctx, execution_id)
        row = await service.join_execution(
            execution_id=execution_id, session_id=ctx.session_id,
        )
    except (RuntimeError, LookupError, KeyError, ValueError) as e:
        return ToolResult.text(str(e), is_error=True)
    return _json({"execution": public_execution(row)})


async def execution_forget_handler(ctx: ToolContext, args: dict) -> ToolResult:
    execution_id = str(args.get("execution_id") or "")
    try:
        service, _ = await _owned_execution(ctx, execution_id)
        row = await service.forget_execution(
            execution_id=execution_id, session_id=ctx.session_id,
        )
    except (RuntimeError, LookupError, KeyError, ValueError) as e:
        return ToolResult.text(str(e), is_error=True)
    return _json({"execution": public_execution(row)})


async def execution_tail_handler(ctx: ToolContext, args: dict) -> ToolResult:
    execution_id = str(args.get("execution_id") or "")
    try:
        service, _ = await _owned_execution(ctx, execution_id)
        limit = max(1, min(int(args.get("limit", DEFAULT_LOG_TAIL_LINES)), 500))
        before = args.get("before")
        raw = await service.tail_logs(
            execution_id=execution_id,
            limit=limit,
            before=int(before) if before is not None else None,
        )
    except (RuntimeError, LookupError, TypeError, ValueError) as e:
        return ToolResult.text(str(e), is_error=True)
    return _json(public_log_tail(raw, requested_limit=limit))


async def execution_cancel_handler(ctx: ToolContext, args: dict) -> ToolResult:
    execution_id = str(args.get("execution_id") or "")
    try:
        service, _ = await _owned_execution(ctx, execution_id)
        row = await service.cancel_execution(
            execution_id=execution_id,
            requested_by=f"session:{ctx.session_id}",
            reason=args.get("reason"),
        )
    except (RuntimeError, LookupError, ValueError) as e:
        return ToolResult.text(str(e), is_error=True)
    return _json({"execution": public_execution(row)})


async def execution_list_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service = _service(ctx)
    if service is None:
        return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    rows = await service.list_executions(
        session_id=ctx.session_id,
        include_terminal=bool(args.get("include_terminal", True)),
        limit=max(1, min(int(args.get("limit", 20)), 100)),
    )
    return _json({"executions": [public_execution(row) for row in rows]})


EXECUTION_SPECS = [
    ToolSpec(
        "execution_kind_list",
        "List compact summaries of configured declarative execution kinds. Use describe only for the kind you need.",
        EXECUTION_KIND_LIST_SCHEMA,
        execution_kind_list_handler,
    ),
    ToolSpec(
        "execution_kind_describe",
        "Describe one execution kind's typed argument schema, resource slots, result rules, timeout, cleanup, and cancellation semantics.",
        EXECUTION_KIND_DESCRIBE_SCHEMA,
        execution_kind_describe_handler,
    ),
    ToolSpec(
        "execution_kind_validate",
        "Validate a kind operation and compile its redacted immutable plan without starting it.",
        EXECUTION_KIND_VALIDATE_SCHEMA,
        execution_kind_validate_handler,
    ),
    ToolSpec(
        "execution_kind_start",
        "Validate and start a configured execution kind. Waits by default; "
        "pass detached=true to return immediately with a durable completion wakeup.",
        EXECUTION_KIND_START_SCHEMA,
        execution_kind_start_handler,
    ),
    ToolSpec(
        "execution_status",
        "Get one execution owned by this session without changing its wait state.",
        EXECUTION_STATUS_SCHEMA,
        execution_status_handler,
    ),
    ToolSpec(
        "execution_join",
        "Wait for one execution owned by this session. Joining consumes its automatic completion wakeup.",
        EXECUTION_JOIN_SCHEMA,
        execution_join_handler,
    ),
    ToolSpec(
        "execution_forget",
        "Stop waiting for an execution without cancelling it or waking this session on completion.",
        EXECUTION_FORGET_SCHEMA,
        execution_forget_handler,
    ),
    ToolSpec(
        "execution_tail",
        "Read a bounded stdout/stderr tail for one execution owned by this session.",
        EXECUTION_TAIL_SCHEMA,
        execution_tail_handler,
    ),
    ToolSpec(
        "execution_cancel",
        "Cancel one active execution owned by this session and suppress its pending completion continuation.",
        EXECUTION_CANCEL_SCHEMA,
        execution_cancel_handler,
    ),
    ToolSpec(
        "execution_list",
        "List executions owned by this session without loading their full logs.",
        EXECUTION_LIST_SCHEMA,
        execution_list_handler,
    ),
]
