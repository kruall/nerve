"""Progressive-disclosure MCP facade for declarative execution profiles."""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    EXECUTION_KIND_DESCRIBE_SCHEMA,
    EXECUTION_KIND_LIST_SCHEMA,
    EXECUTION_KIND_START_SCHEMA,
    EXECUTION_KIND_VALIDATE_SCHEMA,
)
from nerve.executions import ExecutionCatalog, OperationValidationError
from nerve.executions.public import public_execution

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
        started = await service.start(session_id=ctx.session_id, plan=plan)
        if not isinstance(started, Mapping):
            raise TypeError("ExecutionService.start must return a mapping")
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
        "Validate and start a configured execution kind through Nerve's lifecycle service. Arguments never pass through a shell.",
        EXECUTION_KIND_START_SCHEMA,
        execution_kind_start_handler,
    ),
]
