"""Progressive-disclosure MCP facade for declarative execution profiles."""

from __future__ import annotations

from collections.abc import Mapping
import json
import logging

from nerve.agent.tools.registry import ToolContext, ToolResult, ToolSpec
from nerve.agent.tools.schemas import (
    EXECUTION_CANCEL_SCHEMA,
    ARTIFACT_TRANSFER_SCHEMA,
    EXECUTION_KIND_DESCRIBE_SCHEMA,
    EXECUTION_KIND_LIST_SCHEMA,
    EXECUTION_KIND_START_SCHEMA,
    EXECUTION_KIND_VALIDATE_SCHEMA,
    EXECUTION_FORGET_SCHEMA,
    EXECUTION_LIST_SCHEMA,
    EXECUTION_STATUS_SCHEMA,
    EXECUTION_TAIL_SCHEMA,
    RESOURCE_COMMAND_SCHEMA,
    YDB_MAKE_SCHEMA,
    YDB_TEST_SCHEMA,
    YDB_FILE_LIST_SCHEMA,
    YDB_FILE_FIND_SCHEMA,
    YDB_FILE_READ_SCHEMA,
    YDB_HOST_RELEASE_SCHEMA,
    SPIN_VERIFY_REMOTE_SCHEMA, SPIN_REPLAY_REMOTE_SCHEMA,
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


async def _ydb_handler(ctx: ToolContext, args: dict, kind: str) -> ToolResult:
    service = _service(ctx)
    if service is None:
        return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try:
        values = args.get("args", [])
        if not isinstance(values, list):
            raise ValueError("args must be an array")
        started = await service.start_ydb(session_id=ctx.session_id, kind=kind,
                                          worktree=str(args.get("worktree") or ""), args=values,
                                          build_type=str(args.get("build_type") or "relwithdebinfo"),
                                          publish=args.get("publish"),
                                          auto_continue=bool(args.get("detached", False)))
        if not bool(args.get("detached", False)):
            started = await service.join_execution(execution_id=str(started["id"]), session_id=ctx.session_id)
    except Exception as exc:
        logger.warning("YDB operation rejected (%s)", type(exc).__name__)
        return ToolResult.text("Could not start YDB operation: worktree or reviewed YDB service rejected the request.", is_error=True)
    return _json({"kind": kind, "execution": public_execution(started)})


async def ydb_make_handler(ctx: ToolContext, args: dict) -> ToolResult:
    return await _ydb_handler(ctx, args, "ydb_make")

async def artifact_transfer_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service = _service(ctx)
    if service is None: return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try:
        row = await service.start_artifact_transfer(session_id=ctx.session_id, source=args.get("source", {}), destination=args.get("destination", {}), auto_continue=bool(args.get("detached", False)))
        if not args.get("detached", False): row = await service.join_execution(execution_id=str(row["id"]), session_id=ctx.session_id)
    except Exception as exc:
        logger.warning("artifact transfer rejected (%s)", type(exc).__name__)
        return ToolResult.text("Could not start direct artifact transfer.", is_error=True)
    return _json({"kind": "artifact_transfer", "execution": public_execution(row)})


async def resource_command_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service = _service(ctx)
    if service is None:
        return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try:
        row = await service.start_resource_command(
            session_id=ctx.session_id,
            pool=args.get("pool"),
            executable=args.get("executable"),
            args=args.get("args", []),
            timeout_seconds=args.get("timeout_seconds", 3600),
            auto_continue=bool(args.get("detached", False)),
        )
        if not args.get("detached", False):
            row = await service.join_execution(
                execution_id=str(row["id"]), session_id=ctx.session_id,
            )
    except Exception as exc:
        logger.warning("resource command rejected (%s)", type(exc).__name__)
        return ToolResult.text(
            "Could not start resource command: pool or command arguments were rejected.",
            is_error=True,
        )
    return _json({"kind": "resource_command", "execution": public_execution(row)})


async def ydb_test_handler(ctx: ToolContext, args: dict) -> ToolResult:
    return await _ydb_handler(ctx, args, "ydb_test")


async def _ydb_files_handler(ctx: ToolContext, args: dict, operation: str) -> ToolResult:
    service = _service(ctx)
    if service is None: return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try: return _json(await service.inspect_ydb_files(session_id=ctx.session_id, operation=operation, arguments=args))
    except Exception as exc:
        logger.warning("YDB file operation rejected (%s)", type(exc).__name__)
        return ToolResult.text("Could not inspect files in this session's YDB checkout.", is_error=True)


async def ydb_file_list_handler(ctx: ToolContext, args: dict) -> ToolResult: return await _ydb_files_handler(ctx, args, "list")
async def ydb_file_find_handler(ctx: ToolContext, args: dict) -> ToolResult: return await _ydb_files_handler(ctx, args, "find")
async def ydb_file_read_handler(ctx: ToolContext, args: dict) -> ToolResult: return await _ydb_files_handler(ctx, args, "read")


async def ydb_host_release_handler(ctx: ToolContext, args: dict) -> ToolResult:
    service = _service(ctx)
    if service is None: return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try: released = await service.release_ydb_host(session_id=ctx.session_id)
    except Exception as exc:
        logger.warning("YDB host release rejected (%s)", type(exc).__name__)
        return ToolResult.text("Could not safely release this session's YDB host.", is_error=True)
    return _json({"released": released})

async def _spin_handler(ctx: ToolContext, args: dict, replay: bool) -> ToolResult:
    service = _service(ctx)
    if service is None: return ToolResult.text("Execution lifecycle service is unavailable.", is_error=True)
    try:
        row = await (service.start_spin_replay(session_id=ctx.session_id, run_id=args.get("run_id"), auto_continue=bool(args.get("detached", False))) if replay else service.start_spin_verify(session_id=ctx.session_id, model=args.get("model"), profile=args.get("profile", "exhaustive"), timeout_seconds=args.get("timeout_seconds", 60), memory_mb=args.get("memory_mb", 512), max_depth=args.get("max_depth", 100000), hash_bits=args.get("hash_bits", 24), property_name=args.get("property_name"), auto_continue=bool(args.get("detached", False))))
        if not args.get("detached", False): row = await service.join_execution(execution_id=str(row["id"]), session_id=ctx.session_id)
    except Exception as exc:
        logger.warning("SPIN operation rejected (%s)", type(exc).__name__); return ToolResult.text("Could not start safe remote SPIN operation.", is_error=True)
    return _json({"kind": "spin_replay_remote" if replay else "spin_verify_remote", "execution": public_execution(row)})

async def spin_verify_remote_handler(ctx: ToolContext, args: dict) -> ToolResult: return await _spin_handler(ctx, args, False)
async def spin_replay_remote_handler(ctx: ToolContext, args: dict) -> ToolResult: return await _spin_handler(ctx, args, True)


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
    ToolSpec("artifact_transfer", "Copy one confined artifact directly between two leased remote pools; Nerve never relays bytes.", ARTIFACT_TRANSFER_SCHEMA, artifact_transfer_handler),
    ToolSpec("resource_command", "Run a shell-free executable and literal argv on one exclusively leased host from a configured resource pool.", RESOURCE_COMMAND_SCHEMA, resource_command_handler),
    ToolSpec("ydb_make", "Synchronize a configured YDB worktree to the session's reviewed builder and run ya make.", YDB_MAKE_SCHEMA, ydb_make_handler),
    ToolSpec("ydb_test", "Synchronize a configured YDB worktree to the session's reviewed builder and run ya tests.", YDB_TEST_SCHEMA, ydb_test_handler),
    ToolSpec("ydb_file_list", "List bounded paths in this session's synchronized YDB checkout.", YDB_FILE_LIST_SCHEMA, ydb_file_list_handler),
    ToolSpec("ydb_file_find", "Find bounded paths in this session's synchronized YDB checkout.", YDB_FILE_FIND_SCHEMA, ydb_file_find_handler),
    ToolSpec("ydb_file_read", "Read bounded UTF-8 text from this session's synchronized YDB checkout.", YDB_FILE_READ_SCHEMA, ydb_file_read_handler),
    ToolSpec("ydb_host_release", "Release this session's idle YDB builder host.", YDB_HOST_RELEASE_SCHEMA, ydb_host_release_handler),
    ToolSpec("spin_verify_remote", "Safely verify bounded Promela source on the session-affine ydb-builders host.", SPIN_VERIFY_REMOTE_SCHEMA, spin_verify_remote_handler),
    ToolSpec("spin_replay_remote", "Replay a retained SPIN counterexample on its session-affine builder host.", SPIN_REPLAY_REMOTE_SCHEMA, spin_replay_remote_handler),
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
