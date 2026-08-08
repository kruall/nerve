"""Durable session-owned execution lifecycle and continuation outbox."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from nerve.agent.streaming import broadcaster
from nerve.db.executions import ACTIVE_EXECUTION_STATUSES
from nerve.executions.backend import (
    BackendRecovery,
    BackendResult,
    ExecutionBackend,
    ExecutionBackendError,
    LocalExecutionBackend,
    NoResourceLeaseManager,
    ResourceLeaseManager,
)
from nerve.executions.catalog import CompiledExecutionPlan, ExecutionCatalog
from nerve.executions.public import public_execution

logger = logging.getLogger(__name__)

_CONTINUATION_TAIL_LINES = 40
_CONTINUATION_TAIL_CHARS = 32 * 1024


def _duration_ms(row: Mapping[str, Any]) -> int | None:
    start = row.get("started_at") or row.get("queued_at")
    end = row.get("finished_at")
    if not start or not end:
        return None
    try:
        return max(0, int((datetime.fromisoformat(str(end)) - datetime.fromisoformat(str(start))).total_seconds() * 1000))
    except (TypeError, ValueError):
        return None


class ExecutionService:
    """Own execution tasks, backend handles, leases, logs, and continuations."""

    def __init__(
        self,
        *,
        db: Any,
        engine: Any,
        workspace: Path,
        catalog: ExecutionCatalog,
        backend: ExecutionBackend | None = None,
        resource_manager: ResourceLeaseManager | None = None,
        execution_root: Path | None = None,
    ) -> None:
        self.db = db
        self.engine = engine
        self.workspace = Path(workspace)
        self.catalog = catalog
        self.backend = backend or LocalExecutionBackend()
        self.resource_manager = resource_manager or NoResourceLeaseManager()
        self.execution_root = execution_root or (self.workspace / ".nerve" / "executions")
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._continuations: dict[str, asyncio.Task[Any]] = {}
        self._stopping = False
        self._continuations_ready = False

    async def initialize(self, *, dispatch_continuations: bool = True) -> None:
        """Reconcile active rows and recover only unclaimed outbox items."""
        self.execution_root.mkdir(parents=True, exist_ok=True)
        initialize_resources = getattr(self.resource_manager, "initialize", None)
        if callable(initialize_resources):
            await initialize_resources()
        failed_claims = await self.db.fail_claimed_execution_continuations_on_restart()
        if failed_claims:
            logger.warning(
                "Marked %d uncertain execution continuation claim(s) failed after restart",
                failed_claims,
            )
        for row in await self.db.list_active_executions():
            status = row["status"]
            if status == "queued":
                self._spawn_runner(row["id"])
                continue
            if status == "starting" and not row.get("selected_leases"):
                # The durable resource request remains FIFO-queued across a
                # crash. No backend was started, so resume scheduling rather
                # than classifying the execution as a lost remote process.
                self._spawn_runner(row["id"])
                continue
            recovery = await self.backend.recover(row)
            if status == "cancelling":
                if recovery.state == "reattachable":
                    task = asyncio.create_task(self._recover_and_cancel(row))
                    self._track(self._tasks, row["id"], task)
                else:
                    await self._quarantine_or_release(row, recovery)
                    await self.db.finalize_execution_cancelled(row["id"])
                    await self._broadcast(row["id"])
                continue
            if recovery.state == "reattachable":
                task = asyncio.create_task(self._reattach(row))
                self._track(self._tasks, row["id"], task)
            elif recovery.state == "finished" and recovery.result is not None:
                await self._finish_from_result(row["id"], row["plan"], recovery.result)
                await self.resource_manager.release(
                    execution_id=row["id"],
                    leases=row.get("selected_leases") or [],
                )
            else:
                await self._quarantine_or_release(row, recovery)
                won = await self.db.finish_execution(
                    row["id"], status="lost",
                    result={
                        "outcome": "lost",
                        "summary": "backend state could not be safely reattached after daemon restart",
                    },
                )
                if won:
                    await self._broadcast(row["id"])
                    self._schedule_continuation(row["id"])
        if dispatch_continuations:
            await self.start_continuations()

    async def start_continuations(self) -> None:
        """Enable outbox delivery after channels/MCP loopback are ready."""
        self._continuations_ready = True
        for row in await self.db.list_pending_execution_continuations():
            self._schedule_continuation(row["id"])

    async def shutdown(self) -> None:
        """Stop owning volatile handles; restart recovery classifies the rows.

        We intentionally leave their persisted status non-terminal. Local
        children are terminated because their pipes cannot be reattached;
        the next daemon marks those rows ``lost`` and emits one continuation.
        """
        self._stopping = True
        rows = await self.db.list_active_executions()
        for row in rows:
            policy = row.get("plan", {}).get("cancellation", {})
            with contextlib.suppress(Exception):
                await self.backend.cancel(
                    execution_id=row["id"],
                    grace_seconds=int(policy.get("grace_seconds", 5)),
                    mode=str(policy.get("mode", "terminate")),
                )
        for task in (*self._tasks.values(), *self._continuations.values()):
            task.cancel()
        await asyncio.gather(
            *self._tasks.values(), *self._continuations.values(),
            return_exceptions=True,
        )
        self._tasks.clear()
        self._continuations.clear()
        shutdown_resources = getattr(self.resource_manager, "shutdown", None)
        if callable(shutdown_resources):
            await shutdown_resources()

    @staticmethod
    def _track(registry: dict[str, asyncio.Task[Any]], key: str, task: asyncio.Task[Any]) -> None:
        registry[key] = task

        def done(finished: asyncio.Task[Any]) -> None:
            if registry.get(key) is finished:
                registry.pop(key, None)
            if not finished.cancelled():
                error = finished.exception()
                if error is not None:
                    logger.error("execution background task %s failed", key, exc_info=error)

        task.add_done_callback(done)

    def _spawn_runner(self, execution_id: str) -> None:
        if execution_id in self._tasks:
            return
        task = asyncio.create_task(self._run(execution_id))
        self._track(self._tasks, execution_id, task)

    async def start(
        self, *, session_id: str, plan: CompiledExecutionPlan,
    ) -> Mapping[str, Any]:
        session = await self.db.get_session(session_id)
        if (
            not session or session.get("status") == "archived"
            or session.get("source") == "external"
        ):
            raise ValueError("execution owner session is unavailable")
        serialized = plan.as_dict(redact_secrets=False)
        return await self._start_serialized(
            session_id=session_id,
            plan=serialized,
            profile_snapshot={**plan.profile.describe(), "source": plan.profile.source},
        )

    async def _start_serialized(
        self,
        *,
        session_id: str,
        plan: Mapping[str, Any],
        profile_snapshot: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        execution_id = f"exec-{uuid.uuid4().hex[:12]}"
        plan_data = dict(plan)
        plan_data["session_id"] = session_id
        requests = [
            {"slot": slot, "pool": pool, "mode": "exclusive", "state": "requested"}
            for slot, pool in dict(plan_data.get("resources", {})).items()
        ]
        row = await self.db.create_execution(
            execution_id,
            session_id=session_id,
            kind=str(plan_data["kind"]),
            profile_version=str(plan_data["profile_version"]),
            profile_hash=str(plan_data["profile_hash"]),
            profile_snapshot=profile_snapshot,
            plan=plan_data,
            resource_requests=requests,
        )
        await self._broadcast(execution_id)
        self._spawn_runner(execution_id)
        return self._decorate(row)

    async def _run(self, execution_id: str) -> None:
        row = await self.db.get_execution(execution_id)
        if row is None:
            return
        if row["status"] == "queued":
            if not await self.db.transition_execution(
                execution_id, to_status="starting", expect=("queued",),
                fields={"backend_name": self.backend.name},
            ):
                current = await self.db.get_execution(execution_id)
                if current and current["status"] == "cancelling":
                    await self.db.finalize_execution_cancelled(execution_id)
                    await self._broadcast(execution_id)
                return
            await self._broadcast(execution_id)
        elif row["status"] != "starting" or row.get("selected_leases"):
            return
        leases: Sequence[Mapping[str, Any]] = []
        try:
            leases = await self.resource_manager.acquire(
                execution_id=execution_id,
                session_id=row["session_id"],
                requests=row.get("resource_requests") or [],
            )
            # Persist lease selection before any backend call. If the daemon
            # dies in the next instruction, recovery can quarantine/release
            # the exact lease instead of losing ownership evidence.
            if not await self.db.transition_execution(
                execution_id, to_status="starting", expect=("starting",),
                fields={"selected_leases": list(leases)},
            ):
                await self.db.finalize_execution_cancelled(execution_id)
                await self._broadcast(execution_id)
                return
            current = await self.db.get_execution(execution_id)
            if current is None:
                return
            if current["status"] == "cancelling":
                await self.db.finalize_execution_cancelled(execution_id)
                await self._broadcast(execution_id)
                return
            plan = row["plan"]

            async def emit(stream: str, text: str) -> None:
                await self.db.append_execution_log(execution_id, stream=stream, text=text)

            async def started(handle: Mapping[str, Any]) -> None:
                won = await self.db.transition_execution(
                    execution_id, to_status="running", expect=("starting",),
                    fields={
                        "backend_name": self.backend.name,
                        "backend_handle": dict(handle),
                        "selected_leases": list(leases),
                    },
                )
                if not won:
                    policy = plan.get("cancellation", {})
                    await self.backend.cancel(
                        execution_id=execution_id,
                        grace_seconds=int(policy.get("grace_seconds", 5)),
                        mode=str(policy.get("mode", "terminate")),
                    )
                await self._broadcast(execution_id)

            try:
                async with asyncio.timeout(int(plan.get("timeout_seconds", 3600))):
                    result = await self.backend.run(
                        execution_id=execution_id,
                        plan=plan,
                        workspace=self.workspace,
                        execution_dir=self.execution_root / execution_id,
                        emit=emit,
                        started=started,
                    )
            except TimeoutError:
                policy = plan.get("cancellation", {})
                await self.backend.cancel(
                    execution_id=execution_id,
                    grace_seconds=int(policy.get("grace_seconds", 5)),
                    mode=str(policy.get("mode", "terminate")),
                )
                result = BackendResult(None, summary="execution timed out", error="timeout")
            if self._stopping:
                return
            await self._finish_from_result(execution_id, plan, result)
        except asyncio.CancelledError:
            if not self._stopping:
                await self.db.request_execution_cancel(execution_id, reason="lifecycle task cancelled")
                await self.db.finalize_execution_cancelled(execution_id)
                await self._broadcast(execution_id)
            raise
        except Exception as exc:
            if self._stopping:
                return
            logger.warning("Execution %s failed in lifecycle (%s)", execution_id, type(exc).__name__)
            won = await self.db.finish_execution(
                execution_id,
                status="failed",
                result={
                    "outcome": "failed",
                    "summary": "execution lifecycle failed before completion",
                    "error": type(exc).__name__,
                },
            )
            if not won:
                await self.db.finalize_execution_cancelled(execution_id)
            await self._broadcast(execution_id)
            if won:
                self._schedule_continuation(execution_id)
        finally:
            if leases and not self._stopping:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release(execution_id=execution_id, leases=leases)

    async def _finish_from_result(
        self,
        execution_id: str,
        plan: Mapping[str, Any],
        backend_result: BackendResult,
    ) -> None:
        success_codes = set(plan.get("result", {}).get("success_exit_codes", [0]))
        status = "succeeded" if backend_result.exit_code in success_codes else "failed"
        result = backend_result.as_dict()
        if status == "succeeded":
            missing: list[str] = []
            artifacts = plan.get("artifacts", {})
            for name in plan.get("result", {}).get("required_artifacts", []):
                artifact = artifacts.get(name, {})
                root = (
                    self.workspace
                    if artifact.get("root") == "workspace"
                    else self.execution_root / execution_id
                )
                if not (root / str(artifact.get("path") or "")).exists():
                    missing.append(str(name))
            if missing:
                status = "failed"
                result["error"] = "required artifacts are missing"
                result["summary"] = (
                    "missing required artifact(s): " + ", ".join(missing)
                )
                result["missing_artifacts"] = missing
        result["outcome"] = status
        won = await self.db.finish_execution(
            execution_id, status=status, result=result,
        )
        if not won:
            await self.db.finalize_execution_cancelled(execution_id, result={"outcome": "cancelled"})
        await self._broadcast(execution_id)
        if won:
            self._schedule_continuation(execution_id)

    async def _reattach(self, row: Mapping[str, Any]) -> None:
        async def emit(stream: str, text: str) -> None:
            await self.db.append_execution_log(row["id"], stream=stream, text=text)

        try:
            result = await self.backend.reattach(execution=row, emit=emit)
        except Exception as exc:
            logger.warning("Could not reattach execution %s (%s)", row["id"], type(exc).__name__)
            await self.resource_manager.quarantine(
                execution_id=row["id"],
                leases=row.get("selected_leases") or [],
                reason="backend reattachment failed",
            )
            won = await self.db.finish_execution(
                row["id"], status="lost",
                result={"outcome": "lost", "summary": "backend reattachment failed"},
            )
            await self._broadcast(row["id"])
            if won:
                self._schedule_continuation(row["id"])
            return
        await self._finish_from_result(row["id"], row["plan"], result)
        try:
            await self.resource_manager.release(
                execution_id=row["id"],
                leases=row.get("selected_leases") or [],
            )
        except Exception as exc:
            logger.warning(
                "Could not release leases after reattaching %s (%s)",
                row["id"], type(exc).__name__,
            )

    async def _recover_and_cancel(self, row: Mapping[str, Any]) -> None:
        policy = row.get("plan", {}).get("cancellation", {})
        confirmed = await self.backend.cancel(
            execution_id=row["id"],
            grace_seconds=int(policy.get("grace_seconds", 5)),
            mode=str(policy.get("mode", "terminate")),
        )
        leases = row.get("selected_leases") or []
        if confirmed:
            await self.resource_manager.release(
                execution_id=row["id"], leases=leases,
            )
        elif leases:
            await self.resource_manager.quarantine(
                execution_id=row["id"], leases=leases,
                reason="cancellation could not confirm backend quiescence",
            )
        await self.db.finalize_execution_cancelled(row["id"])
        await self._broadcast(row["id"])

    async def _quarantine_or_release(
        self, row: Mapping[str, Any], recovery: BackendRecovery,
    ) -> None:
        leases = row.get("selected_leases") or []
        if not leases:
            return
        if recovery.state in {"orphaned", "missing"}:
            await self.resource_manager.quarantine(
                execution_id=row["id"], leases=leases,
                reason="backend state is unknown after daemon restart",
            )
        else:
            await self.resource_manager.release(execution_id=row["id"], leases=leases)

    def _schedule_continuation(self, execution_id: str) -> None:
        if (
            self._stopping or not self._continuations_ready
            or execution_id in self._continuations
        ):
            return
        task = asyncio.create_task(self._continue(execution_id))
        self._track(self._continuations, execution_id, task)

    async def _continue(self, execution_id: str) -> None:
        claimed, row = await self.db.claim_execution_continuation(execution_id)
        if not claimed or row is None:
            await self._broadcast(execution_id)
            return
        tail = await self.db.tail_execution_logs(
            execution_id, limit=_CONTINUATION_TAIL_LINES,
        )
        text = "".join(
            f"[{entry['stream']}] {entry['text']}"
            for entry in tail.get("entries", [])
        )[-_CONTINUATION_TAIL_CHARS:]
        result = row.get("result") or {}
        prompt = (
            "A session-owned execution reached a terminal state. Continue the same task "
            "from this preserved native thread. Do not assume success; inspect status and "
            "use execution_status/execution_tail if more detail is needed.\n\n"
            f"execution_id: {execution_id}\n"
            f"status: {row.get('status')}\n"
            f"exit_code: {result.get('exit_code')}\n"
            f"duration_ms: {_duration_ms(row)}\n"
            f"bounded_tail:\n{text}"
        )
        try:
            await self.engine.run(
                session_id=row["session_id"],
                user_message=prompt,
                source="execution",
                internal=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self.db.settle_execution_continuation(
                execution_id, success=False, error=type(exc).__name__,
            )
            await self._broadcast(execution_id)
            return
        await self.db.settle_execution_continuation(execution_id, success=True)
        await self._broadcast(execution_id)

    async def _broadcast(self, execution_id: str) -> None:
        row = await self.db.get_execution(execution_id)
        if row is None:
            return
        public = public_execution(self._decorate(row))
        await broadcaster.broadcast(row["session_id"], {
            "type": "execution_update",
            "session_id": row["session_id"],
            "execution": public,
        })
        activity = await self.session_activity(session_ids=[row["session_id"]])
        summary = activity.get(row["session_id"], {})
        await broadcaster.broadcast("__global__", {
            "type": "session_execution_activity",
            "session_id": row["session_id"],
            **summary,
            "is_busy": bool(summary.get("active_execution_count") or self.engine.is_session_running(row["session_id"])),
        })

    def _decorate(self, row: Mapping[str, Any]) -> dict[str, Any]:
        result = dict(row)
        duration = _duration_ms(result)
        if duration is not None:
            result["duration_ms"] = duration
        requests = result.get("resource_requests") or []
        if len(requests) == 1:
            result["requested_pool"] = requests[0].get("pool")
        leases = result.get("selected_leases") or []
        if len(leases) == 1:
            result["lease"] = leases[0]
            result["selected_host"] = leases[0].get("host_id")
        return result

    async def session_activity(self, *, session_ids: Sequence[str]):
        return await self.db.session_execution_activity(session_ids)

    async def list_executions(self, *, session_id: str, include_terminal: bool, limit: int):
        rows = await self.db.list_session_executions(
            session_id, include_terminal=include_terminal, limit=limit,
        )
        return [self._decorate(row) for row in rows]

    async def get_execution(self, *, execution_id: str):
        row = await self.db.get_execution(execution_id)
        return self._decorate(row) if row else None

    async def tail_logs(self, *, execution_id: str, limit: int, before: int | None):
        if await self.db.get_execution(execution_id) is None:
            raise KeyError(execution_id)
        return await self.db.tail_execution_logs(execution_id, limit=limit, before=before)

    async def cancel_execution(self, *, execution_id: str, requested_by: str, reason: str | None):
        row = await self.db.get_execution(execution_id)
        if row is None:
            raise KeyError(execution_id)
        accepted = await self.db.request_execution_cancel(execution_id, reason=reason)
        continuation_task = self._continuations.get(execution_id)
        if accepted and continuation_task is not None:
            continuation_task.cancel()
        if accepted and row["status"] in ACTIVE_EXECUTION_STATUSES:
            policy = row.get("plan", {}).get("cancellation", {})
            cancelled = await self.backend.cancel(
                execution_id=execution_id,
                grace_seconds=int(policy.get("grace_seconds", 5)),
                mode=str(policy.get("mode", "terminate")),
            )
            if not cancelled and row.get("selected_leases"):
                # A timeout/cancel acknowledgement is not proof that remote
                # work stopped.  Keep the physical host unavailable until an
                # administrator confirms quiescence.
                await self.resource_manager.quarantine(
                    execution_id=execution_id,
                    leases=row["selected_leases"],
                    reason="cancellation could not confirm backend quiescence",
                )
            if not cancelled and row["status"] in {"queued", "starting"}:
                await self.db.finalize_execution_cancelled(execution_id)
        await self._broadcast(execution_id)
        current = await self.db.get_execution(execution_id)
        assert current is not None
        return self._decorate(current)

    async def cancel_session(self, session_id: str, *, reason: str = "session stopped") -> bool:
        ids = await self.db.suppress_session_executions(session_id, reason=reason)
        if not ids:
            return False
        for execution_id in ids:
            continuation_task = self._continuations.get(execution_id)
            if continuation_task is not None:
                continuation_task.cancel()
            row = await self.db.get_execution(execution_id)
            if row is None:
                continue
            policy = row.get("plan", {}).get("cancellation", {})
            cancelled = await self.backend.cancel(
                execution_id=execution_id,
                grace_seconds=int(policy.get("grace_seconds", 5)),
                mode=str(policy.get("mode", "terminate")),
            )
            if not cancelled and row.get("selected_leases"):
                await self.resource_manager.quarantine(
                    execution_id=execution_id,
                    leases=row["selected_leases"],
                    reason="session cancellation could not confirm backend quiescence",
                )
            if not cancelled and row["status"] == "cancelling":
                await self.db.finalize_execution_cancelled(execution_id)
            await self._broadcast(execution_id)
        return True

    async def retry_execution(self, *, execution_id: str, profile_mode: str, requested_by: str):
        row = await self.db.get_execution(execution_id)
        if row is None:
            raise KeyError(execution_id)
        if row["status"] in ACTIVE_EXECUTION_STATUSES:
            raise ValueError("active execution cannot be retried")
        if profile_mode == "current":
            plan = self.catalog.compile(
                row["kind"],
                row["plan"].get("arguments", {}),
                row["plan"].get("resources", {}),
            )
            return await self.start(session_id=row["session_id"], plan=plan)
        return await self._start_serialized(
            session_id=row["session_id"],
            plan=row["plan"],
            profile_snapshot=row["profile_snapshot"],
        )
