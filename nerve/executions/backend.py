"""Execution backend contracts and the shell-free local implementation."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


LogSink = Callable[[str, str], Awaitable[None]]
StartedSink = Callable[[Mapping[str, Any]], Awaitable[None]]


class ExecutionBackendError(RuntimeError):
    """A backend cannot execute the normalized plan."""


@dataclass(frozen=True)
class BackendResult:
    exit_code: int | None
    signal: int | None = None
    summary: str = ""
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "exit_code": self.exit_code,
            "signal": self.signal,
            "summary": self.summary,
            "error": self.error,
        }


@dataclass(frozen=True)
class BackendRecovery:
    """Evidence reported while reconciling a non-terminal persisted row."""

    state: str  # reattachable | finished | missing | orphaned
    result: BackendResult | None = None


class ExecutionBackend(Protocol):
    name: str

    async def run(
        self,
        *,
        execution_id: str,
        plan: Mapping[str, Any],
        workspace: Path,
        execution_dir: Path,
        emit: LogSink,
        started: StartedSink,
    ) -> BackendResult: ...

    async def cancel(
        self, *, execution_id: str, grace_seconds: int, mode: str,
    ) -> bool: ...

    async def recover(self, execution: Mapping[str, Any]) -> BackendRecovery: ...

    async def reattach(
        self,
        *,
        execution: Mapping[str, Any],
        emit: LogSink,
    ) -> BackendResult: ...


class ResourceLeaseManager(Protocol):
    """Boundary implemented by host inventory without model coordination."""

    async def acquire(
        self,
        *,
        execution_id: str,
        session_id: str,
        requests: Sequence[Mapping[str, Any]],
    ) -> Sequence[Mapping[str, Any]]: ...

    async def release(
        self,
        *,
        execution_id: str,
        leases: Sequence[Mapping[str, Any]],
    ) -> None: ...

    async def quarantine(
        self,
        *,
        execution_id: str,
        leases: Sequence[Mapping[str, Any]],
        reason: str,
    ) -> None: ...


class NoResourceLeaseManager:
    """Local-only deployment: empty requests are valid, remote slots are not."""

    async def acquire(self, *, execution_id, session_id, requests):
        if requests:
            raise ExecutionBackendError("resource inventory service is unavailable")
        return []

    async def release(self, *, execution_id, leases):
        return None

    async def quarantine(self, *, execution_id, leases, reason):
        return None


class LocalExecutionBackend:
    """Run reviewed local commands directly, never through a shell.

    Pipe ownership cannot survive a daemon restart. Recovery therefore marks
    an observed old local handle ``orphaned``; the lifecycle service records
    ``lost`` and never signals a possibly-reused PID.
    """

    name = "local"
    _CAPTURE_LIMIT = 1024 * 1024

    def __init__(self) -> None:
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._cancel_requested: set[str] = set()

    @staticmethod
    async def _terminate_process(
        proc: asyncio.subprocess.Process,
        *,
        grace_seconds: int,
        mode: str,
    ) -> None:
        sig = signal.SIGINT if mode == "interrupt" else signal.SIGTERM
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            with contextlib.suppress(ProcessLookupError):
                proc.send_signal(sig)
        try:
            await asyncio.wait_for(proc.wait(), timeout=max(0, grace_seconds))
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=5)

    @staticmethod
    def _rooted_path(value: Mapping[str, Any], workspace: Path, execution_dir: Path) -> str:
        root = workspace if value.get("root") == "workspace" else execution_dir
        return str(root / str(value.get("path") or ""))

    def _argv(
        self,
        step: Mapping[str, Any],
        *,
        execution_id: str,
        workspace: Path,
        execution_dir: Path,
        captures: Mapping[str, str],
        session_id: str,
    ) -> list[str]:
        contexts = {
            "workspace": str(workspace),
            "execution_dir": str(execution_dir),
            "execution_id": execution_id,
            "session_id": session_id,
        }
        result: list[str] = []
        for token in step.get("argv", []):
            kind = token.get("type")
            value = token.get("value")
            if kind == "spread":
                result.extend(str(item) for item in (value or []))
            elif kind == "context":
                result.append(contexts[str(value)])
            elif kind == "artifact" and isinstance(value, Mapping):
                result.append(self._rooted_path(value, workspace, execution_dir))
            elif kind == "captured_output":
                result.append(captures.get(str(value), ""))
            else:
                result.append(str(value))
        return result

    async def _drain(
        self,
        reader: asyncio.StreamReader | None,
        *,
        stream: str,
        emit: LogSink,
    ) -> str:
        if reader is None:
            return ""
        captured = bytearray()
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break
            text = chunk.decode("utf-8", errors="replace")
            await emit(stream, text)
            remaining = self._CAPTURE_LIMIT - len(captured)
            if remaining > 0:
                captured.extend(chunk[:remaining])
        return captured.decode("utf-8", errors="replace")

    async def _run_step(
        self,
        *,
        execution_id: str,
        session_id: str,
        step: Mapping[str, Any],
        workspace: Path,
        execution_dir: Path,
        captures: dict[str, str],
        emit: LogSink,
        started: StartedSink,
        announce_start: bool,
    ) -> BackendResult:
        if step.get("transport") != "local":
            raise ExecutionBackendError("resource transport requires a concrete remote backend")
        cwd = workspace if step.get("cwd") == "workspace" else execution_dir
        cwd.mkdir(parents=True, exist_ok=True)
        argv = self._argv(
            step,
            execution_id=execution_id,
            session_id=session_id,
            workspace=workspace,
            execution_dir=execution_dir,
            captures=captures,
        )
        proc = await asyncio.create_subprocess_exec(
            str(step["executable"]),
            *argv,
            cwd=str(cwd),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self._processes[execution_id] = proc
        if announce_start:
            await started({"pid": proc.pid, "reattachable": False})
        stdout_task = asyncio.create_task(self._drain(proc.stdout, stream="stdout", emit=emit))
        stderr_task = asyncio.create_task(self._drain(proc.stderr, stream="stderr", emit=emit))
        try:
            returncode = await proc.wait()
            stdout, stderr = await asyncio.gather(stdout_task, stderr_task)
        except asyncio.CancelledError:
            # ``asyncio.timeout`` and service shutdown cancel this coroutine.
            # Kill the process group here, while the concrete handle is still
            # available, so it cannot escape after the registry entry is
            # removed in ``finally``.
            await self._terminate_process(
                proc, grace_seconds=1, mode="terminate",
            )
            raise
        finally:
            self._processes.pop(execution_id, None)
            for task in (stdout_task, stderr_task):
                if not task.done():
                    task.cancel()
        if step.get("capture_stdout"):
            captures[str(step["capture_stdout"])] = stdout
        if step.get("capture_stderr"):
            captures[str(step["capture_stderr"])] = stderr
        return BackendResult(
            exit_code=returncode if returncode >= 0 else None,
            signal=-returncode if returncode < 0 else None,
            summary=f"step {step.get('id', 'command')} exited with {returncode}",
        )

    async def run(
        self,
        *,
        execution_id: str,
        plan: Mapping[str, Any],
        workspace: Path,
        execution_dir: Path,
        emit: LogSink,
        started: StartedSink,
    ) -> BackendResult:
        execution_dir.mkdir(parents=True, exist_ok=True)
        captures: dict[str, str] = {}
        session_id = str(plan.get("session_id") or "")
        success_codes = set(plan.get("result", {}).get("success_exit_codes", [0]))
        final = BackendResult(exit_code=0, summary="execution completed")
        announce_start = True
        outcome = "success"
        try:
            for step in plan.get("steps", []):
                final = await self._run_step(
                    execution_id=execution_id,
                    session_id=session_id,
                    step=step,
                    workspace=workspace,
                    execution_dir=execution_dir,
                    captures=captures,
                    emit=emit,
                    started=started,
                    announce_start=announce_start,
                )
                announce_start = False
                if final.exit_code not in success_codes:
                    outcome = "cancel" if execution_id in self._cancel_requested else "failure"
                    break
        except asyncio.CancelledError:
            outcome = "cancel"
            raise
        finally:
            cleanup = plan.get("cleanup", {})
            when = cleanup.get("when", "always")
            should_cleanup = when == "always" or when == outcome
            if outcome == "cancel" and not plan.get("cancellation", {}).get("run_cleanup", True):
                should_cleanup = False
            if should_cleanup:
                try:
                    async with asyncio.timeout(int(cleanup.get("timeout_seconds", 60))):
                        for step in cleanup.get("steps", []):
                            try:
                                await self._run_step(
                                    execution_id=execution_id,
                                    session_id=session_id,
                                    step=step,
                                    workspace=workspace,
                                    execution_dir=execution_dir,
                                    captures=captures,
                                    emit=emit,
                                    started=started,
                                    announce_start=False,
                                )
                            except Exception as exc:  # cleanup failure is logged, not hidden
                                await emit("system", f"cleanup failed: {type(exc).__name__}\n")
                except TimeoutError:
                    await emit("system", "cleanup timed out\n")
            self._cancel_requested.discard(execution_id)
        return final

    async def cancel(self, *, execution_id: str, grace_seconds: int, mode: str) -> bool:
        proc = self._processes.get(execution_id)
        if proc is None or proc.returncode is not None:
            return False
        if mode == "none":
            return False
        self._cancel_requested.add(execution_id)
        await self._terminate_process(
            proc, grace_seconds=grace_seconds, mode=mode,
        )
        return True

    async def recover(self, execution: Mapping[str, Any]) -> BackendRecovery:
        return BackendRecovery("orphaned")

    async def reattach(self, *, execution, emit):
        raise ExecutionBackendError("local process pipes cannot be reattached")
