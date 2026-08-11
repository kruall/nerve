"""Durable session-owned execution lifecycle and continuation outbox."""

from __future__ import annotations

import asyncio
import hashlib
import contextlib
import logging
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

from nerve.agent.streaming import broadcaster
from nerve.db.executions import ACTIVE_EXECUTION_STATUSES
from nerve.executions.backend import (
    BackendRecovery,
    BackendResult,
    ExecutionBackend,
    ExecutionBackendError,
    ExecutionBackendUncertain,
    LocalExecutionBackend,
    NoResourceLeaseManager,
    ResourceLeaseManager,
)
from nerve.executions.ssh import SshSupervisorRejectedError, SshTransportError
from nerve.executions.catalog import CompiledExecutionPlan, ExecutionCatalog
from nerve.executions.public import public_execution
from nerve.executions.ydb import snapshot as ydb_snapshot, validate_worktree
from nerve.executions.spin import validate_request as validate_spin_request
from nerve.resources import ResourceRecoveryGate

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


def _bind_session_reservation_slot(
    plan: Mapping[str, Any], reservation: Mapping[str, Any], lease: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach the one compiled resource slot represented by a pinned lease."""
    pool = str(reservation.get("pool") or "")
    slots = [
        str(slot) for slot, selected_pool in dict(plan.get("resources", {})).items()
        if str(selected_pool) == pool
    ]
    if len(slots) != 1:
        raise ValueError("session reservation must map to exactly one resource slot")
    return {**lease, "slot": slots[0]}


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
        ydb_worktree_root: Path | None = None,
        recovery_gate: ResourceRecoveryGate | None = None,
    ) -> None:
        self.db = db
        self.engine = engine
        self.workspace = Path(workspace)
        self.catalog = catalog
        self.backend = backend or LocalExecutionBackend()
        self.resource_manager = resource_manager or NoResourceLeaseManager()
        self.recovery_gate = recovery_gate or getattr(
            self.resource_manager, "recovery_gate", ResourceRecoveryGate(),
        )
        self.execution_root = execution_root or (self.workspace / ".nerve" / "executions")
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._continuations: dict[str, asyncio.Task[Any]] = {}
        self._stopping = False
        self._continuations_ready = False
        self._terminal_changed = asyncio.Condition()
        self.ydb_worktree_root = Path(ydb_worktree_root) if ydb_worktree_root else None
        register_wait_publisher = getattr(self.resource_manager, "set_wait_continuation_publisher", None)
        if callable(register_wait_publisher):
            register_wait_publisher(self._schedule_continuation)

    async def _acquire_ydb_session_handle(
        self, *, session_id: str, worktree: str | Path,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any], bool]:
        """Acquire or validate the private session-pinned builder handle.

        Reviewed wrappers deliberately do not expose resource handles in their
        public schemas.  They still persist the exact session-owned handle and
        host selected here, so the runner can reject an owner, lease, host, or
        fence change before it dispatches a backend operation.
        """
        acquire = getattr(self.resource_manager, "acquire_ydb_handle", None)
        resolve = getattr(self.resource_manager, "_resolve_handle_lease", None)
        if not callable(acquire) or not callable(resolve):
            raise ValueError("YDB retained handles are unavailable")
        existing = {
            str(handle["id"])
            for handle in await self.db.list_session_resource_handles(
                session_id, states=("active",),
            )
            if handle["pool"] == "ydb-builders"
        }
        handle = await acquire(session_id=session_id, worktree=worktree)
        try:
            resolved = await resolve(session_id, str(handle["id"]))
        except BaseException:
            if str(handle["id"]) not in existing:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(handle["id"]))
            raise
        return handle, resolved, str(handle["id"]) not in existing

    async def start_ydb(self, *, session_id: str, kind: str, worktree: str, args: list[str], build_type: str = "relwithdebinfo", publish: Mapping[str, Any] | None = None, auto_continue: bool = True) -> Mapping[str, Any]:
        """Start the two reviewed YDB commands; callers choose neither host nor SSH."""
        self.recovery_gate.require_ready()
        if kind not in {"ydb_make", "ydb_test"} or build_type not in {"debug", "relwithdebinfo", "release", "profile"} or not all(isinstance(x, str) and "\0" not in x for x in args):
            raise ValueError("invalid YDB operation")
        publish_path: PurePosixPath | None = None
        if publish is not None:
            if kind != "ydb_make" or not isinstance(publish, Mapping) or set(publish) != {"output_path"}:
                raise ValueError("invalid YDB publish request")
            output_path = publish.get("output_path")
            path = PurePosixPath(output_path) if isinstance(output_path, str) else None
            if path is None or not output_path or path.is_absolute() or ".." in path.parts or "\0" in output_path:
                raise ValueError("invalid YDB publish output path")
            publish_path = path
        top = validate_worktree(worktree, self.ydb_worktree_root)
        ydb_handle, resolved_handle, created_ydb_handle = await self._acquire_ydb_session_handle(
            session_id=session_id, worktree=top,
        )
        try:
            snap = ydb_snapshot(top)
        except BaseException:
            if created_ydb_handle:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(ydb_handle["id"]))
            raise
        output_dir = ".nerve-ydb-output"
        published = (None if publish_path is None else {
            # ``ya`` output is not part of its source checkout.  Keep it in a
            # fixed relative directory so publication never depends on a
            # compiler-specific default or a caller-supplied remote path.
            "output_path": (PurePosixPath(output_dir) / publish_path).as_posix(), "artifact_root": "artifacts",
            "path": "ydb/" + hashlib.sha256(
                (str(snap.get("snapshot_id")) + "\0" + publish_path.as_posix()).encode()
            ).hexdigest()[:32] + "/" + publish_path.name,
        })
        pack = snap.pop("pack")
        base_pack = snap.pop("base_pack", b"")
        if not isinstance(pack, bytes):
            raise ValueError("YDB snapshot did not produce a binary pack")
        if not isinstance(base_pack, bytes):
            raise ValueError("YDB snapshot did not produce a binary base pack")
        packs = self.execution_root / "ydb-packs"
        packs.mkdir(parents=True, exist_ok=True)
        pack_path = packs / (
            str(snap["snapshot_id"]) + "-" + uuid.uuid4().hex + ".pack"
        )
        base_pack_path = packs / (
            str(snap["head"]) + "-" + uuid.uuid4().hex + ".base.pack"
        )
        # This control-plane-local file is not a request field sent to the
        # worker.  Keeping the binary outside SQLite avoids JSON/base64 growth
        # and makes a queued execution recoverable after a daemon restart.
        fd = os.open(pack_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(pack)
            with open(base_pack_path, "xb") as stream:
                stream.write(base_pack)
        except BaseException:
            with contextlib.suppress(OSError):
                pack_path.unlink()
            with contextlib.suppress(OSError):
                base_pack_path.unlink()
            # Only unwind the compatibility handle created by this failed
            # start.  A pre-existing YDB handle is session-owned and must not
            # be released merely because a later operation could not start.
            if created_ydb_handle:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(ydb_handle["id"]))
            raise

        snap["pack_path"] = str(pack_path)
        snap["pack_length"] = len(pack)
        snap["pack_sha256"] = hashlib.sha256(pack).hexdigest()
        snap["base_pack_path"] = str(base_pack_path)
        snap["base_pack_length"] = len(base_pack)
        snap["base_pack_sha256"] = hashlib.sha256(base_pack).hexdigest()
        test = kind == "ydb_test"
        argv = ["make", "--build", build_type, "--output", output_dir] + (["-tA"] if test else []) + list(args)
        plan = {"kind": kind, "profile_version": "1", "profile_hash": "built-in-ydb-v1",
                "arguments": {"args": list(args), "build_type": build_type}, "resources": {"session": "ydb-builders"},
                "retained_handle_ids": [str(ydb_handle["id"])],
                "resource_hosts": {"session": str(resolved_handle["host_id"])},
                "ydb_session_handle": True,
                "ydb_snapshot": snap,
                **({"ydb_publish": published} if published else {}),
                "steps": [{"id": "ydb", "transport": "resource", "resource_slot": "session", "executable": "./ya", "argv": [{"type": "literal", "value": x} for x in argv], "cwd": "workspace"}],
                # ya test writes test-owned stderr verbatim.  Valid passing
                # tests can therefore contain words such as ERROR (for
                # example, logging-level tests).  The reviewed wrapper's
                # trustworthy success markers are the GOOD summary and final
                # Ok; broad forbidden substrings produce false failures.
                "result": {"success_exit_codes": [0], "required_output": (["GOOD", "Ok"] if test else []),
                           "forbidden_output": []},
                "timeout_seconds": 86400, "cancellation": {"mode": "interrupt", "grace_seconds": 10, "run_cleanup": False}}
        try:
            return await self._start_serialized(session_id=session_id, plan=plan,
                profile_snapshot={"kind": kind, "title": kind, "source": "built-in reviewed YDB operation"},
                auto_continue=auto_continue)
        except BaseException:
            with contextlib.suppress(OSError):
                pack_path.unlink()
            with contextlib.suppress(OSError):
                base_pack_path.unlink()
            if created_ydb_handle:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(ydb_handle["id"]))
            raise

    async def start_spin_verify(self, *, session_id: str, model: Any, profile: Any = "exhaustive", timeout_seconds: Any = 60, memory_mb: Any = 512, max_depth: Any = 100_000, hash_bits: Any = 24, property_name: Any = None, auto_continue: bool = True) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        spec = validate_spin_request(model=model, profile=profile, timeout_seconds=timeout_seconds, memory_mb=memory_mb, max_depth=max_depth, hash_bits=hash_bits, property_name=property_name)
        run_id = "spin-" + uuid.uuid4().hex
        # ``spin -run`` performs generator, compiler, and verifier lifecycle
        # without accepting a shell fragment.  These are the documented pan
        # flags: exhaustive by default; bitstate only when explicitly chosen.
        args = ["-run", "-a", f"-m{spec['max_depth']}", f"-w{spec['hash_bits']}", f"-DMEMLIM={spec['memory_mb']}"]
        if spec["profile"] == "bitstate":
            args.append("-DBITSTATE")
        if spec["property_name"]:
            args.extend(["-N", spec["property_name"]])
        args.append("model.pml")
        handle, resolved, created = await self._acquire_ydb_session_handle(
            session_id=session_id, worktree="spin:" + session_id,
        )
        plan = {"kind":"spin_verify_remote", "profile_version":"1", "profile_hash":"built-in-spin-remote-v1", "arguments":{k:v for k,v in spec.items() if k != "model"}, "resources":{"session":"ydb-builders"}, "retained_handle_ids":[str(handle["id"])], "resource_hosts":{"session":str(resolved["host_id"])}, "ydb_session_handle":True, "spin":{**spec, "run_id":run_id, "retention_seconds":86400}, "steps":[{"id":"spin", "transport":"resource", "resource_slot":"session", "executable":"/usr/bin/spin", "argv":[{"type":"literal", "value":x} for x in args], "cwd":"workspace"}], "result":{"success_exit_codes":[0]}, "timeout_seconds":spec["timeout_seconds"], "cancellation":{"mode":"terminate", "grace_seconds":10, "run_cleanup":False}}
        try:
            return await self._start_serialized(session_id=session_id, plan=plan, profile_snapshot={"kind":"spin_verify_remote","title":"remote SPIN verification","source":"built-in reviewed SPIN operation"}, auto_continue=auto_continue)
        except BaseException:
            if created:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(handle["id"]))
            raise

    async def start_spin_replay(self, *, session_id: str, run_id: Any, auto_continue: bool = True) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        if not isinstance(run_id, str) or not run_id.startswith("spin-") or not run_id[5:].isalnum(): raise ValueError("invalid SPIN run id")
        handle, resolved, created = await self._acquire_ydb_session_handle(
            session_id=session_id, worktree="spin:" + session_id,
        )
        plan = {"kind":"spin_replay_remote", "profile_version":"1", "profile_hash":"built-in-spin-remote-v1", "resources":{"session":"ydb-builders"}, "retained_handle_ids":[str(handle["id"])], "resource_hosts":{"session":str(resolved["host_id"])}, "ydb_session_handle":True, "spin":{"run_id":run_id, "retention_seconds":86400}, "steps":[{"id":"spin", "transport":"resource", "resource_slot":"session", "executable":"/usr/bin/spin", "argv":[{"type":"literal", "value":x} for x in ["-t", "-p", "-g", "-l", "model.pml"]], "cwd":"workspace"}], "result":{"success_exit_codes":[0]}, "timeout_seconds":30, "cancellation":{"mode":"terminate", "grace_seconds":10, "run_cleanup":False}}
        try:
            return await self._start_serialized(session_id=session_id, plan=plan, profile_snapshot={"kind":"spin_replay_remote","title":"remote SPIN replay","source":"built-in reviewed SPIN operation"}, auto_continue=auto_continue)
        except BaseException:
            if created:
                with contextlib.suppress(Exception):
                    await self.resource_manager.release_handle(session_id, str(handle["id"]))
            raise

    async def start_artifact_transfer(self, *, session_id: str, source: Mapping[str, Any], destination: Mapping[str, Any], auto_continue: bool = True) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        """Persist a fenced remote/local transfer plan; endpoints have no coordinates."""
        async def endpoint(value: Mapping[str, Any], name: str) -> tuple[str, str, str, bool, str | None, str | None]:
            if not isinstance(value, Mapping):
                raise ValueError("artifact transfer " + name + " endpoint is invalid")
            local = value.get("host") == "localhost"
            explicit_handle = value.get("handle_id")
            allowed = ({"host", "path", "artifact_root"} if local else
                       {"handle_id", "path", "artifact_root"})
            if set(value) - allowed or (local and set(value) != allowed):
                raise ValueError("artifact transfer " + name + " endpoint is invalid")
            if not local and explicit_handle is not None:
                if not isinstance(explicit_handle, str) or not explicit_handle:
                    raise ValueError("artifact transfer " + name + " handle is invalid")
                resolved = await self.resource_manager._resolve_handle_lease(session_id, explicit_handle)
                pool, host, handle_id = resolved["pool"], resolved["host_id"], explicit_handle
            else:
                pool, host, handle_id = "localhost", None, None
            path, root = value.get("path"), value.get("artifact_root")
            if not all(isinstance(x, str) and x and "\x00" not in x for x in (pool, path, root)) or (host is not None and (not isinstance(host, str) or not host or "\x00" in host)):
                raise ValueError("artifact transfer " + name + " endpoint is invalid")
            from pathlib import PurePosixPath
            for part in (path, root):
                parsed = PurePosixPath(part)
                if parsed.is_absolute() or ".." in parsed.parts or not parsed.parts:
                    raise ValueError("artifact transfer paths must be confined relative paths")
            return pool, path, root, local, host, handle_id
        sp, sx, sr, source_local, sh, source_handle = await endpoint(source, "source")
        dp, dx, dr, destination_local, dh, destination_handle = await endpoint(destination, "destination")
        if source_local and destination_local:
            raise ValueError("localhost-to-localhost artifact transfer is not supported")
        local_roots = getattr(getattr(self.resource_manager, "inventory", None), "local_artifact_roots", {})
        for local, root in ((source_local, sr), (destination_local, dr)):
            if local and root not in local_roots:
                raise ValueError("artifact transfer local artifact root is not configured")
        validate_remote = getattr(self.backend, "validate_artifact_endpoint", None)
        for local, pool, root, host, handle_id, name in (
            (source_local, sp, sr, sh, source_handle, "source"),
            (destination_local, dp, dr, dh, destination_handle, "destination"),
        ):
            if not local:
                if callable(validate_remote):
                    validate_remote(pool, root)
        resources = ({"destination": dp} if source_local else {"source": sp} if destination_local else {"source": sp, "destination": dp})
        source_reservation = False
        if sh is not None:
            reservation = await self.db.get_session_resource_reservation(session_id)
            if reservation is not None and reservation.get("state") == "active" and reservation.get("pool") == sp:
                lease = await self.db.get_resource_lease(str(reservation.get("lease_id") or ""))
                source_reservation = lease is not None and lease.get("state") == "active" and lease.get("host_id") == sh
        handle_ids = [handle for handle in (source_handle, destination_handle) if handle is not None]
        if len(handle_ids) != len(set(handle_ids)):
            raise ValueError("artifact transfer source and destination handles must be distinct")
        plan = {"kind": "artifact_transfer", "profile_version": "1", "profile_hash": "built-in-artifact-transfer-v1",
                "resources": resources, "resource_hosts": {slot: host for slot, host in (("source", sh), ("destination", dh)) if host is not None}, "source_session_reservation": source_reservation, "artifact_transfer": {"transfer_id": "transfer-" + uuid.uuid4().hex, "source_path": sx, "source_root": sr, "source_local": source_local, "destination_path": dx, "destination_root": dr, "destination_local": destination_local},
                "steps": [], "result": {"success_exit_codes": [0]}, "timeout_seconds": 86400,
                "cancellation": {"mode": "terminate", "grace_seconds": 10, "run_cleanup": False}}
        if handle_ids:
            plan["retained_handle_ids"] = handle_ids
        return await self._start_serialized(session_id=session_id, plan=plan, profile_snapshot={"kind": "artifact_transfer", "title": "direct artifact transfer", "source": "built-in reviewed transfer"}, auto_continue=auto_continue)

    async def start_resource_command(
        self, *, session_id: str, executable: Any, args: Any,
        handle_id: Any,
        timeout_seconds: Any = 3600,
        auto_continue: bool = True,
    ) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        """Run one literal argv command on an exclusively leased resource host."""
        if not isinstance(handle_id, str) or not handle_id:
            raise ValueError("resource command handle is invalid")
        resolved = await self.resource_manager._resolve_handle_lease(session_id, handle_id)
        pool, host_id = resolved["pool"], resolved["host_id"]
        if (
            not isinstance(executable, str) or not executable
            or "\x00" in executable or "\n" in executable
        ):
            raise ValueError("resource command executable is invalid")
        if (
            not isinstance(args, list)
            or not all(isinstance(value, str) and "\x00" not in value for value in args)
        ):
            raise ValueError("resource command args must be literal strings")
        if (
            isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int)
            or not 1 <= timeout_seconds <= 86400
        ):
            raise ValueError("resource command timeout is invalid")
        inventory = getattr(self.resource_manager, "inventory", None)
        if inventory is None:
            raise ValueError("resource inventory is unavailable")
        plan = {
            "kind": "resource_command",
            "profile_version": "1",
            "profile_hash": "built-in-resource-command-v1",
            "arguments": {
                "pool": pool, "executable": executable, "args": list(args),
            },
            "resources": {"worker": pool},
            "retained_handle_ids": [handle_id], "resource_hosts": {"worker": host_id},
            "steps": [{
                "id": "command", "transport": "resource",
                "resource_slot": "worker", "executable": executable,
                "argv": [{"type": "literal", "value": value} for value in args],
                "cwd": "execution_dir",
            }],
            "result": {"success_exit_codes": [0]},
            "timeout_seconds": timeout_seconds,
            "cancellation": {
                "mode": "terminate", "grace_seconds": 10,
                "run_cleanup": False,
            },
        }
        return await self._start_serialized(
            session_id=session_id,
            plan=plan,
            profile_snapshot={
                "kind": "resource_command",
                "title": "approved arbitrary resource command",
                "source": "built-in approval-gated operation",
            },
            auto_continue=auto_continue,
        )

    async def inspect_ydb_files(self, *, session_id: str, operation: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        inspect = getattr(self.backend, "inspect_ydb_files", None)
        use = getattr(self.resource_manager, "use_active_ydb_handle", None)
        if not callable(inspect) or not callable(use):
            raise ValueError("YDB remote file inspection is unavailable")
        async with use(session_id=session_id) as handle:
            # Backend's internal argument retains its established shape; it is
            # never a public tool response and contains no new handle data.
            return await inspect(session_id=session_id, reservation=handle, operation=operation, arguments=arguments)

    async def release_ydb_host(self, *, session_id: str) -> bool:
        self.recovery_gate.require_ready()
        active = await self.db.list_session_executions(session_id, include_terminal=False, limit=1)
        if active:
            raise ValueError("YDB host cannot be released while an execution is active")
        release = getattr(self.resource_manager, "release_handle", None)
        if not callable(release):
            raise ValueError("YDB host release is unavailable")
        handles = [handle for handle in await self.db.list_session_resource_handles(session_id, states=("active",))
                   if handle["pool"] == "ydb-builders"]
        if len(handles) != 1:
            return False
        return await release(session_id, str(handles[0]["id"]))

    async def initialize(self, *, dispatch_continuations: bool = True) -> None:
        """Reconcile active rows and recover only unclaimed outbox items."""
        if not self.recovery_gate.ready:
            await self.recovery_gate.open(
                self.resource_manager, self, dispatch_continuations=dispatch_continuations,
            )
            return
        initialize_resources = getattr(self.resource_manager, "initialize", None)
        if callable(initialize_resources):
            await initialize_resources()
        await self._recover_startup(dispatch_continuations=dispatch_continuations)

    async def _recover_startup(self, *, dispatch_continuations: bool = True) -> None:
        """Run only while the shared recovery gate excludes normal handlers."""
        self.execution_root.mkdir(parents=True, exist_ok=True)
        for row in await self.db.list_terminal_executions():
            if await self._has_active_selected_lease(row):
                continue
            await self.db.cancel_resource_requests(row["id"])
        failed_claims = await self.db.fail_claimed_execution_continuations_on_restart()
        if failed_claims:
            logger.warning(
                "Marked %d uncertain execution continuation claim(s) failed after restart",
                failed_claims,
            )
        for row in await self.db.list_active_executions():
            status = row["status"]
            # Resource-handle waits are allocated by LeaseService's durable
            # poller, not an execution backend.  Their terminal transition
            # publishes the normal session continuation, and reattaching the
            # poller before this recovery pass keeps that contract intact.
            if row.get("kind") == "resource_handle_wait":
                continue
            if status == "queued":
                self._spawn_runner(row["id"])
                continue
            if status == "starting" and not row.get("selected_leases"):
                # The durable resource request remains FIFO-queued across a
                # crash. No backend was started, so resume scheduling rather
                # than classifying the execution as a lost remote process.
                self._spawn_runner(row["id"])
                continue
            if status == "cancelling":
                # Cancellation recovery must make its own durable status RPC
                # and log its disposition before a host is released or
                # quarantined; never infer it from a stale startup snapshot.
                owner = await self.db.get_session(str(row["session_id"]))
                if owner and owner.get("status") == "stopped":
                    await self._recover_and_cancel(row)
                else:
                    task = asyncio.create_task(self._recover_and_cancel(row))
                    self._track(self._tasks, row["id"], task)
                continue
            recovery = await self.backend.recover(row)
            if recovery.state == "reattachable":
                task = asyncio.create_task(self._reattach(row))
                self._track(self._tasks, row["id"], task)
            elif recovery.state == "finished" and recovery.result is not None:
                await self._finish_from_result(row["id"], row["plan"], recovery.result)
                if not self._uses_retained_handles(row):
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
        # A crash may occur after the final-stop fence but before backend
        # reconciliation or fenced release.  Replay exactly that unfinished
        # suffix; final_stop_session is CAS/idempotent and cannot revive it.
        for session_id in await self.db.list_final_stop_sessions_needing_cleanup():
            await self.final_stop_session(session_id)
        if dispatch_continuations:
            await self.start_continuations()

    async def _has_active_selected_lease(self, row: Mapping[str, Any]) -> bool:
        for lease in row.get("selected_leases") or []:
            lease_id = str(lease.get("id", "")) if isinstance(lease, Mapping) else ""
            if not lease_id:
                continue
            current = await self.db.get_resource_lease(lease_id)
            if current is not None and current.get("state") in {"active", "revoking"}:
                return True
        return False

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
        completion_target: Mapping[str, str] | None = None,
        auto_continue: bool = True,
        handle_ids: Sequence[str] | None = None,
        execution_id: str | None = None,
        parent_operation_id: str | None = None,
        stage_run_id: str | None = None,
    ) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        session = await self.db.get_session(session_id)
        if (
            not session or session.get("status") == "archived"
            or session.get("source") == "external"
        ):
            raise ValueError("execution owner session is unavailable")
        serialized = plan.as_dict(redact_secrets=False)
        if handle_ids is not None:
            if (isinstance(handle_ids, (str, bytes))
                    or not all(isinstance(handle_id, str) and handle_id for handle_id in handle_ids)):
                raise ValueError("resource handle ids are invalid")
            if len(set(handle_ids)) != len(handle_ids):
                raise ValueError("resource handle ids must be distinct")
            # Resolve before execution creation or backend dispatch. Creation
            # repeats this check atomically while attaching durable refs.
            resolved = [await self.resource_manager._resolve_handle_lease(session_id, handle_id)
                        for handle_id in handle_ids]
            slots = list(dict(serialized.get("resources", {})))
            if slots and len(slots) != len(resolved):
                raise ValueError("resource handle ids must match execution resource slots")
            serialized["retained_handle_ids"] = list(handle_ids)
        elif serialized.get("resources"):
            raise ValueError("resource-bearing operations require retained handle ids")
        return await self._start_serialized(
            session_id=session_id,
            plan=serialized,
            profile_snapshot={**plan.profile.describe(), "source": plan.profile.source},
            completion_target=completion_target,
            auto_continue=auto_continue,
            execution_id=execution_id,
            parent_operation_id=parent_operation_id,
            stage_run_id=stage_run_id,
        )

    async def _start_serialized(
        self,
        *,
        session_id: str,
        plan: Mapping[str, Any],
        profile_snapshot: Mapping[str, Any],
        completion_target: Mapping[str, str] | None = None,
        auto_continue: bool = True,
        execution_id: str | None = None,
        parent_operation_id: str | None = None,
        stage_run_id: str | None = None,
    ) -> Mapping[str, Any]:
        return await self._start_serialized_unlocked(
            session_id=session_id, plan=plan,
            profile_snapshot=profile_snapshot,
            completion_target=completion_target,
            auto_continue=auto_continue,
            execution_id=execution_id, parent_operation_id=parent_operation_id,
            stage_run_id=stage_run_id,
        )

    async def _start_serialized_unlocked(
        self,
        *,
        session_id: str,
        plan: Mapping[str, Any],
        profile_snapshot: Mapping[str, Any],
        completion_target: Mapping[str, str] | None = None,
        auto_continue: bool = True,
        execution_id: str | None = None,
        parent_operation_id: str | None = None,
        stage_run_id: str | None = None,
    ) -> Mapping[str, Any]:
        self.recovery_gate.require_ready()
        execution_id = execution_id or f"exec-{uuid.uuid4().hex[:12]}"
        plan_data = dict(plan)
        plan_data["session_id"] = session_id
        requests = [
            {"slot": slot, "pool": pool, "mode": "exclusive", "state": "requested"}
            for slot, pool in dict(plan_data.get("resources", {})).items()
        ]
        handle_ids = tuple(plan_data.get("retained_handle_ids", ()))
        if not handle_ids and plan_data.get("resources"):
            raise ValueError("resource-bearing operations require retained handle ids")
        if handle_ids:
            requests = []
        try:
            created_row = await self.db.create_execution(
                execution_id,
                session_id=session_id,
                kind=str(plan_data["kind"]),
                profile_version=str(plan_data["profile_version"]),
                profile_hash=str(plan_data["profile_hash"]),
                profile_snapshot=profile_snapshot,
                plan=plan_data,
                resource_requests=requests,
                handle_ids=handle_ids,
                completion_target_type=str((completion_target or {}).get("type", "session")),
                completion_target_id=(completion_target or {}).get("id"),
                auto_continue=auto_continue,
                parent_operation_id=parent_operation_id,
                stage_run_id=stage_run_id,
                return_created=True,
            )
        except Exception:
            raise
        row, created = created_row
        # A deterministic child may already be committed when a process dies
        # before its caller observes the result.  Reconcile/link it, but do
        # not emit a second dispatch or runner task.
        if created:
            await self._broadcast(execution_id)
            self._spawn_runner(execution_id)
        return self._decorate(row)

    @staticmethod
    def _uses_retained_handles(row: Mapping[str, Any]) -> bool:
        return bool(row.get("plan", {}).get("retained_handle_ids"))

    async def _run(self, execution_id: str) -> None:
        row = await self.db.get_execution(execution_id)
        if row is None:
            return
        if row.get("kind") == "resource_handle_wait":
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
        retained_handles = self._uses_retained_handles(row)
        reservation_lease_id: str | None = None
        try:
            if retained_handles:
                resolved = [await self.resource_manager._resolve_handle_lease(
                    row["session_id"], handle_id,
                ) for handle_id in row["plan"]["retained_handle_ids"]]
                slots = list(dict(row["plan"].get("resources", {})))
                if len(resolved) != len(slots):
                    raise ValueError("retained handle slots no longer match execution plan")
                expected_hosts = dict(row["plan"].get("resource_hosts", {}))
                for slot, held in zip(slots, resolved, strict=True):
                    if (held.get("pool") != row["plan"]["resources"].get(slot)
                            or (slot in expected_hosts
                                and held.get("host_id") != expected_hosts[slot])):
                        # Retrying must either reuse this exact owner-bound
                        # host or fail with the same ownership/stale lineage
                        # boundary; it may never reschedule to another host.
                        raise ValueError("retained handle no longer matches execution plan")
                leases = [{**item["lease"], **({"slot": slots[index]} if index < len(slots) else {})}
                          for index, item in enumerate(resolved)]
                if row["plan"].get("ydb_session_handle"):
                    use = getattr(self.resource_manager, "use_active_ydb_handle", None)
                    if not callable(use):
                        raise ValueError("YDB retained handles are unavailable")
                    async with use(session_id=row["session_id"]) as held:
                        if str(held["id"]) not in row["plan"]["retained_handle_ids"]:
                            raise ValueError("YDB session handle changed")
                        await self._run_with_leases(execution_id, row, leases, held)
                else:
                    await self._run_with_leases(execution_id, row, leases, None)
                return
            reservation = row["plan"].get("session_reservation")
            if reservation:
                await self.db.append_execution_log(
                    execution_id,
                    stream="stdout",
                    text=(
                        "stage=reservation_wait "
                        f"pool={reservation['pool']} slot=session\n"
                    ),
                )
                async with self.resource_manager.use_session_reservation(session_id=row["session_id"], pool=str(reservation["pool"]), worktree=str(reservation["worktree"])) as held:
                    lease = _bind_session_reservation_slot(
                        row["plan"], reservation, held["lease"],
                    )
                    await self.db.append_execution_log(
                        execution_id,
                        stream="stdout",
                        text=(
                            "stage=reservation_acquired "
                            f"pool={held['pool']} host={lease['host_id']} "
                            f"lease={lease['id']}\n"
                        ),
                    )
                    await self._run_with_leases(execution_id, row, [lease], held)
                return
            if row["plan"].get("source_session_reservation"):
                use = getattr(self.resource_manager, "use_active_session_reservation", None)
                if not callable(use):
                    raise ValueError("active session reservation is unavailable")
                async with use(session_id=row["session_id"]) as held:
                    requested_hosts = dict(row["plan"].get("resource_hosts", {}))
                    source_host = requested_hosts.get("source")
                    if held.get("pool") != row["plan"].get("resources", {}).get("source") or held["lease"].get("host_id") != source_host:
                        raise ValueError("source session reservation no longer matches the requested host")
                    source_lease = _bind_session_reservation_slot(row["plan"], held, held["lease"])
                    reservation_lease_id = str(source_lease["id"])
                    requests = [
                        {**request, "host": requested_hosts.get(str(request.get("slot")))}
                        for request in row.get("resource_requests") or []
                        if request.get("slot") != "source"
                    ]
                    extra_leases = await self.resource_manager.acquire(execution_id=execution_id, session_id=row["session_id"], requests=requests)
                    leases = [source_lease, *extra_leases]
                    await self._run_with_leases(execution_id, row, leases, held)
                return
            requested_hosts = dict(row["plan"].get("resource_hosts", {}))
            requests = [
                {**request, "host": requested_hosts.get(str(request.get("slot")))}
                for request in row.get("resource_requests") or []
            ]
            leases = await self.resource_manager.acquire(execution_id=execution_id, session_id=row["session_id"], requests=requests)
            await self._run_with_leases(execution_id, row, leases, None)
        except asyncio.CancelledError:
            if not self._stopping:
                await self.db.request_execution_cancel(execution_id, reason="lifecycle task cancelled")
                await self.db.finalize_execution_cancelled(execution_id); await self._broadcast(execution_id)
            raise
        except ExecutionBackendUncertain as exc:
            if not self._stopping:
                await self._quarantine_leases(
                    execution_id=execution_id,
                    leases=leases,
                    reason="remote execution cleanup could not prove quiescence",
                )
                leases = []
                won = await self.db.finish_execution(
                    execution_id,
                    status="failed",
                    result={"outcome": "failed", "summary": str(exc), "error": "remote_quiescence_unknown"},
                )
                await self._broadcast(execution_id)
                if won:
                    self._schedule_continuation(execution_id)
        except SshSupervisorRejectedError as exc:
            # A framed rejection is an authoritative statement that this RPC
            # did not start remote work.  It must not poison a healthy host.
            if not self._stopping:
                won = await self.db.finish_execution(
                    execution_id, status="failed",
                    result={"outcome": "failed", "summary": str(exc), "error": "remote_request_rejected"},
                )
                await self._broadcast(execution_id)
                if won:
                    self._schedule_continuation(execution_id)
        except SshTransportError as exc:
            # A broken SSH RPC is ambiguous until the durable supervisor job
            # says otherwise.  Reconnect by handle before any lease decision.
            current = await self.db.get_execution(execution_id)
            confirmed = bool(current and await self._cancel_with_quiescence(current))
            if not self._stopping:
                if confirmed and not retained_handles:
                    await self.resource_manager.release(execution_id=execution_id, leases=leases)
                    leases = []
                    await self.db.append_execution_log(execution_id, stream="stdout", text="stage=resource_release quiescence=confirmed\n")
                else:
                    await self._quarantine_leases(
                        execution_id=execution_id, leases=leases,
                        reason="remote transport failed and quiescence could not be proven",
                    )
                    leases = []
                    await self.db.append_execution_log(execution_id, stream="stdout", text="stage=resource_quarantine quiescence=unproven\n")
                won = await self.db.finish_execution(
                    execution_id, status="failed",
                    result={"outcome": "failed", "summary": str(exc), "error": "remote_transport_ambiguous"},
                )
                await self._broadcast(execution_id)
                if won:
                    self._schedule_continuation(execution_id)
        except Exception as exc:
            if not self._stopping:
                logger.warning("Execution %s failed in lifecycle (%s)", execution_id, type(exc).__name__)
                won = await self.db.finish_execution(execution_id, status="failed", result={"outcome": "failed", "summary": "execution lifecycle failed before completion", "error": type(exc).__name__})
                if not won: await self.db.finalize_execution_cancelled(execution_id)
                await self._broadcast(execution_id)
                if won: self._schedule_continuation(execution_id)
        finally:
            releasable = ([] if retained_handles else [lease for lease in leases if str(lease.get("id")) != reservation_lease_id])
            if releasable and not self._stopping:
                with contextlib.suppress(Exception): await self.resource_manager.release(execution_id=execution_id, leases=releasable)

    async def _run_with_leases(self, execution_id: str, row: Mapping[str, Any], leases: Sequence[Mapping[str, Any]], reservation: Mapping[str, Any] | None) -> None:
        """The common durable backend path; a reservation lease is never released per command."""
        try:
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
            # A remote backend receives the immutable plan plus the exact
            # persisted lease evidence.  It must not independently select a
            # host after the scheduler has fenced one.
            plan = {**row["plan"], "selected_leases": list(leases)}

            async def emit(stream: str, text: str) -> None:
                await self.db.append_execution_log(execution_id, stream=stream, text=text)

            await emit(
                "stdout",
                "stage=backend_dispatch "
                f"kind={plan.get('kind')} "
                f"host={leases[0].get('host_id', 'local') if leases else 'local'}\n",
            )

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
                    return
                await emit(
                    "stdout",
                    "stage=remote_started "
                    f"job={handle.get('job_id', 'unknown')}\n",
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
        finally:
            # Resource lifetime is managed by the caller: per-execution leases
            # are released in _run, while session reservations remain pinned.
            pass

    async def _finish_from_result(
        self,
        execution_id: str,
        plan: Mapping[str, Any],
        backend_result: BackendResult,
    ) -> None:
        success_codes = set(plan.get("result", {}).get("success_exit_codes", [0]))
        status = "succeeded" if backend_result.exit_code in success_codes else "failed"
        result = backend_result.as_dict()
        # Some tools (notably ssh_ya's historical test wrapper) return a zero
        # pipeline status even when the textual test summary failed.  Reviewed
        # profiles can therefore require/forbid bounded log markers.
        rules = plan.get("result", {})
        required_output = list(rules.get("required_output", []))
        forbidden_output = list(rules.get("forbidden_output", []))
        if required_output or forbidden_output:
            tail = await self.db.tail_execution_logs(execution_id, limit=2000)
            output = "".join(str(entry.get("text", "")) for entry in tail.get("entries", []))
            missing_output = [needle for needle in required_output if needle not in output]
            present_forbidden = [needle for needle in forbidden_output if needle in output]
            if missing_output or present_forbidden:
                status = "failed"
                result["error"] = "textual result validation failed"
                result["missing_output"] = missing_output
                result["forbidden_output"] = present_forbidden
                result["summary"] = "textual result validation failed"
        if plan.get("kind") == "spin_verify_remote":
            output = "".join(str(entry.get("text", "")) for entry in (await self.db.tail_execution_logs(execution_id, limit=2000)).get("entries", []))[-256 * 1024:]
            has_error = bool(__import__("re").search(r"errors:\s*[1-9][0-9]*", output, __import__("re").I))
            profile = plan.get("spin", {}).get("profile")
            result["verification_status"] = "counterexample" if has_error else ("inconclusive" if profile == "bitstate" else ("verified" if status == "succeeded" else "tool_error"))
            if has_error:
                status = "failed"; result["summary"] = "SPIN found a counterexample"
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
            await self._quarantine_leases(
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
            if not self._uses_retained_handles(row):
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
        confirmed = await self._cancel_with_quiescence(row, recovery_required=True)
        leases = row.get("selected_leases") or []
        if confirmed and not self._uses_retained_handles(row):
            await self.resource_manager.release(
                execution_id=row["id"], leases=leases,
            )
            await self.db.append_execution_log(
                row["id"], stream="stdout", text="stage=resource_release quiescence=confirmed\n",
            )
        elif leases:
            await self._quarantine_leases(
                execution_id=row["id"], leases=leases,
                reason="cancellation could not confirm backend quiescence",
            )
            await self.db.append_execution_log(
                row["id"], stream="stdout", text="stage=resource_quarantine quiescence=unproven\n",
            )
        if not confirmed and self._uses_retained_handles(row):
            await self._quarantine_retained_handles(
                row, reason="cancellation could not confirm backend quiescence",
            )
        if confirmed:
            await self.db.finalize_execution_cancelled(row["id"])
        else:
            await self.db.finalize_execution_lost(
                row["id"], result={
                    "outcome": "lost",
                    "summary": "remote quiescence could not be proven during cancellation",
                    "error": "remote_quiescence_unknown",
                },
            )
        await self._broadcast(row["id"])

    async def _cancel_with_quiescence(
        self, row: Mapping[str, Any], *, recovery_required: bool = False,
    ) -> bool:
        """Request cancellation only after reconnecting a durable remote handle.

        The SSH supervisor is one-shot by design, so losing its transport must
        not turn an in-memory map miss into permission to free a host.  Its
        durable job handle is status-checked first, then cancellation is sent
        to that exact fenced job.  Other backends retain their established
        cancellation contract.
        """
        execution_id = str(row["id"])

        async def log(stage: str, **fields: Any) -> None:
            detail = " ".join(f"{key}={value}" for key, value in fields.items())
            await self.db.append_execution_log(
                execution_id, stream="stdout", text=f"stage={stage}{(' ' + detail) if detail else ''}\n",
            )

        await log("cancellation_requested", execution=execution_id)
        policy = row.get("plan", {}).get("cancellation", {})
        if self.backend.name == "ssh-supervisor" or recovery_required:
            await log("remote_reconnect_status", handle="durable")
            try:
                recovery = await self.backend.recover(row)
            except Exception as exc:
                await log("remote_reconnect_status", outcome="ambiguous", error=type(exc).__name__)
                return False
            await log("remote_reconnect_status", outcome=recovery.state)
            if recovery.state == "finished":
                await log("remote_quiescence_confirmed", source="terminal_status")
                return True
            if recovery.state != "reattachable":
                return False
        try:
            confirmed = await self.backend.cancel(
                execution_id=execution_id,
                grace_seconds=int(policy.get("grace_seconds", 5)),
                mode=str(policy.get("mode", "terminate")),
            )
        except Exception as exc:
            await log("remote_cancel_acknowledgement", outcome="ambiguous", error=type(exc).__name__)
            return False
        await log("remote_cancel_acknowledgement", acknowledged=str(bool(confirmed)).lower())
        if confirmed:
            await log("remote_quiescence_confirmed", source="cancel_acknowledgement")
        return confirmed

    async def _quarantine_leases(
        self, *, execution_id: str, leases: Sequence[Mapping[str, Any]], reason: str,
    ) -> None:
        """Quarantine a retained handle's original lease lineage.

        Handle leases predate the Operation that temporarily borrows them, so
        their lease row is fenced by the session-handle allocator id rather
        than this Operation id.  Quarantining with the latter silently loses
        the CAS; preserve that lineage instead.
        """
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for lease in leases:
            owner = str(lease.get("execution_id") or execution_id)
            grouped.setdefault(owner, []).append(lease)
        for owner, owned_leases in grouped.items():
            await self.resource_manager.quarantine(
                execution_id=owner, leases=owned_leases, reason=reason,
            )

    async def _quarantine_retained_handles(
        self, row: Mapping[str, Any], *, reason: str,
    ) -> None:
        """Fence session handles whose borrowed remote work is ambiguous."""
        for handle_id in row.get("plan", {}).get("retained_handle_ids", ()):
            handle = await self.db.get_session_resource_handle(str(handle_id))
            if handle is None or handle.get("state") not in {"active", "releasing"}:
                continue
            lease = await self.db.get_resource_lease(str(handle["lease_id"]))
            if lease is not None and lease.get("state") in {"active", "revoking"}:
                await self.resource_manager.quarantine(
                    execution_id=str(lease["execution_id"]), leases=[lease], reason=reason,
                )
            await self.db.update_session_resource_handle(
                str(handle["id"]), expected_state=str(handle["state"]),
                state="quarantined", release_reason=reason,
            )

    async def _quarantine_or_release(
        self, row: Mapping[str, Any], recovery: BackendRecovery,
    ) -> None:
        leases = row.get("selected_leases") or []
        if not leases:
            return
        if recovery.state in {"orphaned", "missing"}:
            await self._quarantine_leases(
                execution_id=row["id"], leases=leases,
                reason="backend state is unknown after daemon restart",
            )
        elif not self._uses_retained_handles(row):
            await self.resource_manager.release(execution_id=row["id"], leases=leases)

    def _schedule_continuation(self, execution_id: str) -> None:
        # A workflow controller owns its child completion.  It polls durable
        # child state; suppressing the session continuation is the critical
        # boundary that prevents an intermediate model wakeup.
        # (The controller is deliberately restart-safe and does not rely on
        # this in-memory notification.)
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
        if row.get("completion_target_type") == "workflow":
            await self.db.settle_execution_continuation(execution_id, success=True)
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
        async with self._terminal_changed:
            self._terminal_changed.notify_all()
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

    async def dismiss_execution(
        self, *, execution_id: str, session_id: str, requested_by: str,
    ):
        """Hide a settled execution from its owning session's panel.

        The owner check is repeated in the database CAS, so a stale client
        cannot dismiss a different session's row after a lookup race.
        """
        row = await self.db.get_execution(execution_id)
        if row is None or row.get("session_id") != session_id:
            raise KeyError(execution_id)
        if row.get("dismissed_at"):
            return self._decorate(row)
        if not await self.db.dismiss_session_execution(execution_id, session_id=session_id):
            current = await self.db.get_execution(execution_id)
            if current is not None and current.get("session_id") == session_id and current.get("dismissed_at"):
                return self._decorate(current)
            raise ValueError("execution is not eligible for dismissal")
        current = await self.db.get_execution(execution_id)
        assert current is not None
        await self._broadcast(execution_id)
        return self._decorate(current)

    async def join_execution(self, *, execution_id: str, session_id: str):
        row = await self.db.get_execution(execution_id)
        if row is None or row.get("session_id") != session_id:
            raise KeyError(execution_id)
        if not await self.db.suppress_execution_continuation(execution_id):
            current = await self.db.get_execution(execution_id)
            if current and current.get("continuation_state") == "claimed":
                raise ValueError("execution completion is already being delivered")
        task = self._continuations.get(execution_id)
        if task is not None:
            task.cancel()
        while True:
            async with self._terminal_changed:
                row = await self.db.get_execution(execution_id)
                if row is None:
                    raise KeyError(execution_id)
                if row["status"] not in ACTIVE_EXECUTION_STATUSES:
                    return self._decorate(row)
                await self._terminal_changed.wait()

    async def forget_execution(self, *, execution_id: str, session_id: str):
        row = await self.db.get_execution(execution_id)
        if row is None or row.get("session_id") != session_id:
            raise KeyError(execution_id)
        if not await self.db.suppress_execution_continuation(execution_id):
            raise ValueError("execution completion is already being delivered")
        task = self._continuations.get(execution_id)
        if task is not None:
            task.cancel()
        current = await self.db.get_execution(execution_id)
        assert current is not None
        await self._broadcast(execution_id)
        return self._decorate(current)

    async def tail_logs(self, *, execution_id: str, limit: int, before: int | None):
        if await self.db.get_execution(execution_id) is None:
            raise KeyError(execution_id)
        return await self.db.tail_execution_logs(execution_id, limit=limit, before=before)

    async def cancel_execution(self, *, execution_id: str, requested_by: str, reason: str | None):
        self.recovery_gate.require_ready()
        row = await self.db.get_execution(execution_id)
        if row is None:
            raise KeyError(execution_id)
        accepted = await self.db.request_execution_cancel(execution_id, reason=reason)
        continuation_task = self._continuations.get(execution_id)
        if accepted and continuation_task is not None:
            continuation_task.cancel()
        terminal_won = False
        if accepted:
            cancelled = await self._cancel_with_quiescence(row)
            if cancelled and row.get("selected_leases") and not self._uses_retained_handles(row):
                await self.resource_manager.release(
                    execution_id=execution_id, leases=row["selected_leases"],
                )
                await self.db.append_execution_log(
                    execution_id, stream="stdout", text="stage=resource_release quiescence=confirmed\n",
                )
            if not cancelled and row.get("selected_leases"):
                # A timeout/cancel acknowledgement is not proof that remote
                # work stopped.  Keep the physical host unavailable until an
                # administrator confirms quiescence.
                await self._quarantine_leases(
                    execution_id=execution_id,
                    leases=row["selected_leases"],
                    reason="cancellation could not confirm backend quiescence",
                )
                await self.db.append_execution_log(
                    execution_id, stream="stdout", text="stage=resource_quarantine quiescence=unproven\n",
                )
            if cancelled or (not row.get("selected_leases") and row["status"] in {"queued", "starting"}):
                terminal_won = await self.db.finalize_execution_cancelled(execution_id)
            elif not cancelled:
                terminal_won = await self.db.finalize_execution_lost(
                    execution_id, result={
                        "outcome": "lost",
                        "summary": "remote quiescence could not be proven during cancellation",
                        "error": "remote_quiescence_unknown",
                },
            )
        await self._broadcast(execution_id)
        current = await self.db.get_execution(execution_id)
        if terminal_won or (
            accepted and current is not None
            and current["status"] in {"cancelled", "lost"}
            and current["continuation_state"] == "pending"
        ):
            self._schedule_continuation(execution_id)
        assert current is not None
        return self._decorate(current)

    async def cancel_queued_execution(
        self, *, execution_id: str, requested_by: str, reason: str,
    ) -> Mapping[str, Any]:
        """Operator cancellation for a provably not-yet-started queue entry."""
        self.recovery_gate.require_ready()
        row = await self.db.get_execution(execution_id)
        if row is None:
            raise KeyError(execution_id)
        if row.get("status") not in {"queued", "starting"}:
            raise ValueError("only queued or starting executions may be cancelled this way")
        if row.get("selected_leases"):
            raise ValueError("queued cancellation refuses executions with selected leases")
        if not reason.strip():
            raise ValueError("queued cancellation requires a reason")
        accepted = await self.db.request_execution_cancel(execution_id, reason=reason)
        if not accepted:
            raise ValueError("queued execution state changed before cancellation")
        # Finalization settles queued requests in the same transaction as the
        # terminal execution state, so a restart cannot leave an orphaned
        # queue entry between these two durable transitions.
        terminal_won = await self.db.finalize_execution_cancelled(execution_id)
        await self._broadcast(execution_id)
        if terminal_won:
            self._schedule_continuation(execution_id)
        current = await self.db.get_execution(execution_id)
        assert current is not None
        return self._decorate(current)

    async def cancel_session(self, session_id: str, *, reason: str = "session stopped") -> bool:
        self.recovery_gate.require_ready()
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
            cancelled = await self._cancel_with_quiescence(row)
            if cancelled and row.get("selected_leases") and not self._uses_retained_handles(row):
                await self.resource_manager.release(execution_id=execution_id, leases=row["selected_leases"])
                await self.db.append_execution_log(execution_id, stream="stdout", text="stage=resource_release quiescence=confirmed\n")
            if not cancelled and row.get("selected_leases"):
                await self._quarantine_leases(
                    execution_id=execution_id,
                    leases=row["selected_leases"],
                    reason="session cancellation could not confirm backend quiescence",
                )
                await self.db.append_execution_log(execution_id, stream="stdout", text="stage=resource_quarantine quiescence=unproven\n")
            if not cancelled and self._uses_retained_handles(row):
                await self._quarantine_retained_handles(
                    row, reason="session cancellation could not confirm backend quiescence",
                )
            if cancelled or (not row.get("selected_leases") and row["status"] == "cancelling"):
                await self.db.finalize_execution_cancelled(execution_id)
            elif not cancelled:
                await self.db.finalize_execution_lost(
                    execution_id, result={
                        "outcome": "lost",
                        "summary": "remote quiescence could not be proven during session stop",
                        "error": "remote_quiescence_unknown",
                    },
                )
            await self._broadcast(execution_id)
        return True

    async def final_stop_session(self, session_id: str, *, reason: str = "session stopped") -> bool:
        """Finish the durable final-stop protocol without publishing a resume."""
        self.recovery_gate.require_ready()
        operation_ids, wait_ids = await self.db.final_stop_session_resources(
            session_id, reason=reason,
        )
        for wait_id in wait_ids:
            task = self.resource_manager._wait_tasks.get(wait_id)
            if task is not None:
                task.cancel()
        if wait_ids:
            await asyncio.gather(
                *(task for wait_id in wait_ids
                  if (task := self.resource_manager._wait_tasks.get(wait_id)) is not None),
                return_exceptions=True,
            )
        # cancel_session is deliberately idempotent; it reconciles every row
        # this transaction put into cancelling, including a completed-grant
        # race whose continuation was just suppressed.
        cancelled = await self.cancel_session(session_id, reason=reason)
        await self.resource_manager.release_all_session_handles(
            session_id, final_stop=True,
        )
        await self.resource_manager.cleanup_session_reservation(session_id)
        return bool(operation_ids or wait_ids or cancelled)

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
            return await self.start(
                session_id=row["session_id"], plan=plan,
                handle_ids=row["plan"].get("retained_handle_ids", []),
            )
        return await self._start_serialized(
            session_id=row["session_id"],
            plan=row["plan"],
            profile_snapshot=row["profile_snapshot"],
        )
