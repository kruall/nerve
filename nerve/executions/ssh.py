"""Allowlisted SSH transport and fenced remote-supervisor backend.

The model never supplies SSH coordinates or a command string.  A reviewed
profile supplies a structured argv and a resource lease selects one inventory
host; this module resolves that host through a deployment-local connection
catalog and sends a JSON RPC request to ``nerve-remote-supervisor``.
"""
from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import shutil
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from nerve.executions.backend import BackendRecovery, BackendResult, ExecutionBackendError, LogSink, StartedSink


class SshTransportError(ExecutionBackendError):
    """The control plane cannot safely establish or use a trusted connection."""


@dataclass(frozen=True)
class SshConnection:
    name: str
    host: str
    user: str
    port: int = 22
    known_hosts: Path | None = None
    allowed_cidrs: tuple[str, ...] = ()
    remote_roots: tuple[str, ...] = ()
    identity_file: Path | None = None
    connect_timeout_seconds: int = 15
    environment_allowlist: tuple[str, ...] = ()

    def ssh_argv(self) -> list[str]:
        """Fixed, injection-free OpenSSH options for a named connection."""
        args = ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "UserKnownHostsFile=" + str(self.known_hosts),
                "-o", "GlobalKnownHostsFile=/dev/null", "-o", "ProxyCommand=none",
                "-o", f"ConnectTimeout={self.connect_timeout_seconds}", "-p", str(self.port)]
        if self.identity_file:
            args.extend(["-i", str(self.identity_file), "-o", "IdentitiesOnly=yes"])
        args.append(f"{self.user}@{self.host}")
        return args


def _remote_path(value: str, roots: Sequence[str]) -> str:
    path = PurePosixPath(value)
    if not value or not path.is_absolute() or ".." in path.parts:
        raise SshTransportError("remote path must be an absolute normalized path")
    if not any(path == PurePosixPath(root) or PurePosixPath(root) in path.parents for root in roots):
        raise SshTransportError("remote path escapes configured remote roots")
    return path.as_posix()


class SshConnectionCatalog:
    """Deployment-owned named connections; raw endpoint fields are rejected."""
    _FORBIDDEN = frozenset({"proxy_command", "proxycommand", "options", "ssh_options", "command"})

    def __init__(self, raw: Mapping[str, Any] | None) -> None:
        raw = dict(raw or {})
        entries = raw.get("ssh_connections", raw.get("connections_detail", {}))
        if not isinstance(entries, Mapping):
            raise SshTransportError("ssh_connections must be a mapping of named trusted connections")
        self._connections: dict[str, SshConnection] = {}
        for name, value in entries.items():
            if not isinstance(name, str) or not isinstance(value, Mapping):
                raise SshTransportError("SSH connection names and definitions must be mappings")
            if {str(field).lower() for field in value} & self._FORBIDDEN:
                raise SshTransportError("SSH connection contains forbidden raw options")
            allowed = {"host", "user", "port", "known_hosts", "allowed_cidrs", "remote_roots", "identity_file", "connect_timeout_seconds", "environment_allowlist"}
            unknown = set(value) - allowed
            if unknown:
                raise SshTransportError("unknown SSH connection fields: " + ", ".join(sorted(unknown)))
            host, user = value.get("host"), value.get("user")
            if not isinstance(host, str) or not host or not isinstance(user, str) or not user:
                raise SshTransportError("trusted SSH connection requires host and user")
            port = value.get("port", 22)
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise SshTransportError("trusted SSH connection port is invalid")
            known = value.get("known_hosts")
            if not isinstance(known, str) or not known:
                raise SshTransportError("trusted SSH connection requires a dedicated known_hosts file")
            roots = value.get("remote_roots", [])
            if not isinstance(roots, list) or not roots or not all(isinstance(root, str) for root in roots):
                raise SshTransportError("trusted SSH connection requires allowed remote_roots")
            normalized_roots = tuple(_remote_path(root, [root]) for root in roots)
            cidrs = value.get("allowed_cidrs", [])
            if not isinstance(cidrs, list) or not all(isinstance(item, str) for item in cidrs):
                raise SshTransportError("allowed_cidrs must be a list of CIDRs")
            for cidr in cidrs:
                ipaddress.ip_network(cidr, strict=False)
            env = value.get("environment_allowlist", [])
            if not isinstance(env, list) or not all(isinstance(item, str) and item.isidentifier() for item in env):
                raise SshTransportError("environment_allowlist contains an invalid name")
            self._connections[name] = SshConnection(name=name, host=host, user=user, port=port,
                known_hosts=Path(known), allowed_cidrs=tuple(cidrs), remote_roots=normalized_roots,
                identity_file=Path(value["identity_file"]) if value.get("identity_file") else None,
                connect_timeout_seconds=int(value.get("connect_timeout_seconds", 15)), environment_allowlist=tuple(env))

    def resolve(self, name: str) -> SshConnection:
        try:
            connection = self._connections[name]
        except KeyError as exc:
            raise SshTransportError("unknown trusted SSH connection") from exc
        if not connection.known_hosts or not connection.known_hosts.is_file():
            raise SshTransportError("trusted SSH known_hosts file is unavailable")
        return connection


class RemoteSupervisor(Protocol):
    async def start(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def status(self, connection: SshConnection, job_id: str, fencing_token: int, root: str) -> Mapping[str, Any]: ...
    async def tail(self, connection: SshConnection, job_id: str, fencing_token: int, cursor: int, root: str) -> Mapping[str, Any]: ...
    async def cancel(self, connection: SshConnection, job_id: str, fencing_token: int, grace_seconds: int, mode: str, root: str) -> Mapping[str, Any]: ...


class OpenSshSupervisor:
    """JSON-lines client for a pre-installed, non-shell remote supervisor."""
    async def _rpc(self, connection: SshConnection, operation: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        if shutil.which("ssh") is None:
            raise SshTransportError("OpenSSH client is not installed")
        request = json.dumps({"operation": operation, **payload}, separators=(",", ":")).encode() + b"\n"
        # The remote argv is constant.  Payload is stdin JSON, never an SSH
        # command argument and therefore cannot become shell syntax.
        proc = await asyncio.create_subprocess_exec(*connection.ssh_argv(), "nerve", "remote-supervisor", "rpc",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(request), connection.connect_timeout_seconds + 30)
        except (TimeoutError, OSError) as exc:
            proc.kill()
            await proc.wait()
            raise SshTransportError("SSH supervisor is unreachable") from exc
        if proc.returncode != 0:
            raise SshTransportError("SSH supervisor request failed: " + stderr.decode(errors="replace")[-300:])
        try:
            response = json.loads(stdout.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SshTransportError("SSH supervisor returned invalid JSON") from exc
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            raise SshTransportError("SSH supervisor rejected request")
        return response

    async def start(self, connection, request): return await self._rpc(connection, "start", request)
    async def status(self, connection, job_id, fencing_token, root): return await self._rpc(connection, "status", {"job_id": job_id, "fencing_token": fencing_token, "root": root})
    async def tail(self, connection, job_id, fencing_token, cursor, root): return await self._rpc(connection, "tail", {"job_id": job_id, "fencing_token": fencing_token, "cursor": cursor, "root": root})
    async def cancel(self, connection, job_id, fencing_token, grace_seconds, mode, root): return await self._rpc(connection, "cancel", {"job_id": job_id, "fencing_token": fencing_token, "grace_seconds": grace_seconds, "mode": mode, "root": root})


class SshExecutionBackend:
    """Execution backend whose cancellation acknowledgement means quiescence."""
    name = "ssh-supervisor"

    def __init__(self, *, inventory: Any, connections: SshConnectionCatalog, supervisor: RemoteSupervisor | None = None, poll_seconds: float = .25) -> None:
        self.inventory, self.connections, self.supervisor = inventory, connections, supervisor or OpenSshSupervisor()
        self.poll_seconds = poll_seconds
        self._jobs: dict[str, tuple[SshConnection, str, int, str]] = {}

    def _job(self, execution_id: str, plan: Mapping[str, Any]) -> tuple[SshConnection, Mapping[str, Any], int]:
        remote = [step for step in plan.get("steps", []) if step.get("transport") == "resource"]
        if len(remote) != 1:
            raise SshTransportError("SSH backend currently requires exactly one resource command step")
        slot = remote[0].get("resource_slot")
        lease = next((item for item in plan.get("selected_leases", []) if item.get("slot") == slot), None)
        if not isinstance(lease, Mapping):
            raise SshTransportError("resource step has no selected fenced lease")
        host = self.inventory.hosts.get(str(lease.get("host_id")))
        if not host:
            raise SshTransportError("selected lease references unknown host")
        return self.connections.resolve(str(host["connection_ref"])), remote[0], int(lease["fencing_token"])

    @staticmethod
    def _argv(step: Mapping[str, Any], plan: Mapping[str, Any]) -> list[str]:
        values = dict(plan.get("arguments", {}))
        result: list[str] = [str(step["executable"])]
        for token in step.get("argv", []):
            kind, value = token.get("type"), token.get("value")
            if kind == "arg": result.append(str(values[str(value)]))
            elif kind == "spread": result.extend(str(x) for x in values[str(value)])
            elif kind == "literal": result.append(str(value))
            else: raise SshTransportError("SSH steps only permit literal and validated argument argv tokens")
        return result

    async def run(self, *, execution_id: str, plan: Mapping[str, Any], workspace: Path, execution_dir: Path, emit: LogSink, started: StartedSink) -> BackendResult:
        connection, step, token = self._job(execution_id, plan)
        root = _remote_path(str(plan.get("remote_root", connection.remote_roots[0])), connection.remote_roots)
        existing = plan.get("_remote_existing_job")
        if existing:
            job_id = str(existing)
        else:
            request = {"execution_id": execution_id, "lease_id": next(x["id"] for x in plan["selected_leases"] if x.get("slot") == step.get("resource_slot")), "fencing_token": token,
                       "root": root, "argv": self._argv(step, plan), "cwd": str(step.get("cwd", "workspace")), "environment": {k: os.environ[k] for k in connection.environment_allowlist if k in os.environ}}
            reply = await self.supervisor.start(connection, request)
            job_id = str(reply.get("job_id") or "")
            if not job_id or int(reply.get("fencing_token", -1)) != token:
                raise SshTransportError("remote supervisor did not return a matching fenced job")
        self._jobs[execution_id] = (connection, job_id, token, root)
        if not existing:
            await started({"job_id": job_id, "process_group": reply.get("process_group"), "fencing_token": token, "reattachable": True})
        cursor = 0
        try:
            while True:
                tail = await self.supervisor.tail(connection, job_id, token, cursor, root)
                cursor = int(tail.get("cursor", cursor))
                for entry in tail.get("entries", []): await emit(str(entry.get("stream", "stdout")), str(entry.get("text", "")))
                status = await self.supervisor.status(connection, job_id, token, root)
                if status.get("state") in {"succeeded", "failed", "cancelled"}:
                    return BackendResult(status.get("exit_code"), summary=str(status.get("summary", "remote job finished")), error=status.get("error"))
                await asyncio.sleep(self.poll_seconds)
        finally:
            self._jobs.pop(execution_id, None)

    async def cancel(self, *, execution_id: str, grace_seconds: int, mode: str) -> bool:
        job = self._jobs.get(execution_id)
        if not job or mode == "none": return False
        connection, job_id, token, root = job
        try: reply = await self.supervisor.cancel(connection, job_id, token, grace_seconds, mode, root)
        except SshTransportError: return False
        return reply.get("quiescent") is True and reply.get("state") in {"cancelled", "finished"}

    async def recover(self, execution: Mapping[str, Any]) -> BackendRecovery:
        handle = execution.get("backend_handle") or {}; job_id = handle.get("job_id"); token = handle.get("fencing_token")
        if not job_id or token is None: return BackendRecovery("missing")
        try:
            connection, _, _ = self._job(str(execution["id"]), execution["plan"])
            root = _remote_path(str(execution["plan"].get("remote_root", connection.remote_roots[0])), connection.remote_roots)
            status = await self.supervisor.status(connection, str(job_id), int(token), root)
        except (SshTransportError, KeyError, ValueError): return BackendRecovery("orphaned")
        if status.get("state") in {"running", "starting"}: return BackendRecovery("reattachable")
        return BackendRecovery("finished", BackendResult(status.get("exit_code"), summary=str(status.get("summary", "remote job finished"))))

    async def reattach(self, *, execution: Mapping[str, Any], emit: LogSink) -> BackendResult:
        handle = execution.get("backend_handle") or {}
        plan = {**execution["plan"], "selected_leases": execution.get("selected_leases") or [], "_remote_existing_job": handle.get("job_id")}
        return await self.run(execution_id=str(execution["id"]), plan=plan, workspace=Path("."), execution_dir=Path("."), emit=emit, started=lambda _x: asyncio.sleep(0))
