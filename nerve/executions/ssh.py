"""Allowlisted SSH transport and fenced remote-supervisor backend.

The model never supplies SSH coordinates or a command string.  A reviewed
profile supplies a structured argv and a resource lease selects one inventory
host; this module resolves that host through a deployment-local connection
catalog and sends a JSON RPC request to ``nerve-remote-supervisor``.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import ipaddress
import json
import logging
import os
import re
import shutil
import struct
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from nerve.executions.backend import BackendRecovery, BackendResult, ExecutionBackendError, ExecutionBackendUncertain, LogSink, StartedSink

logger = logging.getLogger(__name__)


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
    artifact_roots: tuple[str, ...] = ("artifacts",)
    identity_file: Path | None = None
    connect_timeout_seconds: int = 15
    environment_allowlist: tuple[str, ...] = ()
    supervisor_path: str = "/usr/local/libexec/nerve-remote-supervisor"
    transfer_bind_address: str = "127.0.0.1"
    transfer_port: int = 31999
    transfer_user: str = ""
    sshd_path: str = "/usr/sbin/sshd"
    ssh_path: str = "/usr/bin/ssh"
    ssh_keygen_path: str = "/usr/bin/ssh-keygen"

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
            allowed = {"host", "user", "port", "known_hosts", "allowed_cidrs", "remote_roots", "artifact_roots", "identity_file", "connect_timeout_seconds", "environment_allowlist", "supervisor_path", "transfer_bind_address", "transfer_port", "sshd_path", "ssh_path", "ssh_keygen_path", "transfer_user"}
            unknown = set(value) - allowed
            if unknown:
                raise SshTransportError("unknown SSH connection fields: " + ", ".join(sorted(unknown)))
            host, user = value.get("host"), value.get("user")
            if not isinstance(host, str) or not host or not isinstance(user, str) or not user:
                raise SshTransportError("trusted SSH connection requires host and user")
            transfer_user = value.get("transfer_user", user)
            if not re.fullmatch(r"[a-z_][a-z0-9_-]{0,31}", transfer_user):
                raise SshTransportError("trusted SSH connection transfer_user must be a safe Unix account")
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
            artifact_roots = value.get("artifact_roots", ["artifacts"])
            if (not isinstance(artifact_roots, list) or not artifact_roots
                    or not all(isinstance(item, str) for item in artifact_roots)):
                raise SshTransportError("trusted SSH connection requires artifact_roots")
            normalized_artifact_roots: list[str] = []
            for artifact_root in artifact_roots:
                parsed = PurePosixPath(artifact_root)
                if (not artifact_root or parsed.is_absolute() or ".." in parsed.parts
                        or "\0" in artifact_root or artifact_root != parsed.as_posix()):
                    raise SshTransportError("artifact_roots must be normalized relative paths")
                normalized_artifact_roots.append(parsed.as_posix())
            cidrs = value.get("allowed_cidrs", [])
            if not isinstance(cidrs, list) or not all(isinstance(item, str) for item in cidrs):
                raise SshTransportError("allowed_cidrs must be a list of CIDRs")
            for cidr in cidrs:
                ipaddress.ip_network(cidr, strict=False)
            env = value.get("environment_allowlist", [])
            if not isinstance(env, list) or not all(isinstance(item, str) and item.isidentifier() for item in env):
                raise SshTransportError("environment_allowlist contains an invalid name")
            supervisor_path = value.get("supervisor_path", "/usr/local/libexec/nerve-remote-supervisor")
            normalized_supervisor = PurePosixPath(supervisor_path).as_posix() if isinstance(supervisor_path, str) else ""
            if (not isinstance(supervisor_path, str) or not supervisor_path.startswith("/")
                    or supervisor_path.startswith("//") or ".." in PurePosixPath(supervisor_path).parts
                    or supervisor_path != normalized_supervisor):
                raise SshTransportError("supervisor_path must be a fixed absolute normalized path")
            def fixed_executable(field: str, default: str) -> str:
                candidate = value.get(field, default)
                if not isinstance(candidate, str) or not candidate.startswith("/") or ".." in PurePosixPath(candidate).parts or candidate != PurePosixPath(candidate).as_posix():
                    raise SshTransportError(field + " must be a fixed absolute normalized path")
                return candidate
            bind = value.get("transfer_bind_address", "127.0.0.1")
            try: ipaddress.ip_address(bind)
            except ValueError as exc: raise SshTransportError("transfer_bind_address must be an IP address") from exc
            transfer_port = value.get("transfer_port", 31999)
            if isinstance(transfer_port, bool) or not isinstance(transfer_port, int) or not 1 <= transfer_port <= 65535:
                raise SshTransportError("transfer_port is invalid")
            self._connections[name] = SshConnection(name=name, host=host, user=user, port=port,
                known_hosts=Path(known), allowed_cidrs=tuple(cidrs), remote_roots=normalized_roots,
                artifact_roots=tuple(normalized_artifact_roots),
                identity_file=Path(value["identity_file"]) if value.get("identity_file") else None,
                transfer_user=transfer_user,
                connect_timeout_seconds=int(value.get("connect_timeout_seconds", 15)), environment_allowlist=tuple(env), supervisor_path=supervisor_path,
                transfer_bind_address=bind, transfer_port=transfer_port, sshd_path=fixed_executable("sshd_path", "/usr/sbin/sshd"), ssh_path=fixed_executable("ssh_path", "/usr/bin/ssh"), ssh_keygen_path=fixed_executable("ssh_keygen_path", "/usr/bin/ssh-keygen"))

    def resolve(self, name: str) -> SshConnection:
        try:
            connection = self._connections[name]
        except KeyError as exc:
            raise SshTransportError("unknown trusted SSH connection") from exc
        if not connection.known_hosts or not connection.known_hosts.is_file():
            raise SshTransportError("trusted SSH known_hosts file is unavailable")
        return connection


class RemoteSupervisor(Protocol):
    async def sync(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def spin_prepare(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def start(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def status(self, connection: SshConnection, job_id: str, fencing_token: int, root: str) -> Mapping[str, Any]: ...
    async def tail(self, connection: SshConnection, job_id: str, fencing_token: int, cursor: int, root: str) -> Mapping[str, Any]: ...
    async def cancel(self, connection: SshConnection, job_id: str, fencing_token: int, grace_seconds: int, mode: str, root: str) -> Mapping[str, Any]: ...
    async def files(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_put(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_get(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def ydb_publish(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_prepare_destination(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_prepare_source(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_receive(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_status(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_cancel(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def artifact_transfer_cleanup(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...
    async def reconcile_host(self, connection: SshConnection, request: Mapping[str, Any]) -> Mapping[str, Any]: ...


class OpenSshSupervisor:
    """One-shot framed client for the fixed standalone remote supervisor."""
    _MAGIC = b"NRS1"
    _MAX_HEADER = 64 * 1024
    _MAX_PACK = 512 * 1024 * 1024
    # This must match remote_supervisor._FRAME_OPERATIONS.  Validate before
    # opening SSH so backend additions (for example a mistaken ``spin_run``)
    # fail locally rather than being misclassified as transport ambiguity.
    _OPERATIONS = frozenset({
        "start", "sync", "spin_prepare", "artifact_put", "artifact_get",
        "ydb_publish", "artifact_transfer_prepare_destination",
        "reconcile_host",
        "artifact_transfer_prepare_source", "artifact_transfer_receive",
        "artifact_transfer_status", "artifact_transfer_cancel",
        "artifact_transfer_cleanup", "status", "cancel", "tail", "files",
    })

    @classmethod
    def _frame(cls, request: Mapping[str, Any], pack: bytes = b"") -> bytes:
        header = json.dumps(request, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if len(header) > cls._MAX_HEADER or len(pack) > cls._MAX_PACK:
            raise SshTransportError("SSH supervisor frame exceeds safety limit")
        return cls._MAGIC + struct.pack(">I", len(header)) + header + struct.pack(">Q", len(pack)) + pack

    @classmethod
    def _response(cls, raw: bytes) -> Mapping[str, Any]:
        if len(raw) < 16 or raw[:4] != cls._MAGIC:
            raise SshTransportError("SSH supervisor returned an invalid frame")
        size = struct.unpack(">I", raw[4:8])[0]
        if size > cls._MAX_HEADER or len(raw) != 16 + size:
            raise SshTransportError("SSH supervisor returned a truncated or oversized frame")
        if struct.unpack(">Q", raw[8 + size:16 + size])[0] != 0:
            raise SshTransportError("SSH supervisor returned unexpected binary data")
        try:
            response = json.loads(raw[8:8 + size].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SshTransportError("SSH supervisor returned invalid JSON") from exc
        if not isinstance(response, Mapping):
            raise SshTransportError("SSH supervisor returned a non-object response")
        if response.get("version") != 1:
            raise SshTransportError("SSH supervisor returned an unsupported frame version")
        return response

    async def _rpc(self, connection: SshConnection, operation: str, payload: Mapping[str, Any]) -> Mapping[str, Any]:
        # Never log connection coordinates, payload fields, or artifact data.
        if operation not in self._OPERATIONS:
            raise SshTransportError("unsupported SSH supervisor operation")
        logger.info("ssh_rpc started operation=%s connection=%s", operation, connection.name)
        if shutil.which("ssh") is None:
            logger.warning("ssh_rpc failed operation=%s connection=%s reason=no_client", operation, connection.name)
            raise SshTransportError("OpenSSH client is not installed")
        request = dict(payload)
        pack = b""
        if operation == "sync":
            snapshot = dict(request.get("snapshot", {})); path = snapshot.pop("pack_path", None)
            expected = snapshot.pop("pack_sha256", None); length = snapshot.pop("pack_length", None)
            if not isinstance(path, str) or not isinstance(expected, str) or not isinstance(length, int):
                raise SshTransportError("YDB snapshot has no verified local pack")
            try: pack = Path(path).read_bytes()
            except OSError as exc: raise SshTransportError("YDB snapshot pack is unavailable") from exc
            if len(pack) != length or hashlib.sha256(pack).hexdigest() != expected:
                raise SshTransportError("YDB snapshot pack verification failed")
            request["snapshot"] = snapshot
        elif operation == "artifact_put":
            source = request.pop("source_path", None)
            expected, length = request.get("sha256"), request.get("size")
            if not isinstance(source, str) or not isinstance(expected, str) or not isinstance(length, int):
                raise SshTransportError("artifact transfer has no verified local source")
            try: pack = Path(source).read_bytes()
            except OSError as exc: raise SshTransportError("artifact transfer source is unavailable") from exc
            if len(pack) != length or hashlib.sha256(pack).hexdigest() != expected:
                raise SshTransportError("artifact transfer source verification failed")
            artifact_root = request.get("artifact_root", "artifacts")
            if artifact_root not in connection.artifact_roots:
                raise SshTransportError("artifact transfer root is not configured for this connection")
        request = self._frame({"version": 1, "operation": operation, **request}, pack)
        # The remote argv is a reviewed configured absolute path.  Payload is
        # stdin only, never shell syntax or a remote command argument.
        proc = await asyncio.create_subprocess_exec(*connection.ssh_argv(), connection.supervisor_path, "rpc",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(request), connection.connect_timeout_seconds + 30)
        except (TimeoutError, OSError) as exc:
            proc.kill()
            await proc.wait()
            logger.warning("ssh_rpc failed operation=%s connection=%s reason=unreachable error=%s", operation, connection.name, type(exc).__name__)
            raise SshTransportError("SSH supervisor is unreachable") from exc
        if proc.returncode != 0:
            detail = stderr.decode(errors="replace")[-300:]
            logger.warning("ssh_rpc failed operation=%s connection=%s reason=exit_%s detail=%s", operation, connection.name, proc.returncode, detail)
            raise SshTransportError("SSH supervisor request failed: " + detail)
        try:
            response = self._response(stdout)
        except SshTransportError as exc:
            logger.warning("ssh_rpc failed operation=%s connection=%s reason=invalid_response detail=%s", operation, connection.name, str(exc))
            raise
        if not isinstance(response, Mapping) or response.get("ok") is not True:
            detail = str(response.get("error", ""))[:300] if isinstance(response, Mapping) else ""
            logger.warning("ssh_rpc failed operation=%s connection=%s reason=rejected detail=%s", operation, connection.name, detail)
            raise SshTransportError("SSH supervisor rejected request" + (": " + detail if detail else ""))
        logger.info("ssh_rpc completed operation=%s connection=%s", operation, connection.name)
        return response

    async def start(self, connection, request): return await self._rpc(connection, "start", request)
    async def sync(self, connection, request): return await self._rpc(connection, "sync", request)
    async def status(self, connection, job_id, fencing_token, root): return await self._rpc(connection, "status", {"job_id": job_id, "fencing_token": fencing_token, "root": root})
    async def tail(self, connection, job_id, fencing_token, cursor, root): return await self._rpc(connection, "tail", {"job_id": job_id, "fencing_token": fencing_token, "cursor": cursor, "root": root})
    async def cancel(self, connection, job_id, fencing_token, grace_seconds, mode, root): return await self._rpc(connection, "cancel", {"job_id": job_id, "fencing_token": fencing_token, "grace_seconds": grace_seconds, "mode": mode, "root": root})
    async def files(self, connection, request): return await self._rpc(connection, "files", request)
    async def artifact_put(self, connection, request): return await self._rpc(connection, "artifact_put", request)
    async def artifact_get(self, connection, request): return await self._rpc(connection, "artifact_get", request)
    async def spin_prepare(self, connection, request): return await self._rpc(connection, "spin_prepare", request)
    async def ydb_publish(self, connection, request): return await self._rpc(connection, "ydb_publish", request)
    async def artifact_transfer_prepare_destination(self, connection, request): return await self._rpc(connection, "artifact_transfer_prepare_destination", request)
    async def artifact_transfer_prepare_source(self, connection, request): return await self._rpc(connection, "artifact_transfer_prepare_source", request)
    async def artifact_transfer_receive(self, connection, request): return await self._rpc(connection, "artifact_transfer_receive", request)
    async def artifact_transfer_status(self, connection, request): return await self._rpc(connection, "artifact_transfer_status", request)
    async def artifact_transfer_cancel(self, connection, request): return await self._rpc(connection, "artifact_transfer_cancel", request)
    async def artifact_transfer_cleanup(self, connection, request): return await self._rpc(connection, "artifact_transfer_cleanup", request)
    async def reconcile_host(self, connection, request): return await self._rpc(connection, "reconcile_host", request)


class SshExecutionBackend:
    """Execution backend whose cancellation acknowledgement means quiescence."""
    name = "ssh-supervisor"

    def __init__(self, *, inventory: Any, connections: SshConnectionCatalog, supervisor: RemoteSupervisor | None = None, poll_seconds: float = .25) -> None:
        self.inventory, self.connections, self.supervisor = inventory, connections, supervisor or OpenSshSupervisor()
        self.poll_seconds = poll_seconds
        self._jobs: dict[str, tuple[SshConnection, str, int, str]] = {}
        self._transfers: dict[str, tuple[SshConnection, Mapping[str, Any], SshConnection, Mapping[str, Any], str]] = {}

    async def reconcile_host(self, host_id: str, generation: int) -> bool:
        """Require every configured supervisor root to prove its host lock idle."""
        host = self.inventory.hosts.get(host_id)
        if host is None:
            return False
        connection = self.connections.resolve(str(host["connection_ref"]))
        replies = await asyncio.gather(*(self.supervisor.reconcile_host(connection, {
            "root": root, "recovery_generation": generation,
        }) for root in connection.remote_roots))
        return bool(replies) and all(reply.get("quiescent") is True and int(reply.get("generation", -1)) == generation for reply in replies)

    def validate_artifact_endpoint(self, pool: str, artifact_root: str) -> None:
        """Reject unusable reviewed endpoints before an execution can queue."""
        members = self.inventory.members(pool)
        if not members:
            raise SshTransportError("artifact transfer pool has no hosts")
        for host_id in members:
            host = self.inventory.hosts.get(host_id, {})
            connection = self.connections.resolve(host.get("connection_ref"))
            if artifact_root not in connection.artifact_roots:
                raise SshTransportError(
                    "artifact transfer root is not configured for every host in the pool"
                )

    def _local_artifact(self, root_id: Any, relative: Any, *, output: bool) -> Path:
        roots = getattr(self.inventory, "local_artifact_roots", {})
        root = roots.get(root_id)
        if root is None:
            raise SshTransportError("local artifact root is not configured")
        if not isinstance(relative, str) or not relative or "\0" in relative:
            raise SshTransportError("invalid local artifact path")
        rel = PurePosixPath(relative)
        if rel.is_absolute() or ".." in rel.parts:
            raise SshTransportError("local artifact path escapes configured root")
        root = Path(root).resolve(strict=True)
        target = root.joinpath(*rel.parts)
        parent = target.parent.resolve(strict=False)
        if parent != root and root not in parent.parents:
            raise SshTransportError("local artifact path symlink escapes configured root")
        if output:
            target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            parent = target.parent.resolve(strict=True)
            if parent != root and root not in parent.parents:
                raise SshTransportError("local artifact path symlink escapes configured root")
            return target
        try:
            resolved = target.resolve(strict=True)
        except OSError as exc:
            raise SshTransportError("local artifact source is unavailable") from exc
        if resolved != root and root not in resolved.parents or not resolved.is_file():
            raise SshTransportError("local artifact path symlink escapes configured root")
        return resolved

    @staticmethod
    def _file_digest(path: Path) -> tuple[int, str]:
        digest = hashlib.sha256(); size = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk); digest.update(chunk)
        return size, digest.hexdigest()

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

    async def inspect_ydb_files(self, *, session_id: str, reservation: Mapping[str, Any], operation: str, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        if operation not in {"list", "find", "read"}: raise SshTransportError("invalid YDB file operation")
        host, lease = self.inventory.hosts.get(str(reservation.get("host_id"))), reservation.get("lease")
        if not isinstance(host, Mapping) or not isinstance(lease, Mapping): raise SshTransportError("session reservation has no usable host")
        connection = self.connections.resolve(str(host.get("connection_ref"))); root = connection.remote_roots[0]
        request: dict[str, Any] = {
            "root": root,
            "session_id": session_id,
            "fencing_token": int(lease["fencing_token"]),
            "action": operation,
        }
        if operation == "list":
            request.update(path=arguments.get("path", "."), depth=arguments.get("depth", 1),
                           limit=arguments.get("limit", 200))
        elif operation == "find":
            request.update(relative_root=arguments.get("root", "."),
                           pattern=arguments.get("pattern"), limit=arguments.get("limit", 200))
        else:
            request.update(path=arguments.get("path"), offset=arguments.get("offset", 0),
                           limit=arguments.get("limit", 65536))
        reply = await self.supervisor.files(connection, request)
        if len(json.dumps(reply, separators=(",", ":"), ensure_ascii=False).encode()) > 512 * 1024: raise SshTransportError("remote file response exceeds safety limit")
        return dict(reply)

    @staticmethod
    def _argv(step: Mapping[str, Any], plan: Mapping[str, Any]) -> list[str]:
        values = dict(plan.get("arguments", {}))
        result: list[str] = [str(step["executable"])]
        for token in step.get("argv", []):
            kind, value = token.get("type"), token.get("value")
            if kind in {"arg", "value"}: result.append(str(value) if kind == "value" else str(values[str(value)]))
            elif kind == "spread": result.extend(str(x) for x in values[str(value)])
            elif kind == "literal": result.append(str(value))
            else: raise SshTransportError("SSH steps only permit literal and validated argument argv tokens")
        return result

    async def run(self, *, execution_id: str, plan: Mapping[str, Any], workspace: Path, execution_dir: Path, emit: LogSink, started: StartedSink) -> BackendResult:
        if plan.get("kind") == "artifact_transfer":
            return await self._run_artifact_transfer(execution_id, plan, emit, started)
        connection, step, token = self._job(execution_id, plan)
        root = _remote_path(str(plan.get("remote_root", connection.remote_roots[0])), connection.remote_roots)
        remote_workspace = root
        snapshot = plan.get("ydb_snapshot")
        spin = plan.get("spin")
        spin_version = None
        if isinstance(spin, Mapping):
            reply = await self.supervisor.spin_prepare(connection, {"execution_id": execution_id, "lease_id": next(x["id"] for x in plan["selected_leases"] if x.get("slot") == step.get("resource_slot")), "fencing_token": token, "root": root, "session_id": str(plan.get("session_id", "")), **dict(spin)})
            remote_workspace = _remote_path(str(reply.get("workspace") or ""), connection.remote_roots)
            spin_version = str(reply.get("spin_version") or "unavailable")[:256]
        # Recovery attaches to the durable supervisor job; it must never
        # rewrite that job's checkout while it may still be compiling.
        if isinstance(snapshot, Mapping) and not plan.get("_remote_existing_job"):
            request = {"execution_id": execution_id, "lease_id": next(x["id"] for x in plan["selected_leases"] if x.get("slot") == step.get("resource_slot")),
                       "fencing_token": token, "root": root, "session_id": str(plan.get("session_id", "")), "snapshot": dict(snapshot)}
            pack_path = snapshot.get("pack_path")
            try:
                reply = await self.supervisor.sync(connection, request)
            finally:
                if isinstance(pack_path, str):
                    try:
                        Path(pack_path).unlink()
                    except FileNotFoundError:
                        pass
                    except OSError as exc:
                        raise SshTransportError(
                            "could not remove the consumed YDB snapshot pack"
                        ) from exc
            remote_workspace = _remote_path(str(reply.get("workspace") or ""), connection.remote_roots)
        existing = plan.get("_remote_existing_job")
        if existing:
            job_id = str(existing)
        else:
            request = {"execution_id": execution_id, "lease_id": next(x["id"] for x in plan["selected_leases"] if x.get("slot") == step.get("resource_slot")), "fencing_token": token,
                       "root": root, "argv": self._argv(step, plan), "cwd": remote_workspace if step.get("cwd") == "workspace" else "job", "environment": {k: os.environ[k] for k in connection.environment_allowlist if k in os.environ}}
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
                if status.get("state") in {"succeeded", "failed", "cancelled", "finished"}:
                    # The command may write its last bytes after the preceding
                    # tail RPC and publish terminal state before the next loop.
                    # Drain once more so short jobs and final diagnostics are
                    # not lost.
                    tail = await self.supervisor.tail(connection, job_id, token, cursor, root)
                    for entry in tail.get("entries", []):
                        await emit(str(entry.get("stream", "stdout")), str(entry.get("text", "")))
                    result = BackendResult(status.get("exit_code"), summary=str(status.get("summary", "remote job finished")), error=status.get("error"))
                    if spin_version is not None:
                        result = BackendResult(
                            result.exit_code, signal=result.signal, error=result.error,
                            summary=(result.summary + "; SPIN " + spin_version)[:512],
                            spin_version=spin_version,
                        )
                    publish = plan.get("ydb_publish")
                    if result.exit_code == 0 and isinstance(publish, Mapping):
                        await emit("stdout", "network stage ydb_publish started\n")
                        try:
                            reply = await self.supervisor.ydb_publish(connection, {"root": root, "lease_id": next(x["id"] for x in plan["selected_leases"] if x.get("slot") == step.get("resource_slot")), "fencing_token": token, "workspace": str(remote_workspace), **dict(publish)})
                        except SshTransportError as exc:
                            await emit("stderr", "network stage ydb_publish failed: " + str(exc) + "\n")
                            raise
                        await emit("stdout", "published artifact " + str(reply.get("artifact_root")) + "/" + str(reply.get("path")) + "\\n")
                    return result
                await asyncio.sleep(self.poll_seconds)
        finally:
            self._jobs.pop(execution_id, None)

    def _transfer_endpoints(self, plan: Mapping[str, Any]):
        leases = {str(x.get("slot")): x for x in plan.get("selected_leases", [])}
        source, destination = leases.get("source"), leases.get("destination")
        if not isinstance(source, Mapping) or not isinstance(destination, Mapping): raise SshTransportError("artifact transfer requires source and destination leases")
        def endpoint(lease):
            host = self.inventory.hosts.get(str(lease.get("host_id")))
            if not isinstance(host, Mapping): raise SshTransportError("artifact transfer lease references unknown host")
            return self.connections.resolve(str(host.get("connection_ref"))), lease
        return endpoint(source), endpoint(destination)

    async def _run_artifact_transfer(self, execution_id, plan, emit, started):
        spec = plan.get("artifact_transfer")
        if not isinstance(spec, Mapping): raise SshTransportError("artifact transfer plan is malformed")
        if spec.get("source_local"):
            return await self._run_local_to_remote(execution_id, plan, spec, emit, started)
        if spec.get("destination_local"):
            return await self._run_remote_to_local(execution_id, plan, spec, emit, started)
        (source_connection, source_lease), (destination_connection, destination_lease) = self._transfer_endpoints(plan)
        transfer_id = str(spec.get("transfer_id") or "")
        source_root = spec.get("source_root")
        destination_root = spec.get("destination_root")
        if source_root not in source_connection.artifact_roots:
            raise SshTransportError("source artifact root is not configured for source connection")
        if destination_root not in destination_connection.artifact_roots:
            raise SshTransportError("destination artifact root is not configured for destination connection")
        root_source, root_destination = source_connection.remote_roots[0], destination_connection.remote_roots[0]
        common = {"transfer_id": transfer_id, "execution_id": execution_id}
        self._transfers[execution_id] = (source_connection, source_lease, destination_connection, destination_lease, transfer_id)
        cleanup_targets = []
        try:
            destination_key = await self.supervisor.artifact_transfer_prepare_destination(destination_connection, {**common, "root": root_destination, "artifact_root": spec["destination_root"], "path": spec["destination_path"], "lease_id": destination_lease["id"], "fencing_token": int(destination_lease["fencing_token"]), "ssh_keygen_path": destination_connection.ssh_keygen_path})
            cleanup_targets.append((destination_connection, root_destination, destination_lease))
            # Once source preparation is requested, a transport failure is
            # ambiguous: sshd may have started even if its reply was lost.
            cleanup_targets.append((source_connection, root_source, source_lease))
            prepared = await self.supervisor.artifact_transfer_prepare_source(source_connection, {**common, "root": root_source, "artifact_root": spec["source_root"], "path": spec["source_path"], "lease_id": source_lease["id"], "fencing_token": int(source_lease["fencing_token"]), "client_public_key": destination_key["client_public_key"], "transfer_user": source_connection.transfer_user, "bind_address": source_connection.transfer_bind_address, "port": source_connection.transfer_port, "sshd_path": source_connection.sshd_path, "ssh_keygen_path": source_connection.ssh_keygen_path, "supervisor_path": source_connection.supervisor_path})
            await started({"transfer_id": transfer_id, "source_fencing_token": int(source_lease["fencing_token"]), "destination_fencing_token": int(destination_lease["fencing_token"]), "reattachable": False})
            await self.supervisor.artifact_transfer_receive(destination_connection, {**common, "root": root_destination, "artifact_root": spec["destination_root"], "path": spec["destination_path"], "lease_id": destination_lease["id"], "fencing_token": int(destination_lease["fencing_token"]), "source_address": prepared["address"], "source_port": prepared["port"], "source_host_key": prepared["host_public_key"], "size": prepared["size"], "sha256": prepared["sha256"], "transfer_user": prepared["transfer_user"], "ssh_path": destination_connection.ssh_path})
            await emit("stdout", "direct artifact transfer completed\n")
            return BackendResult(0, summary="artifact transferred directly")
        finally:
            cleanup = await asyncio.gather(
                *(self.supervisor.artifact_transfer_cleanup(connection, {
                    **common, "root": root, "fencing_token": int(lease["fencing_token"]),
                }) for connection, root, lease in cleanup_targets),
                return_exceptions=True,
            )
            self._transfers.pop(execution_id, None)
            if any(
                isinstance(item, BaseException) or item.get("quiescent") is not True
                for item in cleanup
            ):
                raise ExecutionBackendUncertain(
                    "artifact transfer cleanup could not prove quiescence"
                )

    async def _run_local_to_remote(self, execution_id, plan, spec, emit, started):
        lease = next((x for x in plan.get("selected_leases", []) if x.get("slot") == "destination"), None)
        if not isinstance(lease, Mapping): raise SshTransportError("artifact transfer requires destination lease")
        host = self.inventory.hosts.get(str(lease.get("host_id")))
        if not isinstance(host, Mapping): raise SshTransportError("artifact transfer lease references unknown host")
        connection = self.connections.resolve(str(host["connection_ref"]))
        if spec.get("destination_root") not in connection.artifact_roots: raise SshTransportError("destination artifact root is not configured")
        source = self._local_artifact(spec.get("source_root"), spec.get("source_path"), output=False)
        size, sha256 = self._file_digest(source)
        await started({"destination_fencing_token": int(lease["fencing_token"]), "reattachable": False})
        await self.supervisor.artifact_put(connection, {"root": connection.remote_roots[0], "lease_id": lease["id"], "fencing_token": int(lease["fencing_token"]), "artifact_root": spec["destination_root"], "path": spec["destination_path"], "source_path": str(source), "size": size, "sha256": sha256})
        await emit("stdout", "artifact transferred to remote host\n")
        return BackendResult(0, summary="artifact transferred to remote host")

    async def _run_remote_to_local(self, execution_id, plan, spec, emit, started):
        lease = next((x for x in plan.get("selected_leases", []) if x.get("slot") == "source"), None)
        if not isinstance(lease, Mapping): raise SshTransportError("artifact transfer requires source lease")
        host = self.inventory.hosts.get(str(lease.get("host_id")))
        if not isinstance(host, Mapping): raise SshTransportError("artifact transfer lease references unknown host")
        connection = self.connections.resolve(str(host["connection_ref"]))
        if spec.get("source_root") not in connection.artifact_roots: raise SshTransportError("source artifact root is not configured")
        target = self._local_artifact(spec.get("destination_root"), spec.get("destination_path"), output=True)
        await started({"source_fencing_token": int(lease["fencing_token"]), "reattachable": False})
        reply = await self.supervisor.artifact_get(connection, {"root": connection.remote_roots[0], "lease_id": lease["id"], "fencing_token": int(lease["fencing_token"]), "artifact_root": spec["source_root"], "path": spec["source_path"]})
        encoded = reply.get("data")
        if not isinstance(encoded, str): raise SshTransportError("remote artifact response is malformed")
        try: data = base64.b64decode(encoded, validate=True)
        except (ValueError, base64.binascii.Error) as exc: raise SshTransportError("remote artifact response is malformed") from exc
        size, sha256 = reply.get("size"), reply.get("sha256")
        if not isinstance(size, int) or not isinstance(sha256, str) or len(data) != size or hashlib.sha256(data).hexdigest() != sha256:
            raise SshTransportError("remote artifact verification failed")
        temporary = target.with_name("." + target.name + ".nerve-transfer-" + os.urandom(8).hex())
        try:
            with open(temporary, "xb") as stream:
                stream.write(data); stream.flush(); os.fsync(stream.fileno())
            temporary.replace(target)
        finally:
            with contextlib.suppress(FileNotFoundError): temporary.unlink()
        await emit("stdout", "artifact transferred to local root\n")
        return BackendResult(0, summary="artifact transferred to local root")

    async def cancel(self, *, execution_id: str, grace_seconds: int, mode: str) -> bool:
        # Transfer state is persisted on both workers, so cancellation remains
        # meaningful after this daemon lost its in-memory process table.
        transfer = self._transfers.get(execution_id)
        if transfer:
            source, source_lease, destination, destination_lease, transfer_id = transfer
            try:
                left = await self.supervisor.artifact_transfer_cancel(source, {"root": source.remote_roots[0], "transfer_id": transfer_id, "fencing_token": int(source_lease["fencing_token"])})
                right = await self.supervisor.artifact_transfer_cancel(destination, {"root": destination.remote_roots[0], "transfer_id": transfer_id, "fencing_token": int(destination_lease["fencing_token"])})
                return left.get("quiescent") is True and right.get("quiescent") is True
            except SshTransportError:
                return False
        if execution_id not in self._jobs: return False
        job = self._jobs.get(execution_id)
        if not job or mode == "none": return False
        connection, job_id, token, root = job
        try: reply = await self.supervisor.cancel(connection, job_id, token, grace_seconds, mode, root)
        except SshTransportError: return False
        return reply.get("quiescent") is True and reply.get("state") in {
            "succeeded", "failed", "cancelled", "finished",
        }

    async def recover(self, execution: Mapping[str, Any]) -> BackendRecovery:
        if execution.get("kind") == "artifact_transfer":
            try:
                plan = {**execution["plan"], "selected_leases": execution.get("selected_leases") or []}
                (source, source_lease), (destination, destination_lease) = self._transfer_endpoints(plan)
                transfer_id = plan["artifact_transfer"]["transfer_id"]
                _source_status = await self.supervisor.artifact_transfer_status(source, {"root": source.remote_roots[0], "transfer_id": transfer_id, "fencing_token": int(source_lease["fencing_token"])})
                destination_status = await self.supervisor.artifact_transfer_status(destination, {"root": destination.remote_roots[0], "transfer_id": transfer_id, "fencing_token": int(destination_lease["fencing_token"])})
            except (SshTransportError, KeyError, ValueError): return BackendRecovery("orphaned")
            if destination_status.get("state") == "succeeded": return BackendRecovery("finished", BackendResult(0, summary="artifact transferred directly"))
            return BackendRecovery("orphaned")
        handle = execution.get("backend_handle") or {}; job_id = handle.get("job_id"); token = handle.get("fencing_token")
        if not job_id or token is None: return BackendRecovery("missing")
        try:
            connection, _, _ = self._job(str(execution["id"]), execution["plan"])
            root = _remote_path(str(execution["plan"].get("remote_root", connection.remote_roots[0])), connection.remote_roots)
            status = await self.supervisor.status(connection, str(job_id), int(token), root)
        except (SshTransportError, KeyError, ValueError): return BackendRecovery("orphaned")
        # A reconnect deliberately reconstructs the in-memory convenience
        # cache from the durable fenced handle.  This lets the subsequent
        # cancel RPC address the same supervisor job; it never starts a new
        # command or selects another host.
        self._jobs[str(execution["id"])] = (connection, str(job_id), int(token), root)
        if status.get("state") in {"running", "starting"}: return BackendRecovery("reattachable")
        state = str(status.get("state"))
        return BackendRecovery("finished", BackendResult(None if state == "finished" else status.get("exit_code"), summary=str(status.get("summary", "remote job finished")), error=status.get("error")))

    async def reattach(self, *, execution: Mapping[str, Any], emit: LogSink) -> BackendResult:
        handle = execution.get("backend_handle") or {}
        plan = {**execution["plan"], "selected_leases": execution.get("selected_leases") or [], "_remote_existing_job": handle.get("job_id")}
        return await self.run(execution_id=str(execution["id"]), plan=plan, workspace=Path("."), execution_dir=Path("."), emit=emit, started=lambda _x: asyncio.sleep(0))
