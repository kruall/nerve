#!/usr/bin/env python3
"""Standalone, stdlib-only remote half of the SSH execution protocol.

This file is deliberately installable as ``nerve-remote-supervisor`` on a
worker; it does not import Nerve or require a Nerve installation there.  Its
only interface is one framed request on stdin and one framed response on
stdout.  State is durable under the caller-approved root; every operation
checks its fencing token.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
import shutil
import shlex
import struct
import contextlib
import socket
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


_MAGIC = b"NRS1"
_VERSION = 1
_MAX_HEADER = 64 * 1024
_MAX_PACK = 512 * 1024 * 1024
# artifact_get is deliberately bounded until the streaming NRS extension is
# deployed everywhere.  It is not advertised as a large-artifact operation.
_MAX_ARTIFACT_GET = 32 * 1024
_TERMINAL_STATE_BY_UNKNOWN_EXIT_CODE = "failed"
_SPIN_RETENTION_SECONDS = 24 * 60 * 60
# Keep this explicit protocol surface small.  The SSH client has a matching
# allowlist so a new backend method cannot accidentally emit an operation the
# installed standalone supervisor will reject.
_FRAME_OPERATIONS = frozenset({
    "start", "sync", "spin_prepare", "artifact_put", "artifact_get",
    "ydb_publish", "artifact_transfer_prepare_destination",
    "artifact_transfer_prepare_source", "artifact_transfer_receive",
    "artifact_transfer_status", "artifact_transfer_cancel",
    "artifact_transfer_cleanup", "status", "cancel", "tail", "files",
    "reconcile_host", "capabilities",
})
_PACK_OPERATIONS = frozenset({"sync", "artifact_put"})


def _decode_frame(raw: bytes) -> tuple[dict[str, Any], bytes]:
    """Parse exactly one bounded binary-safe request frame."""
    if len(raw) < 16 or raw[:4] != _MAGIC:
        raise ValueError("invalid frame magic or truncated frame")
    header_size = struct.unpack(">I", raw[4:8])[0]
    if header_size > _MAX_HEADER or len(raw) < 16 + header_size:
        raise ValueError("oversized or truncated frame header")
    pack_size = struct.unpack(">Q", raw[8 + header_size:16 + header_size])[0]
    if pack_size > _MAX_PACK or len(raw) != 16 + header_size + pack_size:
        raise ValueError("oversized, truncated, or trailing frame data")
    try:
        request = json.loads(raw[8:8 + header_size].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid frame JSON header") from exc
    if not isinstance(request, dict) or request.get("version") != _VERSION:
        raise ValueError("unsupported frame version")
    operation = request.get("operation")
    if operation not in _FRAME_OPERATIONS:
        raise ValueError("invalid frame operation")
    if pack_size and operation not in _PACK_OPERATIONS:
        raise ValueError("binary pack is only permitted for sync or artifact_put")
    return request, raw[16 + header_size:]


def _encode_frame(response: Mapping[str, Any]) -> bytes:
    header = json.dumps(
        {"version": _VERSION, **response},
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    if len(header) > _MAX_HEADER:
        # This should be impossible for the deliberately bounded operations,
        # but keep the protocol safe if a future response grows unexpectedly.
        header = b'{"ok":false,"error":"response exceeds frame limit"}'
    return _MAGIC + struct.pack(">I", len(header)) + header + struct.pack(">Q", 0)


def _safe_root(value: str) -> Path:
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("invalid job root")
    return Path(path)


_UNIX_ACCOUNT = re.compile(r"[a-z_][a-z0-9_-]{0,31}$")


def _validate_unix_account(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _UNIX_ACCOUNT.fullmatch(value):
        raise ValueError(f"invalid {field}")
    return value


def _job_dir(root: Path, job_id: str) -> Path:
    if not job_id.startswith("job-") or not job_id[4:].isalnum():
        raise ValueError("invalid job id")
    return root / ".nerve-jobs" / job_id


def _read(job: Path, token: int) -> dict[str, Any]:
    state = json.loads((job / "state.json").read_text())
    if state.get("fencing_token") != token:
        raise PermissionError("stale fencing token")
    return state


def _write(job: Path, state: Mapping[str, Any]) -> None:
    temp = job / ".state.tmp"
    temp.write_text(json.dumps(state, separators=(",", ":")))
    temp.replace(job / "state.json")


def _cas_state(job: Path, token: int, expected_state: str, updates: Mapping[str, Any]) -> dict[str, Any]:
    with open(job / ".state.lock", "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        state = _read(job, token)
        if state.get("state") != expected_state:
            return state
        state.update(updates)
        _write(job, state)
        return state


def _alive(pgid: int) -> bool:
    # Unit/in-process callers can still be the detached monitor's parent. Reap
    # an exited child so a zombie is not mistaken for a live process group.
    # Normal one-shot RPC status calls are not the parent and take the
    # ChildProcessError path without changing production behavior.
    try:
        reaped, _ = os.waitpid(pgid, os.WNOHANG)
        if reaped == pgid:
            return False
    except ChildProcessError:
        pass
    try: os.killpg(pgid, 0)
    except ProcessLookupError: return False
    except PermissionError: return True
    return True


def _stream_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        while True:
            chunk = stream.read(1 << 16)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _wait_for_transfer_listener(address: str, port: int, process_group: int) -> bool:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        if not _alive(process_group):
            return False
        try:
            with socket.create_connection((address, port), timeout=0.1):
                return True
        except OSError:
            time.sleep(.05)
    return False


def _wait_for_quiescence(process_group: int, grace_seconds: float) -> bool:
    end = time.monotonic() + max(0.0, float(grace_seconds))
    while time.monotonic() < end:
        if not _alive(process_group):
            return True
        time.sleep(.05)
    return not _alive(process_group)


def _kill_process_group(process_group: int, grace_seconds: float) -> bool:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process_group, signal.SIGTERM)
    if _wait_for_quiescence(process_group, grace_seconds):
        return True
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process_group, signal.SIGKILL)
    return _wait_for_quiescence(process_group, 0.25)


def _monitor(payload_path: str) -> None:
    payload = json.loads(Path(payload_path).read_text())
    job = _job_dir(_safe_root(str(payload["root"])), str(payload["job_id"]))
    token = int(payload["fencing_token"])
    argv = payload.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) and "\0" not in x for x in argv):
        raise ValueError("invalid structured argv")
    cwd = _safe_root(str(payload["cwd"])) if not isinstance(payload.get("cwd"), PurePosixPath) else payload["cwd"]
    environment = payload.get("environment")
    if not isinstance(environment, Mapping) or not all(isinstance(k, str) and isinstance(v, str) for k, v in environment.items()):
        raise ValueError("invalid execution environment")
    lock_fd = payload.get("lock_fd")
    if isinstance(lock_fd, bool) or not isinstance(lock_fd, int) or lock_fd < 0:
        raise ValueError("invalid inherited host lock")
    lock_open = True

    def release_host_lock() -> None:
        nonlocal lock_open
        if lock_open:
            os.close(lock_fd)
            lock_open = False
    out = open(job / "stdout.log", "ab", buffering=0)
    err = open(job / "stderr.log", "ab", buffering=0)
    try:
        # The RPC parent must durably publish the monitor PID/process group
        # before the command can finish and attempt its terminal CAS.  This
        # also gives a concurrent cancel a stable process group to signal.
        deadline = time.monotonic() + 5
        while True:
            try:
                initial = _read(job, token)
            except FileNotFoundError:
                initial = None
            if initial is not None and int(initial.get("process_group", -1)) == os.getpgrp():
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("monitor initial state was not published")
            time.sleep(.01)
        child_environment = dict(environment)
        # Commands run with an explicit, intentionally small environment.  Keep
        # the non-secret remote account identity available: tools such as ya
        # use getpass.getuser(), and some fleet UIDs have no passwd entry.
        for name in ("USER", "LOGNAME", "HOME", "PATH"):
            if name not in child_environment and name in os.environ:
                child_environment[name] = os.environ[name]
        proc = subprocess.Popen(argv, cwd=str(cwd), env=child_environment, stdin=subprocess.DEVNULL, stdout=out, stderr=err)
        exit_code = proc.wait()
        summary = "succeeded" if exit_code == 0 else "failed"
        release_host_lock()
        _cas_state(job, token, "running", {
            "state": summary,
            "exit_code": exit_code,
            "finished_at": time.time(),
            "summary": "remote command " + summary,
            "pid": proc.pid,
        })
    except Exception as exc:
        with contextlib.suppress(OSError):
            release_host_lock()
        with contextlib.suppress(Exception):
            _cas_state(job, token, "running", {
                "state": _TERMINAL_STATE_BY_UNKNOWN_EXIT_CODE,
                "exit_code": None,
                "finished_at": time.time(),
                "summary": "monitor failed before command exit",
                "error": type(exc).__name__ + ": " + str(exc),
            })
    finally:
        with contextlib.suppress(OSError):
            release_host_lock()
        out.close()
        err.close()
        with contextlib.suppress(FileNotFoundError):
            Path(payload_path).unlink()


def _start(request: Mapping[str, Any]) -> dict[str, Any]:
    root = _safe_root(str(request["root"])); root.mkdir(parents=True, exist_ok=True)
    argv = request.get("argv")
    if not isinstance(argv, list) or not argv or not all(isinstance(x, str) and "\0" not in x for x in argv):
        raise ValueError("invalid structured argv")
    token = int(request["fencing_token"]); execution_id = str(request["execution_id"])
    job_id = "job-" + os.urandom(12).hex(); job = _job_dir(root, job_id); job.mkdir(parents=True)
    lock = open(root / ".nerve-host.lock", "a+")
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock.close(); raise RuntimeError("host lock is held")
    supplied_cwd = request.get("cwd")
    if supplied_cwd == "workspace":
        cwd = root
    elif supplied_cwd in {"execution_dir", "job", None}:
        cwd = job
    elif isinstance(supplied_cwd, str):
        cwd = _safe_root(supplied_cwd)
    else:
        raise ValueError("invalid cwd")
    if cwd != root and root not in cwd.parents:
        raise ValueError("cwd escapes job root")
    cwd.mkdir(parents=True, exist_ok=True)
    environment = request.get("environment")
    if not isinstance(environment, Mapping) or not all(isinstance(k, str) and isinstance(v, str) for k, v in environment.items()):
        raise ValueError("invalid execution environment")
    payload = job / ".monitor.json"
    encoded_payload = json.dumps({
        "argv": argv,
        "cwd": str(cwd),
        "environment": dict(environment),
        "execution_id": execution_id,
        "job_id": job_id,
        "fencing_token": token,
        "lock_fd": lock.fileno(),
        "root": str(root),
    }, separators=(",", ":")).encode()
    payload_fd = os.open(payload, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(payload_fd, "wb") as stream:
        stream.write(encoded_payload)
    # ``lock`` remains inherited by the monitor process group leader.  A
    # second central lease cannot overlap physically even if the control plane
    # has lost the first worker.
    try:
        proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "_monitor", str(payload)],
                                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True, pass_fds=(lock.fileno(),))
    except OSError:
        lock.close()
        with contextlib.suppress(FileNotFoundError):
            payload.unlink()
        raise RuntimeError("failed to launch detached monitor process")
    # The monitor now owns the inherited flock fd; the RPC process must release
    # its copy so worker disconnects cannot keep a dead job's lease forever.
    lock.close()
    state = {"job_id": job_id, "execution_id": execution_id, "fencing_token": token,
             "pid": proc.pid, "process_group": proc.pid, "state": "running", "exit_code": None,
             "started_at": time.time()}
    _write(job, state)
    return {"ok": True, **state}


def _sync(request: Mapping[str, Any], pack: bytes) -> dict[str, Any]:
    """Atomically build a session checkout from a cached base and thin pack."""
    root = _safe_root(str(request["root"])); root.mkdir(parents=True, exist_ok=True)
    session = str(request.get("session_id") or "")
    snapshot = request.get("snapshot")
    if not session or not session.replace("-", "").isalnum() or not isinstance(snapshot, Mapping):
        raise ValueError("invalid sync identity")
    ident = str(snapshot.get("snapshot_id") or "")
    head = str(snapshot.get("head") or "")
    if len(ident) not in {40, 64} or any(c not in "0123456789abcdef" for c in ident):
        raise ValueError("invalid snapshot")
    if len(head) not in {40, 64} or any(c not in "0123456789abcdef" for c in head):
        raise ValueError("invalid snapshot base")
    if not isinstance(pack, bytes) or len(pack) > _MAX_PACK:
        raise ValueError("invalid sync pack")
    base_pack = b""
    if snapshot.get("base_pack"):
        if len(pack) < 16:
            raise ValueError("invalid sync base pack")
        base_size, snapshot_size = struct.unpack(">QQ", pack[:16])
        if base_size + snapshot_size != len(pack) - 16:
            raise ValueError("invalid sync pack lengths")
        base_pack = pack[16:16 + base_size]
        pack = pack[16 + base_size:]
    safe_session = hashlib.sha256(session.encode()).hexdigest()[:24]
    token = int(request["fencing_token"])
    fences = root / ".nerve-ydb-fences"; fences.mkdir(parents=True, exist_ok=True)
    fence = fences / (safe_session + ".json")
    if fence.exists():
        previous = json.loads(fence.read_text())
        if int(previous.get("fencing_token", -1)) > token:
            raise PermissionError("stale fencing token")
    temporary_fence = fence.with_suffix(".tmp")
    temporary_fence.write_text(json.dumps({"fencing_token": token, "lease_id": str(request.get("lease_id", ""))}))
    temporary_fence.replace(fence)
    target = root / ".nerve-ydb-worktrees" / safe_session
    staging = root / ".nerve-ydb-staging" / (safe_session + "-" + ident[:12])
    shutil.rmtree(staging, ignore_errors=True); staging.mkdir(parents=True)
    cache = root / ".nerve-ydb-object-cache"
    try:
        if not cache.exists():
            subprocess.run(["git", "init", "--bare", str(cache)], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        base = subprocess.run(["git", "--git-dir", str(cache), "cat-file", "-e", head + "^{commit}"], stderr=subprocess.DEVNULL)
        if base.returncode:
            if not base_pack:
                raise ValueError("remote YDB object cache lacks requested base HEAD")
            subprocess.run(["git", "--git-dir", str(cache), "index-pack", "--stdin", "--fix-thin", "--keep"],
                           input=base_pack, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            base = subprocess.run(["git", "--git-dir", str(cache), "cat-file", "-e", head + "^{commit}"], stderr=subprocess.DEVNULL)
            if base.returncode:
                raise ValueError("automatic YDB cache provisioning did not contain requested base HEAD")
        subprocess.run(["git", "--git-dir", str(cache), "index-pack", "--stdin", "--fix-thin", "--keep"],
                       input=pack, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        subprocess.run(["git", "--git-dir", str(cache), "fsck", "--connectivity-only", ident],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        snapshot_ref = "refs/nerve/snapshots/" + safe_session
        subprocess.run(["git", "--git-dir", str(cache), "update-ref", snapshot_ref, ident],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        subprocess.run(["git", "clone", "--no-checkout", str(cache), str(staging / "tree")], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        tree = staging / "tree"
        subprocess.run(["git", "-C", str(tree), "checkout", "--detach", ident], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        # Ignored files are compilation cache.  Carry only ignored paths from
        # the prior checkout; the new Git checkout removes every stale tracked
        # and non-ignored file by construction.
        if target.is_dir():
            ignored = subprocess.run(["git", "-C", str(target), "ls-files", "--others", "-i", "--exclude-standard", "-z"], stdout=subprocess.PIPE, check=True).stdout.split(b"\0")
            for raw in ignored:
                if raw:
                    src, dst = target / raw.decode("utf-8", "surrogateescape"), tree / raw.decode("utf-8", "surrogateescape")
                    if src.is_file() and not src.is_symlink() and not dst.exists(): dst.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(src, dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        backup = target.with_name(target.name + ".old")
        shutil.rmtree(backup, ignore_errors=True)
        moved_previous = False
        try:
            if target.exists():
                target.replace(backup); moved_previous = True
            tree.replace(target)
        except Exception:
            if moved_previous and not target.exists() and backup.exists():
                backup.replace(target)
            raise
        shutil.rmtree(backup, ignore_errors=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return {"ok": True, "workspace": str(target), "snapshot_id": ident}


def _cleanup_expired_spin_runs(root: Path, now: float) -> None:
    """Best-effort expiry of retained source; never follows links outside root."""
    runs = root / ".nerve-spin-runs"
    if not runs.is_dir():
        return
    for session_dir in runs.iterdir():
        if not session_dir.is_dir() or session_dir.is_symlink():
            continue
        for run_dir in session_dir.iterdir():
            metadata = run_dir / "retention.json"
            try:
                expires_at = float(json.loads(metadata.read_text(encoding="utf-8"))["expires_at"])
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                continue
            if now >= expires_at and run_dir.is_dir() and not run_dir.is_symlink():
                shutil.rmtree(run_dir)
        with contextlib.suppress(OSError):
            session_dir.rmdir()


def _spin_prepare(request: Mapping[str, Any]) -> dict[str, Any]:
    """Persist only bounded Promela source below the current fenced lease."""
    root = _safe_root(str(request["root"])); root.mkdir(parents=True, exist_ok=True)
    session, run_id, token = str(request.get("session_id") or ""), str(request.get("run_id") or ""), int(request["fencing_token"])
    if not session or not session.replace("-", "").isalnum() or not run_id.startswith("spin-") or not run_id[5:].isalnum(): raise ValueError("invalid SPIN run identity")
    retention = request.get("retention_seconds", _SPIN_RETENTION_SECONDS)
    if isinstance(retention, bool) or not isinstance(retention, int) or not 60 <= retention <= _SPIN_RETENTION_SECONDS:
        raise ValueError("invalid SPIN retention")
    now = time.time(); _cleanup_expired_spin_runs(root, now)
    model = request.get("model")
    if model is not None and (not isinstance(model, str) or not model or len(model.encode()) > 128 * 1024 or "\0" in model or re.search(r"^\s*#\s*include|\bc_(?:code|expr|decl|state|track)\b", model, re.M)): raise ValueError("invalid bounded SPIN source")
    ident = hashlib.sha256(session.encode()).hexdigest()[:24]; fences = root / ".nerve-spin-fences"; fences.mkdir(parents=True, exist_ok=True); fence = fences / (ident + ".json")
    if fence.exists() and int(json.loads(fence.read_text()).get("fencing_token", -1)) > token: raise PermissionError("stale fencing token")
    temporary = fence.with_suffix(".tmp"); temporary.write_text(json.dumps({"fencing_token": token, "lease_id": str(request.get("lease_id", ""))})); os.chmod(temporary, 0o600); temporary.replace(fence)
    directory = root / ".nerve-spin-runs" / ident / run_id; directory.mkdir(parents=True, mode=0o700, exist_ok=True); source = directory / "model.pml"
    if model is not None:
        temporary = source.with_suffix(".tmp"); temporary.write_text(model, encoding="utf-8"); os.chmod(temporary, 0o600); temporary.replace(source)
    if not source.is_file(): raise FileNotFoundError("retained SPIN run is unavailable")
    metadata = directory / "retention.json"
    temporary = metadata.with_suffix(".tmp")
    temporary.write_text(json.dumps({"expires_at": now + retention}), encoding="utf-8")
    os.chmod(temporary, 0o600); temporary.replace(metadata)
    try: version = subprocess.run(["/usr/bin/spin", "-V"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=5, check=False).stdout.strip()[:256]
    except OSError: version = "unavailable"
    return {"ok": True, "workspace": str(directory), "spin_version": version, "expires_at": int(now + retention)}


def _status(request: Mapping[str, Any]) -> dict[str, Any]:
    job = _job_dir(_safe_root(str(request["root"])), str(request["job_id"])); state = _read(job, int(request["fencing_token"]))
    if state["state"] == "running" and not _alive(int(state["process_group"])):
        # A reaped child does not reveal an exit code after reconnect.  It is
        # nevertheless quiescent; control plane classifies an unknown code as
        # failed instead of fabricating success.
        state = _cas_state(job, int(request["fencing_token"]), "running", {
            "state": "failed",
            "exit_code": state.get("exit_code"),
            "summary": "remote command disappeared without recorded exit code",
            "finished_at": time.time(),
        })
    return {"ok": True, **state}


def _cancel(request: Mapping[str, Any]) -> dict[str, Any]:
    job = _job_dir(_safe_root(str(request["root"])), str(request["job_id"])); state = _read(job, int(request["fencing_token"]))
    pgid = int(state["process_group"])
    if _alive(pgid):
        signal_kind = signal.SIGINT if request.get("mode") == "interrupt" else signal.SIGTERM
        try: os.killpg(pgid, signal_kind)
        except (ProcessLookupError, PermissionError): pass
        deadline = time.monotonic() + max(0, int(request.get("grace_seconds", 5)))
        while _alive(pgid) and time.monotonic() < deadline: time.sleep(.05)
        if _alive(pgid):
            try: os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError): pass
        deadline = time.monotonic() + 5
        while _alive(pgid) and time.monotonic() < deadline: time.sleep(.05)
    quiescent = not _alive(pgid)
    if quiescent: state = _cas_state(job, int(request["fencing_token"]), "running", {"state": "cancelled", "finished_at": time.time()})
    return {"ok": True, "state": state["state"], "quiescent": quiescent}


def _tail(request: Mapping[str, Any]) -> dict[str, Any]:
    job = _job_dir(_safe_root(str(request["root"])), str(request["job_id"])); _read(job, int(request["fencing_token"]))
    cursor = max(0, int(request.get("cursor", 0))); entries = []
    for stream, name in (("stdout", "stdout.log"), ("stderr", "stderr.log")):
        data = (job / name).read_bytes()[cursor:cursor + 65536] if (job / name).exists() else b""
        if data: entries.append({"stream": stream, "text": data.decode(errors="replace")})
    return {"ok": True, "entries": entries, "cursor": cursor + max((len(x["text"].encode()) for x in entries), default=0)}


def _checkout(request: Mapping[str, Any]) -> Path:
    root = _safe_root(str(request["root"])); session = str(request.get("session_id") or "")
    if not session or not session.replace("-", "").isalnum(): raise ValueError("invalid session identity")
    ident = hashlib.sha256(session.encode()).hexdigest()[:24]; token = int(request["fencing_token"])
    fence = root / ".nerve-ydb-fences" / (ident + ".json")
    if not fence.exists() or int(json.loads(fence.read_text()).get("fencing_token", -1)) != token: raise PermissionError("stale or missing session fence")
    checkout = (root / ".nerve-ydb-worktrees" / ident).resolve()
    if not checkout.is_dir(): raise FileNotFoundError("session checkout is unavailable")
    return checkout


def _target(checkout: Path, value: Any) -> Path:
    if not isinstance(value, str) or "\0" in value or "\n" in value or "\r" in value: raise ValueError("invalid relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts: raise ValueError("path escapes checkout")
    resolved = checkout.joinpath(*path.parts).resolve(strict=True)
    if resolved != checkout and checkout not in resolved.parents: raise ValueError("symlink escapes checkout")
    return resolved


def _files(request: Mapping[str, Any]) -> dict[str, Any]:
    checkout = _checkout(request); action = request.get("action"); limit = min(200, max(1, int(request.get("limit", 200))))
    if action == "read":
        file, offset, count = _target(checkout, request.get("path")), int(request.get("offset", 0)), int(request.get("limit", 65536))
        if not file.is_file() or offset < 0 or not 1 <= count <= 131072: raise ValueError("invalid read request")
        size = file.stat().st_size
        if size > 16 * 1024 * 1024: raise ValueError("file is too large to inspect safely")
        data = file.read_bytes()
        if b"\0" in data: raise ValueError("binary files cannot be read")
        try: text = data[offset:offset + count].decode("utf-8")
        except UnicodeDecodeError as exc: raise ValueError("file is not UTF-8 text") from exc
        return {"ok": True, "text": text, "offset": offset, "size": size, "returned": len(text.encode()), "truncated": offset + count < size}
    base = _target(
        checkout,
        request.get("path", ".") if action == "list"
        else request.get("relative_root", "."),
    )
    if not base.is_dir(): raise ValueError("file target is not a directory")
    if action == "list":
        depth = int(request.get("depth", 1))
        if not 0 <= depth <= 20: raise ValueError("invalid list depth")
        values = []
        for item in sorted(base.rglob("*"), key=lambda x: x.as_posix()):
            rel = item.relative_to(base)
            if len(rel.parts) > depth or item.is_symlink(): continue
            resolved = item.resolve()
            if resolved != checkout and checkout not in resolved.parents: continue
            values.append({"path": rel.as_posix(), "type": "directory" if item.is_dir() else "file", "size": item.stat().st_size if item.is_file() else None})
            if len(values) >= limit: break
        return {"ok": True, "entries": values, "truncated": len(values) >= limit}
    if action == "find":
        import fnmatch
        pattern = request.get("pattern")
        if not isinstance(pattern, str) or not pattern or any(x in pattern for x in ("\0", "\n", "\r")): raise ValueError("invalid find pattern")
        values = [item.relative_to(base).as_posix() for item in sorted(base.rglob("*"), key=lambda x: x.as_posix()) if item.is_file() and not item.is_symlink() and fnmatch.fnmatch(item.relative_to(base).as_posix(), pattern)][:limit]
        return {"ok": True, "entries": values, "truncated": len(values) >= limit}
    raise ValueError("invalid file action")


def _artifact_root(request: Mapping[str, Any]) -> Path:
    """Return a configured artifact root without accepting arbitrary paths.

    The control plane supplies ``root`` from a named connection's reviewed
    configuration.  ``artifact_root`` is deliberately relative to it, so an
    RPC caller cannot turn artifact transfer into a general remote file write.
    """
    root = _safe_root(str(request["root"])).resolve()
    value = request.get("artifact_root", "artifacts")
    if not isinstance(value, str) or "\0" in value:
        raise ValueError("invalid artifact root")
    relative = PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("artifact root escapes configured root")
    result = root.joinpath(*relative.parts)
    result.mkdir(parents=True, exist_ok=True)
    resolved = result.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("artifact root escapes configured root")
    return resolved


def _artifact_target(root: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or "\n" in value or "\r" in value:
        raise ValueError("invalid artifact relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.name in {"", "."}:
        raise ValueError("artifact path escapes configured root")
    target = root.joinpath(*path.parts)
    # Existing parents must resolve beneath the configured root.  Create one
    # component at a time so a pre-existing symlink cannot redirect writes.
    parent = root
    for component in path.parts[:-1]:
        parent = parent / component
        if parent.exists():
            resolved = parent.resolve()
            if resolved != root and root not in resolved.parents:
                raise ValueError("artifact path symlink escapes configured root")
        else:
            parent.mkdir(mode=0o700)
    return target


def _advance_artifact_fence(root: Path, request: Mapping[str, Any]) -> tuple[int, str]:
    token = int(request["fencing_token"])
    lease_id = str(request.get("lease_id") or "")
    if not lease_id or not lease_id.replace("-", "").isalnum():
        raise ValueError("invalid artifact lease id")
    fence = root / ".nerve-artifact-fence.json"
    if fence.exists():
        previous = json.loads(fence.read_text())
        previous_token = int(previous.get("fencing_token", -1))
        previous_lease_id = str(previous.get("lease_id") or "")
        if previous_token > token:
            raise PermissionError("stale fencing token")
        if previous_token == token and previous_lease_id != lease_id:
            raise PermissionError("artifact fence belongs to a different lease")
    temporary = fence.with_suffix(".tmp")
    temporary.write_text(json.dumps({
        "fencing_token": token, "lease_id": lease_id, "updated_at": time.time(),
    }))
    os.chmod(temporary, 0o600)
    temporary.replace(fence)
    return token, lease_id


def _artifact_put(request: Mapping[str, Any], pack: bytes) -> dict[str, Any]:
    """Install one verified artifact through the authenticated supervisor RPC.

    This is intentionally only a control-host-to-leased-host primitive.  It
    has no SSH coordinate, command, forwarding, or shell input.  A future
    remote-to-remote data plane must use the separately designed isolated
    ephemeral sshd protocol rather than silently relaying bytes here.
    """
    root = _artifact_root(request)
    _advance_artifact_fence(root, request)
    expected_size, expected_sha = request.get("size"), request.get("sha256")
    if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size < 0:
        raise ValueError("invalid artifact size")
    if not isinstance(expected_sha, str) or len(expected_sha) != 64 or any(c not in "0123456789abcdef" for c in expected_sha):
        raise ValueError("invalid artifact SHA-256")
    if len(pack) != expected_size or hashlib.sha256(pack).hexdigest() != expected_sha:
        raise ValueError("artifact checksum or size verification failed")
    target = _artifact_target(root, request.get("path"))
    temporary = target.with_name("." + target.name + ".nerve-transfer-" + os.urandom(8).hex())
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(pack)
            stream.flush()
            os.fsync(stream.fileno())
        if temporary.stat().st_size != expected_size or _stream_sha256(temporary) != expected_sha:
            raise ValueError("artifact verification failed after staging")
        temporary.replace(target)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    return {"ok": True, "path": str(request["path"]), "size": expected_size, "sha256": expected_sha}


def _artifact_get(request: Mapping[str, Any]) -> dict[str, Any]:
    """Read one fenced artifact below a reviewed root.

    The NRS1 response is JSON-only, so this compatibility operation is capped
    at 8 MiB.  The client verifies the returned digest before atomic install.
    """
    root = _artifact_root(request)
    # Persisting/validating the fence is also required for reads: a stale
    # lease must not observe an artifact that a newer owner replaced.
    _advance_artifact_fence(root, request)
    source = _artifact_target(root, request.get("path"))
    if not source.is_file() or source.is_symlink():
        raise ValueError("artifact source is unavailable")
    size = source.stat().st_size
    if size > _MAX_ARTIFACT_GET:
        raise ValueError("artifact exceeds configured NRS1 artifact_get limit")
    digest = hashlib.sha256()
    chunks: list[bytes] = []
    with source.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk); chunks.append(chunk)
    import base64
    return {"ok": True, "size": size, "sha256": digest.hexdigest(), "data": base64.b64encode(b"".join(chunks)).decode("ascii")}


def _ydb_publish(request: Mapping[str, Any]) -> dict[str, Any]:
    """Atomically expose one regular file from a fenced YDB workspace."""
    root = _safe_root(str(request["root"]))
    artifact_root = _artifact_root(request)
    _advance_artifact_fence(artifact_root, request)
    workspace = _safe_root(str(request.get("workspace") or ""))
    if workspace != root and root not in workspace.parents:
        raise ValueError("YDB workspace escapes configured root")
    relative = request.get("output_path")
    source = _artifact_target(workspace, relative)
    if not source.is_file() or source.is_symlink():
        raise ValueError("YDB publish source is unavailable")
    target = _artifact_target(artifact_root, request.get("path"))
    temporary = target.with_name("." + target.name + ".nerve-publish-" + os.urandom(8).hex())
    try:
        shutil.copyfile(source, temporary)
        os.chmod(temporary, 0o700)
        temporary.replace(target)
    finally:
        with contextlib.suppress(FileNotFoundError): temporary.unlink()
    return {"ok": True, "artifact_root": str(request["artifact_root"]), "path": str(request["path"]), "size": target.stat().st_size, "sha256": _stream_sha256(target)}


def _transfer_dir(root: Path, ident: Any) -> Path:
    if not isinstance(ident, str) or not ident.startswith("transfer-") or not ident[9:].isalnum(): raise ValueError("invalid transfer id")
    value = root / ".nerve-transfers" / ident; value.mkdir(parents=True, mode=0o700, exist_ok=True); return value

def _transfer_save(directory: Path, state: Mapping[str, Any]) -> None:
    temporary = directory / ".state.tmp"; temporary.write_text(json.dumps(state, separators=(",", ":"))); os.chmod(temporary, 0o600); temporary.replace(directory / "state.json")

def _transfer_load(request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    directory = _transfer_dir(_safe_root(str(request["root"])), request.get("transfer_id")); state = json.loads((directory / "state.json").read_text())
    if state.get("fencing_token") != int(request["fencing_token"]): raise PermissionError("stale fencing token")
    return directory, state

def _artifact_transfer_prepare_destination(request: Mapping[str, Any]) -> dict[str, Any]:
    _advance_artifact_fence(_artifact_root(request), request)
    directory = _transfer_dir(_safe_root(str(request["root"])), request.get("transfer_id")); key = directory / "client_key"; keygen = str(request.get("ssh_keygen_path"))
    if not keygen.startswith("/") or ".." in PurePosixPath(keygen).parts: raise ValueError("invalid ssh-keygen path")
    subprocess.run([keygen, "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL); os.chmod(key, 0o600)
    _transfer_save(directory, {"role":"destination", "state":"prepared", "fencing_token":int(request["fencing_token"]), "client_key":str(key)})
    return {"ok":True, "client_public_key":key.with_suffix(".pub").read_text().strip()}

def _artifact_transfer_prepare_source(request: Mapping[str, Any]) -> dict[str, Any]:
    _advance_artifact_fence(_artifact_root(request), request)
    root = _safe_root(str(request["root"])); directory = _transfer_dir(root, request.get("transfer_id")); source = _artifact_target(_artifact_root(request), request.get("path"))
    public, sshd, supervisor = request.get("client_public_key"), str(request.get("sshd_path")), str(request.get("supervisor_path"))
    transfer_user = _validate_unix_account(request.get("transfer_user"), "transfer_user")
    if not source.is_file() or not isinstance(public, str) or not public.startswith("ssh-ed25519 ") or "\n" in public or not all(x.startswith("/") and ".." not in PurePosixPath(x).parts for x in (sshd, supervisor)): raise ValueError("invalid direct transfer setup")
    address, port = str(request.get("bind_address")), int(request.get("port")); host_key, authorized, config = directory/"host_key", directory/"authorized_keys", directory/"sshd_config"
    subprocess.run([str(request.get("ssh_keygen_path") or "/usr/bin/ssh-keygen"), "-q", "-t", "ed25519", "-N", "", "-f", str(host_key)], check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    forced = " ".join(shlex.quote(value) for value in (
        supervisor, "artifact-send", str(request["transfer_id"]), str(root),
    ))
    authorized.write_text('restrict ' + public + "\n")
    config.write_text("\n".join(["Port "+str(port), "ListenAddress "+address, "HostKey "+str(host_key), "AuthorizedKeysFile "+str(authorized), "PidFile "+str(directory/"sshd.pid"), "AuthenticationMethods publickey", "PubkeyAuthentication yes", "PasswordAuthentication no", "KbdInteractiveAuthentication no", "PermitRootLogin prohibit-password", "PermitTTY no", "AllowUsers "+transfer_user, "ForceCommand "+forced, "DisableForwarding yes", "AllowTcpForwarding no", "AllowAgentForwarding no", "X11Forwarding no", "PermitTunnel no", "GatewayPorts no", "UsePAM no", "LogLevel ERROR"]) + "\n")
    proc = subprocess.Popen([sshd, "-D", "-f", str(config), "-E", str(directory/"sshd.log")], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    process_group = os.getpgid(proc.pid)
    state={"role":"source", "state":"serving", "fencing_token":int(request["fencing_token"]), "pid":proc.pid, "process_group":process_group, "source":str(source), "size":source.stat().st_size, "sha256":_stream_sha256(source), "transfer_user":transfer_user}; _transfer_save(directory,state)
    if not _wait_for_transfer_listener(address, port, process_group):
        _kill_process_group(process_group, 0)
        raise RuntimeError("direct transfer source did not start listening")
    return {"ok":True,"address":address,"port":port,"host_public_key":host_key.with_suffix(".pub").read_text().strip(),"size":state["size"],"sha256":state["sha256"],"transfer_user":transfer_user}

def _artifact_transfer_receive(request: Mapping[str, Any]) -> dict[str, Any]:
    directory,state=_transfer_load(request)
    target=_artifact_target(_artifact_root(request), request.get("path"))
    address, port = str(request.get("source_address")), int(request.get("source_port"))
    ssh, transfer_user = str(request.get("ssh_path")), _validate_unix_account(request.get("transfer_user"), "transfer_user")
    known = directory/"known_hosts"
    known.write_text("["+address+"]:"+str(port)+" "+str(request["source_host_key"])+"\n")
    temporary=target.with_name("."+target.name+".nerve-transfer-"+os.urandom(8).hex())
    try:
        with open(temporary,"xb",buffering=0) as output:
            proc=subprocess.Popen([ssh,"-T","-o","BatchMode=yes","-o","StrictHostKeyChecking=yes","-o","UserKnownHostsFile="+str(known),"-o","GlobalKnownHostsFile=/dev/null","-o","IdentitiesOnly=yes","-o","ForwardAgent=no","-o","ClearAllForwardings=yes","-o","RequestTTY=no","-i",state["client_key"],"-p",str(port),transfer_user+"@"+address],stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.DEVNULL,start_new_session=True)
            state["state"]="receiving"; state["pid"]=proc.pid; state["process_group"]=os.getpgid(proc.pid); _transfer_save(directory,state)
            returncode=proc.wait()
        if returncode or temporary.stat().st_size != request.get("size") or _stream_sha256(temporary)!=request.get("sha256"): raise ValueError("direct artifact transfer verification failed")
        temporary.replace(target); state["state"]="succeeded"; _transfer_save(directory,state); return {"ok":True,"size":request["size"],"sha256":request["sha256"]}
    finally:
        with contextlib.suppress(FileNotFoundError): temporary.unlink()

def _artifact_transfer_cleanup(request: Mapping[str, Any]) -> dict[str, Any]:
    directory,state=_transfer_load(request)
    process_group = state.get("process_group")
    if process_group is None:
        process_group = state.get("pid")
    if isinstance(process_group, bool) or not isinstance(process_group, int):
        process_group = None
    grace = request.get("grace_seconds", 2)
    if process_group is None:
        quiescent = True
    else:
        quiescent = _kill_process_group(int(process_group), float(grace))
    if quiescent:
        for item in directory.iterdir():
            if item.name!="state.json":
                with contextlib.suppress(OSError):
                    if item.is_dir():
                        shutil.rmtree(item, ignore_errors=True)
                    else:
                        item.unlink()
        state["state"]="cleaned"; _transfer_save(directory,state)
    return {"ok":True,"quiescent":quiescent}

def _artifact_transfer_cancel(request: Mapping[str, Any]) -> dict[str, Any]: return _artifact_transfer_cleanup(request)
def _artifact_transfer_status(request: Mapping[str, Any]) -> dict[str, Any]:
    _,state=_transfer_load(request); return {"ok":True,"state":state.get("state"),"quiescent":state.get("state") in {"succeeded","cleaned","cancelled"}}

def _reconcile_host(request: Mapping[str, Any]) -> dict[str, Any]:
    """Fenced host-wide proof over every durable job and transfer lineage."""
    root = _safe_root(str(request["root"])); root.mkdir(parents=True, exist_ok=True)
    generation = int(request["recovery_generation"])
    fence = root / ".nerve-recovery-fence.json"
    if fence.exists() and int(json.loads(fence.read_text()).get("generation", -1)) > generation:
        raise PermissionError("stale recovery generation")
    lock = open(root / ".nerve-host.lock", "a+")
    try:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"ok": True, "quiescent": False}
        temporary = fence.with_suffix(".tmp")
        temporary.write_text(json.dumps({"generation": generation}))
        temporary.replace(fence)
        # A free lock alone is not proof: completed RPCs leave detached job and
        # direct-transfer process groups behind.  Reconcile their durable state
        # while holding the same lock used by start's monitor lineage.
        jobs = root / ".nerve-jobs"
        if jobs.is_dir():
            for directory in jobs.iterdir():
                state_file = directory / "state.json"
                try:
                    state = json.loads(state_file.read_text())
                    if state.get("state") == "running" and _alive(int(state["process_group"])):
                        return {"ok": True, "quiescent": False, "lineage": "job"}
                except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                    # An unreadable lineage is uncertainty, never quiescence.
                    return {"ok": True, "quiescent": False, "lineage": "job-unknown"}
        transfers = root / ".nerve-transfers"
        if transfers.is_dir():
            for directory in transfers.iterdir():
                state_file = directory / "state.json"
                try:
                    state = json.loads(state_file.read_text())
                    process_group = state.get("process_group", state.get("pid"))
                    if state.get("state") in {"serving", "receiving"} and isinstance(process_group, int) and _alive(process_group):
                        return {"ok": True, "quiescent": False, "lineage": "transfer"}
                    if state.get("state") in {"serving", "receiving"}:
                        return {"ok": True, "quiescent": False, "lineage": "transfer-unknown"}
                except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                    return {"ok": True, "quiescent": False, "lineage": "transfer-unknown"}
        return {"ok": True, "quiescent": True, "generation": generation}
    finally:
        lock.close()


def _capabilities(request: Mapping[str, Any]) -> dict[str, Any]:
    """Compatibility probe; no root, fence, or remote state is touched."""
    return {"ok": True, "operations": sorted(_FRAME_OPERATIONS)}

def _artifact_send(ident: str, root: str) -> None:
    directory=_transfer_dir(_safe_root(root),ident); state=json.loads((directory/"state.json").read_text())
    if state.get("role")!="source" or state.get("state")!="serving": raise SystemExit(1)
    with open(state["source"],"rb") as stream: shutil.copyfileobj(stream,sys.stdout.buffer)

def rpc() -> None:
    try:
        request, pack = _decode_frame(sys.stdin.buffer.read())
        operation = request.pop("operation")
        request.pop("version")
        handlers = {"start": _start, "spin_prepare": _spin_prepare, "status": _status, "cancel": _cancel, "tail": _tail, "files": _files, "artifact_get": _artifact_get, "ydb_publish": _ydb_publish, "artifact_transfer_prepare_destination": _artifact_transfer_prepare_destination, "artifact_transfer_prepare_source": _artifact_transfer_prepare_source, "artifact_transfer_receive": _artifact_transfer_receive, "artifact_transfer_status": _artifact_transfer_status, "artifact_transfer_cancel": _artifact_transfer_cancel, "artifact_transfer_cleanup": _artifact_transfer_cleanup, "reconcile_host": _reconcile_host, "capabilities": _capabilities}
        result = _sync(request, pack) if operation == "sync" else (_artifact_put(request, pack) if operation == "artifact_put" else handlers[operation](request))
    except Exception as exc:
        # Keep errors useful to the control plane without turning this fixed
        # protocol into an unbounded remote stderr channel.
        result = {"ok": False, "error": (type(exc).__name__ + ": " + str(exc))[:300]}
    # stdout is protocol-only: diagnostics and child output stay on stderr or
    # their per-job log files, never mixed with a machine-readable response.
    sys.stdout.buffer.write(_encode_frame(result))
    sys.stdout.buffer.flush()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "_monitor":
        if len(sys.argv) != 3:
            raise SystemExit("monitor mode requires exactly one request path argument")
        _monitor(sys.argv[2])
    elif len(sys.argv) == 4 and sys.argv[1] == "artifact-send":
        _artifact_send(sys.argv[2], sys.argv[3])
    else:
        rpc()
