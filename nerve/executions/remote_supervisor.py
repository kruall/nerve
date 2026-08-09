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
import signal
import subprocess
import sys
import time
import shutil
import struct
import contextlib
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


_MAGIC = b"NRS1"
_VERSION = 1
_MAX_HEADER = 64 * 1024
_MAX_PACK = 512 * 1024 * 1024
_TERMINAL_STATE_BY_UNKNOWN_EXIT_CODE = "failed"


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
    if operation not in {"start", "sync", "status", "cancel", "tail", "files"}:
        raise ValueError("invalid frame operation")
    if pack_size and operation != "sync":
        raise ValueError("binary pack is only permitted for sync")
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
        proc = subprocess.Popen(argv, cwd=str(cwd), env=dict(environment), stdin=subprocess.DEVNULL, stdout=out, stderr=err)
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
        # A worker is provisioned with this cache out of band.  Deliberately do
        # not fetch or accept a full bundle when the requested base is absent.
        base = subprocess.run(["git", "--git-dir", str(cache), "cat-file", "-e", head + "^{commit}"], stderr=subprocess.DEVNULL)
        if base.returncode:
            raise ValueError("remote YDB object cache lacks requested base HEAD; provision the cache before retrying")
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


def rpc() -> None:
    try:
        request, pack = _decode_frame(sys.stdin.buffer.read())
        operation = request.pop("operation")
        request.pop("version")
        handlers = {"start": _start, "status": _status, "cancel": _cancel, "tail": _tail, "files": _files}
        result = _sync(request, pack) if operation == "sync" else handlers[operation](request)
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
    else:
        rpc()
