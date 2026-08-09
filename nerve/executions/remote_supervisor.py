"""Small remote half of the SSH execution protocol.

Install the same Nerve package on a worker and expose it only through the
fixed ``nerve remote-supervisor rpc`` SSH forced command.  State is durable
under the caller-approved root; every operation checks its fencing token.
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
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


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


def _alive(pgid: int) -> bool:
    try: os.killpg(pgid, 0)
    except ProcessLookupError: return False
    except PermissionError: return True
    return True


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
    out = open(job / "stdout.log", "ab", buffering=0); err = open(job / "stderr.log", "ab", buffering=0)
    # ``lock`` remains inherited by the process group leader.  Therefore a
    # second central lease cannot overlap physically even if the control plane
    # has lost the first worker.
    proc = subprocess.Popen(argv, cwd=cwd, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                            start_new_session=True, pass_fds=(lock.fileno(),))
    # The child now owns the inherited flock fd; the RPC process must release
    # its copy so worker disconnects cannot keep a dead job's lease forever.
    lock.close(); out.close(); err.close()
    state = {"job_id": job_id, "execution_id": execution_id, "fencing_token": token,
             "pid": proc.pid, "process_group": proc.pid, "state": "running", "exit_code": None,
             "started_at": time.time()}
    _write(job, state)
    return {"ok": True, **state}


def _sync(request: Mapping[str, Any]) -> dict[str, Any]:
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
    try:
        import base64
        pack = base64.b64decode(str(snapshot["pack_b64"]), validate=True)
    except (KeyError, ValueError) as exc:
        raise ValueError("invalid sync payload") from exc
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
        state.update(state="finished", exit_code=state.get("exit_code"), finished_at=time.time()); _write(job, state)
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
    if quiescent: state.update(state="cancelled", finished_at=time.time()); _write(job, state)
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
        request = json.loads(sys.stdin.buffer.readline()); operation = request.pop("operation")
        result = {"start": _start, "sync": _sync, "status": _status, "cancel": _cancel, "tail": _tail, "files": _files}[operation](request)
    except Exception as exc:
        # Keep errors useful to the control plane without turning this fixed
        # protocol into an unbounded remote stderr channel.
        result = {"ok": False, "error": (type(exc).__name__ + ": " + str(exc))[:300]}
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
