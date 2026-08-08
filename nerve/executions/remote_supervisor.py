"""Small remote half of the SSH execution protocol.

Install the same Nerve package on a worker and expose it only through the
fixed ``nerve remote-supervisor rpc`` SSH forced command.  State is durable
under the caller-approved root; every operation checks its fencing token.
"""
from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time
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
    cwd = root if request.get("cwd") == "workspace" else job
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


def rpc() -> None:
    try:
        request = json.loads(sys.stdin.buffer.readline()); operation = request.pop("operation")
        result = {"start": _start, "status": _status, "cancel": _cancel, "tail": _tail}[operation](request)
    except Exception as exc:
        result = {"ok": False, "error": type(exc).__name__}
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
