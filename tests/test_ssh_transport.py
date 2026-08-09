from __future__ import annotations

import io
import sys
import time
import struct
from typing import Any
from types import SimpleNamespace

import pytest

from nerve.executions import remote_supervisor
from nerve.executions.ssh import OpenSshSupervisor, SshConnectionCatalog, SshTransportError
from nerve.executions.ssh import SshExecutionBackend


def test_connection_catalog_requires_pinned_named_connection(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "192.0.2.10", "user": "nerve", "known_hosts": str(known),
        "remote_roots": ["/srv/nerve"], "allowed_cidrs": ["192.0.2.0/24"],
    }}})
    connection = catalog.resolve("worker")
    argv = connection.ssh_argv()
    assert "StrictHostKeyChecking=yes" in argv
    assert "ProxyCommand=none" in argv


def test_connection_catalog_rejects_raw_option_injection(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("x")
    with pytest.raises(SshTransportError, match="forbidden"):
        SshConnectionCatalog({"ssh_connections": {"worker": {
            "host": "x", "user": "x", "known_hosts": str(known),
            "remote_roots": ["/srv/nerve"], "ProxyCommand": "evil",
        }}})


@pytest.mark.parametrize("path", ["relative/supervisor", "/opt/../supervisor", "/opt/./supervisor", "/opt/supervisor/"])
def test_connection_catalog_requires_a_fixed_normalized_supervisor_path(tmp_path, path):
    known = tmp_path / "known_hosts"; known.write_text("x")
    with pytest.raises(SshTransportError, match="supervisor_path"):
        SshConnectionCatalog({"ssh_connections": {"worker": {
            "host": "x", "user": "x", "known_hosts": str(known),
            "remote_roots": ["/srv/nerve"], "supervisor_path": path,
        }}})


def test_framed_protocol_round_trips_binary_pack_and_rejects_bad_frames():
    pack = b"PACK\x00\xff\nnot-json"
    raw = OpenSshSupervisor._frame({"version": 1, "operation": "sync"}, pack)
    request, received = remote_supervisor._decode_frame(raw)
    assert request == {"version": 1, "operation": "sync"}
    assert received == pack
    malformed = [b"", b"oops" + raw[4:], raw[:10], raw + b"trailing"]
    oversized_header = b"NRS1" + struct.pack(">I", remote_supervisor._MAX_HEADER + 1) + b"{}" + struct.pack(">Q", 0)
    invalid_json = b"NRS1" + struct.pack(">I", 1) + b"[" + struct.pack(">Q", 0)
    malformed.extend([oversized_header, invalid_json])
    for frame in malformed:
        with pytest.raises(ValueError):
            remote_supervisor._decode_frame(frame)


def test_remote_rpc_stdout_is_a_single_framed_response(monkeypatch):
    output = io.BytesIO()
    request = remote_supervisor._encode_frame({"version": 1, "operation": "status", "root": "/bad", "job_id": "bad", "fencing_token": 1})
    monkeypatch.setattr(remote_supervisor.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(request)))
    monkeypatch.setattr(remote_supervisor.sys, "stdout", SimpleNamespace(buffer=output))
    remote_supervisor.rpc()
    response = OpenSshSupervisor._response(output.getvalue())
    assert response["ok"] is False
    assert response["version"] == 1


def test_catalog_does_not_accept_unknown_connection():
    catalog = SshConnectionCatalog({"ssh_connections": {}})
    with pytest.raises(SshTransportError, match="unknown trusted"):
        catalog.resolve("model-supplied-host")


def test_remote_supervisor_rejects_stale_fencing_token(tmp_path):
    started = remote_supervisor._start({
        "root": str(tmp_path), "execution_id": "exec-a", "fencing_token": 7,
        "argv": [sys.executable, "-c", "import time; time.sleep(10)"], "cwd": "execution_dir", "environment": {},
    })
    request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 6}
    with pytest.raises(PermissionError, match="stale"):
        remote_supervisor._status(request)
    cancelled = remote_supervisor._cancel({**request, "fencing_token": 7, "grace_seconds": 0})
    assert cancelled["quiescent"] is True
    assert cancelled["state"] == "cancelled"


def test_remote_supervisor_files_are_fenced_relative_and_text_only(tmp_path):
    session = "session-a"; ident = remote_supervisor.hashlib.sha256(session.encode()).hexdigest()[:24]
    checkout = tmp_path / ".nerve-ydb-worktrees" / ident; checkout.mkdir(parents=True)
    (tmp_path / ".nerve-ydb-fences").mkdir()
    (tmp_path / ".nerve-ydb-fences" / (ident + ".json")).write_text('{"fencing_token":7}')
    (checkout / "a.txt").write_text("hello")
    request = {"root": str(tmp_path), "session_id": session, "fencing_token": 7}
    assert remote_supervisor._files({**request, "action": "list", "path": "."})["entries"][0]["path"] == "a.txt"
    assert remote_supervisor._files({**request, "action": "read", "path": "a.txt"})["text"] == "hello"
    assert remote_supervisor._files({
        **request, "action": "find", "relative_root": ".", "pattern": "*.txt",
    })["entries"] == ["a.txt"]
    (checkout / "binary").write_bytes(b"bad\0data")
    with pytest.raises(ValueError, match="binary"):
        remote_supervisor._files({**request, "action": "read", "path": "binary"})
    outside = tmp_path / "outside.txt"
    outside.write_text("secret")
    (checkout / "link").symlink_to(outside)
    with pytest.raises(ValueError, match="symlink escapes"):
        remote_supervisor._files({**request, "action": "read", "path": "link"})
    with pytest.raises(ValueError, match="escapes"):
        remote_supervisor._files({**request, "action": "read", "path": "../secret"})


def test_remote_supervisor_monitor_records_success_and_failure_exit_codes(tmp_path):
    for argv, expected_state, expected_exit_code in (
        ([sys.executable, "-c", "raise SystemExit(0)"], "succeeded", 0),
        ([sys.executable, "-c", "raise SystemExit(1)"], "failed", 1),
    ):
        started = remote_supervisor._start({
            "root": str(tmp_path), "execution_id": "exec-a", "fencing_token": 7,
            "argv": argv, "cwd": "execution_dir", "environment": {},
        })
        request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 7}
        for _ in range(1000):
            status = remote_supervisor._status(request)
            if status["state"] in {"succeeded", "failed", "cancelled", "finished"}:
                break
            time.sleep(.01)
        else:
            raise AssertionError("remote monitor did not persist a terminal state")
        assert status["state"] == expected_state
        assert status["exit_code"] == expected_exit_code


def test_remote_supervisor_preserves_remote_account_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("USER", "builder")
    monkeypatch.setenv("LOGNAME", "builder")
    started = remote_supervisor._start({
        "root": str(tmp_path), "execution_id": "exec-env", "fencing_token": 7,
        "argv": [sys.executable, "-c", "import os; print(os.environ['USER'], os.environ['LOGNAME'])"],
        "cwd": "execution_dir", "environment": {},
    })
    request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 7}
    for _ in range(1000):
        status = remote_supervisor._status(request)
        if status["state"] != "running":
            break
        time.sleep(.01)
    assert status["state"] == "succeeded"
    tail = remote_supervisor._tail({**request, "cursor": 0})
    assert tail["entries"] == [{"stream": "stdout", "text": "builder builder\n"}]


def test_remote_supervisor_status_preserves_monitor_terminal_state(tmp_path):
    started = remote_supervisor._start({
        "root": str(tmp_path), "execution_id": "exec-race", "fencing_token": 7,
        "argv": [sys.executable, "-c", "raise SystemExit(0)"], "cwd": "execution_dir", "environment": {},
    })
    request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 7}
    terminal = None
    for _ in range(1000):
        status = remote_supervisor._status(request)
        if status["state"] in {"succeeded", "failed", "cancelled"}:
            terminal = status
            break
        time.sleep(.01)
    assert terminal is not None
    assert remote_supervisor._status(request)["state"] == terminal["state"]


def test_remote_supervisor_cancel_drives_quiescence(tmp_path):
    started = remote_supervisor._start({
        "root": str(tmp_path), "execution_id": "exec-cancel", "fencing_token": 7,
        "argv": [sys.executable, "-c", "import time; time.sleep(5)"], "cwd": "execution_dir", "environment": {},
    })
    request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 7}
    cancelled = remote_supervisor._cancel({**request, "grace_seconds": 0, "mode": "terminate"})
    assert cancelled["quiescent"] is True
    assert cancelled["state"] == "cancelled"


@pytest.mark.asyncio
async def test_ssh_backend_treats_legacy_finished_state_as_terminal(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "127.0.0.1", "user": "root", "known_hosts": str(known), "remote_roots": [str(tmp_path)],
    }}})

    class Supervisor:
        async def start(self, connection, request): return {"ok": True, "job_id": "job-legacy", "process_group": 1, "fencing_token": int(request["fencing_token"])}
        async def status(self, connection, job_id, fencing_token, root): return {"state": "finished", "exit_code": None, "summary": "legacy finished", "fencing_token": fencing_token}
        async def tail(self, connection, job_id, fencing_token, cursor, root): return {"entries": [], "cursor": cursor}
        async def cancel(self, connection, job_id, fencing_token, grace_seconds, mode, root): return {"state": "finished", "quiescent": True}

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {"hosts": {"1": {"connection_ref": "worker"}}})(),
        connections=catalog, supervisor=Supervisor(),
    )
    plan = {
        "steps": [{"transport": "resource", "resource_slot": "slot", "executable": "/bin/true", "argv": []}],
        "selected_leases": [{"slot": "slot", "host_id": "1", "fencing_token": 7, "id": "lease-a"}],
        "arguments": {},
        "remote_root": str(tmp_path),
    }
    async def started(payload: dict[str, Any]) -> None:
        assert payload["job_id"] == "job-legacy"

    async def emit(_stream: str, _text: str) -> None:
        return None

    result = await backend.run(
        execution_id="exec-legacy",
        plan=plan,
        workspace=tmp_path,
        execution_dir=tmp_path,
        emit=emit,
        started=started,
    )
    assert result.exit_code is None
    assert result.summary == "legacy finished"


@pytest.mark.asyncio
async def test_ssh_backend_drains_logs_after_observing_terminal_state(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "127.0.0.1", "user": "root", "known_hosts": str(known), "remote_roots": [str(tmp_path)],
    }}})

    class Supervisor:
        tails = 0
        async def start(self, connection, request): return {"job_id": "job-fast", "process_group": 1, "fencing_token": 7}
        async def status(self, connection, job_id, fencing_token, root): return {"state": "failed", "exit_code": 3, "summary": "failed"}
        async def tail(self, connection, job_id, fencing_token, cursor, root):
            self.tails += 1
            return {"entries": [] if self.tails == 1 else [{"stream": "stderr", "text": "final error\n"}], "cursor": cursor}

    supervisor = Supervisor()
    backend = SshExecutionBackend(
        inventory=type("inventory", (), {"hosts": {"1": {"connection_ref": "worker"}}})(),
        connections=catalog, supervisor=supervisor,
    )
    plan = {
        "steps": [{"transport": "resource", "resource_slot": "slot", "executable": "/bin/false", "argv": []}],
        "selected_leases": [{"slot": "slot", "host_id": "1", "fencing_token": 7, "id": "lease-a"}],
        "arguments": {}, "remote_root": str(tmp_path),
    }
    emitted = []
    async def emit(stream, value):
        emitted.append((stream, value))

    async def started(_payload):
        return None

    result = await backend.run(
        execution_id="exec-fast", plan=plan, workspace=tmp_path, execution_dir=tmp_path,
        emit=emit, started=started,
    )
    assert result.exit_code == 3
    assert emitted == [("stderr", "final error\n")]


@pytest.mark.asyncio
async def test_ssh_backend_recovers_legacy_finished_state_as_failed(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "127.0.0.1", "user": "root", "known_hosts": str(known), "remote_roots": [str(tmp_path)],
    }}})

    class Supervisor:
        async def start(self, connection, request): raise RuntimeError("should not start during recovery")
        async def status(self, connection, job_id, fencing_token, root): return {"state": "finished", "summary": "legacy finished", "fencing_token": fencing_token}
        async def tail(self, connection, job_id, fencing_token, cursor, root): return {}
        async def cancel(self, connection, job_id, fencing_token, grace_seconds, mode, root): return {}

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {"hosts": {"1": {"connection_ref": "worker"}}})(),
        connections=catalog, supervisor=Supervisor(),
    )
    execution = {
        "id": "exec-legacy-recover",
        "plan": {
            "steps": [{"transport": "resource", "resource_slot": "slot", "executable": "/bin/true", "argv": []}],
            "selected_leases": [{"slot": "slot", "host_id": "1", "fencing_token": 7, "id": "lease-a"}],
            "arguments": {},
            "remote_root": str(tmp_path),
        },
        "backend_handle": {"job_id": "job-legacy", "fencing_token": 7},
    }
    recovered = await backend.recover(execution)
    assert recovered.state == "finished"
    assert recovered.result is not None
    assert recovered.result.exit_code is None
