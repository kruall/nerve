from __future__ import annotations

import io
import sys
import time
import struct
import hashlib
import base64
import os
from typing import Any
from types import SimpleNamespace

import pytest

from nerve.executions import remote_supervisor
from nerve.executions.ssh import OpenSshSupervisor, SshConnectionCatalog, SshTransportError
from nerve.executions.ssh import SshExecutionBackend


def _artifact_backend(tmp_path, supervisor):
    local_root = tmp_path / "local-artifacts"
    local_root.mkdir()
    remote_root = tmp_path / "remote"
    remote_root.mkdir()
    known = tmp_path / "known_hosts"
    known.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "127.0.0.1", "user": "worker", "known_hosts": str(known),
        "remote_roots": [str(remote_root)], "artifact_roots": ["artifacts"],
    }}})
    inventory = type("Inventory", (), {
        "hosts": {"worker-1": {"connection_ref": "worker"}},
        "local_artifact_roots": {"control": local_root},
        "members": lambda _self, pool: ["worker-1"] if pool == "workers" else [],
    })()
    return SshExecutionBackend(
        inventory=inventory, connections=catalog, supervisor=supervisor,
    ), local_root


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


def test_connection_catalog_rejects_unconfined_artifact_root(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("x")
    with pytest.raises(SshTransportError, match="artifact_roots"):
        SshConnectionCatalog({"ssh_connections": {"worker": {
            "host": "x", "user": "x", "known_hosts": str(known),
            "remote_roots": ["/srv/nerve"], "artifact_roots": ["../escape"],
        }}})


@pytest.mark.parametrize("path", ["relative/supervisor", "/opt/../supervisor", "/opt/./supervisor", "/opt/supervisor/"])
def test_connection_catalog_requires_a_fixed_normalized_supervisor_path(tmp_path, path):
    known = tmp_path / "known_hosts"; known.write_text("x")
    with pytest.raises(SshTransportError, match="supervisor_path"):
        SshConnectionCatalog({"ssh_connections": {"worker": {
            "host": "x", "user": "x", "known_hosts": str(known),
            "remote_roots": ["/srv/nerve"], "supervisor_path": path,
        }}})


def test_connection_catalog_defaults_transfer_user_to_connection_user(tmp_path):
    known = tmp_path / "known_hosts"; known.write_text("x")
    catalog = SshConnectionCatalog({"ssh_connections": {"worker": {
        "host": "x", "user": "alice", "known_hosts": str(known), "remote_roots": ["/srv/nerve"],
    }}})
    assert catalog.resolve("worker").transfer_user == "alice"


@pytest.mark.parametrize("value", ["bad user", "-alpha", ""])
def test_connection_catalog_validates_transfer_user(tmp_path, value):
    known = tmp_path / "known_hosts"; known.write_text("x")
    with pytest.raises(SshTransportError, match="transfer_user"):
        SshConnectionCatalog({"ssh_connections": {"worker": {
            "host": "x", "user": "alice", "known_hosts": str(known),
            "remote_roots": ["/srv/nerve"], "transfer_user": value,
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


def test_remote_supervisor_artifact_put_is_fenced_confined_and_atomic(tmp_path):
    payload = b"artifact\x00bytes"
    request = {
        "root": str(tmp_path), "artifact_root": "approved-artifacts",
        "lease_id": "lease-a", "fencing_token": 7, "path": "nested/result.bin",
        "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
    }
    result = remote_supervisor._artifact_put(request, payload)
    target = tmp_path / "approved-artifacts" / "nested" / "result.bin"
    assert result["sha256"] == request["sha256"] and target.read_bytes() == payload
    assert not list(target.parent.glob(".result.bin.nerve-transfer-*"))
    with pytest.raises(ValueError, match="escapes"):
        remote_supervisor._artifact_put({**request, "path": "../outside"}, payload)
    with pytest.raises(ValueError, match="checksum"):
        remote_supervisor._artifact_put({**request, "sha256": "0" * 64}, payload)
    with pytest.raises(PermissionError, match="stale"):
        remote_supervisor._artifact_put({**request, "fencing_token": 6}, payload)


def test_remote_supervisor_ydb_publish_is_fenced_and_confined(tmp_path):
    workspace = tmp_path / "sessions" / "owner" / "snapshot"
    source = workspace / "ydb/tools/ydb_bench/ydb_bench"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"benchmark")
    request = {
        "root": str(tmp_path), "workspace": str(workspace),
        "artifact_root": "artifacts", "lease_id": "lease-a", "fencing_token": 7,
        "output_path": "ydb/tools/ydb_bench/ydb_bench", "path": "ydb/hash/ydb_bench",
    }
    result = remote_supervisor._ydb_publish(request)
    target = tmp_path / "artifacts" / "ydb/hash/ydb_bench"
    assert result["path"] == "ydb/hash/ydb_bench" and target.read_bytes() == b"benchmark"
    with pytest.raises(ValueError, match="source is unavailable"):
        remote_supervisor._ydb_publish({**request, "output_path": "missing"})
    with pytest.raises(ValueError, match="escapes"):
        remote_supervisor._ydb_publish({**request, "output_path": "../secret"})


def test_artifact_frame_allows_binary_only_for_the_fixed_put_operation():
    payload = b"x"
    request, received = remote_supervisor._decode_frame(
        OpenSshSupervisor._frame({"version": 1, "operation": "artifact_put"}, payload)
    )
    assert request["operation"] == "artifact_put" and received == payload


def test_remote_supervisor_frame_allows_ydb_publish_without_binary_payload():
    request, received = remote_supervisor._decode_frame(
        OpenSshSupervisor._frame({"version": 1, "operation": "ydb_publish"})
    )
    assert request["operation"] == "ydb_publish" and received == b""


def test_artifact_endpoint_validation_rejects_unknown_root_before_lease(tmp_path):
    backend, _local_root = _artifact_backend(tmp_path, SimpleNamespace())

    backend.validate_artifact_endpoint("workers", "artifacts")
    with pytest.raises(SshTransportError, match="not configured for every host"):
        backend.validate_artifact_endpoint("workers", "missing")


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


def test_remote_supervisor_artifact_transfer_receive_uses_transfer_user_in_ssh_argv(tmp_path, monkeypatch):
    transfer_id = "transfer-abc"
    directory = remote_supervisor._transfer_dir(tmp_path, transfer_id)
    payload = b"artifact-data"
    client_key = directory / "client_key"; client_key.write_text("secret")
    remote_supervisor._transfer_save(directory, {
        "role": "source", "state": "serving", "fencing_token": 7,
        "process_group": 1234, "client_key": str(client_key),
    })
    captured = {}

    class Process:
        pid = os.getpid()

        def wait(self):
            return 0

    def fake_popen(*args, **kwargs):
        captured["argv"] = args[0]
        kwargs["stdout"].write(payload)
        return Process()

    monkeypatch.setattr(remote_supervisor.subprocess, "Popen", fake_popen)
    request = {
        "root": str(tmp_path), "artifact_root": "artifacts", "path": "result.bin",
        "transfer_id": transfer_id, "fencing_token": 7, "source_address": "192.0.2.55", "source_port": 32456,
        "source_host_key": "ssh-ed25519 KEY", "ssh_path": "/usr/bin/ssh", "transfer_user": "builder",
        "size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
    }
    response = remote_supervisor._artifact_transfer_receive(request)
    assert response["ok"] is True
    assert "builder@192.0.2.55" in captured["argv"]
    assert all("nerve-transfer@" not in str(item) for item in map(str, captured["argv"]))


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
async def test_ssh_backend_rejects_unconfigured_artifact_roots_before_transfer_rpc(tmp_path):
    known_source = tmp_path / "source_known_hosts"; known_source.write_text("worker ssh-ed25519 AAAA\n")
    known_destination = tmp_path / "destination_known_hosts"; known_destination.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {
        "source": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_source),
            "remote_roots": [str(tmp_path / "source")], "artifact_roots": ["source-artifacts"],
        },
        "destination": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_destination),
            "remote_roots": [str(tmp_path / "destination")], "artifact_roots": ["destination-artifacts"],
        },
    }})

    async def never(*_args, **_kwargs):
        raise AssertionError("artifact transfer RPC should not be called")

    supervisor = type("Supervisor", (), {
        "artifact_transfer_prepare_destination": never,
        "artifact_transfer_prepare_source": never,
        "artifact_transfer_receive": never,
        "artifact_transfer_status": never,
        "artifact_transfer_cancel": never,
        "artifact_transfer_cleanup": never,
    })()

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {
            "hosts": {
                "1": {"connection_ref": "source"},
                "2": {"connection_ref": "destination"},
            },
        })(),
        connections=catalog,
        supervisor=supervisor,
    )
    plan = {
        "kind": "artifact_transfer",
        "selected_leases": [
            {"slot": "source", "host_id": "1", "fencing_token": 7, "id": "lease-source"},
            {"slot": "destination", "host_id": "2", "fencing_token": 7, "id": "lease-destination"},
        ],
        "artifact_transfer": {
            "transfer_id": "transfer-invalid",
            "source_root": "invalid-root",
            "destination_root": "destination-artifacts",
            "source_path": "payload.bin",
            "destination_path": "payload.bin",
        },
    }

    async def emit(_stream: str, _text: str) -> None:
        return None

    async def started(_payload: dict[str, Any]) -> None:
        return None

    with pytest.raises(SshTransportError, match="source artifact root"):
        await backend.run(
            execution_id="exec-transfer-invalid",
            plan=plan,
            workspace=tmp_path,
            execution_dir=tmp_path,
            emit=emit,
            started=started,
        )


@pytest.mark.asyncio
async def test_ssh_backend_artifact_transfer_orchestrates_two_slots_and_addresses_and_cleanup(tmp_path):
    known_source = tmp_path / "source_known_hosts"; known_source.write_text("worker ssh-ed25519 AAAA\n")
    known_destination = tmp_path / "destination_known_hosts"; known_destination.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {
        "source": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_source),
            "remote_roots": [str(tmp_path / "source")], "artifact_roots": ["source-artifacts"],
            "transfer_user": "source-transfer",
        },
        "destination": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_destination),
            "remote_roots": [str(tmp_path / "destination")], "artifact_roots": ["destination-artifacts"],
        },
    }})
    calls: list[tuple[str, Any]] = []

    class Supervisor:
        async def artifact_transfer_prepare_destination(self, connection, request):
            calls.append(("prepare_destination", connection.name, request["artifact_root"]))
            return {"client_public_key": "CLIENT_KEY"}

        async def artifact_transfer_prepare_source(self, connection, request):
            calls.append(("prepare_source", connection.name, request["transfer_user"], request["bind_address"]))
            return {
                "address": "10.10.10.10",
                "port": 40123,
                "host_public_key": "HOST_KEY",
                "size": 3,
                "sha256": hashlib.sha256(b"abc").hexdigest(),
                "transfer_user": request["transfer_user"],
            }

        async def artifact_transfer_receive(self, connection, request):
            calls.append(("receive", connection.name, request["source_address"], request["transfer_user"]))
            return {"ok": True}

        async def artifact_transfer_cleanup(self, connection, request):
            calls.append(("cleanup", connection.name, request["transfer_id"]))
            return {"ok": True, "quiescent": True}

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {
            "hosts": {
                "1": {"connection_ref": "source"},
                "2": {"connection_ref": "destination"},
            },
        })(),
        connections=catalog,
        supervisor=Supervisor(),
    )
    plan = {
        "kind": "artifact_transfer",
        "selected_leases": [
            {"slot": "source", "host_id": "1", "fencing_token": 11, "id": "lease-source"},
            {"slot": "destination", "host_id": "2", "fencing_token": 13, "id": "lease-destination"},
        ],
        "artifact_transfer": {
            "transfer_id": "transfer-dual",
            "source_root": "source-artifacts",
            "source_path": "input.bin",
            "destination_root": "destination-artifacts",
            "destination_path": "output.bin",
        },
    }
    started_calls = []

    async def emit(_stream: str, _text: str) -> None:
        return None

    async def started(payload: dict[str, Any]) -> None:
        started_calls.append(payload)

    result = await backend.run(
        execution_id="exec-transfer-dual", plan=plan, workspace=tmp_path,
        execution_dir=tmp_path, emit=emit, started=started,
    )
    assert result.exit_code == 0
    assert started_calls == [{"transfer_id": "transfer-dual", "source_fencing_token": 11, "destination_fencing_token": 13, "reattachable": False}]
    assert calls[0][0] == "prepare_destination"
    assert calls[1][0] == "prepare_source"
    assert calls[2][0] == "receive"
    assert calls[2][2] == "10.10.10.10"
    assert calls[2][3] == "source-transfer"
    assert [entry[0] for entry in calls if entry[0] == "cleanup"] == ["cleanup", "cleanup"]


@pytest.mark.asyncio
async def test_ssh_backend_cancel_artifact_transfer_returns_false_when_not_quiescent(tmp_path):
    known_source = tmp_path / "source_known_hosts"; known_source.write_text("worker ssh-ed25519 AAAA\n")
    known_destination = tmp_path / "destination_known_hosts"; known_destination.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {
        "source": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_source),
            "remote_roots": [str(tmp_path / "source")], "artifact_roots": ["source-artifacts"],
        },
        "destination": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_destination),
            "remote_roots": [str(tmp_path / "destination")], "artifact_roots": ["destination-artifacts"],
        },
    }})

    async def artifact_transfer_cancel(*_args, **_kwargs):
        return {"ok": True, "quiescent": False}

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {
            "hosts": {
                "1": {"connection_ref": "source"},
                "2": {"connection_ref": "destination"},
            },
        })(),
        connections=catalog,
        supervisor=type("Supervisor", (), {"artifact_transfer_cancel": artifact_transfer_cancel})(),
    )
    backend._transfers["exec-transfer-cancel"] = (
        catalog.resolve("source"), {"fencing_token": 11},
        catalog.resolve("destination"), {"fencing_token": 13},
        "transfer-dual",
    )

    assert await backend.cancel(execution_id="exec-transfer-cancel", grace_seconds=0, mode="terminate") is False


@pytest.mark.asyncio
async def test_ssh_backend_recover_incomplete_artifact_transfer_is_orphaned(tmp_path):
    known_source = tmp_path / "source_known_hosts"; known_source.write_text("worker ssh-ed25519 AAAA\n")
    known_destination = tmp_path / "destination_known_hosts"; known_destination.write_text("worker ssh-ed25519 AAAA\n")
    catalog = SshConnectionCatalog({"ssh_connections": {
        "source": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_source),
            "remote_roots": [str(tmp_path / "source")], "artifact_roots": ["source-artifacts"],
        },
        "destination": {
            "host": "127.0.0.1", "user": "root", "known_hosts": str(known_destination),
            "remote_roots": [str(tmp_path / "destination")], "artifact_roots": ["destination-artifacts"],
        },
    }})

    async def artifact_transfer_status(_self, connection, request):
        if connection.name == "source":
            return {"state": "serving"}
        return {"state": "prepared"}

    backend = SshExecutionBackend(
        inventory=type("inventory", (), {
            "hosts": {
                "1": {"connection_ref": "source"},
                "2": {"connection_ref": "destination"},
            },
        })(),
        connections=catalog,
        supervisor=type("Supervisor", (), {"artifact_transfer_status": artifact_transfer_status})(),
    )
    recovered = await backend.recover({
        "kind": "artifact_transfer",
        "id": "exec-transfer-recover",
        "plan": {
            "kind": "artifact_transfer",
            "artifact_transfer": {
                "transfer_id": "transfer-dual",
                "source_root": "source-artifacts",
                "source_path": "input.bin",
                "destination_root": "destination-artifacts",
                "destination_path": "output.bin",
            },
        },
        "selected_leases": [
            {"slot": "source", "host_id": "1", "fencing_token": 11, "id": "lease-source"},
            {"slot": "destination", "host_id": "2", "fencing_token": 13, "id": "lease-destination"},
        ],
    })
    assert recovered.state == "orphaned"


@pytest.mark.asyncio
async def test_artifact_transfer_local_to_remote_uses_one_leased_destination(tmp_path):
    calls = []

    class Supervisor:
        async def artifact_put(self, connection, request):
            calls.append((connection.name, request))
            return {"ok": True}

    backend, local_root = _artifact_backend(tmp_path, Supervisor())
    payload = b"local payload"
    (local_root / "input.bin").write_bytes(payload)
    started = []
    result = await backend.run(
        execution_id="exec-local-upload",
        plan={
            "kind": "artifact_transfer",
            "selected_leases": [{
                "slot": "destination", "host_id": "worker-1",
                "fencing_token": 3, "id": "lease-destination",
            }],
            "artifact_transfer": {
                "source_local": True, "destination_local": False,
                "source_root": "control", "source_path": "input.bin",
                "destination_root": "artifacts", "destination_path": "output.bin",
            },
        },
        workspace=tmp_path, execution_dir=tmp_path,
        emit=lambda *_args: __import__("asyncio").sleep(0),
        started=lambda value: (__import__("asyncio").sleep(0, result=started.append(value))),
    )
    assert result.exit_code == 0
    assert started == [{"destination_fencing_token": 3, "reattachable": False}]
    assert calls[0][1]["source_path"] == str(local_root / "input.bin")
    assert calls[0][1]["sha256"] == hashlib.sha256(payload).hexdigest()


@pytest.mark.asyncio
async def test_artifact_transfer_remote_to_local_verifies_and_atomically_installs(tmp_path):
    payload = b"remote payload"

    class Supervisor:
        async def artifact_get(self, _connection, request):
            assert request["path"] == "input.bin"
            return {
                "ok": True, "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "data": base64.b64encode(payload).decode("ascii"),
            }

    backend, local_root = _artifact_backend(tmp_path, Supervisor())
    result = await backend.run(
        execution_id="exec-local-download",
        plan={
            "kind": "artifact_transfer",
            "selected_leases": [{
                "slot": "source", "host_id": "worker-1",
                "fencing_token": 4, "id": "lease-source",
            }],
            "artifact_transfer": {
                "source_local": False, "destination_local": True,
                "source_root": "artifacts", "source_path": "input.bin",
                "destination_root": "control", "destination_path": "nested/output.bin",
            },
        },
        workspace=tmp_path, execution_dir=tmp_path,
        emit=lambda *_args: __import__("asyncio").sleep(0),
        started=lambda _value: __import__("asyncio").sleep(0),
    )
    assert result.exit_code == 0
    assert (local_root / "nested/output.bin").read_bytes() == payload
    assert not list((local_root / "nested").glob(".output.bin.nerve-transfer-*"))


def test_local_artifact_rejects_symlink_escape(tmp_path):
    backend, local_root = _artifact_backend(tmp_path, object())
    outside = tmp_path / "outside"
    outside.mkdir()
    (local_root / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(SshTransportError, match="symlink escapes"):
        backend._local_artifact("control", "link/output.bin", output=True)


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
