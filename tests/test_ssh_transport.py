from __future__ import annotations

import io
import struct
from types import SimpleNamespace

import pytest

from nerve.executions import remote_supervisor
from nerve.executions.ssh import OpenSshSupervisor, SshConnectionCatalog, SshTransportError


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
        "argv": ["/bin/sleep", "10"], "cwd": "execution_dir",
    })
    request = {"root": str(tmp_path), "job_id": started["job_id"], "fencing_token": 6}
    with pytest.raises(PermissionError, match="stale"):
        remote_supervisor._status(request)
    cancelled = remote_supervisor._cancel({**request, "fencing_token": 7, "grace_seconds": 0})
    # A locally spawned process may still be a zombie until its supervisor
    # reaps it.  Reporting ambiguity as non-quiescent is the safe outcome: the
    # lifecycle quarantines the lease rather than releasing the physical host.
    assert cancelled["quiescent"] is False


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
