from __future__ import annotations

import pytest

from nerve.executions import remote_supervisor
from nerve.executions.ssh import SshConnectionCatalog, SshTransportError


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
