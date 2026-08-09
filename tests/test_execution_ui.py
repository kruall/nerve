"""Public contracts for detached execution and resource administration UI."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from nerve.executions.public import (
    MAX_LOG_LINE_CHARS,
    MAX_LOG_TAIL_CHARS,
    public_execution,
    public_log_tail,
    public_resource_snapshot,
)


class FakeExecutionService:
    def __init__(self):
        self.execution = {
            "id": "exec-1",
            "session_id": "session-1",
            "kind": "ydb.build",
            "status": "running",
            "revision": 3,
            "requested_pool": "builders",
            "selected_host": {
                "id": "host-a", "display_name": "Builder A",
                "hostname": "secret.internal",
            },
            "backend_handle": "secret-process-handle",
        }
        self.calls: list[tuple[str, dict]] = []

    async def session_activity(self, *, session_ids):
        return {
            session_id: {
                "active_execution_count": 1,
                "execution_statuses": ["running"],
            }
            for session_id in session_ids
        }

    async def list_executions(self, **kwargs):
        self.calls.append(("list", kwargs))
        return [self.execution]

    async def get_execution(self, **kwargs):
        self.calls.append(("get", kwargs))
        return self.execution

    async def tail_logs(self, **kwargs):
        self.calls.append(("logs", kwargs))
        return {
            "entries": [
                {"sequence": index, "stream": "stdout", "text": f"line {index}"}
                for index in range(800)
            ],
            "has_more": True,
        }

    async def cancel_execution(self, **kwargs):
        self.calls.append(("cancel", kwargs))
        return {**self.execution, "status": "cancelling", "revision": 4}

    async def retry_execution(self, **kwargs):
        self.calls.append(("retry", kwargs))
        return {**self.execution, "id": "exec-2", "status": "queued"}

    async def dismiss_execution(self, **kwargs):
        self.calls.append(("dismiss", kwargs))
        return {**self.execution, "status": "succeeded", "dismissed_at": "2026-01-01T00:00:00+00:00"}

    async def resource_snapshot(self):
        return {
            "hosts": [{
                "id": "host-a", "display_name": "Builder A", "state": "leased",
                "connection_ref": "private-ssh-config",
                "hostname": "secret.internal",
            }],
            "pools": [{"id": "builders", "total_hosts": 1, "available_hosts": 0}],
            "leases": [],
            "queue": [],
        }

    async def set_host_draining(self, **kwargs):
        self.calls.append(("drain", kwargs))
        return {"id": kwargs["host_id"], "state": "draining", "draining": True}

    async def recover_host(self, **kwargs):
        self.calls.append(("recover", kwargs))
        return {"id": kwargs["host_id"], "state": "healthy", "quarantined": False}


@pytest.fixture
def ui_service(monkeypatch):
    from nerve.gateway.routes import _deps

    service = FakeExecutionService()
    engine = SimpleNamespace(execution_service=service, resource_service=service)
    monkeypatch.setattr(_deps, "_deps", _deps.RouteDeps(engine=engine, db=None))
    return service


def test_public_execution_drops_transport_details():
    public = public_execution(FakeExecutionService().execution)
    assert public["selected_host"] == {
        "id": "host-a", "display_name": "Builder A",
    }
    assert "backend_handle" not in public
    assert "hostname" not in public["selected_host"]


def test_public_execution_exposes_dismissal_and_boolean_auto_continue():
    public = public_execution({
        "id": "exec-1", "session_id": "session-1", "kind": "build", "status": "succeeded",
        "auto_continue": 0, "dismissed_at": "2026-01-01T00:00:00+00:00",
    })
    assert public["auto_continue"] is False
    assert public["dismissed_at"] == "2026-01-01T00:00:00+00:00"


def test_public_resource_snapshot_drops_connection_material():
    raw = {
        "hosts": [{
            "id": "host-a", "state": "healthy", "hostname": "private",
            "connection_ref": "ssh-private", "ssh_options": ["-J", "private"],
        }],
        "pools": [], "leases": [], "queue": [],
    }
    host = public_resource_snapshot(raw)["hosts"][0]
    assert host == {"id": "host-a", "state": "healthy"}


def test_public_log_tail_is_bounded_by_lines_and_bytes():
    raw = {"entries": [{"text": "x" * (MAX_LOG_LINE_CHARS * 2)} for _ in range(900)]}
    tail = public_log_tail(raw, requested_limit=1000)
    assert len(tail["entries"]) <= 500
    assert max(len(entry["text"]) for entry in tail["entries"]) <= MAX_LOG_LINE_CHARS
    assert sum(len(entry["text"]) for entry in tail["entries"]) <= MAX_LOG_TAIL_CHARS
    assert tail["truncated"] is True


@pytest.mark.asyncio
async def test_execution_routes_are_bounded_and_preserve_profile_retry_choice(ui_service):
    from nerve.gateway.routes.executions import (
        RetryExecutionRequest,
        get_execution_logs,
        list_session_executions,
        retry_execution,
    )

    listed = await list_session_executions("session-1", limit=999, user={})
    assert listed["executions"][0]["id"] == "exec-1"
    assert ui_service.calls[-1][1]["limit"] == 100

    tail = await get_execution_logs("exec-1", limit=9999, user={})
    assert tail["limit"] == 500
    assert len(tail["entries"]) == 500

    retried = await retry_execution(
        "exec-1", RetryExecutionRequest(profile_mode="current"), user={"sub": "operator"},
    )
    assert retried["execution"]["id"] == "exec-2"
    assert ui_service.calls[-1][1]["profile_mode"] == "current"


@pytest.mark.asyncio
async def test_dismiss_route_is_session_scoped_and_returns_the_dismissed_record(ui_service):
    from nerve.gateway.routes.executions import dismiss_execution

    dismissed = await dismiss_execution("session-1", "exec-1", user={"sub": "operator"})
    assert dismissed["execution"]["dismissed_at"] == "2026-01-01T00:00:00+00:00"
    assert ui_service.calls[-1] == ("dismiss", {
        "execution_id": "exec-1", "session_id": "session-1", "requested_by": "operator",
    })


@pytest.mark.asyncio
async def test_host_actions_require_matching_confirmation_and_quiescence(ui_service):
    from nerve.gateway.routes.executions import (
        DrainHostRequest,
        RecoverHostRequest,
        recover_host,
        set_host_draining,
    )

    with pytest.raises(HTTPException, match="host confirmation"):
        await set_host_draining(
            "host-a", DrainHostRequest(draining=True, confirm_host_id="host-b"), user={},
        )
    with pytest.raises(HTTPException, match="remote quiescence"):
        await recover_host(
            "host-a",
            RecoverHostRequest(confirm_host_id="host-a", remote_quiescence_confirmed=False),
            user={},
        )

    recovered = await recover_host(
        "host-a",
        RecoverHostRequest(confirm_host_id="host-a", remote_quiescence_confirmed=True),
        user={"sub": "operator"},
    )
    assert recovered["host"]["state"] == "healthy"
    assert ui_service.calls[-1][1]["remote_quiescence_confirmed"] is True


@pytest.mark.asyncio
async def test_session_activity_keeps_agent_and_execution_state_orthogonal(ui_service):
    from nerve.gateway.routes.executions import session_execution_activity
    from nerve.gateway.routes.sessions import _attach_execution_activity

    activity = await session_execution_activity(["session-1"])
    assert activity == {
        "session-1": {
            "active_execution_count": 1,
            "execution_statuses": ["running"],
        },
    }
    sessions = [{"id": "session-1", "is_running": False}]
    await _attach_execution_activity(sessions)
    assert sessions[0]["is_running"] is False
    assert sessions[0]["active_execution_count"] == 1
    assert sessions[0]["is_busy"] is True
