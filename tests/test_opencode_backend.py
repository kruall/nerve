"""Focused unit tests for the minimal OpenCode backend."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from nerve.agent.backends import events as ev
from nerve.agent.backends.base import SessionSpec, TurnInput
from nerve.agent.backends.opencode import OpenCodeBackend, OpenCodeClient
from nerve.config import NerveConfig


def _backend(tmp_path, *, gateway_port: int | None = 8989) -> OpenCodeBackend:
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path),
        "opencode": {"home_dir": str(tmp_path / "opencode")},
    })
    return OpenCodeBackend(SimpleNamespace(
        config=config, gateway_port=lambda: gateway_port,
        mint_session_token=lambda session_id: f"token-{session_id}",
    ))


def _spec(tmp_path) -> SessionSpec:
    return SessionSpec(
        session_id="session-1", source="web", model="openai/gpt-test",
        effort="high", system_prompt="system", cwd=str(tmp_path),
    )


def test_runtime_env_is_session_private_and_mints_mcp_token(tmp_path):
    env = _backend(tmp_path).runtime_env(_spec(tmp_path))
    assert env["NERVE_MCP_TOKEN"] == "token-session-1"
    assert env["OPENCODE_CONFIG_DIR"].endswith("opencode/session-1/config")
    assert '"permission": "allow"' in env["OPENCODE_CONFIG_CONTENT"]
    assert '"url": "http://127.0.0.1:8989/mcp/v1/"' in env["OPENCODE_CONFIG_CONTENT"]


@pytest.mark.asyncio
async def test_turn_maps_open_code_message_parts(tmp_path, monkeypatch):
    client = OpenCodeClient(_backend(tmp_path), _spec(tmp_path))
    client._native_session_id = "oc-1"

    async def request(method, path, **kwargs):
        assert method == "POST" and path == "/session/oc-1/message"
        assert kwargs["json"]["system"] == "system"
        return {"info": {"modelID": "openai/gpt-test"}, "parts": [
            {"type": "text", "text": "hello"},
            {"type": "tool", "input": {"ignored": True}},
            {"type": "text", "text": " world"},
        ]}

    monkeypatch.setattr(client, "_request", request)
    await client.start_turn(TurnInput(text="hi"))
    events = [event async for event in client.receive_turn()]
    assert isinstance(events[0], ev.ModelObserved)
    assert "".join(event.text for event in events if isinstance(event, ev.TextDelta)) == "hello world"
    assert events[-1].status == "completed"


def test_opencode_can_be_selected_as_a_new_backend(tmp_path):
    config = NerveConfig.from_dict({
        "workspace": str(tmp_path), "agent": {"backend": "opencode"},
        "opencode": {"home_dir": str(tmp_path / "opencode")},
    })
    assert config.agent.backend == "opencode"
