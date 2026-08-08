from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.agent.interactive import InteractionOutcome
from nerve.agent.tools import ToolContext
from nerve.mcp_server import http


@pytest.mark.asyncio
async def test_bound_prompt_uses_session_interactive_handler(monkeypatch):
    config = SimpleNamespace(codex=SimpleNamespace(extra_config={
        "mcp_servers.nerve.default_tools_approval_mode": "prompt",
    }))
    engine = SimpleNamespace(config=config)
    handler = SimpleNamespace(request_approval=AsyncMock(
        return_value=InteractionOutcome(),
    ), interactive_capable=True)
    monkeypatch.setattr(http, "get_handler", lambda session_id: handler)
    approve = http.build_approval_resolver(engine)
    ctx = ToolContext(
        session_id="s1", runtime_metadata={"session_bound": "true"},
    )

    assert await approve(ctx, "task_done", {"task_id": "t1"}) is True
    handler.request_approval.assert_awaited_once_with(
        "mcp_approval",
        {
            "server": "nerve",
            "tool": "task_done",
            "arguments": {"task_id": "t1"},
        },
    )


@pytest.mark.asyncio
async def test_bound_prompt_fails_closed_without_handler(monkeypatch):
    config = SimpleNamespace(codex=SimpleNamespace(extra_config={
        "mcp_servers.nerve.default_tools_approval_mode": "prompt",
    }))
    monkeypatch.setattr(http, "get_handler", lambda session_id: None)
    approve = http.build_approval_resolver(SimpleNamespace(config=config))
    ctx = ToolContext(
        session_id="s1", runtime_metadata={"session_bound": "true"},
    )

    assert await approve(ctx, "task_done", {}) is False


@pytest.mark.asyncio
async def test_noninteractive_bound_session_preserves_preapproval(monkeypatch):
    config = SimpleNamespace(codex=SimpleNamespace(extra_config={
        "mcp_servers.nerve.default_tools_approval_mode": "prompt",
    }))
    handler = SimpleNamespace(
        interactive_capable=False,
        request_approval=AsyncMock(),
    )
    monkeypatch.setattr(http, "get_handler", lambda session_id: handler)
    approve = http.build_approval_resolver(SimpleNamespace(config=config))
    ctx = ToolContext(
        session_id="cron:job", runtime_metadata={"session_bound": "true"},
    )

    assert await approve(ctx, "task_done", {}) is True
    handler.request_approval.assert_not_awaited()


@pytest.mark.asyncio
async def test_per_tool_approve_and_external_clients_bypass_nerve_prompt(monkeypatch):
    config = SimpleNamespace(codex=SimpleNamespace(extra_config={
        "mcp_servers.nerve.default_tools_approval_mode": "prompt",
        "mcp_servers.nerve.tools.task_search.approval_mode": "approve",
    }))
    monkeypatch.setattr(
        http, "get_handler", lambda session_id: pytest.fail("must not prompt"),
    )
    approve = http.build_approval_resolver(SimpleNamespace(config=config))

    bound = ToolContext(
        session_id="s1", runtime_metadata={"session_bound": "true"},
    )
    external = ToolContext(session_id="external")
    assert await approve(bound, "task_search", {}) is True
    assert await approve(external, "task_done", {}) is True
