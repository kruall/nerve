from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from nerve.agent.tools.handlers.executions import ydb_make_handler
from nerve.agent.tools.registry import ToolContext
from nerve.executions.ydb import YdbWorktreeError
from nerve.resources import ResourceInventoryError


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    YdbWorktreeError("worktree must be the Git top-level"),
    ResourceInventoryError("session reservation is pinned to a different worktree"),
])
async def test_ydb_make_returns_safe_validation_and_lease_diagnostics(error):
    service = SimpleNamespace(start_ydb=AsyncMock(side_effect=error))
    result = await ydb_make_handler(
        ToolContext(session_id="session", execution_service=service),
        {"worktree": "/tmp/ydb", "args": []},
    )

    assert result.is_error
    assert str(error) in result.content[0]["text"]


@pytest.mark.asyncio
async def test_ydb_make_does_not_return_unexpected_error_text():
    service = SimpleNamespace(start_ydb=AsyncMock(side_effect=RuntimeError("secret argv")))
    result = await ydb_make_handler(
        ToolContext(session_id="session", execution_service=service),
        {"worktree": "/tmp/ydb", "args": []},
    )

    assert result.is_error
    assert "RuntimeError" in result.content[0]["text"]
    assert "secret argv" not in result.content[0]["text"]
