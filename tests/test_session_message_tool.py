from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.engine import AgentEngine
from nerve.agent.tools.handlers.notifications import send_session_message_handler
from nerve.agent.tools.registry import ToolContext


@pytest.mark.asyncio
async def test_session_message_steers_an_active_target():
    engine = AgentEngine.__new__(AgentEngine)
    engine.db = MagicMock()
    engine.db.get_session = AsyncMock(return_value={
        "id": "target", "source": "discord", "status": "active",
    })
    engine.steer = AsyncMock(return_value=True)
    engine.sessions = MagicMock()

    result = await engine.send_session_message(
        source_session_id="source",
        target_session_id="target",
        message="please check the deploy",
    )

    assert result == "steered"
    engine.steer.assert_awaited_once_with(
        "target",
        "[Message from Nerve session source]\n\nplease check the deploy",
        channel=None,
    )


@pytest.mark.asyncio
async def test_session_message_starts_idle_target_without_callers_channel():
    engine = AgentEngine.__new__(AgentEngine)
    engine.db = MagicMock()
    engine.db.get_session = AsyncMock(return_value={
        "id": "target", "source": "discord", "status": "idle",
    })
    engine.steer = AsyncMock(return_value=False)
    engine.sessions = MagicMock()
    engine.sessions.is_running.return_value = False
    engine.run = AsyncMock(return_value="done")
    engine.register_task = MagicMock()

    result = await engine.send_session_message(
        source_session_id="source",
        target_session_id="target",
        message="please resume",
    )
    await asyncio.sleep(0)

    assert result == "started"
    engine.run.assert_awaited_once_with(
        session_id="target",
        user_message="[Message from Nerve session source]\n\nplease resume",
        source="session",
        channel=None,
    )
    engine.register_task.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target, error",
    [
        (None, "target session was not found"),
        ({"source": "external", "status": "idle"}, "externally managed"),
        ({"source": "web", "status": "archived"}, "archived"),
    ],
)
async def test_session_message_refuses_unsafe_target(target, error):
    engine = AgentEngine.__new__(AgentEngine)
    engine.db = MagicMock()
    engine.db.get_session = AsyncMock(return_value=target)

    with pytest.raises(ValueError, match=error):
        await engine.send_session_message(
            source_session_id="source",
            target_session_id="target",
            message="hello",
        )


@pytest.mark.asyncio
async def test_session_message_refuses_self_target():
    engine = AgentEngine.__new__(AgentEngine)
    engine.db = MagicMock()

    with pytest.raises(ValueError, match="cannot send a message to itself"):
        await engine.send_session_message(
            source_session_id="shared",
            target_session_id="shared",
            message="hello",
        )

    engine.db.get_session.assert_not_called()


@pytest.mark.asyncio
async def test_session_message_handler_reports_steered_delivery():
    engine = MagicMock()
    engine.send_session_message = AsyncMock(return_value="steered")

    result = await send_session_message_handler(
        ToolContext(session_id="source", engine=engine),
        {"session_id": "target", "message": "  hand off  "},
    )

    assert result.is_error is False
    assert "active turn" in result.content[0]["text"]
    engine.send_session_message.assert_awaited_once_with(
        source_session_id="source",
        target_session_id="target",
        message="hand off",
    )


@pytest.mark.asyncio
async def test_session_message_handler_rejects_invalid_input():
    engine = MagicMock()

    result = await send_session_message_handler(
        ToolContext(session_id="source", engine=engine),
        {"session_id": "target", "message": " "},
    )

    assert result.is_error is True
    engine.send_session_message.assert_not_called()
