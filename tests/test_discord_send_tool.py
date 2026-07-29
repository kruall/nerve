from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.notifications import discord_send_handler
from nerve.agent.tools.registry import ToolContext


@pytest.mark.asyncio
async def test_discord_send_delivers_from_wakeup_via_session_binding():
    engine = MagicMock()
    engine.get_active_channel.return_value = None
    engine.router.send_text = AsyncMock(return_value=True)

    result = await discord_send_handler(
        ToolContext(session_id="shared", engine=engine),
        {"message": "Hello, Discord"},
    )

    assert result.is_error is False
    engine.router.send_text.assert_awaited_once_with(
        "shared",
        "Hello, Discord",
        channel="discord",
    )
    engine.get_active_channel.assert_not_called()


@pytest.mark.asyncio
async def test_discord_send_refuses_session_without_binding():
    engine = MagicMock()
    engine.router.send_text = AsyncMock(return_value=False)

    result = await discord_send_handler(
        ToolContext(session_id="shared", engine=engine),
        {"message": "must not leak"},
    )

    assert result.is_error is True
    engine.router.send_text.assert_awaited_once_with(
        "shared",
        "must not leak",
        channel="discord",
    )


@pytest.mark.parametrize("message", ["  ", None, 42])
@pytest.mark.asyncio
async def test_discord_send_refuses_invalid_message(message):
    engine = MagicMock()

    result = await discord_send_handler(
        ToolContext(session_id="shared", engine=engine),
        {"message": message},
    )

    assert result.is_error is True
    engine.router.send_text.assert_not_called()
