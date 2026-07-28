from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nerve.agent.tools.handlers.notifications import discord_send_handler
from nerve.agent.tools.registry import ToolContext


@pytest.mark.asyncio
async def test_discord_send_delivers_only_through_active_discord_session():
    engine = MagicMock()
    engine.get_active_channel.return_value = "discord"
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


@pytest.mark.asyncio
async def test_discord_send_refuses_non_discord_session():
    engine = MagicMock()
    engine.get_active_channel.return_value = "web"
    engine.router.send_text = AsyncMock()

    result = await discord_send_handler(
        ToolContext(session_id="shared", engine=engine),
        {"message": "must not leak"},
    )

    assert result.is_error is True
    engine.router.send_text.assert_not_awaited()


@pytest.mark.parametrize("message", ["  ", None, 42])
@pytest.mark.asyncio
async def test_discord_send_refuses_invalid_message(message):
    engine = MagicMock()

    result = await discord_send_handler(
        ToolContext(session_id="shared", engine=engine),
        {"message": message},
    )

    assert result.is_error is True
    engine.get_active_channel.assert_not_called()
