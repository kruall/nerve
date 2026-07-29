from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import discord
import pytest

from nerve.agent.streaming import StreamBroadcaster
from nerve.channels.discord_presence import DiscordPresence


def _presence(
    *,
    running: int = 0,
    rate_limits: dict | None = None,
) -> DiscordPresence:
    client = MagicMock()
    client.change_presence = AsyncMock()
    presence = DiscordPresence(
        client=client,
        running_session_count=lambda: running,
        rate_limit_reader=AsyncMock(return_value=rate_limits),
        refresh_interval_seconds=300,
        stream=StreamBroadcaster(),
    )
    presence._started_at = 1
    return presence


def test_render_text_reports_idle_without_unknown_quota():
    assert _presence()._render_text() == "Nerve · idle"


def test_render_text_reports_running_sessions_and_remaining_quota():
    presence = _presence(running=3)
    presence._rate_limits = {
        "primary": {"usedPercent": 27.4},
    }

    assert presence._render_text() == "Nerve · 3 active · Codex 73% left"


@pytest.mark.parametrize(
    ("used", "remaining"),
    [
        (-10, 100),
        (0, 100),
        (99.6, 0),
        (120, 0),
    ],
)
def test_remaining_quota_is_rounded_and_clamped(used, remaining):
    presence = _presence()
    presence._rate_limits = {"primary": {"usedPercent": used}}
    assert presence._codex_remaining_percent() == remaining


@pytest.mark.asyncio
async def test_presence_update_uses_online_game_activity_and_deduplicates():
    presence = _presence(running=2)
    presence._rate_limits = {"primary": {"usedPercent": 40}}

    await presence._update_presence()
    await presence._update_presence()

    presence.client.change_presence.assert_awaited_once()
    kwargs = presence.client.change_presence.await_args.kwargs
    assert kwargs["status"] is discord.Status.online
    assert isinstance(kwargs["activity"], discord.Game)
    assert kwargs["activity"].name == "Nerve · 2 active · Codex 60% left"


@pytest.mark.asyncio
async def test_live_rate_limit_event_updates_cached_quota_and_schedules_update():
    presence = _presence()
    with patch.object(presence, "_schedule_update") as schedule:
        await presence._on_stream_event("s1", {
            "type": "backend_status",
            "subtype": "codex_rate_limits",
            "data": {
                "rateLimits": {
                    "primary": {"usedPercent": 12},
                },
            },
        })

    assert presence._codex_remaining_percent() == 88
    schedule.assert_called_once_with()


@pytest.mark.asyncio
async def test_global_running_event_schedules_update():
    presence = _presence()
    with patch.object(presence, "_schedule_update") as schedule:
        await presence._on_stream_event("__global__", {
            "type": "session_running",
            "session_id": "s1",
            "is_running": True,
        })

    schedule.assert_called_once_with()
