"""Compact operational status exposed through the Discord bot presence."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections.abc import Awaitable, Callable
from typing import Any

import discord

from nerve.agent.streaming import StreamBroadcaster, broadcaster

logger = logging.getLogger(__name__)

_INITIAL_UPDATE_DELAY_SECONDS = 5.0
_MIN_UPDATE_INTERVAL_SECONDS = 15.0

RunningSessionCount = Callable[[], int]
RateLimitReader = Callable[[], Awaitable[dict[str, Any] | None]]


class DiscordPresence:
    """Project Nerve health and capacity into one compact bot activity."""

    def __init__(
        self,
        *,
        client: discord.Client,
        running_session_count: RunningSessionCount,
        rate_limit_reader: RateLimitReader,
        refresh_interval_seconds: float,
        stream: StreamBroadcaster = broadcaster,
    ):
        self.client = client
        self.running_session_count = running_session_count
        self.rate_limit_reader = rate_limit_reader
        self.refresh_interval_seconds = refresh_interval_seconds
        self.stream = stream
        self._listener_id = f"discord-presence:{id(self)}"
        self._refresh_task: asyncio.Task[None] | None = None
        self._update_task: asyncio.Task[None] | None = None
        self._update_lock = asyncio.Lock()
        self._rate_limits: dict[str, Any] | None = None
        self._last_text = ""
        self._last_update_at = 0.0
        self._started_at = 0.0

    async def start(self) -> None:
        """Subscribe to live state and start periodic quota refreshes."""
        if self._refresh_task is not None:
            return
        self._started_at = time.monotonic()
        await self.stream.register_global(
            self._listener_id,
            self._on_stream_event,
        )
        self._refresh_task = asyncio.create_task(
            self._refresh_loop(),
            name="discord-presence-refresh",
        )
        self._schedule_update()

    async def stop(self) -> None:
        """Stop background updates before the Discord client closes."""
        await self.stream.unregister_global(self._listener_id)
        tasks = [
            task
            for task in (self._refresh_task, self._update_task)
            if task is not None
        ]
        self._refresh_task = None
        self._update_task = None
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _refresh_loop(self) -> None:
        try:
            while True:
                try:
                    rate_limits = await self.rate_limit_reader()
                    if isinstance(rate_limits, dict):
                        self._rate_limits = rate_limits
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning(
                        "Failed to refresh Codex quota for Discord presence",
                        exc_info=True,
                    )
                self._schedule_update()
                await asyncio.sleep(self.refresh_interval_seconds)
        except asyncio.CancelledError:
            raise

    async def _on_stream_event(
        self,
        session_id: str,
        event: dict[str, Any],
    ) -> None:
        """Capture running-session and Codex quota changes without blocking."""
        event_type = str(event.get("type") or "")
        if session_id == "__global__" and event_type == "session_running":
            self._schedule_update()
            return
        if (
            event_type == "backend_status"
            and event.get("subtype") == "codex_rate_limits"
        ):
            data = event.get("data")
            rate_limits = (
                data.get("rateLimits")
                if isinstance(data, dict)
                else None
            )
            if isinstance(rate_limits, dict):
                self._rate_limits = rate_limits
                self._schedule_update()

    def _schedule_update(self) -> None:
        if self._update_task is not None and not self._update_task.done():
            return
        now = time.monotonic()
        delay = max(
            0.0,
            self._started_at + _INITIAL_UPDATE_DELAY_SECONDS - now,
            self._last_update_at + _MIN_UPDATE_INTERVAL_SECONDS - now,
        )
        self._update_task = asyncio.create_task(
            self._delayed_update(delay),
            name="discord-presence-update",
        )

    async def _delayed_update(self, delay: float) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self._update_presence()

    async def _update_presence(self) -> None:
        async with self._update_lock:
            text = self._render_text()
            if text == self._last_text:
                return
            try:
                await self.client.change_presence(
                    status=discord.Status.online,
                    activity=discord.Game(name=text),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Failed to update Discord bot presence",
                    exc_info=True,
                )
                return
            self._last_text = text
            self._last_update_at = time.monotonic()

    def _render_text(self) -> str:
        try:
            running = max(0, int(self.running_session_count()))
        except Exception:
            logger.warning(
                "Failed to count running sessions for Discord presence",
                exc_info=True,
            )
            running = 0

        parts = [
            "Nerve",
            f"{running} active" if running else "idle",
        ]
        remaining = self._codex_remaining_percent()
        if remaining is not None:
            parts.append(f"Codex {remaining}% left")
        return " · ".join(parts)

    def _codex_remaining_percent(self) -> int | None:
        primary = (
            self._rate_limits.get("primary")
            if isinstance(self._rate_limits, dict)
            else None
        )
        used = primary.get("usedPercent") if isinstance(primary, dict) else None
        if not isinstance(used, (int, float)) or not math.isfinite(used):
            return None
        return round(max(0.0, min(100.0, 100.0 - float(used))))
