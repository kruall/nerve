"""Graceful, agent-coordinated daemon restart scheduling.

The coordinator deliberately lives beside the engine rather than in one
channel: a requested restart must stop every new turn, including wakeups,
cron continuations, web requests, and Discord messages.  Existing turns are
allowed to finish.  A long-running turn is asked through its native steer
path whether it can yield now or needs a bounded extension.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig

logger = logging.getLogger(__name__)

DEFAULT_PROMPT_AFTER_SECONDS = 300
DEFAULT_CHECK_INTERVAL_SECONDS = 2.0
MIN_WAIT_SECONDS = 60
MAX_WAIT_SECONDS = 3600


class RestartScheduledError(RuntimeError):
    """A new or resumed agent turn was rejected while draining for restart."""


@dataclass
class _RestartSchedule:
    requester_session_id: str
    requested_at: float
    prompt_after_seconds: int
    prompted: set[str] = field(default_factory=set)
    ready: set[str] = field(default_factory=set)
    next_prompt_at: dict[str, float] = field(default_factory=dict)
    restart_started: bool = False


class RestartCoordinator:
    """Drain active sessions, then invoke the normal ``nerve restart`` path."""

    def __init__(
        self,
        engine: "AgentEngine",
        config: "NerveConfig",
        *,
        check_interval_seconds: float = DEFAULT_CHECK_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        launcher: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.engine = engine
        self.config = config
        self._check_interval_seconds = max(0.01, check_interval_seconds)
        self._clock = clock
        self._launcher = launcher or self._launch_restart
        self._schedule: _RestartSchedule | None = None
        self._monitor_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()

    @property
    def pending(self) -> bool:
        return self._schedule is not None

    def pending_message(self) -> str:
        return (
            "A graceful Nerve restart is scheduled. New and resumed sessions "
            "are paused until the restart completes."
        )

    async def schedule(
        self,
        requester_session_id: str,
        *,
        prompt_after_seconds: int = DEFAULT_PROMPT_AFTER_SECONDS,
    ) -> dict:
        """Start one idempotent drain and return its observable state."""
        prompt_after_seconds = max(
            MIN_WAIT_SECONDS, min(MAX_WAIT_SECONDS, int(prompt_after_seconds)),
        )
        async with self._lock:
            if self._schedule is not None:
                return self._snapshot(self._schedule, already_pending=True)
            schedule = _RestartSchedule(
                requester_session_id=requester_session_id,
                requested_at=self._clock(),
                prompt_after_seconds=prompt_after_seconds,
            )
            self._schedule = schedule
            self._monitor_task = asyncio.create_task(
                self._monitor(schedule), name="scheduled-nerve-restart",
            )
            return self._snapshot(schedule, already_pending=False)

    async def mark_ready(self, session_id: str) -> bool:
        """Record that a prompted active turn permits being stopped now."""
        async with self._lock:
            schedule = self._schedule
            if schedule is None or session_id not in schedule.prompted:
                return False
            schedule.ready.add(session_id)
            schedule.next_prompt_at.pop(session_id, None)
            return True

    async def request_more_time(self, session_id: str, seconds: int) -> int | None:
        """Defer another restart prompt for a previously prompted turn."""
        seconds = max(MIN_WAIT_SECONDS, min(MAX_WAIT_SECONDS, int(seconds)))
        async with self._lock:
            schedule = self._schedule
            if schedule is None or session_id not in schedule.prompted:
                return None
            schedule.ready.discard(session_id)
            # It may be prompted again after the requested extension.
            schedule.prompted.discard(session_id)
            schedule.next_prompt_at[session_id] = self._clock() + seconds
            return seconds

    async def shutdown(self) -> None:
        """Stop only the local monitor; process shutdown itself is in progress."""
        task = self._monitor_task
        self._monitor_task = None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _snapshot(self, schedule: _RestartSchedule, *, already_pending: bool) -> dict:
        running = self.engine.sessions.get_running_ids()
        return {
            "already_pending": already_pending,
            "active_sessions": len(running),
            "prompt_after_seconds": schedule.prompt_after_seconds,
            "restart_started": schedule.restart_started,
        }

    async def _monitor(self, schedule: _RestartSchedule) -> None:
        try:
            while self._schedule is schedule:
                running = self.engine.sessions.get_running_ids()
                if not running:
                    async with self._lock:
                        if self._schedule is not schedule:
                            return
                        schedule.restart_started = True
                    logger.warning("All sessions drained; starting scheduled restart")
                    await self._launcher()
                    return

                now = self._clock()
                for session_id in running:
                    if session_id in schedule.ready:
                        # The model explicitly granted permission.  The normal
                        # stop path preserves existing cleanup invariants.
                        await self.engine.stop_session(session_id)
                        continue
                    next_at = schedule.next_prompt_at.get(
                        session_id,
                        schedule.requested_at + schedule.prompt_after_seconds,
                    )
                    if now < next_at or session_id in schedule.prompted:
                        continue
                    steered = await self.engine.steer(
                        session_id=session_id,
                        user_message=(
                            "[Scheduled Nerve restart]\n"
                            "A user-requested graceful restart is waiting for "
                            "this active session. If it is safe to end this "
                            "turn now, call `restart_ready`. If you need more "
                            "time, call `restart_wait` with the shortest "
                            "realistic delay in seconds. Do not start new work "
                            "or schedule a wakeup while deciding."
                        ),
                        channel=self.engine.get_active_channel(session_id),
                    )
                    if steered:
                        async with self._lock:
                            if self._schedule is schedule:
                                schedule.prompted.add(session_id)
                await asyncio.sleep(self._check_interval_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Scheduled restart monitor failed")
        finally:
            if self._monitor_task is asyncio.current_task():
                self._monitor_task = None

    async def _launch_restart(self) -> None:
        """Delegate restart mechanics to the established CLI implementation."""
        command = [
            sys.executable, "-m", "nerve", "-c", str(Path(self.config.config_dir)),
            "restart",
        ]
        try:
            await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as exc:
            # Keep the gate in place: proceeding with new work after a failed
            # requested restart is less safe than a visible operational pause.
            logger.error("Could not launch scheduled Nerve restart: %s", exc)
            raise
