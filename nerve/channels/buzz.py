"""Native Buzz channel backed by the official ``buzz`` CLI.

Buzz exposes an authenticated Nostr relay. Its CLI owns protocol details and
signing; this adapter owns Nerve's channel contract: polling, durable event
de-duplication, authorization, and Router dispatch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from nerve.channels.base import BaseChannel, ChannelCapability, InboundMessage, OutboundMessage
from nerve.config import NerveConfig
from nerve.sources.models import SourceRecord

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.db import Database

logger = logging.getLogger(__name__)
_POLL_LIMIT = 100
_COMMAND_TIMEOUT_SECONDS = 30.0


class BuzzChannel(BaseChannel):
    """Connect selected Buzz channels to Nerve's normal channel router."""

    def __init__(self, config: NerveConfig, router: ChannelRouter, db: Database):
        self.config = config.buzz
        self.router = router
        self.db = db
        self._poll_task: asyncio.Task[None] | None = None
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._stop_event = asyncio.Event()
        self._private_key = ""

    @property
    def name(self) -> str:
        return "buzz"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.MARKDOWN

    async def start(self) -> None:
        if self._poll_task is not None:
            return
        self._validate_config()
        self._private_key = self._load_private_key()
        await self._prime_cursors()
        self._stop_event.clear()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="buzz-channel")
        logger.info("Buzz channel started for %d channel(s)", len(self.config.channel_ids))

    async def stop(self) -> None:
        self._stop_event.set()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        for task in list(self._dispatch_tasks):
            task.cancel()
        if self._dispatch_tasks:
            await asyncio.gather(*self._dispatch_tasks, return_exceptions=True)
        self._dispatch_tasks.clear()

    async def send(self, message: OutboundMessage) -> None:
        if message.target not in self.config.channel_ids:
            raise ValueError("Refusing to send to a Buzz channel not configured for Nerve")
        await self._run_cli("messages", "send", "--channel", message.target, "--content", message.text)

    def _validate_config(self) -> None:
        missing: list[str] = []
        if not self.config.relay_url:
            missing.append("relay_url")
        if not self.config.channel_ids:
            missing.append("channel_ids")
        if not self.config.allowed_pubkeys:
            missing.append("allowed_pubkeys")
        if not self.config.bot_pubkey:
            missing.append("bot_pubkey")
        if not (self.config.private_key or self.config.private_key_file):
            missing.append("private_key or private_key_file")
        if missing:
            raise ValueError("buzz is enabled but missing: " + ", ".join(missing))

    def _load_private_key(self) -> str:
        if self.config.private_key:
            return self.config.private_key.strip()
        path = self.config.private_key_file
        if path is None:
            raise ValueError("Buzz private key is not configured")
        try:
            content = path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(f"Cannot read Buzz private key file {path}: {exc}") from exc
        for line in content.splitlines():
            if line.startswith("Secret key:"):
                return line.split(":", 1)[1].strip()
        if len(content.splitlines()) == 1:
            return content
        raise ValueError("Buzz private key file must contain one key or a 'Secret key:' line")

    async def _prime_cursors(self) -> None:
        """Record the current relay head without replying to old history."""
        for channel_id in self.config.channel_ids:
            source = self._source_name(channel_id)
            if await self.db.get_sync_cursor(source) is not None:
                continue
            events = await self._fetch_events(channel_id)
            latest = max((self._event_timestamp(event) for event in events), default=0)
            await self.db.set_sync_cursor(source, str(latest))
            logger.info("Buzz channel %s primed at timestamp %d", channel_id, latest)

    async def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                for channel_id in self.config.channel_ids:
                    await self._poll_channel(channel_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Buzz poll failed")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self.config.poll_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass

    async def _poll_channel(self, channel_id: str) -> None:
        source = self._source_name(channel_id)
        raw_cursor = await self.db.get_sync_cursor(source)
        cursor = int(raw_cursor or 0)
        # The relay's `since` is strict, so overlap one second and rely on IDs
        # stored in source_messages to retain events sharing a timestamp.
        events = await self._fetch_events(channel_id, since=max(0, cursor - 1))
        latest = cursor
        for event in sorted(events, key=lambda item: (self._event_timestamp(item), str(item.get("id", "")))):
            latest = max(latest, self._event_timestamp(event))
            event_id = str(event.get("id", ""))
            if not event_id or await self.db.get_source_message(source, event_id):
                continue
            record = self._record_from_event(source, channel_id, event)
            await self.db.insert_source_messages([record], source=source)
            if self._accepts(event):
                self._start_dispatch(channel_id, event)
        if latest > cursor:
            await self.db.set_sync_cursor(source, str(latest))

    async def _fetch_events(self, channel_id: str, since: int | None = None) -> list[dict[str, Any]]:
        args = ["messages", "get", "--channel", channel_id, "--limit", str(_POLL_LIMIT)]
        if since is not None:
            args.extend(["--since", str(since)])
        output = await self._run_cli(*args)
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Buzz CLI returned invalid JSON") from exc
        if not isinstance(decoded, list):
            raise RuntimeError("Buzz CLI returned an unexpected response")
        return [event for event in decoded if isinstance(event, dict)]

    async def _run_cli(self, *args: str) -> str:
        env = os.environ.copy()
        env["BUZZ_PRIVATE_KEY"] = self._private_key
        env["BUZZ_RELAY_URL"] = self.config.relay_url
        process = await asyncio.create_subprocess_exec(
            str(self.config.binary_path), *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        try:
            stdout, _stderr = await asyncio.wait_for(
                process.communicate(), timeout=_COMMAND_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise RuntimeError("Buzz CLI timed out") from None
        if process.returncode:
            raise RuntimeError(f"Buzz CLI exited with status {process.returncode}")
        return stdout.decode("utf-8", errors="replace")

    def _accepts(self, event: dict[str, Any]) -> bool:
        author = str(event.get("pubkey", "")).lower()
        if not author or author == self.config.bot_pubkey:
            return False
        if author not in self.config.allowed_pubkeys:
            return False
        if not self.config.require_mention:
            return True
        return any(
            isinstance(tag, list) and len(tag) > 1 and tag[0] == "p"
            and str(tag[1]).lower() == self.config.bot_pubkey
            for tag in event.get("tags", [])
        )

    def _start_dispatch(self, channel_id: str, event: dict[str, Any]) -> None:
        task = asyncio.create_task(self._dispatch(channel_id, event), name="buzz-inbound")
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def _dispatch(self, channel_id: str, event: dict[str, Any]) -> None:
        author = str(event["pubkey"]).lower()
        content = str(event.get("content", "")).strip()
        if not content:
            return
        await self.router.handle_message(InboundMessage(
            channel_name=self.name,
            channel_key=f"buzz:{channel_id}:{author}",
            sender_id=channel_id,
            text=("[Это сообщение из общего канала Buzz; ответ увидят все его участники.]\n\n" + content),
            metadata={
                "message_id": str(event["id"]),
                "buzz_channel_id": channel_id,
                "buzz_author_pubkey": author,
            },
        ))

    @staticmethod
    def _event_timestamp(event: dict[str, Any]) -> int:
        try:
            return int(event.get("created_at", 0))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _record_from_event(cls, source: str, channel_id: str, event: dict[str, Any]) -> SourceRecord:
        timestamp = datetime.fromtimestamp(cls._event_timestamp(event), tz=timezone.utc).isoformat()
        author = str(event.get("pubkey", "")).lower()
        return SourceRecord(
            id=str(event["id"]), source=source, record_type="buzz_message",
            summary=f"Buzz message from {author[:12]}",
            content=str(event.get("content", "")), timestamp=timestamp,
            metadata={"channel_id": channel_id, "author_pubkey": author, "kind": event.get("kind"), "tags": event.get("tags", [])},
        )

    @staticmethod
    def _source_name(channel_id: str) -> str:
        return f"buzz:{channel_id}"
