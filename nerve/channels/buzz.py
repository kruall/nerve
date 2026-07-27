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
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from urllib.parse import urlsplit

from nerve.channels.base import BaseChannel, ChannelCapability, InboundMessage, OutboundMessage
from nerve.config import NerveConfig
from nerve.sources.models import SourceRecord

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter
    from nerve.db import Database

logger = logging.getLogger(__name__)
_POLL_LIMIT = 100
_DM_LIMIT = 200
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
        self._channel_names: dict[str, str] = {}
        self._source_names: dict[str, str] = {}
        self._author_names: dict[str, str] = {}
        self._dm_channel_ids: set[str] = set()
        self._next_dm_refresh = 0.0

    @property
    def name(self) -> str:
        return "buzz"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.MARKDOWN

    @property
    def automatic_responses(self) -> bool:
        """Buzz messages are published only through the explicit MCP tool."""
        return False

    async def start(self) -> None:
        if self._poll_task is not None:
            return
        self._validate_config()
        self._private_key = self._load_private_key()
        await self._load_channel_names()
        try:
            await self._load_author_names()
        except Exception:
            # Profile lookup is best-effort: a transient relay failure must not
            # prevent the Buzz channel from starting. Dispatch retains the
            # unambiguous pubkey fallback when no profile name is available.
            logger.warning("Buzz author profile lookup failed", exc_info=True)
        await self._migrate_legacy_source_names()
        await self._prime_cursors()
        self._stop_event.clear()
        self._poll_task = asyncio.create_task(self._poll_loop(), name="buzz-channel")
        logger.info(
            "Buzz channel started for %d configured channel(s) and %d DM(s)",
            len(self.config.channel_ids),
            len(self._dm_channel_ids),
        )

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
        if (
            message.target not in self.config.channel_ids
            and message.target not in self._dm_channel_ids
        ):
            raise ValueError("Refusing to send to a Buzz channel not configured for Nerve")
        await self._run_cli("messages", "send", "--channel", message.target, "--content", message.text)

    def _validate_config(self) -> None:
        missing: list[str] = []
        if not self.config.relay_url:
            missing.append("relay_url")
        if not self.config.channel_ids and not self.config.direct_messages:
            missing.append("channel_ids or direct_messages")
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
        await self._refresh_dm_channel_ids(force=True)
        for channel_id in self._poll_targets():
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
                await self._refresh_dm_channel_ids()
                for channel_id in self._poll_targets():
                    await self._poll_channel(
                        channel_id,
                        is_dm=channel_id in self._dm_channel_ids,
                    )
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

    async def _poll_channel(self, channel_id: str, *, is_dm: bool = False) -> None:
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
            record = self._record_from_event(source, channel_id, event, is_dm=is_dm)
            await self.db.insert_source_messages([record], source=source)
            if self._accepts(event, is_dm=is_dm):
                self._start_dispatch(channel_id, event, is_dm=is_dm)
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

    async def _load_channel_names(self) -> None:
        """Resolve configured UUIDs to human-readable Buzz source names."""
        output = await self._run_cli("--format", "compact", "channels", "list")
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Buzz CLI returned invalid channel JSON") from exc
        if not isinstance(decoded, list):
            raise RuntimeError("Buzz CLI returned an unexpected channel response")

        names = {
            str(item.get("channel_id", "")): str(item.get("name", "")).strip()
            for item in decoded
            if isinstance(item, dict) and item.get("channel_id") and item.get("name")
        }
        missing = [channel_id for channel_id in self.config.channel_ids if channel_id not in names]
        if missing:
            raise ValueError(
                "Configured Buzz channel IDs were not returned by 'channels list': "
                + ", ".join(missing)
            )

        community = self._slug(
            self.config.community_name or self._community_from_relay_url(self.config.relay_url)
        )
        candidates = {
            channel_id: f"buzz:{community}:{self._slug(names[channel_id])}"
            for channel_id in self.config.channel_ids
        }
        collisions: dict[str, list[str]] = {}
        for channel_id, source in candidates.items():
            collisions.setdefault(source, []).append(channel_id)
        for source, channel_ids in collisions.items():
            if len(channel_ids) > 1:
                for channel_id in channel_ids:
                    candidates[channel_id] = f"{source}-{channel_id[:8]}"

        self._channel_names = {
            channel_id: names[channel_id] for channel_id in self.config.channel_ids
        }
        self._source_names = candidates

    async def _load_author_names(self) -> None:
        """Cache canonical mention names for configured Buzz authors."""
        pubkeys = sorted(set(self.config.allowed_pubkeys))
        if not pubkeys:
            self._author_names = {}
            return

        args = ["--format", "json", "users", "get"]
        for pubkey in pubkeys:
            args.extend(["--pubkey", pubkey])
        output = await self._run_cli(*args)
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Buzz CLI returned invalid user profile JSON") from exc
        if not isinstance(decoded, list):
            raise RuntimeError("Buzz CLI returned an unexpected user profile response")

        names: dict[str, str] = {}
        allowed = set(pubkeys)
        for profile in decoded:
            if not isinstance(profile, dict):
                continue
            pubkey = str(profile.get("pubkey", "")).lower()
            if pubkey not in allowed:
                continue
            name = self._profile_mention_name(profile)
            if name:
                names[pubkey] = name
        self._author_names = names

    @staticmethod
    def _profile_mention_name(profile: dict[str, Any]) -> str:
        value = profile.get("display_name") or profile.get("name")
        if not isinstance(value, str):
            return ""
        # Buzz supports multi-word display-name mentions. Collapse whitespace
        # so profile metadata cannot break the one-line author context.
        name = " ".join(value.strip().removeprefix("@").split())
        if not name or len(name) > 100 or any(char in name for char in "[]"):
            return ""
        return name

    async def _migrate_legacy_source_names(self) -> None:
        for channel_id, source in self._source_names.items():
            await self.db.rename_source(f"buzz:{channel_id}", source)

    async def _fetch_dm_channel_ids(self) -> set[str]:
        """Return relay-confirmed DM conversations visible to the bot."""
        output = await self._run_cli("dms", "list", "--limit", str(_DM_LIMIT))
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Buzz CLI returned invalid DM JSON") from exc
        if not isinstance(decoded, list):
            raise RuntimeError("Buzz CLI returned an unexpected DM response")
        allowed = set(self.config.allowed_pubkeys)
        permitted = allowed | {self.config.bot_pubkey}
        dm_channel_ids: set[str] = set()
        for item in decoded:
            if not isinstance(item, dict):
                continue
            dm_id = item.get("dm_id")
            participants = item.get("participants")
            if not isinstance(dm_id, str) or not dm_id:
                continue
            if not isinstance(participants, list):
                continue
            members = {str(pubkey).lower() for pubkey in participants}
            if members and members <= permitted and members & allowed:
                dm_channel_ids.add(dm_id)
        return dm_channel_ids

    async def _refresh_dm_channel_ids(self, *, force: bool = False) -> None:
        if not self.config.direct_messages:
            self._dm_channel_ids.clear()
            return
        now = asyncio.get_running_loop().time()
        if force or now >= self._next_dm_refresh:
            self._dm_channel_ids = await self._fetch_dm_channel_ids()
            self._next_dm_refresh = (
                now + self.config.direct_message_refresh_seconds
            )

    def _poll_targets(self) -> list[str]:
        return sorted(set(self.config.channel_ids) | self._dm_channel_ids)

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

    def _accepts(self, event: dict[str, Any], *, is_dm: bool = False) -> bool:
        author = str(event.get("pubkey", "")).lower()
        if not author or author == self.config.bot_pubkey:
            return False
        if author not in self.config.allowed_pubkeys:
            return False
        if is_dm or not self.config.require_mention:
            return True
        return any(
            isinstance(tag, list) and len(tag) > 1 and tag[0] == "p"
            and str(tag[1]).lower() == self.config.bot_pubkey
            for tag in event.get("tags", [])
        )

    def _start_dispatch(
        self,
        channel_id: str,
        event: dict[str, Any],
        *,
        is_dm: bool = False,
    ) -> None:
        task = asyncio.create_task(
            self._dispatch(channel_id, event, is_dm=is_dm),
            name="buzz-inbound",
        )
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def _dispatch(
        self,
        channel_id: str,
        event: dict[str, Any],
        *,
        is_dm: bool = False,
    ) -> None:
        author = str(event["pubkey"]).lower()
        content = str(event.get("content", "")).strip()
        if not content:
            return
        context = (
            "[Это личное сообщение Buzz; ответ увидит только этот чат.]\n\n"
            if is_dm
            else "[Это сообщение из общего канала Buzz; ответ увидят все его участники.]\n\n"
        )
        author_name = self._author_names.get(author, "")
        author_context = (
            f"[Автор Buzz: @{author_name} (pubkey: {author}). "
            f"Для обращения используйте @{author_name}, не pubkey.]\n\n"
            if author_name
            else f"[Автор Buzz: {author}]\n\n"
        )
        await self.router.handle_message(InboundMessage(
            channel_name=self.name,
            channel_key=f"buzz:{channel_id}",
            sender_id=channel_id,
            text=context + author_context + content,
            session_title=self._session_title(
                channel_id, peer_pubkey=author, is_dm=is_dm,
            ),
            metadata={
                "message_id": str(event["id"]),
                "buzz_channel_id": channel_id,
                "buzz_channel_name": self._channel_names.get(channel_id, ""),
                "buzz_source": self._source_name(channel_id),
                "buzz_author_pubkey": author,
                "buzz_author_name": author_name,
                "buzz_is_dm": is_dm,
            },
            steer_if_busy=True,
        ))

    @staticmethod
    def _event_timestamp(event: dict[str, Any]) -> int:
        try:
            return int(event.get("created_at", 0))
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _record_from_event(
        cls,
        source: str,
        channel_id: str,
        event: dict[str, Any],
        *,
        is_dm: bool = False,
    ) -> SourceRecord:
        timestamp = datetime.fromtimestamp(cls._event_timestamp(event), tz=timezone.utc).isoformat()
        author = str(event.get("pubkey", "")).lower()
        return SourceRecord(
            id=str(event["id"]), source=source, record_type="buzz_message",
            summary=f"Buzz message from {author[:12]}",
            content=str(event.get("content", "")), timestamp=timestamp,
            metadata={
                "channel_id": channel_id,
                "author_pubkey": author,
                "kind": event.get("kind"),
                "tags": event.get("tags", []),
                "is_dm": is_dm,
            },
        )

    def _source_name(self, channel_id: str) -> str:
        return self._source_names.get(channel_id, f"buzz:{channel_id}")

    def _session_title(
        self,
        channel_id: str,
        *,
        peer_pubkey: str,
        is_dm: bool,
    ) -> str:
        if is_dm:
            return f"Buzz DM · {peer_pubkey[:12]}"
        channel_name = self._channel_names.get(channel_id)
        return f"Buzz · {channel_name or channel_id[:12]}"

    @staticmethod
    def _community_from_relay_url(relay_url: str) -> str:
        parsed = urlsplit(relay_url)
        return parsed.hostname or parsed.path or "community"

    @staticmethod
    def _slug(value: str) -> str:
        normalized = unicodedata.normalize("NFKC", value).strip().casefold()
        normalized = re.sub(r"[^\w.-]+", "-", normalized, flags=re.UNICODE)
        return normalized.strip("-._") or "unnamed"
