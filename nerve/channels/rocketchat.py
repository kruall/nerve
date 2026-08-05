"""Fail-closed Rocket.Chat bot channel backed by its REST and realtime APIs.

One Nerve process authenticates as one dedicated Rocket.Chat bot.  It listens
only to explicitly configured room IDs and author IDs, then hands accepted
messages to :class:`ChannelRouter`.  Public replies are deliberate: agents use
the session-bound ``rocketchat_send`` tool rather than automatically exposing
their complete session stream.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import stat
from collections import OrderedDict
from typing import Any, TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets

from nerve.channels.base import (
    BaseChannel,
    ChannelCapability,
    ChannelConstraints,
    InboundMessage,
    OutboundMessage,
)
from nerve.config import NerveConfig

if TYPE_CHECKING:
    from nerve.channels.router import ChannelRouter

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT_SECONDS = 30.0
_MAX_MESSAGE_LENGTH = 4_000
_RECENT_MESSAGE_IDS = 4_096
_RECONNECT_DELAY_SECONDS = 5.0


def split_rocketchat_message(
    text: str,
    limit: int = _MAX_MESSAGE_LENGTH,
) -> list[str]:
    """Split text on a readable boundary within Rocket.Chat's message limit."""
    remaining = text.strip()
    chunks: list[str] = []
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, limit + 1)
        if cut < limit // 2:
            cut = remaining.rfind(" ", 0, limit + 1)
        if cut <= 0:
            cut = limit
        chunk = remaining[:cut].rstrip()
        if chunk:
            chunks.append(chunk)
        remaining = remaining[cut:].lstrip()
    return chunks


class RocketChatChannel(BaseChannel):
    """Connect one dedicated Rocket.Chat bot to Nerve's channel router."""

    def __init__(self, config: NerveConfig, router: ChannelRouter):
        self._nerve_config = config
        self.config = config.rocketchat
        self.router = router
        self._client: httpx.AsyncClient | None = None
        self._auth_token = ""
        self._bot_user_id = ""
        self._password = ""
        self._connection_task: asyncio.Task[None] | None = None
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._stopped = asyncio.Event()
        self._ready = asyncio.Event()
        self._startup_error: Exception | None = None
        self._recent_message_ids: OrderedDict[str, None] = OrderedDict()
        self._room_ids = set(self.config.room_ids)
        self._allowed_author_ids = set(self.config.allowed_author_ids)
        self._mention_pattern: re.Pattern[str] | None = None

    @property
    def name(self) -> str:
        return "rocketchat"

    @property
    def capabilities(self) -> ChannelCapability:
        return ChannelCapability.SEND_TEXT | ChannelCapability.MARKDOWN

    @property
    def automatic_responses(self) -> bool:
        """Rocket.Chat replies must be deliberately sent by the agent."""
        return False

    @property
    def constraints(self) -> ChannelConstraints:
        return ChannelConstraints(max_message_length=_MAX_MESSAGE_LENGTH)

    def _validate_config(self) -> None:
        missing: list[str] = []
        if not self.config.url:
            missing.append("url")
        elif urlsplit(self.config.url).scheme not in {"http", "https"}:
            raise ValueError("rocketchat.url must use http or https")
        if not self.config.username:
            missing.append("username")
        has_auth_token = bool(
            self.config.auth_token or self.config.auth_token_file
        )
        has_password = bool(self.config.password or self.config.password_file)
        if not has_auth_token and not has_password:
            missing.append("auth_token(_file) or password(_file)")
        if has_auth_token and not self.config.user_id:
            missing.append("user_id (required with auth_token)")
        if not self._room_ids:
            missing.append("room_ids")
        if not self._allowed_author_ids:
            missing.append("allowed_author_ids")
        if missing:
            raise ValueError(
                "rocketchat is enabled but missing: " + ", ".join(missing)
            )
        if self.config.password_file:
            try:
                mode = stat.S_IMODE(self.config.password_file.stat().st_mode)
            except OSError as exc:
                raise ValueError(
                    "rocketchat.password_file cannot be read"
                ) from exc
            if mode & 0o077:
                raise ValueError(
                    "rocketchat.password_file must not be readable by group "
                    "or others"
                )
        if self.config.auth_token_file:
            try:
                mode = stat.S_IMODE(self.config.auth_token_file.stat().st_mode)
            except OSError as exc:
                raise ValueError(
                    "rocketchat.auth_token_file cannot be read"
                ) from exc
            if mode & 0o077:
                raise ValueError(
                    "rocketchat.auth_token_file must not be readable by group "
                    "or others"
                )

    def _load_auth_token(self) -> str:
        if self.config.auth_token_file:
            try:
                token = self.config.auth_token_file.read_text(
                    encoding="utf-8",
                ).strip()
            except OSError as exc:
                raise ValueError(
                    "rocketchat.auth_token_file cannot be read"
                ) from exc
            if token:
                return token
        return self.config.auth_token

    def _load_password(self) -> str:
        if self.config.password_file:
            try:
                password = self.config.password_file.read_text(
                    encoding="utf-8",
                ).strip()
            except OSError as exc:
                raise ValueError(
                    "rocketchat.password_file cannot be read"
                ) from exc
            if password:
                return password
        if self.config.password:
            return self.config.password
        raise ValueError("rocketchat bot password is empty")

    @staticmethod
    def _websocket_url(url: str) -> str:
        parsed = urlsplit(url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        path = f"{parsed.path.rstrip('/')}/websocket"
        return urlunsplit((scheme, parsed.netloc, path, "", ""))

    async def _login(self) -> None:
        if self._client is None:
            raise RuntimeError("Rocket.Chat HTTP client is not initialized")
        response = await self._client.post(
            "/api/v1/login",
            json={"user": self.config.username, "password": self._password},
        )
        response.raise_for_status()
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        token = data.get("authToken") if isinstance(data, dict) else None
        user_id = data.get("userId") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token:
            raise RuntimeError("Rocket.Chat login did not return authToken")
        if not isinstance(user_id, str) or not user_id:
            raise RuntimeError("Rocket.Chat login did not return userId")
        self._auth_token = token
        self._bot_user_id = user_id

    async def _authenticate_token(self, token: str) -> None:
        if self._client is None:
            raise RuntimeError("Rocket.Chat HTTP client is not initialized")
        response = await self._client.get(
            "/api/v1/me",
            headers={
                "X-Auth-Token": token,
                "X-User-Id": self.config.user_id,
            },
        )
        response.raise_for_status()
        body = response.json()
        user_id = body.get("_id") if isinstance(body, dict) else None
        if not isinstance(user_id, str) or user_id != self.config.user_id:
            raise RuntimeError("Rocket.Chat auth token belongs to another user")
        self._auth_token = token
        self._bot_user_id = user_id

    def _set_mention_pattern(self) -> None:
        self._mention_pattern = re.compile(
            rf"(?<![\w-])@{re.escape(self.config.username)}\b",
            flags=re.IGNORECASE,
        )

    async def start(self) -> None:
        self._validate_config()
        self._stopped.clear()
        self._ready.clear()
        self._startup_error = None
        self._client = httpx.AsyncClient(
            base_url=self.config.url,
            timeout=httpx.Timeout(_CONNECT_TIMEOUT_SECONDS),
        )
        try:
            auth_token = self._load_auth_token()
            if auth_token:
                await self._authenticate_token(auth_token)
            else:
                self._password = self._load_password()
                await self._login()
            self._set_mention_pattern()
        except Exception:
            await self._client.aclose()
            self._client = None
            raise
        self._connection_task = asyncio.create_task(
            self._connection_loop(),
            name="rocketchat-channel",
        )
        try:
            await asyncio.wait_for(
                self._ready.wait(), timeout=_CONNECT_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            await self.stop()
            if self._startup_error is not None:
                raise RuntimeError(
                    "Rocket.Chat realtime connection failed"
                ) from self._startup_error
            raise RuntimeError(
                "Rocket.Chat realtime connection did not become ready"
            ) from exc

    async def stop(self) -> None:
        self._stopped.set()
        if self._connection_task is not None:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass
            self._connection_task = None
        tasks = list(self._dispatch_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._auth_token = ""
        self._password = ""

    async def _connection_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await self._run_connection()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._ready.is_set() and self._startup_error is None:
                    self._startup_error = exc
                logger.warning(
                    "Rocket.Chat realtime connection ended: %s", exc,
                )
            if self._stopped.is_set():
                return
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=_RECONNECT_DELAY_SECONDS,
                )
            except TimeoutError:
                pass

    async def _run_connection(self) -> None:
        async with websockets.connect(
            self._websocket_url(self.config.url),
            open_timeout=_CONNECT_TIMEOUT_SECONDS,
            ping_interval=None,
        ) as socket:
            await socket.send(json.dumps({
                "msg": "connect", "version": "1", "support": ["1"],
            }))
            await self._wait_for(
                socket, lambda payload: payload.get("msg") == "connected",
            )
            login_id = "nerve-login"
            await socket.send(json.dumps({
                "msg": "method",
                "method": "login",
                "id": login_id,
                "params": [{"resume": self._auth_token}],
            }))
            login = await self._wait_for(
                socket,
                lambda payload: (
                    payload.get("msg") == "result"
                    and payload.get("id") == login_id
                ),
            )
            if login.get("error") or not login.get("result"):
                raise RuntimeError("Rocket.Chat realtime login was rejected")
            for index, room_id in enumerate(sorted(self._room_ids)):
                await socket.send(json.dumps({
                    "msg": "sub",
                    "id": f"nerve-room-{index}",
                    "name": "stream-room-messages",
                    "params": [room_id, False],
                }))
            self._ready.set()
            while not self._stopped.is_set():
                raw = await socket.recv()
                payload = self._decode_payload(raw)
                if payload is None:
                    continue
                if payload.get("msg") == "ping":
                    await socket.send(json.dumps({"msg": "pong"}))
                    continue
                await self._handle_realtime_event(payload)

    async def _wait_for(self, socket: Any, predicate: Any) -> dict[str, Any]:
        while True:
            raw = await socket.recv()
            payload = self._decode_payload(raw)
            if payload is None:
                continue
            if payload.get("msg") == "ping":
                await socket.send(json.dumps({"msg": "pong"}))
                continue
            if predicate(payload):
                return payload

    @staticmethod
    def _decode_payload(raw: str | bytes) -> dict[str, Any] | None:
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None

    async def _handle_realtime_event(self, payload: dict[str, Any]) -> None:
        if (
            payload.get("msg") != "changed"
            or payload.get("collection") != "stream-room-messages"
        ):
            return
        fields = payload.get("fields")
        if not isinstance(fields, dict):
            return
        args = fields.get("args")
        if not isinstance(args, list) or not args:
            return
        message = args[0]
        if not isinstance(message, dict):
            return
        await self._ingest(message)

    async def _ingest(self, message: dict[str, Any]) -> None:
        message_id = str(message.get("_id") or "")
        room_id = str(message.get("rid") or "")
        author = message.get("u")
        author_id = str(author.get("_id") or "") if isinstance(author, dict) else ""
        raw_text = str(message.get("msg") or "").strip()
        if (
            not message_id
            or room_id not in self._room_ids
            or not author_id
            or author_id == self._bot_user_id
            or author_id not in self._allowed_author_ids
            or not raw_text
        ):
            return
        if message_id in self._recent_message_ids:
            return
        self._recent_message_ids[message_id] = None
        if len(self._recent_message_ids) > _RECENT_MESSAGE_IDS:
            self._recent_message_ids.popitem(last=False)
        if self.config.require_mention:
            mentions = message.get("mentions")
            mentioned = isinstance(mentions, list) and any(
                isinstance(item, dict) and item.get("_id") == self._bot_user_id
                for item in mentions
            )
            mentioned = mentioned or bool(
                self._mention_pattern and self._mention_pattern.search(raw_text)
            )
            if not mentioned:
                return
        text = (
            self._mention_pattern.sub("", raw_text).strip()
            if self._mention_pattern is not None
            else raw_text
        )
        if not text:
            return
        thread_id = str(message.get("tmid") or "")
        target = self._target(room_id, thread_id)
        task = asyncio.create_task(
            self._dispatch(
                target=target,
                room_id=room_id,
                thread_id=thread_id,
                author_id=author_id,
                message_id=message_id,
                text=text,
            ),
            name=f"rocketchat-ingest:{message_id}",
        )
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        task.add_done_callback(self._dispatch_done)

    @staticmethod
    def _dispatch_done(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("Rocket.Chat message dispatch failed")

    async def _dispatch(
        self,
        *,
        target: str,
        room_id: str,
        thread_id: str,
        author_id: str,
        message_id: str,
        text: str,
    ) -> None:
        location = "thread" if thread_id else "room"
        context = (
            f"[This is a Rocket.Chat {location}. Public replies are visible "
            "to its members. Use rocketchat_send only for deliberate replies.]\n\n"
        )
        await self.router.handle_message(InboundMessage(
            channel_name=self.name,
            channel_key=(
                f"rocketchat:{room_id}:{thread_id or 'root'}"
            ),
            sender_id=target,
            text=context + text,
            session_title=(
                f"Rocket.Chat · {room_id}"
                + (f" · thread {thread_id}" if thread_id else "")
            ),
            metadata={
                "message_id": message_id,
                "rocketchat_room_id": room_id,
                "rocketchat_thread_id": thread_id,
                "rocketchat_author_id": author_id,
            },
            steer_if_busy=True,
        ))

    @staticmethod
    def _target(room_id: str, thread_id: str) -> str:
        return f"{room_id}|{thread_id}"

    @staticmethod
    def _parse_target(target: str) -> tuple[str, str]:
        room_id, separator, thread_id = target.partition("|")
        if not separator or not room_id:
            raise ValueError("Rocket.Chat target is invalid")
        return room_id, thread_id

    async def send(self, message: OutboundMessage) -> None:
        if self._client is None or not self._auth_token:
            raise RuntimeError("Rocket.Chat channel is not connected")
        room_id, thread_id = self._parse_target(message.target)
        for chunk in split_rocketchat_message(message.text):
            payload: dict[str, Any] = {"rid": room_id, "msg": chunk}
            if thread_id:
                payload["tmid"] = thread_id
            response = await self._client.post(
                "/api/v1/chat.sendMessage",
                headers={
                    "X-Auth-Token": self._auth_token,
                    "X-User-Id": self._bot_user_id,
                },
                json={"message": payload},
            )
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or body.get("success") is not True:
                raise RuntimeError("Rocket.Chat rejected the message")
