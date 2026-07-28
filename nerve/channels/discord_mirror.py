"""Mirror Nerve sessions into one Discord forum thread per session.

The mirror is an outbound-only projection. It observes the shared agent event
stream for live updates and reconciles persisted messages/session events for
restart safety and idempotency. It never registers the audit forum as an
inbound Discord target.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

import discord

from nerve.agent.streaming import StreamBroadcaster, broadcaster
from nerve.db import Database

logger = logging.getLogger(__name__)

_MAX_DISCORD_MESSAGE = 2000
_MAX_THREAD_NAME = 100
_RECONCILE_INTERVAL_SECONDS = 5.0
_EDIT_DEBOUNCE_SECONDS = 0.75
_RECONCILE_PAGE_SIZE = 100
_MAX_LIVE_BLOCK_CHARS = 20_000
_MAX_LIVE_RENDER_CHARS = 12_000


def _split_message(text: str, limit: int = _MAX_DISCORD_MESSAGE) -> list[str]:
    """Split text into non-empty Discord-sized chunks."""
    text = text.strip()
    if not text:
        return ["*(empty)*"]
    chunks: list[str] = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit + 1)
        if split_at <= 0:
            split_at = text.rfind(" ", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        chunk = text[:split_at].rstrip()
        if not chunk:
            chunk = text[:limit]
            split_at = limit
        chunks.append(chunk)
        text = text[split_at:].lstrip()
    return chunks


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


def _timestamp(value: Any) -> str:
    text = str(value or "").replace("T", " ")
    return text[:19] + (" UTC" if text else "")


class DiscordSessionMirror:
    """Durable projector from Nerve sessions to a Discord forum."""

    def __init__(
        self,
        *,
        client: discord.Client,
        db: Database,
        guild_id: int,
        forum_id: int,
        stream: StreamBroadcaster = broadcaster,
    ):
        self.client = client
        self.db = db
        self.guild_id = guild_id
        self.forum_id = forum_id
        self.stream = stream
        self._forum: Any | None = None
        self._worker_task: asyncio.Task[None] | None = None
        self._reconcile_task: asyncio.Task[None] | None = None
        self._started_at = ""
        self._dirty_queue: asyncio.Queue[str] = asyncio.Queue()
        self._dirty: set[str] = set()
        self._live_blocks: dict[str, list[dict[str, Any]]] = {}
        self._terminal_sessions: set[str] = set()
        self._listener_id = f"discord-session-mirror:{forum_id}"

    async def start(self) -> None:
        """Validate the forum, subscribe to events, and start reconciliation."""
        if self._worker_task is not None:
            return
        self._forum = await self._resolve_forum()
        self._started_at = datetime.now(timezone.utc).isoformat()
        await self.stream.register_global(
            self._listener_id,
            self._on_stream_event,
        )
        self._worker_task = asyncio.create_task(
            self._worker_loop(),
            name="discord-session-mirror-worker",
        )
        self._reconcile_task = asyncio.create_task(
            self._reconcile_loop(),
            name="discord-session-mirror-reconcile",
        )
        logger.info(
            "Discord session mirror started for forum %s",
            self.forum_id,
        )

    async def stop(self) -> None:
        """Stop background work without discarding durable checkpoints."""
        await self.stream.unregister_global(self._listener_id)
        tasks = [
            task
            for task in (self._worker_task, self._reconcile_task)
            if task is not None
        ]
        self._worker_task = None
        self._reconcile_task = None
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        logger.info("Discord session mirror stopped")

    async def _resolve_forum(self) -> Any:
        channel = self.client.get_channel(self.forum_id)
        if channel is None:
            channel = await self.client.fetch_channel(self.forum_id)
        channel_guild_id = getattr(
            getattr(channel, "guild", None),
            "id",
            0,
        )
        if int(channel_guild_id) != self.guild_id:
            raise ValueError(
                "Discord audit forum must belong to discord.guild_id"
            )
        if not isinstance(channel, discord.ForumChannel):
            raise ValueError("discord.audit_forum_id must identify a forum channel")
        return channel

    async def _on_stream_event(
        self, session_id: str, event: dict[str, Any],
    ) -> None:
        """Capture one live event and enqueue a debounced projection update."""
        event_type = str(event.get("type") or "")
        if event_type == "thinking":
            # Nerve may store model reasoning for its own UI, but the audit
            # mirror intentionally exposes only user-visible output.
            return
        if event_type == "done":
            self._terminal_sessions.add(session_id)
        elif event_type == "stopped":
            self._terminal_sessions.add(session_id)
            self._append_live_block(
                session_id,
                {"kind": "system", "label": "stopped", "content": ""},
            )
        elif event_type == "token":
            blocks = self._live_blocks.setdefault(session_id, [])
            content = str(event.get("content") or "")
            if blocks and blocks[-1].get("kind") == "assistant":
                blocks[-1]["content"] = (
                    str(blocks[-1].get("content") or "") + content
                )[-_MAX_LIVE_BLOCK_CHARS:]
            else:
                blocks.append({"kind": "assistant", "content": content})
        elif event_type == "tool_use":
            self._append_live_block(session_id, {
                "kind": "tool",
                "tool": str(event.get("tool") or "tool"),
                "tool_use_id": event.get("tool_use_id"),
                "input": event.get("input"),
                "result": None,
                "is_error": False,
            })
        elif event_type in {"tool_result", "tool_output"}:
            self._merge_live_tool_event(session_id, event)
        elif event_type:
            payload = {
                key: value
                for key, value in event.items()
                if key not in {"type", "session_id"}
            }
            self._append_live_block(session_id, {
                "kind": "system",
                "label": event_type,
                "content": _json(payload) if payload else "",
            })
        self._mark_dirty(session_id)

    def _append_live_block(
        self, session_id: str, block: dict[str, Any],
    ) -> None:
        blocks = self._live_blocks.setdefault(session_id, [])
        blocks.append(block)
        if len(blocks) > 100:
            del blocks[:-100]

    def _merge_live_tool_event(
        self, session_id: str, event: dict[str, Any],
    ) -> None:
        blocks = self._live_blocks.setdefault(session_id, [])
        tool_use_id = event.get("tool_use_id")
        block = next(
            (
                candidate
                for candidate in reversed(blocks)
                if candidate.get("kind") == "tool"
                and candidate.get("tool_use_id") == tool_use_id
            ),
            None,
        )
        if block is None:
            block = {
                "kind": "tool",
                "tool": "tool",
                "tool_use_id": tool_use_id,
                "input": None,
                "result": "",
                "is_error": False,
            }
            blocks.append(block)
        if event.get("type") == "tool_output":
            block["result"] = (
                str(block.get("result") or "")
                + str(event.get("content") or "")
            )[-_MAX_LIVE_BLOCK_CHARS:]
        else:
            block["result"] = event.get("result")
            block["is_error"] = bool(event.get("is_error"))

    def _mark_dirty(self, session_id: str) -> None:
        if session_id in self._dirty:
            return
        self._dirty.add(session_id)
        self._dirty_queue.put_nowait(session_id)

    async def _worker_loop(self) -> None:
        while True:
            session_id = await self._dirty_queue.get()
            try:
                if (
                    session_id in self._live_blocks
                    and session_id not in self._terminal_sessions
                ):
                    await asyncio.sleep(_EDIT_DEBOUNCE_SECONDS)
                self._dirty.discard(session_id)
                await self._sync_session(session_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Discord session mirror sync failed for %s",
                    session_id,
                    exc_info=True,
                )
            finally:
                self._dirty_queue.task_done()

    async def _reconcile_loop(self) -> None:
        offset = 0
        while True:
            try:
                sessions = await self.db.list_discord_mirror_sessions(
                    active_after=self._started_at,
                    offset=offset,
                    limit=_RECONCILE_PAGE_SIZE,
                )
                if not sessions:
                    offset = 0
                else:
                    for session in sessions:
                        self._mark_dirty(str(session["id"]))
                    offset += len(sessions)
                    if len(sessions) < _RECONCILE_PAGE_SIZE:
                        offset = 0
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Discord session mirror reconciliation failed",
                    exc_info=True,
                )
            await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)

    async def _sync_session(self, session_id: str) -> None:
        session = await self.db.get_session(session_id)
        if session is None:
            self._live_blocks.pop(session_id, None)
            self._terminal_sessions.discard(session_id)
            return

        mirror, thread = await self._ensure_thread(session)
        await self._sync_header(session, mirror, thread)

        persisted = await self.db.get_discord_mirror_content_items(session_id)
        checkpoints = await self.db.get_discord_mirror_items(session_id)
        changed_items = [
            item
            for item in persisted
            if (
                (checkpoint := checkpoints.get(
                    (str(item["item_kind"]), int(item["item_id"]))
                )) is None
                or checkpoint.get("content_hash")
                != _digest(self._render_item(item))
            )
        ]

        live_ids = [int(value) for value in mirror.get("live_message_ids", [])]
        if live_ids and (
            changed_items
            or session_id in self._terminal_sessions
            or session_id not in self._live_blocks
        ):
            await self._delete_messages(thread, live_ids)
            live_ids = []
            await self.db.set_discord_mirror_live_messages(session_id, [])

        for item in persisted:
            key = (str(item["item_kind"]), int(item["item_id"]))
            text = self._render_item(item)
            content_hash = _digest(text)
            checkpoint = checkpoints.get(key)
            if checkpoint and checkpoint.get("content_hash") == content_hash:
                continue
            existing_ids = [
                int(value)
                for value in (checkpoint or {}).get("discord_message_ids", [])
            ]
            message_ids = await self._upsert_chunks(
                thread,
                _split_message(text),
                existing_ids,
            )
            await self.db.upsert_discord_mirror_item(
                session_id,
                item_kind=key[0],
                item_id=key[1],
                discord_message_ids=message_ids,
                content_hash=content_hash,
            )

        if session_id in self._terminal_sessions:
            if live_ids:
                await self._delete_messages(thread, live_ids)
                await self.db.set_discord_mirror_live_messages(session_id, [])
            self._live_blocks.pop(session_id, None)
            self._terminal_sessions.discard(session_id)
            return

        blocks = self._live_blocks.get(session_id)
        if blocks:
            live_text = self._render_live(blocks)
            new_live_ids = await self._upsert_chunks(
                thread,
                _split_message(live_text),
                live_ids,
            )
            await self.db.set_discord_mirror_live_messages(
                session_id,
                new_live_ids,
            )

    async def _ensure_thread(
        self, session: dict[str, Any],
    ) -> tuple[dict[str, Any], Any]:
        session_id = str(session["id"])
        mirror = await self.db.get_discord_session_mirror(session_id)
        if mirror and int(mirror["forum_id"]) == self.forum_id:
            try:
                thread = self.client.get_channel(int(mirror["thread_id"]))
                if thread is None:
                    thread = await self.client.fetch_channel(
                        int(mirror["thread_id"])
                    )
                return mirror, thread
            except discord.NotFound:
                logger.warning(
                    "Discord mirror thread %s disappeared; recreating",
                    mirror["thread_id"],
                )

        header = self._render_header(session)
        result = await self._forum.create_thread(
            name=self._thread_name(session),
            content=header,
            allowed_mentions=discord.AllowedMentions.none(),
            reason=f"Nerve session mirror {session_id[:32]}",
        )
        thread = result.thread
        starter = result.message
        await self.db.upsert_discord_session_mirror(
            session_id,
            guild_id=self.guild_id,
            forum_id=self.forum_id,
            thread_id=int(thread.id),
            starter_message_id=int(starter.id),
            header_hash=_digest(header),
        )
        mirror = await self.db.get_discord_session_mirror(session_id)
        if mirror is None:
            raise RuntimeError("Discord session mirror checkpoint was not saved")
        return mirror, thread

    async def _sync_header(
        self,
        session: dict[str, Any],
        mirror: dict[str, Any],
        thread: Any,
    ) -> None:
        header = self._render_header(session)
        header_hash = _digest(header)
        if mirror.get("header_hash") == header_hash:
            return
        starter = await thread.fetch_message(int(mirror["starter_message_id"]))
        await starter.edit(
            content=header,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        await self.db.update_discord_mirror_header(
            str(session["id"]),
            header_hash,
        )

    async def _upsert_chunks(
        self,
        thread: Any,
        chunks: list[str],
        existing_ids: list[int],
    ) -> list[int]:
        message_ids: list[int] = []
        common = min(len(chunks), len(existing_ids))
        for index in range(common):
            try:
                message = await thread.fetch_message(existing_ids[index])
                await message.edit(
                    content=chunks[index],
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                message_ids.append(int(message.id))
            except discord.NotFound:
                message = await thread.send(
                    chunks[index],
                    allowed_mentions=discord.AllowedMentions.none(),
                )
                message_ids.append(int(message.id))
        for chunk in chunks[common:]:
            message = await thread.send(
                chunk,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            message_ids.append(int(message.id))
        await self._delete_messages(thread, existing_ids[len(chunks):])
        return message_ids

    async def _delete_messages(
        self, thread: Any, message_ids: list[int],
    ) -> None:
        for message_id in message_ids:
            try:
                message = await thread.fetch_message(message_id)
                await message.delete()
            except discord.NotFound:
                continue

    def _thread_name(self, session: dict[str, Any]) -> str:
        title = " ".join(str(session.get("title") or session["id"]).split())
        suffix = f" · {str(session['id'])[:8]}"
        available = _MAX_THREAD_NAME - len(suffix)
        return (title[:available].rstrip() or "Nerve session") + suffix

    def _render_header(self, session: dict[str, Any]) -> str:
        return "\n".join([
            "**Nerve session mirror**",
            f"- ID: `{session['id']}`",
            f"- Title: {session.get('title') or session['id']}",
            f"- Source: `{session.get('source') or 'unknown'}`",
            f"- Backend: `{session.get('backend') or 'unknown'}`",
            f"- Status: `{session.get('status') or 'unknown'}`",
            f"- Created: {_timestamp(session.get('created_at'))}",
        ])

    def _render_item(self, item: dict[str, Any]) -> str:
        created = _timestamp(item.get("created_at"))
        if item["item_kind"] == "event":
            text = f"**event · {item['item_type']}**"
            if created:
                text += f" · `{created}`"
            details = item.get("details")
            if details:
                text += "\n```json\n" + _json(details) + "\n```"
            return text

        role = str(item.get("item_type") or "message")
        text = f"**{role}**"
        if created:
            text += f" · `{created}`"
        body = self._render_message_body(item)
        return text + ("\n" + body if body else "\n*(empty)*")

    def _render_message_body(self, item: dict[str, Any]) -> str:
        content = str(item.get("content") or "")
        blocks = item.get("details")
        if not isinstance(blocks, list):
            return content

        rendered: list[str] = []
        has_text_block = False
        for block in blocks:
            if not isinstance(block, dict):
                rendered.append(_json(block))
                continue
            block_type = str(block.get("type") or "")
            if block_type == "thinking":
                continue
            if block_type == "text":
                has_text_block = True
                rendered.append(str(block.get("content") or ""))
                continue
            if block_type == "tool_call":
                rendered.append(self._render_tool_block(block))
                continue
            rendered.append(
                f"**{block_type or 'block'}**\n```json\n{_json(block)}\n```"
            )
        if content and not has_text_block:
            rendered.insert(0, content)
        return "\n\n".join(part for part in rendered if part)

    def _render_tool_block(self, block: dict[str, Any]) -> str:
        tool = str(block.get("tool") or "tool")
        parts = [f"**tool · `{tool}`**"]
        if block.get("input") is not None:
            parts.append("input:\n```json\n" + _json(block["input"]) + "\n```")
        if block.get("result") is not None:
            label = "error" if block.get("is_error") else "result"
            result = block["result"]
            rendered = result if isinstance(result, str) else _json(result)
            parts.append(f"{label}:\n```\n{rendered}\n```")
        if block.get("workflow") is not None:
            parts.append(
                "workflow:\n```json\n" + _json(block["workflow"]) + "\n```"
            )
        return "\n".join(parts)

    def _render_live(self, blocks: list[dict[str, Any]]) -> str:
        rendered = ["**live turn** · updating"]
        for block in blocks:
            kind = block.get("kind")
            if kind == "assistant":
                rendered.append(str(block.get("content") or ""))
            elif kind == "tool":
                rendered.append(self._render_tool_block(block))
            else:
                label = str(block.get("label") or "system")
                content = str(block.get("content") or "")
                rendered.append(
                    f"**system · `{label}`**"
                    + (f"\n```\n{content}\n```" if content else "")
                )
        text = "\n\n".join(part for part in rendered if part)
        if len(text) <= _MAX_LIVE_RENDER_CHARS:
            return text
        tail = text[-_MAX_LIVE_RENDER_CHARS:]
        return (
            "**live turn** · updating\n\n"
            "*Older live output is omitted; the completed turn is mirrored "
            "in full.*\n\n"
            + tail
        )
