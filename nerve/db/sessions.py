"""Session data access methods."""

from __future__ import annotations

import json
from datetime import datetime, timezone


class SessionStore:
    """Mixin providing session CRUD and lifecycle operations."""

    async def create_session(
        self,
        session_id: str,
        title: str | None = None,
        source: str = "web",
        metadata: dict | None = None,
        status: str = "created",
        parent_session_id: str | None = None,
        forked_from_message: str | None = None,
        backend: str = "claude",
        model: str | None = None,
        model_tier: str | None = None,
        reasoning_effort: str | None = None,
        model_pinned: bool = False,
        cwd: str | None = None,
    ) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT OR IGNORE INTO sessions
               (id, title, source, metadata, status, parent_session_id,
                forked_from_message, backend, model, model_tier,
                reasoning_effort, model_pinned, cwd, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (session_id, title or session_id, source,
             json.dumps(metadata or {}), status,
             parent_session_id, forked_from_message, backend, model, model_tier,
             reasoning_effort, 1 if model_pinned else 0, cwd,
             now, now),
        )
        return {
            "id": session_id, "title": title or session_id,
            "source": source, "status": status,
            "parent_session_id": parent_session_id,
            "backend": backend, "model": model, "model_tier": model_tier,
            "reasoning_effort": reasoning_effort,
            "model_pinned": 1 if model_pinned else 0, "cwd": cwd,
        }

    async def get_session(self, session_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM sessions WHERE id = ?", (session_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def list_sessions(
        self, limit: int = 50, include_archived: bool = False,
    ) -> list[dict]:
        """List sessions, most recently updated first.

        Starred sessions are always included regardless of ``limit``;
        the limit only bounds the non-starred ones.
        """
        if include_archived:
            query = (
                "SELECT * FROM sessions"
                " WHERE starred = 1"
                "    OR id IN (SELECT id FROM sessions WHERE starred = 0"
                "              ORDER BY updated_at DESC LIMIT ?)"
                " ORDER BY updated_at DESC"
            )
        else:
            query = (
                "SELECT * FROM sessions"
                " WHERE (starred = 1 AND status != 'archived')"
                "    OR id IN (SELECT id FROM sessions"
                "              WHERE starred = 0 AND status != 'archived'"
                "              ORDER BY updated_at DESC LIMIT ?)"
                " ORDER BY updated_at DESC"
            )
        async with self.db.execute(query, (limit,)) as cursor:
            return [dict(row) async for row in cursor]

    async def count_sessions(self, include_archived: bool = False) -> int:
        """Count sessions without loading them. Used by diagnostics."""
        if include_archived:
            sql = "SELECT COUNT(*) FROM sessions"
            params: tuple = ()
        else:
            sql = "SELECT COUNT(*) FROM sessions WHERE status != 'archived'"
            params = ()
        async with self.db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def search_sessions(self, query: str, limit: int = 100) -> list[dict]:
        """Search sessions by title (LIKE match), across all non-archived sessions."""
        sql = (
            "SELECT * FROM sessions "
            "WHERE title LIKE ? AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT ?"
        )
        async with self.db.execute(sql, (f"%{query}%", limit)) as cursor:
            return [dict(row) async for row in cursor]

    async def get_model_routing_audit_batch(
        self, limit: int = 20,
    ) -> tuple[dict, list[dict]]:
        """Return new interactive Codex sessions after the durable cursor."""
        async with self.db.execute(
            "SELECT * FROM model_routing_audit_state WHERE id = 1"
        ) as cursor:
            row = await cursor.fetchone()
        state = dict(row) if row else {
            "cursor_updated_at": "",
            "cursor_session_id": "",
            "last_run_at": None,
            "last_summary": None,
        }
        updated_at = state.get("cursor_updated_at") or ""
        session_id = state.get("cursor_session_id") or ""
        async with self.db.execute(
            """
            SELECT * FROM sessions
            WHERE backend = 'codex'
              AND source NOT IN ('cron', 'hook', 'external')
              AND (
                    updated_at > ?
                    OR (updated_at = ? AND id > ?)
              )
            ORDER BY updated_at ASC, id ASC
            LIMIT ?
            """,
            (updated_at, updated_at, session_id, max(1, min(limit, 100))),
        ) as cursor:
            sessions = [dict(item) async for item in cursor]
        return state, sessions

    async def advance_model_routing_audit_cursor(
        self,
        *,
        updated_at: str,
        session_id: str,
        summary: str,
    ) -> None:
        """Commit a completed audit cursor without allowing it to move back."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """
            UPDATE model_routing_audit_state
            SET cursor_updated_at = ?,
                cursor_session_id = ?,
                last_run_at = ?,
                last_summary = ?
            WHERE id = 1
              AND (
                    cursor_updated_at < ?
                    OR (
                    cursor_updated_at = ?
                        AND cursor_session_id < ?
                    )
              )
            """,
            (
                updated_at, session_id, now, summary[:2000],
                updated_at, updated_at, session_id,
            ),
        )

    async def touch_session(self, session_id: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            "UPDATE sessions SET updated_at = ? WHERE id = ?", (now, session_id)
        )

    async def update_session_title(self, session_id: str, title: str) -> None:
        await self._write(
            "UPDATE sessions SET title = ? WHERE id = ?", (title, session_id)
        )

    async def delete_session(self, session_id: str) -> None:
        async with self._atomic():
            await self.db.execute(
                "DELETE FROM session_run_recovery WHERE session_id = ?",
                (session_id,),
            )
            await self.db.execute("DELETE FROM session_file_snapshots WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM session_events WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM session_usage WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM channel_sessions WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            await self.db.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    async def update_session_metadata(self, session_id: str, metadata: dict) -> None:
        """Update the metadata JSON for a session.

        Also syncs dedicated columns (sdk_session_id, connected_at) for
        backward compatibility with callers that still use the metadata blob.
        Both updates run in one transaction (the column sync is inlined —
        calling :meth:`update_session_fields` here would deadlock on the
        non-reentrant write lock).
        """
        col_sync: dict[str, str | None] = {}
        if "sdk_session_id" in metadata:
            col_sync["sdk_session_id"] = metadata["sdk_session_id"]
        if "connected_at" in metadata:
            col_sync["connected_at"] = metadata["connected_at"]
        built = self._build_session_fields_update(session_id, col_sync)
        async with self._atomic():
            await self.db.execute(
                "UPDATE sessions SET metadata = ? WHERE id = ?",
                (json.dumps(metadata), session_id),
            )
            if built is not None:
                await self.db.execute(*built)

    @staticmethod
    def _build_session_fields_update(
        session_id: str, fields: dict,
    ) -> tuple[str, tuple] | None:
        """Build the (sql, params) for a whitelisted session-columns UPDATE.

        Returns ``None`` when no allowed field is present. Shared by
        :meth:`update_session_fields` and :meth:`update_session_metadata`.

        Deliberately does NOT touch ``updated_at``: it means "last message
        activity" and is bumped only by the ``add_message*`` methods (and
        the explicit :meth:`touch_session`). Metadata writes — status flips,
        open/switch bookkeeping, star/rename, memorization watermarks — must
        not reorder the session list.
        """
        allowed = {
            "status", "sdk_session_id", "connected_at", "last_activity_at",
            "archived_at", "title", "message_count", "total_cost_usd",
            "parent_session_id", "forked_from_message", "last_memorized_at",
            "starred", "model", "model_tier", "reasoning_effort",
            "model_pinned", "backend", "cwd",
        }
        set_clauses: list[str] = []
        params: list = []
        for key, value in fields.items():
            if key in allowed:
                set_clauses.append(f"{key} = ?")
                params.append(value)
        if not set_clauses:
            return None
        params.append(session_id)
        return (
            f"UPDATE sessions SET {', '.join(set_clauses)} WHERE id = ?",
            tuple(params),
        )

    async def update_session_fields(self, session_id: str, fields: dict) -> None:
        """Update specific session columns atomically. Merges, doesn't replace."""
        built = self._build_session_fields_update(session_id, fields)
        if built is None:
            return
        await self._write(*built)

    async def bind_native_thread(
        self, backend: str, native_thread_id: str, session_id: str,
    ) -> None:
        """Bind one backend-native thread to exactly one Nerve session.

        The unique backend/session constraint prevents a session from silently
        switching native threads while the primary key prevents rollout/MCP
        ingestion from creating a duplicate satellite for a Nerve-owned thread.
        """
        if not backend or not native_thread_id or not session_id:
            raise ValueError("backend, native_thread_id, and session_id are required")
        async with self._atomic():
            await self.db.execute(
                "DELETE FROM session_native_threads "
                "WHERE backend = ? AND session_id = ? AND native_thread_id != ?",
                (backend, session_id, native_thread_id),
            )
            await self.db.execute(
                """INSERT INTO session_native_threads
                       (backend, native_thread_id, session_id, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(backend, native_thread_id) DO UPDATE SET
                       session_id = excluded.session_id,
                       updated_at = excluded.updated_at""",
                (backend, native_thread_id, session_id),
            )

    async def get_session_for_native_thread(
        self, backend: str, native_thread_id: str,
    ) -> str | None:
        async with self.db.execute(
            "SELECT session_id FROM session_native_threads "
            "WHERE backend = ? AND native_thread_id = ?",
            (backend, native_thread_id),
        ) as cursor:
            row = await cursor.fetchone()
        return str(row[0]) if row else None

    async def get_sessions_with_metadata_key(self, key: str) -> list[dict]:
        """Find sessions whose metadata JSON contains a specific key.

        Legacy method — prefer querying dedicated columns instead.
        """
        async with self.db.execute("SELECT * FROM sessions") as cursor:
            results = []
            async for row in cursor:
                d = dict(row)
                meta = json.loads(d.get("metadata", "{}") or "{}")
                if key in meta:
                    d["_parsed_metadata"] = meta
                    results.append(d)
            return results

    async def archive_session(self, old_id: str, new_id: str) -> None:
        """Rename a session (move messages to new ID)."""
        async with self._atomic():
            await self.db.execute("UPDATE session_events SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE messages SET session_id = ? WHERE session_id = ?", (new_id, old_id))
            await self.db.execute("UPDATE sessions SET id = ? WHERE id = ?", (new_id, old_id))

    # --- Session lifecycle operations (V3) ---

    async def log_session_event(
        self, session_id: str, event_type: str, details: dict | None = None,
    ) -> int:
        """Log a session lifecycle event."""
        now = datetime.now(timezone.utc).isoformat()
        result = await self._write(
            "INSERT INTO session_events (session_id, event_type, details, created_at) VALUES (?, ?, ?, ?)",
            (session_id, event_type, json.dumps(details) if details else None, now),
        )
        return result.lastrowid

    async def get_session_events(
        self, session_id: str, limit: int = 50,
    ) -> list[dict]:
        """Get lifecycle events for a session, newest first."""
        async with self.db.execute(
            "SELECT * FROM session_events WHERE session_id = ? ORDER BY created_at DESC LIMIT ?",
            (session_id, limit),
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (json.JSONDecodeError, TypeError):
                    pass
        return rows

    # --- Restart recovery checkpoints (V43) ---

    async def set_session_run_recovery(
        self,
        session_id: str,
        *,
        source: str,
        channel: str | None,
        user_message: str,
        channel_context: dict | None,
    ) -> None:
        """Checkpoint one in-flight engine turn."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT INTO session_run_recovery
                   (session_id, source, channel, user_message, channel_context,
                    started_at, recovery_attempts, last_recovery_at)
               VALUES (?, ?, ?, ?, ?, ?, 0, NULL)
               ON CONFLICT(session_id) DO UPDATE SET
                   source = excluded.source,
                   channel = excluded.channel,
                   user_message = excluded.user_message,
                   channel_context = excluded.channel_context,
                   started_at = excluded.started_at,
                   recovery_attempts = 0,
                   last_recovery_at = NULL""",
            (
                session_id,
                source,
                channel,
                user_message,
                json.dumps(channel_context) if channel_context else None,
                now,
            ),
        )

    async def list_session_run_recoveries(self) -> list[dict]:
        """Return interrupted turns, oldest first."""
        async with self.db.execute(
            "SELECT * FROM session_run_recovery ORDER BY started_at, session_id"
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        for row in rows:
            raw_context = row.get("channel_context")
            if raw_context:
                try:
                    row["channel_context"] = json.loads(raw_context)
                except (json.JSONDecodeError, TypeError):
                    row["channel_context"] = None
        return rows

    async def get_session_run_recovery(
        self, session_id: str,
    ) -> dict | None:
        async with self.db.execute(
            "SELECT * FROM session_run_recovery WHERE session_id = ?",
            (session_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        raw_context = result.get("channel_context")
        if raw_context:
            try:
                result["channel_context"] = json.loads(raw_context)
            except (json.JSONDecodeError, TypeError):
                result["channel_context"] = None
        return result

    async def mark_session_run_recovering(self, session_id: str) -> None:
        """Record a startup recovery attempt without consuming its checkpoint."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """UPDATE session_run_recovery
               SET recovery_attempts = recovery_attempts + 1,
                   last_recovery_at = ?
               WHERE session_id = ?""",
            (now, session_id),
        )

    async def append_session_run_recovery_input(
        self,
        session_id: str,
        *,
        user_message: str,
        channel: str | None,
        channel_context: dict | None,
    ) -> None:
        """Include accepted steering input in the current checkpoint."""
        suffix = f"\n\n[Steered follow-up]\n{user_message}"
        await self._write(
            """UPDATE session_run_recovery
               SET user_message = user_message || ?,
                   channel = COALESCE(?, channel),
                   channel_context = COALESCE(?, channel_context)
               WHERE session_id = ?""",
            (
                suffix,
                channel,
                json.dumps(channel_context) if channel_context else None,
                session_id,
            ),
        )

    async def clear_session_run_recovery(self, session_id: str) -> None:
        """Delete a checkpoint after a definitive turn outcome."""
        await self._write(
            "DELETE FROM session_run_recovery WHERE session_id = ?",
            (session_id,),
        )

    # --- Durable Discord delivery binding (V44) ---

    async def get_discord_session_binding(
        self, session_id: str,
    ) -> dict | None:
        """Return the immutable Discord target bound to ``session_id``."""
        async with self.db.execute(
            """SELECT session_id, guild_id, thread_id, created_at
               FROM discord_session_bindings
               WHERE session_id = ?""",
            (session_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None

    async def bind_discord_session(
        self,
        session_id: str,
        *,
        guild_id: str | int,
        thread_id: str | int,
    ) -> dict:
        """Bind a session to one Discord thread, refusing later retargeting.

        The target is transport-owned input from the Discord gateway, never an
        agent tool argument. ``INSERT OR IGNORE`` makes concurrent first
        messages safe; the read-back comparison enforces immutability.
        """
        guild = str(guild_id).strip()
        thread = str(thread_id).strip()
        if not guild.isdigit() or not thread.isdigit():
            raise ValueError(
                "Discord session binding requires numeric guild and thread IDs"
            )

        await self._write(
            """INSERT OR IGNORE INTO discord_session_bindings
                   (session_id, guild_id, thread_id)
               VALUES (?, ?, ?)""",
            (session_id, guild, thread),
        )
        binding = await self.get_discord_session_binding(session_id)
        if binding is None:
            raise RuntimeError(
                f"Discord session binding was not persisted for {session_id}"
            )
        if (
            str(binding["guild_id"]) != guild
            or str(binding["thread_id"]) != thread
        ):
            raise ValueError(
                f"Session {session_id} is already bound to another Discord thread"
            )
        return binding

    # --- Channel session mapping (V3) ---

    async def get_channel_session(self, channel_key: str) -> dict | None:
        """Get the persisted session for a channel."""
        async with self.db.execute(
            "SELECT * FROM channel_sessions WHERE channel_key = ?", (channel_key,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def set_channel_session(self, channel_key: str, session_id: str) -> None:
        """Persist a channel-to-session mapping."""
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            "INSERT OR REPLACE INTO channel_sessions (channel_key, session_id, updated_at) VALUES (?, ?, ?)",
            (channel_key, session_id, now),
        )

    # --- Session cleanup queries (V3) ---

    async def get_sessions_by_status(self, statuses: list[str]) -> list[dict]:
        """Find sessions with any of the given statuses."""
        placeholders = ",".join("?" for _ in statuses)
        async with self.db.execute(
            f"SELECT * FROM sessions WHERE status IN ({placeholders})",
            tuple(statuses),
        ) as cursor:
            return [dict(row) async for row in cursor]

    async def get_stale_sessions(
        self, before_iso: str, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get idle/stopped/error sessions not updated since before_iso.

        Starred sessions are excluded: a starred session is never
        auto-archived, regardless of how long it has been idle.
        """
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND starred = 0
                AND updated_at < ?
                AND id NOT IN ({placeholders})
            """
            params = (before_iso, *excludes)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status IN ('idle', 'stopped', 'error')
                AND starred = 0
                AND updated_at < ?
            """
            params = (before_iso,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def get_stale_interactive_sessions(
        self, before_iso: str, sources: list[str],
    ) -> list[dict]:
        """Idle/stopped/error *interactive* sessions not updated since before_iso.

        Scoped by ``source`` (web/telegram/…) so cron and persistent sessions
        keep their own long rotation and are never archived at the short
        interactive cutoff. Starred sessions are also excluded — a starred
        session is never auto-archived. Returns [] when ``sources`` is empty.
        """
        if not sources:
            return []
        src = ",".join("?" for _ in sources)
        query = f"""
            SELECT * FROM sessions
            WHERE status IN ('idle', 'stopped', 'error')
              AND starred = 0
              AND source IN ({src})
              AND updated_at < ?
        """
        async with self.db.execute(query, (*sources, before_iso)) as cursor:
            return [dict(row) async for row in cursor]

    async def count_active_sessions(self, exclude_starred: bool = False) -> int:
        """Count non-archived sessions.

        When ``exclude_starred`` is set, starred sessions are omitted: they are
        off-budget for the ``max_sessions`` cap, so the overflow check counts
        only cap-eligible sessions.
        """
        query = "SELECT COUNT(*) FROM sessions WHERE status != 'archived'"
        if exclude_starred:
            query += " AND starred = 0"
        async with self.db.execute(query) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_oldest_sessions(
        self, count: int, exclude_ids: list[str] | None = None,
    ) -> list[dict]:
        """Get the oldest non-active, non-archived sessions for cleanup.

        Starred sessions are excluded: they are never evicted to enforce the
        session-count limit.
        """
        excludes = exclude_ids or []
        if excludes:
            placeholders = ",".join("?" for _ in excludes)
            query = f"""
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                AND starred = 0
                AND id NOT IN ({placeholders})
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (*excludes, count)
        else:
            query = """
                SELECT * FROM sessions
                WHERE status NOT IN ('active', 'archived')
                AND starred = 0
                ORDER BY updated_at ASC LIMIT ?
            """
            params = (count,)
        async with self.db.execute(query, params) as cursor:
            return [dict(row) async for row in cursor]

    async def increment_message_count(self, session_id: str) -> None:
        """Atomically increment the message counter for a session."""
        await self._write(
            "UPDATE sessions SET message_count = COALESCE(message_count, 0) + 1 WHERE id = ?",
            (session_id,),
        )

    async def get_sessions_needing_memorization(self) -> list[dict]:
        """Find non-archived sessions that have un-memorized messages.

        Returns sessions where:
        - status is not 'archived'
        - message_count > 0
        - last_memorized_at is NULL (never memorized) OR
          messages exist with created_at > last_memorized_at

        The ``last_memorized_at`` value is normalised in the comparison to
        match SQLite's ``CURRENT_TIMESTAMP`` format (``YYYY-MM-DD HH:MM:SS``)
        so the string comparison works correctly regardless of the stored
        format (ISO 8601 with ``T``/``Z`` or plain space-separated).
        """
        async with self.db.execute("""
            SELECT s.* FROM sessions s
            WHERE s.status != 'archived'
            AND s.message_count > 0
            AND (
                s.last_memorized_at IS NULL
                OR EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.session_id = s.id
                    AND m.created_at > SUBSTR(
                        REPLACE(REPLACE(s.last_memorized_at, 'T', ' '), 'Z', ''),
                        1, 19
                    )
                )
            )
        """) as cursor:
            return [dict(row) async for row in cursor]

    async def get_last_telegram_channel_key(self) -> str | None:
        """Get the most recently updated telegram:* channel key."""
        async with self.db.execute(
            "SELECT channel_key FROM channel_sessions WHERE channel_key LIKE 'telegram:%' ORDER BY updated_at DESC LIMIT 1",
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
