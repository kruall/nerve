"""Persistence for restart-safe Discord project-task completion audits."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def _decode(value: Any, default: Any) -> Any:
    try:
        decoded = json.loads(value or "")
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return decoded


class DiscordProjectTaskAuditStore:
    """Mixin for the audit cursor, baseline, completion evidence, and results."""

    async def get_discord_project_task_audit_state(self) -> dict[str, Any]:
        async with self.db.execute(
            "SELECT * FROM discord_project_task_audit_state WHERE id = 1"
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return {
                "id": 1,
                "activated_at": "",
                "baseline_initialized": 0,
                "baseline_thread_ids": [],
                "last_run_at": None,
                "last_summary": None,
            }
        result = dict(row)
        baseline = _decode(result.get("baseline_thread_ids"), [])
        result["baseline_thread_ids"] = (
            [str(item) for item in baseline] if isinstance(baseline, list) else []
        )
        return result

    async def initialize_discord_project_task_audit_baseline(
        self, thread_ids: list[str],
    ) -> None:
        """Record the first observed completed set exactly once."""
        await self._write(
            """UPDATE discord_project_task_audit_state
               SET baseline_initialized = 1,
                   baseline_thread_ids = ?
               WHERE id = 1 AND baseline_initialized = 0""",
            (json.dumps(sorted({str(item) for item in thread_ids})),),
        )

    async def list_discord_project_task_completion_approvals(
        self,
    ) -> list[dict[str, Any]]:
        """Return successful completion approvals, newest first.

        The Discord button result is not enough by itself: the dispatcher
        outcome in metadata must also confirm that the tag/archive mutation
        succeeded.
        """
        async with self.db.execute(
            """SELECT * FROM notifications
               WHERE target_kind = 'discord-project-task-completion'
                 AND answer = 'approve'
                 AND status = 'answered'
               ORDER BY COALESCE(answered_at, created_at) DESC"""
        ) as cursor:
            rows = [dict(row) async for row in cursor]
        result: list[dict[str, Any]] = []
        for row in rows:
            metadata = _decode(row.get("metadata"), {})
            if not isinstance(metadata, dict):
                metadata = {}
            outcome = metadata.get("approval_dispatch")
            if not isinstance(outcome, dict):
                # Keep compatibility with early persisted/test records.
                outcome = metadata.get("dispatch_outcome")
            if not isinstance(outcome, dict) or not outcome.get("ok"):
                continue
            row["metadata_decoded"] = metadata
            row["completion_at"] = row.get("answered_at") or row.get("created_at")
            result.append(row)
        return result

    async def get_discord_project_task_audit(
        self, thread_id: str | int,
    ) -> dict[str, Any] | None:
        async with self.db.execute(
            "SELECT * FROM discord_project_task_audits WHERE thread_id = ?",
            (str(thread_id),),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        result["completion_record"] = _decode(
            result.get("completion_record"), {},
        )
        follow_ups = _decode(result.get("follow_up_task_ids"), [])
        result["follow_up_task_ids"] = (
            [str(item) for item in follow_ups] if isinstance(follow_ups, list) else []
        )
        return result

    async def record_discord_project_task_audit(
        self,
        *,
        thread_id: str | int,
        guild_id: str | int,
        project: str,
        completion_notification_id: str = "",
        completion_record: dict[str, Any] | None = None,
        result: str,
        summary: str,
        follow_up_task_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        if result not in {"verified", "follow-up-created"}:
            raise ValueError("Unsupported Discord project-task audit result")
        now = datetime.now(timezone.utc).isoformat()
        await self._write(
            """INSERT OR IGNORE INTO discord_project_task_audits
                   (thread_id, guild_id, project, completion_notification_id,
                    completion_record, result, summary, follow_up_task_ids,
                    created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(thread_id), str(guild_id), project,
                completion_notification_id,
                json.dumps(completion_record or {}, ensure_ascii=False),
                result, summary[:4000],
                json.dumps(
                    [str(item) for item in (follow_up_task_ids or [])],
                    ensure_ascii=False,
                ),
                now, now,
            ),
        )
        await self._write(
            """UPDATE discord_project_task_audit_state
               SET last_run_at = ?, last_summary = ?
               WHERE id = 1""",
            (now, summary[:4000]),
        )
        stored = await self.get_discord_project_task_audit(thread_id)
        if stored is None:
            raise RuntimeError("Discord project-task audit result was not persisted")
        return stored

    async def get_discord_session_binding_by_thread(
        self, guild_id: str | int, thread_id: str | int,
    ) -> dict[str, Any] | None:
        """Resolve the immutable binding, with a legacy channel fallback."""
        guild = str(guild_id)
        thread = str(thread_id)
        async with self.db.execute(
            """SELECT session_id, guild_id, thread_id, created_at
               FROM discord_session_bindings
               WHERE guild_id = ? AND thread_id = ?
               ORDER BY created_at DESC
               LIMIT 1""",
            (guild, thread),
        ) as cursor:
            row = await cursor.fetchone()
        if row is not None:
            return dict(row)
        async with self.db.execute(
            """SELECT session_id, ? AS guild_id, ? AS thread_id, updated_at
               FROM channel_sessions
               WHERE channel_key = ?""",
            (guild, thread, f"discord:{guild}:{thread}"),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        result = dict(row)
        result["created_at"] = result.pop("updated_at", None)
        return result
