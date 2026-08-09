"""Durable state for pinned, stage-by-stage workflow presets."""
from __future__ import annotations

import json
from typing import Any, Mapping

from nerve.utils.time import utc_now_iso

ACTIVE_PRESET_WORKFLOWS = ("queued", "running", "cancelling")
TERMINAL_PRESET_WORKFLOWS = ("succeeded", "failed", "cancelled", "lost", "blocked")
ACTIVE_STAGE_RUNS = ("queued", "starting", "running", "cancelling")
TERMINAL_STAGE_RUNS = ("succeeded", "failed", "cancelled", "lost")


def _row(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    result = dict(value)
    for name in ("plan", "spec", "result", "artifact"):
        if result.get(name) is not None:
            try: result[name] = json.loads(result[name])
            except (TypeError, ValueError): result[name] = {}
    return result


class PresetWorkflowStore:
    async def create_preset_workflow(self, workflow_id: str, *, session_id: str, plan: Mapping[str, Any], preset_hash: str, spec_hash: str) -> dict:
        now = utc_now_iso()
        await self._write("""INSERT INTO preset_workflows
            (id, observer_session_id, plan, preset_hash, spec_hash, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)""",
            (workflow_id, session_id, json.dumps(dict(plan)), preset_hash, spec_hash, now, now))
        return (await self.get_preset_workflow(workflow_id))  # type: ignore[return-value]

    async def get_preset_workflow(self, workflow_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM preset_workflows WHERE id = ?", (workflow_id,)) as c:
            return _row(await c.fetchone())

    async def list_preset_workflows(self, *, limit: int = 100) -> list[dict]:
        async with self.db.execute("SELECT * FROM preset_workflows ORDER BY created_at DESC LIMIT ?", (limit,)) as c:
            return [_row(row) async for row in c]  # type: ignore[misc]

    async def count_preset_workflows(self) -> int:
        async with self.db.execute("SELECT COUNT(*) FROM preset_workflows") as c:
            return int((await c.fetchone())[0])

    async def active_preset_workflows(self) -> list[dict]:
        marks = ",".join("?" for _ in ACTIVE_PRESET_WORKFLOWS)
        async with self.db.execute(f"SELECT * FROM preset_workflows WHERE status IN ({marks}) ORDER BY created_at", ACTIVE_PRESET_WORKFLOWS) as c:
            return [_row(row) async for row in c]  # type: ignore[misc]

    async def pending_preset_workflow_completions(self) -> list[dict]:
        """Terminal workflows whose durable observer continuation was not claimed."""
        async with self.db.execute("""SELECT w.* FROM preset_workflows AS w
            JOIN workflow_completion_outbox AS o ON o.workflow_id = w.id
            WHERE w.status IN ('succeeded', 'failed', 'lost', 'blocked')
              AND o.state = 'pending' ORDER BY w.finished_at""") as c:
            return [_row(row) async for row in c]  # type: ignore[misc]

    async def get_preset_workflow_completion(self, workflow_id: str) -> dict[str, Any] | None:
        """Return the durable observer-delivery state for one workflow."""
        async with self.db.execute(
            """SELECT state, claimed_at, completed_at, error, created_at, updated_at
               FROM workflow_completion_outbox WHERE workflow_id = ?""",
            (workflow_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def claim_preset_workflow_completion(self, workflow_id: str) -> bool:
        """Atomically claim a pending observer continuation."""
        now = utc_now_iso()
        outcome = await self._write(
            """UPDATE workflow_completion_outbox
               SET state = 'claimed', claimed_at = ?, updated_at = ?
               WHERE workflow_id = ? AND state = 'pending'""",
            (now, now, workflow_id),
        )
        return bool(outcome.rowcount)

    async def settle_preset_workflow_completion(
        self, workflow_id: str, *, success: bool, error: str | None = None,
    ) -> bool:
        """Persist the result before callers publish a completion event."""
        now = utc_now_iso()
        outcome = await self._write(
            """UPDATE workflow_completion_outbox
               SET state = ?, completed_at = ?, error = ?, updated_at = ?
               WHERE workflow_id = ? AND state = 'claimed'""",
            (
                "completed" if success else "failed", now,
                None if success else (error or "continuation failed")[:1000], now,
                workflow_id,
            ),
        )
        return bool(outcome.rowcount)

    async def fail_claimed_preset_workflow_completions_on_restart(self) -> int:
        """Record uncertain claims rather than risking a duplicate observer turn."""
        now = utc_now_iso()
        outcome = await self._write(
            """UPDATE workflow_completion_outbox
               SET state = 'failed', completed_at = ?,
                   error = 'daemon restarted after completion claim', updated_at = ?
               WHERE state = 'claimed'""",
            (now, now),
        )
        return int(outcome.rowcount or 0)

    async def active_preset_workflow_count(self, session_id: str) -> int:
        marks = ",".join("?" for _ in ACTIVE_PRESET_WORKFLOWS)
        async with self.db.execute(f"SELECT COUNT(*) FROM preset_workflows WHERE observer_session_id = ? AND status IN ({marks})", (session_id, *ACTIVE_PRESET_WORKFLOWS)) as c:
            return int((await c.fetchone())[0])

    async def transition_preset_workflow(self, workflow_id: str, *, to_status: str, expect: tuple[str, ...] = ACTIVE_PRESET_WORKFLOWS, result: Mapping[str, Any] | None = None) -> bool:
        if to_status not in (*ACTIVE_PRESET_WORKFLOWS, *TERMINAL_PRESET_WORKFLOWS): raise ValueError("unknown workflow status")
        now = utc_now_iso(); marks = ",".join("?" for _ in expect)
        fields = ["status = ?", "updated_at = ?", "revision = revision + 1"]
        params: list[Any] = [to_status, now]
        if result is not None: fields.append("result = ?"); params.append(json.dumps(dict(result)))
        if to_status in TERMINAL_PRESET_WORKFLOWS: fields.append("finished_at = ?"); params.append(now)
        if to_status == "running": fields.append("started_at = COALESCE(started_at, ?)"); params.append(now)
        params.extend([workflow_id, *expect])
        outcome = await self._write(f"UPDATE preset_workflows SET {', '.join(fields)} WHERE id = ? AND status IN ({marks})", tuple(params))
        return bool(outcome.rowcount)

    async def terminalize_preset_workflow(self, workflow_id: str, *, to_status: str,
                                          result: Mapping[str, Any],
                                          expect: tuple[str, ...] = ACTIVE_PRESET_WORKFLOWS) -> bool:
        """CAS a workflow to terminal and create its outbox record atomically."""
        if to_status not in TERMINAL_PRESET_WORKFLOWS:
            raise ValueError("terminal status required")
        now = utc_now_iso(); marks = ",".join("?" for _ in expect)
        outbox_state = "suppressed" if to_status == "cancelled" else "pending"
        async with self._atomic():
            cursor = await self.db.execute(
                f"""UPDATE preset_workflows SET status=?, result=?, finished_at=?,
                    updated_at=?, revision=revision+1
                    WHERE id=? AND status IN ({marks})""",
                (to_status, json.dumps(dict(result)), now, now, workflow_id, *expect),
            )
            changed = bool(cursor.rowcount)
            await cursor.close()
            if changed:
                await self.db.execute(
                    """INSERT OR IGNORE INTO workflow_completion_outbox
                    (workflow_id,state,created_at,updated_at) VALUES (?, ?, ?, ?)""",
                    (workflow_id, outbox_state, now, now),
                )
        return changed

    async def create_stage_run(self, stage_run_id: str, *, workflow_id: str, stage_id: str, runner: str, spec: Mapping[str, Any], spec_hash: str) -> dict:
        now = utc_now_iso()
        await self._write("""INSERT INTO workflow_stage_runs
          (id, workflow_id, stage_id, runner, spec, spec_hash, status, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?)""", (stage_run_id, workflow_id, stage_id, runner, json.dumps(dict(spec)), spec_hash, now, now))
        return (await self.get_stage_run(stage_run_id))  # type: ignore[return-value]

    async def get_stage_run(self, stage_run_id: str) -> dict | None:
        async with self.db.execute("SELECT * FROM workflow_stage_runs WHERE id = ?", (stage_run_id,)) as c: return _row(await c.fetchone())

    async def list_stage_runs(self, workflow_id: str) -> list[dict]:
        async with self.db.execute("SELECT * FROM workflow_stage_runs WHERE workflow_id = ? ORDER BY created_at", (workflow_id,)) as c: return [_row(row) async for row in c]  # type: ignore[misc]

    async def transition_stage_run(self, stage_run_id: str, *, to_status: str, expect: tuple[str, ...] = ACTIVE_STAGE_RUNS, child_type: str | None = None, child_id: str | None = None, result: Mapping[str, Any] | None = None, artifact: Mapping[str, Any] | None = None) -> bool:
        if to_status not in (*ACTIVE_STAGE_RUNS, *TERMINAL_STAGE_RUNS): raise ValueError("unknown stage status")
        now = utc_now_iso(); marks = ",".join("?" for _ in expect); fields=["status = ?", "updated_at = ?", "revision = revision + 1"]; params: list[Any]=[to_status, now]
        for name, value in (("child_type", child_type), ("child_id", child_id)):
            if value is not None: fields.append(f"{name} = ?"); params.append(value)
        for name, value in (("result", result), ("artifact", artifact)):
            if value is not None: fields.append(f"{name} = ?"); params.append(json.dumps(dict(value)))
        if to_status in TERMINAL_STAGE_RUNS: fields.append("finished_at = ?"); params.append(now)
        if to_status == "running": fields.append("started_at = COALESCE(started_at, ?)"); params.append(now)
        params.extend([stage_run_id, *expect]); outcome=await self._write(f"UPDATE workflow_stage_runs SET {', '.join(fields)} WHERE id = ? AND status IN ({marks})", tuple(params)); return bool(outcome.rowcount)
