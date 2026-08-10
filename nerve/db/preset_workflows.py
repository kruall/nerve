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
    async def create_preset_workflow_with_parent_operation(
        self, workflow_id: str, parent_operation_id: str, *, session_id: str,
        plan: Mapping[str, Any], preset_hash: str, spec_hash: str,
        parent_plan: Mapping[str, Any], handle_ids: tuple[str, ...] = (),
        allocation_state: str = "finalized",
    ) -> dict:
        """Atomically create a workflow and its private handle-owning parent.

        This is deliberately a persistence primitive: callers must acquire
        handles through a durable intent before entering this transaction.
        The parent has no dispatcher-visible work, and its ordered refs are
        the sole proof that the workflow owns each retained handle.
        """
        if len(handle_ids) != len(set(handle_ids)):
            raise ValueError("resource handle ids must be distinct")
        if allocation_state not in ("pending", "finalized"):
            raise ValueError("invalid workflow allocation state")
        now = utc_now_iso()
        async with self._atomic():
            await self.db.execute(
                """INSERT INTO executions
                   (id, session_id, kind, profile_version, profile_hash,
                    profile_snapshot, plan, resource_requests, selected_leases,
                    completion_target_type, completion_target_id, auto_continue,
                    private_operation, status, created_at, queued_at, updated_at)
                   VALUES (?, ?, 'workflow_parent', '1', ?, '{}', ?, '[]', '[]',
                           'workflow', ?, 0, 1, 'queued', ?, ?, ?)""",
                (parent_operation_id, session_id, spec_hash, json.dumps(dict(parent_plan)),
                 workflow_id, now, now, now),
            )
            await self.db.execute(
                """INSERT INTO preset_workflows
                   (id, observer_session_id, parent_operation_id, plan, preset_hash,
                    spec_hash, status, allocation_state, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (workflow_id, session_id, parent_operation_id, json.dumps(dict(plan)),
                 preset_hash, spec_hash, allocation_state, now, now),
            )
            for position, handle_id in enumerate(handle_ids):
                cursor = await self.db.execute(
                    """INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at)
                       SELECT ?, h.id, ?, ? FROM session_resource_handles AS h
                       JOIN resource_leases AS l ON l.id=h.lease_id
                       WHERE h.id=? AND h.session_id=? AND h.state='active'
                         AND l.session_id=h.session_id AND l.host_id=h.host_id
                         AND l.fencing_token=h.fencing_token AND l.state='active'""",
                    (parent_operation_id, position, now, handle_id, session_id),
                )
                if not cursor.rowcount:
                    await cursor.close()
                    raise ValueError("resource handle does not belong to this session")
                await cursor.close()
        row = await self.get_preset_workflow(workflow_id)
        assert row is not None
        return row

    async def finalize_preset_workflow_parent_allocation(
        self, workflow_id: str, *, parent_plan: Mapping[str, Any], handle_ids: tuple[str, ...],
    ) -> dict:
        """Atomically attach an already-acquired bundle to its durable parent."""
        if len(handle_ids) != len(set(handle_ids)):
            raise ValueError("resource handle ids must be distinct")
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute(
                "SELECT observer_session_id, parent_operation_id, status, allocation_state FROM preset_workflows WHERE id=?",
                (workflow_id,),
            ) as cursor:
                workflow = await cursor.fetchone()
            if (workflow is None or workflow["status"] != "queued"
                    or workflow["allocation_state"] != "pending" or not workflow["parent_operation_id"]):
                raise ValueError("workflow allocation intent is unavailable")
            parent_id, session_id = workflow["parent_operation_id"], workflow["observer_session_id"]
            await self.db.execute("UPDATE executions SET plan=?, updated_at=?, revision=revision+1 WHERE id=? AND private_operation=1 AND status='queued'",
                                  (json.dumps(dict(parent_plan)), now, parent_id))
            for position, handle_id in enumerate(handle_ids):
                cursor = await self.db.execute(
                    """INSERT INTO operation_resource_refs(operation_id, handle_id, position, created_at)
                       SELECT ?, h.id, ?, ? FROM session_resource_handles h
                       JOIN resource_leases l ON l.id=h.lease_id
                       WHERE h.id=? AND h.session_id=? AND h.state='active'
                         AND l.session_id=h.session_id AND l.host_id=h.host_id
                         AND l.fencing_token=h.fencing_token AND l.state='active'""",
                    (parent_id, position, now, handle_id, session_id),
                )
                if not cursor.rowcount:
                    await cursor.close()
                    raise ValueError("resource handle does not belong to this session")
                await cursor.close()
            cursor = await self.db.execute(
                "UPDATE preset_workflows SET allocation_state='finalized', updated_at=?, revision=revision+1 "
                "WHERE id=? AND allocation_state='pending' AND status='queued'",
                (now, workflow_id),
            )
            if not cursor.rowcount:
                await cursor.close()
                raise ValueError("workflow allocation intent is unavailable")
            await cursor.close()
        row = await self.get_preset_workflow(workflow_id)
        assert row is not None
        return row

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

    async def fail_unfinalized_preset_workflows_on_restart(self) -> list[str]:
        """Fail closed durable allocation intents that cannot be replayed safely.

        No handles are released here: an acquired handle not yet recorded in
        operation_resource_refs may be an explicit pre-existing handle.
        Session recovery remains responsible for such retained handles.
        """
        async with self.db.execute(
            "SELECT id FROM preset_workflows WHERE status='queued' AND allocation_state='pending'"
        ) as cursor:
            ids = [str(row[0]) async for row in cursor]
        for workflow_id in ids:
            changed = await self.terminalize_preset_workflow(
                workflow_id, to_status="failed", expect=("queued",),
                result={"outcome": "failed", "error": "allocation_interrupted"},
            )
            if changed:
                await self._write(
                    "UPDATE preset_workflows SET allocation_state='failed' WHERE id=?", (workflow_id,)
                )
        return ids

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
                # The private parent never reaches a backend.  Settling it in
                # this transaction gives session-final-stop and recovery one
                # durable terminal boundary shared with the workflow/outbox.
                await self.db.execute(
                    """UPDATE executions SET status=?, result=?, finished_at=?, updated_at=?,
                           revision=revision+1, continuation_state='suppressed'
                       WHERE id=(SELECT parent_operation_id FROM preset_workflows WHERE id=?)
                         AND private_operation=1
                         AND status IN ('queued','starting','running','cancelling')""",
                    (to_status if to_status != "blocked" else "failed",
                     json.dumps(dict(result)), now, now, workflow_id),
                )
                await self.db.execute(
                    """DELETE FROM operation_resource_refs
                       WHERE operation_id=(SELECT parent_operation_id FROM preset_workflows WHERE id=?)""",
                    (workflow_id,),
                )
                await self.db.execute(
                    """INSERT OR IGNORE INTO workflow_completion_outbox
                    (workflow_id,state,created_at,updated_at)
                    SELECT id, CASE WHEN ? = 'cancelled'
                       THEN 'suppressed' ELSE 'pending' END, ?, ?
                    FROM preset_workflows WHERE id = ?""",
                    (to_status, now, now, workflow_id),
                )
        return changed

    async def abandon_preset_workflow(self, workflow_id: str, *, revision: int, actor: str,
                                      reason: str | None, idempotency_key: str) -> str:
        """CAS a blocked workflow to cancelled and append its decision audit.

        Returns ``applied``, ``duplicate``, ``stale`` or ``not_available``.
        The terminal transition and audit are one transaction, so a retry never
        creates another observer continuation or another audit decision.
        """
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute("SELECT 1 FROM workflow_action_audit WHERE workflow_id=? AND idempotency_key=?", (workflow_id, idempotency_key)) as cursor:
                if await cursor.fetchone(): return "duplicate"
            cursor = await self.db.execute(
                """UPDATE preset_workflows SET status='cancelled', result=?, finished_at=?,
                    updated_at=?, revision=revision+1 WHERE id=? AND status='blocked' AND revision=?""",
                (json.dumps({"outcome":"abandoned"}), now, now, workflow_id, revision),
            )
            changed = bool(cursor.rowcount); await cursor.close()
            if not changed:
                async with self.db.execute("SELECT revision, status FROM preset_workflows WHERE id=?", (workflow_id,)) as cursor:
                    current = await cursor.fetchone()
                if current is None: return "not_available"
                return "stale" if current["revision"] != revision else "not_available"
            # Abandon is the blocked-workflow variant of terminalization.  The
            # private parent is still the durable owner of any retained
            # handles, so settle it and detach its refs in this same
            # transaction as the workflow, audit, and suppressed outbox.
            await self.db.execute(
                """UPDATE executions SET status='cancelled', result=?, finished_at=?,
                       updated_at=?, revision=revision+1, continuation_state='suppressed'
                   WHERE id=(SELECT parent_operation_id FROM preset_workflows WHERE id=?)
                     AND private_operation=1
                     AND status IN ('queued','starting','running','cancelling')""",
                (json.dumps({"outcome": "abandoned"}), now, now, workflow_id),
            )
            await self.db.execute(
                """DELETE FROM operation_resource_refs
                   WHERE operation_id=(SELECT parent_operation_id FROM preset_workflows WHERE id=?)""",
                (workflow_id,),
            )
            await self.db.execute("""INSERT INTO workflow_completion_outbox
                (workflow_id,state,created_at,updated_at) VALUES (?, 'suppressed', ?, ?)""", (workflow_id, now, now))
            await self.db.execute("""INSERT INTO workflow_action_audit
                (workflow_id,actor,action,reason,idempotency_key,prior_revision,resulting_status,created_at)
                VALUES (?, ?, 'abandon', ?, ?, ?, 'cancelled', ?)""",
                (workflow_id, actor, (reason or None), idempotency_key, revision, now))
        return "applied"

    async def request_cancel_preset_workflow(self, workflow_id: str, *, reason: str) -> bool:
        """Cancel a workflow and settle childless dispatch intents atomically."""
        now = utc_now_iso()
        async with self._atomic():
            cursor = await self.db.execute(
                """UPDATE preset_workflows SET status='cancelling', updated_at=?,
                       revision=revision+1 WHERE id=? AND status IN ('queued','running')""",
                (now, workflow_id),
            )
            changed = bool(cursor.rowcount)
            await cursor.close()
            if not changed:
                return False
            await self.db.execute(
                """UPDATE workflow_stage_runs
                   SET status='cancelled', result=?, finished_at=?, updated_at=?, revision=revision+1
                   WHERE workflow_id=? AND status IN ('queued','starting') AND child_id IS NULL""",
                (json.dumps({"outcome": "cancelled", "reason": reason}), now, now, workflow_id),
            )
        return True

    async def has_preset_workflow_action(self, workflow_id: str, idempotency_key: str) -> bool:
        async with self.db.execute("SELECT 1 FROM workflow_action_audit WHERE workflow_id=? AND idempotency_key=?", (workflow_id, idempotency_key)) as cursor:
            return bool(await cursor.fetchone())

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

    async def restore_workflow_child_handles(self, stage_run_id: str) -> bool:
        """Return a terminal execution child's borrowed refs to its parent.

        Parent positions are reconstructed from the immutable manifest, so a
        later stage always sees the same ordered subset after recovery.
        """
        now = utc_now_iso()
        async with self._atomic():
            async with self.db.execute("""SELECT s.workflow_id, s.stage_id, s.child_id,
                w.parent_operation_id, p.plan FROM workflow_stage_runs s
                JOIN preset_workflows w ON w.id=s.workflow_id
                JOIN executions p ON p.id=w.parent_operation_id WHERE s.id=?""", (stage_run_id,)) as c:
                row = await c.fetchone()
            if row is None or not row["child_id"]:
                return False
            plan = json.loads(row["plan"])
            handles = list(plan.get("stage_handle_manifest", {}).get(row["stage_id"], ()))
            async with self.db.execute("SELECT handle_id FROM operation_resource_refs WHERE operation_id=? ORDER BY position", (row["child_id"],)) as c:
                child_handles = [str(item[0]) async for item in c]
            if child_handles not in (handles, []):
                return False
            all_handles: list[str] = []
            for subset in plan.get("stage_handle_manifest", {}).values():
                for handle in subset:
                    if handle not in all_handles: all_handles.append(handle)
            parent_refs = {}
            async with self.db.execute(
                "SELECT handle_id, position FROM operation_resource_refs WHERE operation_id=?",
                (row["parent_operation_id"],),
            ) as c:
                parent_refs = {str(item[0]): int(item[1]) async for item in c}
            # A terminal execution normally detaches its own refs as part of
            # its finish transaction.  Treat that empty child projection as
            # an already-detached handoff and replay the parent insert
            # idempotently; a crash after the insert is likewise harmless.
            if child_handles:
                await self.db.execute("DELETE FROM operation_resource_refs WHERE operation_id=?", (row["child_id"],))
            for handle in handles:
                position = all_handles.index(handle)
                if parent_refs.get(handle) == position:
                    continue
                async with self.db.execute(
                    "SELECT operation_id FROM operation_resource_refs WHERE handle_id=?",
                    (handle,),
                ) as c:
                    owner = await c.fetchone()
                if owner is not None:
                    return False
                await self.db.execute(
                    "INSERT INTO operation_resource_refs(operation_id,handle_id,position,created_at) VALUES (?,?,?,?)",
                    (row["parent_operation_id"], handle, position, now),
                )
        return True
