"""Durable sequential controller for a pinned workflow-preset plan.

Only persisted rows decide what is dispatched.  The task is therefore merely a
prompt wakeup; after a restart ``initialize`` can safely reconstruct it.
"""
from __future__ import annotations
import asyncio, hashlib, json, uuid
from collections.abc import Mapping
from typing import Any
from nerve.agent.streaming import broadcaster
from nerve.db.preset_workflows import ACTIVE_STAGE_RUNS, TERMINAL_STAGE_RUNS
from nerve.workflows.stages import AgentStageResolver, validate_artifact, StageArtifactError

def _hash(v: Any) -> str: return hashlib.sha256(json.dumps(v, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()

class WorkflowPresetService:
    def __init__(self, *, db: Any, engine: Any, executions: Any, agent_runs: Any | None):
        self.db, self.engine, self.executions, self.agent_runs = db, engine, executions, agent_runs
        self._tasks: dict[str, asyncio.Task] = {}; self._stopping = False

    async def initialize(self) -> None:
        for row in await self.db.active_preset_workflows(): self._schedule(row["id"])

    async def shutdown(self) -> None:
        self._stopping = True
        for task in self._tasks.values(): task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()

    async def start(self, *, session_id: str, plan: Any) -> dict:
        preset = plan.preset
        snapshot = {"preset": preset.describe(), "inputs": dict(plan.inputs), "preset_hash": plan.preset_hash}
        workflow_id = f"wfp-{uuid.uuid4().hex[:12]}"
        row = await self.db.create_preset_workflow(workflow_id, session_id=session_id, plan=snapshot, preset_hash=plan.preset_hash, spec_hash=_hash(snapshot))
        self._schedule(workflow_id); await self._broadcast(row); return row

    async def _changed(self, workflow_id: str) -> None:
        """Publish a redacted workflow snapshot after every durable transition."""
        row = await self.db.get_preset_workflow(workflow_id)
        if row: await self._broadcast(row)

    def _schedule(self, workflow_id: str) -> None:
        if self._stopping or workflow_id in self._tasks: return
        task = asyncio.create_task(self._drive(workflow_id)); self._tasks[workflow_id] = task
        task.add_done_callback(lambda finished: self._tasks.pop(workflow_id, None))

    async def cancel_session(self, session_id: str, *, reason: str = "session stopped") -> bool:
        changed = False
        for workflow in await self.db.active_preset_workflows():
            if workflow["observer_session_id"] != session_id: continue
            changed |= await self.db.transition_preset_workflow(workflow["id"], to_status="cancelling")
            for stage in await self.db.list_stage_runs(workflow["id"]):
                if stage["status"] not in ACTIVE_STAGE_RUNS: continue
                await self.db.transition_stage_run(stage["id"], to_status="cancelling")
                if stage.get("child_type") == "execution": await self.executions.cancel_execution(execution_id=stage["child_id"], requested_by="workflow", reason=reason)
                elif stage.get("child_type") == "agent" and self.agent_runs: await self.agent_runs.kill_run(stage["child_id"], reason=reason, killed_by="workflow")
            await self._changed(workflow["id"])
        return changed

    async def _drive(self, workflow_id: str) -> None:
        while not self._stopping:
            workflow = await self.db.get_preset_workflow(workflow_id)
            if not workflow or workflow["status"] not in ("queued", "running", "cancelling"): return
            if workflow["status"] == "cancelling":
                await self._terminal(workflow, "cancelled", {"outcome":"cancelled"}); return
            if workflow["status"] == "queued": await self.db.transition_preset_workflow(workflow_id, to_status="running", expect=("queued",)); await self._changed(workflow_id); workflow = await self.db.get_preset_workflow(workflow_id)
            stages = await self.db.list_stage_runs(workflow_id)
            active = next((s for s in stages if s["status"] in ACTIVE_STAGE_RUNS), None)
            if active:
                if await self._reconcile_child(active): continue
                await asyncio.sleep(.15); continue
            done = {s["stage_id"]:s for s in stages if s["status"] == "succeeded"}
            preset = workflow["plan"]["preset"]; next_stage = next((s for s in preset["stages"] if s["id"] not in {x["stage_id"] for x in stages} and all(d in done for d in s.get("depends_on", []))), None)
            if next_stage is None:
                failed = next((s for s in stages if s["status"] in TERMINAL_STAGE_RUNS and s["status"] != "succeeded"), None)
                await self._terminal(workflow, "failed" if failed else "succeeded", {"outcome": "failed" if failed else "succeeded"}); return
            await self._dispatch(workflow, next_stage, done)

    async def _dispatch(self, workflow: Mapping[str, Any], stage: Mapping[str, Any], done: Mapping[str, Mapping[str, Any]]) -> None:
        sid=f"wfs-{uuid.uuid4().hex[:12]}"; await self.db.create_stage_run(sid, workflow_id=workflow["id"], stage_id=stage["id"], runner=stage["runner"], spec=stage, spec_hash=_hash(stage)); await self.db.transition_stage_run(sid, to_status="starting", expect=("queued",)); await self._changed(workflow["id"])
        artifacts={k:v.get("artifact") for k,v in done.items() if v.get("artifact") is not None}
        try:
            if stage["runner"] == "execution":
                raw=stage["spec"]; plan=self.executions.catalog.compile(raw["kind"], raw.get("arguments", {}), raw.get("resources", {})); child=await self.executions.start(session_id=workflow["observer_session_id"], plan=plan, completion_target={"type":"workflow","id":workflow["id"]}); await self.db.transition_stage_run(sid,to_status="running",expect=("starting",),child_type="execution",child_id=child["id"])
            else:
                if self.agent_runs is None: raise RuntimeError("agent runner is unavailable")
                # The resolver pins all mutable model inputs before invoking the adapter.
                from nerve.workflows.presets import WorkflowStage
                ps=stage["spec"]; spec=WorkflowStage(stage["id"],tuple(stage.get("depends_on",[])),"agent",stage.get("inputs",{}),stage.get("outputs",{}),stage["timeout_seconds"],ps)
                resolver=AgentStageResolver(skills=self.engine._skill_manager, registry=self.engine.registry, configured_models={str(self.engine.config.agent.model),str(self.engine.config.codex.model)}, external_servers=set())
                resolved=await resolver.resolve(stage=spec,workflow={"id":workflow["id"]},task_contract=workflow["plan"]["inputs"],prompt=str(workflow["plan"]["inputs"].get("prompt", stage["id"])),artifacts=artifacts,budget_usd=float(workflow["plan"]["preset"]["budget_usd"])/len(workflow["plan"]["preset"]["stages"]))
                child=await self.agent_runs.start_agent_stage(resolved); await self.db.transition_stage_run(sid,to_status="running",expect=("starting",),child_type="agent",child_id=child["id"])
        except Exception as e: await self.db.transition_stage_run(sid,to_status="failed",expect=("starting",),result={"error":type(e).__name__})
        finally: await self._changed(workflow["id"])

    async def _reconcile_child(self, stage: Mapping[str, Any]) -> bool:
        child = await (self.executions.get_execution(execution_id=stage["child_id"]) if stage.get("child_type")=="execution" else self.agent_runs.get_run(stage["child_id"]))
        if not child: return False
        status = child.get("status"); mapping={"succeeded":"succeeded","done":"succeeded","failed":"failed","cancelled":"cancelled","killed":"cancelled","lost":"lost","budget_exhausted":"failed"}
        if status not in mapping: return False
        result=child.get("result") or {}; artifact=None
        if mapping[status]=="succeeded" and stage["runner"]=="agent":
            try: artifact=validate_artifact(str(result.get("response") or result.get("text") or "{}"), stage["spec"].get("outputs",{}))
            except StageArtifactError as e: mapping[status]="failed"; result={"error":str(e)}
        await self.db.transition_stage_run(stage["id"],to_status=mapping[status],expect=("running","cancelling"),result=result,artifact=artifact); await self._changed(stage["workflow_id"]); return True

    async def _terminal(self, workflow: Mapping[str, Any], status: str, result: Mapping[str, Any]) -> None:
        if not await self.db.transition_preset_workflow(workflow["id"],to_status=status,result=result): return
        now = __import__("nerve.utils.time",fromlist=["utc_now_iso"]).utc_now_iso()
        if status == "cancelled":
            # Stop/archive/delete wins every race: retain an auditable outbox
            # record but never re-open the observer session.
            await self.db._write("INSERT OR IGNORE INTO workflow_completion_outbox(workflow_id,state,created_at,updated_at) VALUES (?, 'suppressed', ?, ?)",(workflow["id"],now,now))
            await self._broadcast(await self.db.get_preset_workflow(workflow["id"]))
            return
        await self.db._write("INSERT OR IGNORE INTO workflow_completion_outbox(workflow_id,state,created_at,updated_at) VALUES (?, 'pending', ?, ?)",(workflow["id"],now,now))
        await self._broadcast(await self.db.get_preset_workflow(workflow["id"]))
        # Claim is durable; an uncertain claimed row remains blocked rather than duplicate a turn.
        claimed=await self.db._write("UPDATE workflow_completion_outbox SET state='claimed',claimed_at=?,updated_at=? WHERE workflow_id=? AND state='pending'",(now,now,workflow["id"]))
        if not claimed.rowcount: return
        try: await self.engine.run(session_id=workflow["observer_session_id"],user_message=f"Workflow {workflow['id']} finished with status: {status}.",source="workflow",internal=True)
        except Exception as e: await self.db._write("UPDATE workflow_completion_outbox SET state='failed',error=?,updated_at=? WHERE workflow_id=?",(type(e).__name__,now,workflow["id"])); return
        await self.db._write("UPDATE workflow_completion_outbox SET state='completed',completed_at=?,updated_at=? WHERE workflow_id=?",(now,now,workflow["id"]))

    async def _broadcast(self, workflow: Mapping[str, Any]) -> None:
        # Live events are an invalidation signal only; never put the pinned
        # plan (which can contain prompts/arguments) on the websocket.
        safe = {key: workflow.get(key) for key in ("id", "observer_session_id", "preset_hash", "status", "revision", "created_at", "started_at", "finished_at", "updated_at")}
        await broadcaster.broadcast(workflow["observer_session_id"], {"type":"workflow_update","workflow":safe})
