"""The preset controller must not wake the observer between child stages."""
from __future__ import annotations
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from nerve.workflows.controller import WorkflowPresetService

class _Executions:
    def __init__(self):
        self.catalog=SimpleNamespace(compile=lambda *args: object())
        self.child={"id":"exec-child","status":"running","result":{}}
    async def start(self, **kwargs):
        assert kwargs["completion_target"] == {"type":"workflow","id":kwargs["completion_target"]["id"]}
        return self.child
    async def get_execution(self, **kwargs): return self.child
    async def cancel_execution(self, **kwargs): return self.child

async def _wait(predicate):
    async with asyncio.timeout(2):
        while not await predicate(): await asyncio.sleep(.01)

@pytest.mark.asyncio
async def test_execution_stage_only_resumes_observer_after_workflow_terminal(db):
    await db.create_session("owner", source="web", backend="codex", status="idle")
    engine=SimpleNamespace(run=AsyncMock(), _skill_manager=None, registry=None, config=SimpleNamespace(agent=SimpleNamespace(model="x"),codex=SimpleNamespace(model="x")))
    executions=_Executions(); service=WorkflowPresetService(db=db,engine=engine,executions=executions,agent_runs=None)
    preset={"name":"p","version":"1","preset_hash":"pinned","budget_usd":1,"stages":[{"id":"one","depends_on":[],"runner":"execution","inputs":{},"outputs":{},"timeout_seconds":10,"spec":{"kind":"test","arguments":{},"resources":{}}}]}
    plan=SimpleNamespace(preset=SimpleNamespace(describe=lambda:preset),inputs={},preset_hash="pinned")
    workflow=await service.start(session_id="owner",plan=plan)
    await _wait(lambda: _has_stage(db,workflow["id"]))
    assert engine.run.await_count == 0
    executions.child={"id":"exec-child","status":"succeeded","result":{"outcome":"succeeded"}}
    await _wait(lambda: _done(db,workflow["id"]))
    assert engine.run.await_count == 1
    await service.shutdown()

async def _has_stage(db, workflow_id): return bool(await db.list_stage_runs(workflow_id))
async def _done(db, workflow_id): return (await db.get_preset_workflow(workflow_id))["status"] == "succeeded"

@pytest.mark.asyncio
async def test_cancelled_workflow_suppresses_final_continuation(db):
    await db.create_session("owner", source="web", backend="codex", status="idle")
    engine=SimpleNamespace(run=AsyncMock()); service=WorkflowPresetService(db=db,engine=engine,executions=_Executions(),agent_runs=None)
    row=await db.create_preset_workflow("w",session_id="owner",plan={},preset_hash="x",spec_hash="x")
    await service._terminal(row,"cancelled",{"outcome":"cancelled"})
    async with db.db.execute("SELECT state FROM workflow_completion_outbox WHERE workflow_id='w'") as c: assert (await c.fetchone())[0] == "suppressed"
    engine.run.assert_not_awaited()
