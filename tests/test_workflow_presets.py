from pathlib import Path
import json
from types import SimpleNamespace

import pytest
import yaml

from nerve.agent.tools.handlers.workflow_presets import join_handler, start_handler
from nerve.agent.tools.registry import ToolContext
from nerve.executions import ExecutionCatalog
from nerve.workflows.presets import WorkflowPresetCatalog, WorkflowPresetError


@pytest.mark.asyncio
async def test_preset_start_result_is_compact_and_does_not_echo_pinned_plan():
    preset = SimpleNamespace(name="fast-discover", title="Fast discovery")
    plan = SimpleNamespace(preset=preset, inputs={"prompt": "private prompt"})
    service = SimpleNamespace(start=lambda **_: _started())
    catalog = SimpleNamespace(compile=lambda *_: plan)
    engine = SimpleNamespace(workflow_preset_catalog=catalog, workflow_preset_service=service)

    result = await start_handler(ToolContext(session_id="session", engine=engine), {"name": "fast-discover", "inputs": {}})

    body = json.loads(result.content[0]["text"])
    assert body == {
        "workflow_id": "wfp-123abc",
        "preset": {"name": "fast-discover", "title": "Fast discovery"},
        "status": "queued",
        "links": {"workflow": "/api/preset-workflows/wfp-123abc", "workflows": "/workflows"},
    }
    assert "private prompt" not in result.content[0]["text"]


async def _started():
    return {"id": "wfp-123abc", "status": "queued", "plan": {"inputs": {"prompt": "private prompt"}}}

@pytest.mark.asyncio
async def test_preset_join_uses_observer_session_and_returns_terminal_summary():
    service = SimpleNamespace(join=lambda workflow_id, session_id: _joined(workflow_id, session_id))
    engine = SimpleNamespace(workflow_preset_service=service)
    result = await join_handler(ToolContext(session_id="owner", engine=engine), {"workflow_id":"wfp-123"})
    assert json.loads(result.content[0]["text"]) == {"workflow_id":"wfp-123", "status":"succeeded", "result":{"outcome":"succeeded"}, "stages":[]}

async def _joined(workflow_id, session_id):
    assert session_id == "owner"
    return {"workflow_id":workflow_id, "status":"succeeded", "result":{"outcome":"succeeded"}, "stages":[]}

def _execution(ws):
    p=ws/"config/executions/kinds/echo.yaml"; p.parent.mkdir(parents=True)
    p.write_text(yaml.safe_dump({"schema_version":1,"kind":"echo","version":1,"title":"Echo","description":"","arguments":{},"resource_slots":{},"artifacts":{},"steps":[{"id":"x","type":"command","transport":"local","executable":"/bin/true","argv":[]}],"result":{},"timeout_seconds":10,"cleanup":{"steps":[]},"cancellation":{}}))
    c=ExecutionCatalog(ws); c.reload(); return c

def _preset(**changes):
    value={"schema_version":1,"name":"verify.change","version":1,"title":"Verify","description":"x","inputs":{"task":{"type":"string"}},"budget_usd":2,"timeout_seconds":60,"terminal_policy":"fail_fast","stages":[{"id":"research","runner":"agent","timeout_seconds":10,"outputs":{"type":"object"},"agent":{"model":"codex-mini","sandbox":"workspace-write","mcp":{"allow":["nerve.memory_recall"]},"skills":[]}},{"id":"check","depends_on":["research"],"runner":"execution","execution":{"kind":"echo","arguments":{}}}]}
    value.update(changes); return value

def _write(ws, value):
    p=ws/"config/workflows/presets/example.yaml"; p.parent.mkdir(parents=True,exist_ok=True); p.write_text(yaml.safe_dump(value,sort_keys=False)); return p

def test_resolves_hashes_pins_and_rolls_back(tmp_path):
    c=WorkflowPresetCatalog(tmp_path,_execution(tmp_path)); path=_write(tmp_path,_preset()); first=c.reload(); plan=c.compile("verify.change",{"task":"a"})
    assert plan.preset_hash == first.presets["verify.change"].preset_hash
    path.write_text("schema_version: [bad")
    with pytest.raises(WorkflowPresetError): c.reload()
    assert c.snapshot is first and plan.preset.title == "Verify"

def test_rejects_cycles_missing_execution_and_agent_without_explicit_deny_profile(tmp_path):
    c=WorkflowPresetCatalog(tmp_path,_execution(tmp_path))
    bad=_preset(stages=[{"id":"a","depends_on":["b"],"runner":"execution","execution":{"kind":"echo"}},{"id":"b","depends_on":["a"],"runner":"execution","execution":{"kind":"missing"}}])
    _write(tmp_path,bad)
    with pytest.raises(WorkflowPresetError): c.reload()
    _write(tmp_path,_preset(stages=[{"id":"a","runner":"agent","agent":{"model":"m","sandbox":"s"}}]))
    with pytest.raises(WorkflowPresetError,match="only its agent specification|mcp"): c.reload()

def test_hash_is_canonical_across_yaml_key_order(tmp_path):
    c=WorkflowPresetCatalog(tmp_path,_execution(tmp_path)); path=_write(tmp_path,_preset()); one=c.reload().presets["verify.change"].preset_hash
    path.write_text(yaml.safe_dump(_preset(),sort_keys=True)); two=c.reload().presets["verify.change"].preset_hash
    assert one == two

def test_agent_stage_accepts_pinned_prompt(tmp_path):
    value = _preset()
    value["stages"][0]["agent"]["prompt"] = "Follow this reviewed procedure."
    c = WorkflowPresetCatalog(tmp_path, _execution(tmp_path))
    _write(tmp_path, value)
    assert c.reload().presets["verify.change"].stages[0].spec["prompt"] == "Follow this reviewed procedure."


def test_summaries_are_safe_and_sorted_while_describe_keeps_details(tmp_path):
    c = WorkflowPresetCatalog(tmp_path, _execution(tmp_path))
    first = _preset(name="zeta", title="Zeta")
    _write(tmp_path, first)
    second = _preset(name="alpha", title="Alpha")
    (tmp_path / "config/workflows/presets/second.yaml").write_text(yaml.safe_dump(second, sort_keys=False))
    snapshot = c.reload()
    summaries = snapshot.summaries()
    assert [item["name"] for item in summaries] == ["alpha", "zeta"]
    assert set(summaries[0]) == {"name", "title", "description", "stages"}
    described = c.describe("alpha")
    assert "inputs" in described and "preset_hash" in described
