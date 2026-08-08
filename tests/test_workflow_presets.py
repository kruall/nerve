from pathlib import Path
import pytest
import yaml
from nerve.executions import ExecutionCatalog
from nerve.workflows.presets import WorkflowPresetCatalog, WorkflowPresetError

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
