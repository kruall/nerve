"""Declarative execution catalog, compiler, reload, and facade tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from nerve.agent.tools import ToolContext, build_default_registry
from nerve.executions import (
    ExecutionCatalog,
    ExecutionProfileError,
    OperationValidationError,
)
from nerve.config_validate import validate_config_bundle


def _profile(**updates):
    value = {
        "schema_version": 1,
        "kind": "local.echo",
        "version": 1,
        "title": "Safe echo",
        "description": "A local non-destructive example.",
        "arguments": {
            "message": {"type": "string", "required": True},
            "extra": {"type": "string_list", "default": []},
            "relative_input": {"type": "path", "default": "inputs/default.txt"},
            "token": {"type": "string", "secret": True},
        },
        "resource_slots": {
            "worker": {
                "allowed_pools": ["local.default", "local.large"],
                "default_pool": "local.default",
            },
        },
        "artifacts": {
            "report": {"root": "execution_dir", "path": "out/report.json", "required": True},
        },
        "steps": [
            {
                "id": "echo",
                "type": "command",
                "transport": "local",
                "executable": "/usr/bin/printf",
                "argv": [
                    {"literal": "%s"},
                    {"arg": "message"},
                    {"spread": "extra"},
                    {"context": "execution_id"},
                    {"artifact": "report"},
                ],
                "capture_stdout": "echoed",
            },
            {
                "id": "consume",
                "type": "command",
                "transport": "resource",
                "resource_slot": "worker",
                "executable": "/usr/bin/wc",
                "argv": [{"captured_output": "echoed"}],
            },
        ],
        "result": {
            "success_exit_codes": [0],
            "required_artifacts": ["report"],
            "output_capture": "echoed",
        },
        "timeout_seconds": 120,
        "cleanup": {
            "when": "always",
            "timeout_seconds": 10,
            "steps": [
                {
                    "id": "cleanup",
                    "type": "command",
                    "transport": "local",
                    "executable": "/usr/bin/true",
                    "argv": [],
                },
            ],
        },
        "cancellation": {"mode": "terminate", "grace_seconds": 5, "run_cleanup": True},
    }
    value.update(updates)
    return value


def _write(workspace: Path, value=None, name: str = "local-echo.yaml") -> Path:
    directory = workspace / "config" / "executions" / "kinds"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(yaml.safe_dump(value or _profile(), sort_keys=False), encoding="utf-8")
    return path


def test_compile_keeps_argv_boundaries_and_expands_spread(tmp_path):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()
    injection = "hello; touch /tmp/should-not-exist"
    plan = catalog.compile(
        "local.echo",
        {"message": injection, "extra": ["--", "$(uname)"], "token": "private"},
        {"worker": "local.large"},
    )

    argv = plan.steps[0].argv
    assert [node.value for node in argv[:4]] == ["%s", injection, "--", "$(uname)"]
    assert all(node.kind in {"literal", "value", "context", "artifact"} for node in argv)
    assert plan.steps[0].executable == "/usr/bin/printf"
    assert plan.resources == {"worker": "local.large"}
    assert plan.profile_hash == plan.profile.profile_hash
    assert plan.profile_version == "1"


def test_secret_values_are_redacted_from_diagnostics(tmp_path):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()
    plan = catalog.compile("local.echo", {"message": "ok", "token": "super-secret"})
    rendered = json.dumps(plan.as_dict())
    assert "super-secret" not in rendered
    assert "<redacted>" in rendered
    assert plan.arguments["token"] == "super-secret"


@pytest.mark.parametrize("value", ["../outside", "/absolute", "a/../../b", "a\\..\\b", ".", "./inside"])
def test_path_argument_rejects_escape(tmp_path, value):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()
    with pytest.raises(OperationValidationError, match="relative path|absolute|contain"):
        catalog.compile("local.echo", {"message": "ok", "relative_input": value})


def test_artifact_path_rejects_escape(tmp_path):
    profile = _profile(artifacts={"report": {"path": "../../outside"}})
    _write(tmp_path, profile)
    with pytest.raises(ExecutionProfileError, match="must not be absolute|contain"):
        ExecutionCatalog(tmp_path).reload()


def test_unknown_operation_fields_and_disallowed_pool_are_rejected(tmp_path):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()
    with pytest.raises(OperationValidationError, match="unknown argument"):
        catalog.compile("local.echo", {"message": "ok", "surprise": True})
    with pytest.raises(OperationValidationError, match="allowed pools"):
        catalog.compile("local.echo", {"message": "ok"}, {"worker": "remote.prod"})


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"unexpected": True}, "unknown field"),
        ({"steps": [{"id": "x", "type": "shell", "transport": "local", "executable": "echo", "argv": []}]}, "only supports 'command'"),
        ({"steps": [{"id": "x", "type": "command", "transport": "ssh", "executable": "echo", "argv": []}]}, "transport"),
        ({"steps": [{"id": "x", "type": "command", "transport": "local", "executable": {"arg": "message"}, "argv": []}]}, "executable"),
        ({"steps": [{"id": "x", "type": "command", "transport": "local", "executable": "echo", "argv": ["${message}"]}]}, "must be a mapping"),
    ],
)
def test_profiles_reject_unknown_or_shell_like_shapes(tmp_path, change, message):
    profile = _profile(**change)
    _write(tmp_path, profile)
    with pytest.raises(ExecutionProfileError, match=message):
        ExecutionCatalog(tmp_path).reload()


def test_captured_output_must_be_declared_by_an_earlier_step(tmp_path):
    profile = _profile(steps=[{
        "id": "bad", "type": "command", "transport": "local",
        "executable": "echo", "argv": [{"captured_output": "later"}],
        "capture_stdout": "later",
    }])
    _write(tmp_path, profile)
    with pytest.raises(ExecutionProfileError, match="before it is produced"):
        ExecutionCatalog(tmp_path).reload()


def test_duplicate_kind_is_rejected(tmp_path):
    _write(tmp_path)
    _write(tmp_path, _profile(), "duplicate.yaml")
    with pytest.raises(ExecutionProfileError, match="duplicate execution kind"):
        ExecutionCatalog(tmp_path).reload()


def test_duplicate_yaml_key_is_rejected(tmp_path):
    path = _write(tmp_path)
    text = path.read_text(encoding="utf-8")
    path.write_text(text + "kind: overwritten.kind\n", encoding="utf-8")
    with pytest.raises(ExecutionProfileError, match="duplicate key 'kind'"):
        ExecutionCatalog(tmp_path).reload()


def test_reload_is_atomic_and_existing_plan_pins_profile(tmp_path):
    path = _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    first = catalog.reload()
    old_plan = catalog.compile("local.echo", {"message": "before"})

    path.write_text("schema_version: [broken\n", encoding="utf-8")
    with pytest.raises(ExecutionProfileError):
        catalog.reload()
    assert catalog.snapshot is first
    assert catalog.compile("local.echo", {"message": "still valid"}).profile_hash == old_plan.profile_hash

    changed = _profile(version=2, title="Changed")
    path.write_text(yaml.safe_dump(changed, sort_keys=False), encoding="utf-8")
    second = catalog.reload()
    new_plan = catalog.compile("local.echo", {"message": "after"})
    assert second.generation == first.generation + 1
    assert new_plan.profile_hash != old_plan.profile_hash
    assert old_plan.profile.title == "Safe echo"
    assert old_plan.profile_hash == first.profiles["local.echo"].profile_hash


def test_hash_is_stable_across_yaml_key_order_and_comments(tmp_path):
    path = _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    first_hash = catalog.reload().profiles["local.echo"].profile_hash
    path.write_text("# reordered by serializer\n" + yaml.safe_dump(_profile(), sort_keys=True), encoding="utf-8")
    second_hash = catalog.reload().profiles["local.echo"].profile_hash
    assert first_hash == second_hash


def test_profile_files_and_directories_cannot_be_symlinks(tmp_path):
    outside = tmp_path / "outside.yaml"
    outside.write_text(yaml.safe_dump(_profile()), encoding="utf-8")
    directory = tmp_path / "config" / "executions" / "kinds"
    directory.mkdir(parents=True)
    (directory / "linked.yaml").symlink_to(outside)
    with pytest.raises(ExecutionProfileError, match="must not be a symlink"):
        ExecutionCatalog(tmp_path).reload()


@pytest.mark.asyncio
async def test_progressive_tools_and_start_service(tmp_path):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()

    class Service:
        def __init__(self):
            self.plan = None

        async def start(self, *, session_id, plan, auto_continue=True):
            self.plan = plan
            return {"id": "exec-1", "session_id": session_id}

        async def join_execution(self, *, execution_id, session_id):
            return {
                "id": execution_id, "session_id": session_id,
                "status": "succeeded", "continuation": {"state": "suppressed"},
            }

    service = Service()
    ctx = ToolContext(
        session_id="session-1", execution_catalog=catalog,
        execution_service=service,
    )
    registry = build_default_registry()
    listed = await registry.invoke("execution_kind_list", ctx, {})
    assert "local.echo" in listed.content[0]["text"]
    described = await registry.invoke("execution_kind_describe", ctx, {"kind": "local.echo"})
    assert '"additionalProperties": false' in described.content[0]["text"]
    validated = await registry.invoke(
        "execution_kind_validate", ctx,
        {"kind": "local.echo", "arguments": {"message": "ok"}},
    )
    assert validated.is_error is False
    started = await registry.invoke(
        "execution_kind_start", ctx,
        {"kind": "local.echo", "arguments": {"message": "ok"}},
    )
    assert started.is_error is False
    assert service.plan.profile_hash == catalog.snapshot.profiles["local.echo"].profile_hash


@pytest.mark.asyncio
async def test_start_reports_missing_lifecycle_service(tmp_path):
    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()
    registry = build_default_registry()
    result = await registry.invoke(
        "execution_kind_start",
        ToolContext(session_id="s", execution_catalog=catalog),
        {"kind": "local.echo", "arguments": {"message": "ok"}},
    )
    assert result.is_error is True
    assert "lifecycle service is not installed" in result.content[0]["text"]
    unknown = await registry.invoke(
        "execution_kind_start",
        ToolContext(session_id="s", execution_catalog=catalog),
        {"kind": "missing.kind"},
    )
    assert unknown.is_error is True
    assert "unknown execution kind" in unknown.content[0]["text"]


@pytest.mark.asyncio
async def test_resource_command_tool_forwards_literal_argv_and_waits():
    class Service:
        start_resource_command = AsyncMock(
            return_value={"id": "exec-command", "session_id": "s"},
        )
        join_execution = AsyncMock(
            return_value={
                "id": "exec-command", "session_id": "s", "status": "succeeded",
                "continuation": {"state": "suppressed"},
            },
        )

    service = Service()
    result = await build_default_registry().invoke(
        "resource_command",
        ToolContext(session_id="s", execution_service=service),
        {
            "pool": "test-machines", "executable": "/bin/echo",
            "args": ["hello world"], "timeout_seconds": 30,
        },
    )

    assert result.is_error is False
    service.start_resource_command.assert_awaited_once_with(
        session_id="s", pool="test-machines", executable="/bin/echo",
        args=["hello world"], timeout_seconds=30, auto_continue=False,
    )
    service.join_execution.assert_awaited_once_with(
        execution_id="exec-command", session_id="s",
    )


def test_config_validation_treats_profile_as_portable_reviewed_config(tmp_path):
    workspace = tmp_path / "workspace"
    _write(workspace)
    result = validate_config_bundle(
        tmp_path / "machine-config",
        workspace_override=workspace,
        portable_only=True,
        strict_keys=True,
    )
    assert result.ok, result.errors
    assert any("execution catalog: 1 kind(s)" in line for line in result.info)
    assert not any("nothing to validate" in error for error in result.errors)


def test_config_validation_rejects_malformed_profile(tmp_path):
    workspace = tmp_path / "workspace"
    path = _write(workspace)
    path.write_text("kind: [unterminated\n", encoding="utf-8")
    result = validate_config_bundle(
        tmp_path / "machine-config",
        workspace_override=workspace,
        portable_only=True,
    )
    assert not result.ok
    assert any("execution catalog:" in error for error in result.errors)


def test_config_pr_marks_execution_profile_as_executable_effect(tmp_path):
    from nerve.config_pr import _executable_effect

    reason = _executable_effect(
        "config/executions/kinds/local-echo.yaml",
        tmp_path / "local-echo.yaml",
        yaml.safe_dump(_profile()),
    )
    assert reason is not None
    assert "declares executable steps" in reason


@pytest.mark.asyncio
async def test_rest_discovery_validation_and_start(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from nerve.gateway.routes import _deps
    from nerve.gateway.routes.executions import (
        describe_execution_kind,
        list_execution_kinds,
        start_execution_kind,
        validate_execution_kind,
    )

    _write(tmp_path)
    catalog = ExecutionCatalog(tmp_path)
    catalog.reload()

    class Service:
        async def start(self, *, session_id, plan):
            return {"id": "exec-api", "session_id": session_id, "hash": plan.profile_hash}

    engine = SimpleNamespace(execution_catalog=catalog, execution_service=Service())
    monkeypatch.setattr(_deps, "_deps", _deps.RouteDeps(engine=engine, db=None))
    listed = await list_execution_kinds(user={})
    assert listed["kinds"][0]["kind"] == "local.echo"
    described = await describe_execution_kind("local.echo", user={})
    assert described["arguments"]["additionalProperties"] is False
    validated = await validate_execution_kind(
        "local.echo", {"arguments": {"message": "ok"}}, user={},
    )
    assert validated["valid"] is True
    started = await start_execution_kind(
        "local.echo", {"arguments": {"message": "ok"}}, user={},
    )
    assert started["execution"]["id"] == "exec-api"
    assert started["profile_hash"] == catalog.snapshot.profiles["local.echo"].profile_hash


def test_shipped_example_profiles_are_valid(tmp_path):
    examples = Path(__file__).parents[1] / "examples" / "execution-kinds"
    destination = tmp_path / "config" / "executions" / "kinds"
    destination.mkdir(parents=True)
    for source in examples.glob("*.yaml"):
        (destination / source.name).write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    snapshot = ExecutionCatalog(tmp_path).reload()
    assert set(snapshot.profiles) == {"local.echo", "local.file_size", "ydb.build", "ydb.test"}


def test_engine_startup_rejects_malformed_catalog(tmp_path):
    from nerve.agent.engine import AgentEngine
    from nerve.config import NerveConfig
    from nerve.db import Database

    path = _write(tmp_path)
    path.write_text("schema_version: [broken\n", encoding="utf-8")
    with pytest.raises(ExecutionProfileError):
        AgentEngine(NerveConfig(workspace=tmp_path), Database(tmp_path / "db.sqlite"))


@pytest.mark.asyncio
async def test_unified_reload_reports_catalog_error_and_keeps_previous_snapshot(tmp_path):
    from nerve.config import NerveConfig, set_config
    from nerve.config_reload import reload_all, reload_failures

    workspace = tmp_path / "workspace"
    profile_path = _write(workspace)
    config_dir = tmp_path / "machine"
    config_dir.mkdir()
    (config_dir / "config.yaml").write_text(
        f"workspace: {workspace}\n", encoding="utf-8",
    )
    config = NerveConfig(workspace=workspace, config_dir=config_dir)
    set_config(config)

    class Engine:
        def __init__(self):
            self.config = config
            self.execution_catalog = ExecutionCatalog(workspace)
            self.execution_catalog.reload()
            self.notification_service = None

        async def reload_mcp_config(self):
            return []

    engine = Engine()
    previous = engine.execution_catalog.snapshot
    profile_path.write_text("schema_version: [broken\n", encoding="utf-8")
    summary = await reload_all(engine, None, config_dir)
    assert "executions" in reload_failures(summary)
    assert engine.execution_catalog.snapshot is previous
    assert engine.execution_catalog.snapshot.profiles["local.echo"].profile_hash
