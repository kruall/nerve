# Workflow presets

Workflow presets are reviewed YAML files under `config/workflows/presets/`.
They compile a static, bounded stage graph and pin its canonical hash before a
controller starts it. Invalid reloads leave the previous catalog active.

```yaml
schema_version: 1
name: verify.change
version: 1
title: Verify change
inputs: {task: {type: string}}
budget_usd: 2
timeout_seconds: 600
terminal_policy: fail_fast
stages:
  - id: research
    runner: agent
    outputs: {type: object}
    agent:
      model: codex-mini
      reasoning_effort: high
      sandbox: workspace-write
      skills: [repository-research]
      mcp: {allow: [nerve.memory_recall]}
  - id: test
    depends_on: [research]
    runner: execution
    execution: {kind: local.echo, arguments: {message: ok}}
```

Use `workflow_preset_list`, `workflow_preset_describe`,
`workflow_preset_validate`, and `workflow_preset_start`; REST equivalents are
under `/api/workflow-presets`. The initial catalog does not schedule stages or
run agents: `start` delegates the already-pinned plan to an installed controller.

An agent stage is resolved before its session starts. The controller expands
required skill dependencies, pins their exact revisions, resolves the explicit
default-deny `server.tool` allowlist and captures its input schemas, then
journals a bounded `StageContext` hash. Only `read-only` and `workspace-write`
sandboxes are legal for stages. A stage response must be JSON satisfying its
declared output contract; malformed output is recorded as `invalid_output`.
