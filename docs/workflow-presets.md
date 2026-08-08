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
    agent:
      model: codex-mini
      sandbox: workspace-write
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
