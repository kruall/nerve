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

The web UI rehydrates preset workflows from `/api/preset-workflows` and uses
WebSocket `workflow_update` only as an invalidation signal. The public shape
intentionally excludes inputs, prompts, raw arguments, skill contents and
artifacts. `available_actions` is controller-owned. It is an extensible, empty-by-default
projection of `{id,label,description,workflow_revision,reason,destructive,confirmation_required}`;
clients must never infer actions from a status. The initial registry exposes only `abandon`
for a `blocked` workflow (a child stage failed and the controller stopped without observer
delivery). It requires confirmation and an optimistic `workflow_revision`, records a durable
actor/reason/idempotency audit row, terminalizes as `cancelled` with outcome `abandoned`, and
suppresses the completion outbox. Repeating the same idempotency key returns the resulting
projection without a second transition. `retry` requires attempt journaling and `accept`
requires a separately pinned valid acceptance artifact, so neither is advertised yet.
Action execution is `POST /api/preset-workflows/{id}/actions/{action}` with revision,
idempotency key and confirmation. Unknown actions are 404; stale or unavailable actions are
409. New kinds require an explicit server registry and state-matrix extension.
`spent_usd` is `null` until the controller exposes normalized stage metering.

An agent stage is resolved before its session starts. The controller expands
required skill dependencies, pins their exact revisions, resolves the explicit
default-deny `server.tool` allowlist and captures its input schemas, then
journals a bounded `StageContext` hash. Only `read-only` and `workspace-write`
sandboxes are legal for stages. A stage response must be JSON satisfying its
declared output contract; malformed output is recorded as `invalid_output`.
