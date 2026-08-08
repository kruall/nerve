# Declarative execution kinds

Nerve discovers one YAML profile per file in
`<workspace>/config/executions/kinds/`. Profiles are reviewed, executable
configuration: they choose commands, transports, resource pools, cleanup, and
cancellation policy. Under lockdown the directory is covered by the existing
reviewed-surface write guard and config PR flow.

The catalog is loaded strictly at startup. An explicit config reload builds a
complete candidate and swaps it in only if every file is valid; otherwise the
previous generation remains active. A compiled plan carries the selected
profile's declared `version`, SHA-256 hash, and immutable profile snapshot, so
queued or running work is unaffected by later reloads.

## Profile contract

```yaml
schema_version: 1
kind: build.check
version: 3
title: Check a target
description: Run a reviewed checker through a selected worker pool.

arguments:
  target:
    type: string
    required: true
  flags:
    type: string_list
    default: []
  manifest:
    type: path
    default: manifests/default.yaml
  credential:
    type: string
    secret: true

resource_slots:
  worker:
    allowed_pools: [builders.default, builders.large]
    default_pool: builders.default

artifacts:
  report:
    root: execution_dir
    path: reports/result.json
    required: true

steps:
  - id: check
    type: command
    transport: resource
    resource_slot: worker
    executable: /usr/local/bin/checker
    cwd: workspace
    argv:
      - literal: --target
      - arg: target
      - spread: flags
      - literal: --report
      - artifact: report
    capture_stdout: checker_output

result:
  success_exit_codes: [0]
  required_artifacts: [report]
  output_capture: checker_output
timeout_seconds: 1800

cleanup:
  when: always
  timeout_seconds: 60
  steps: []

cancellation:
  mode: terminate
  grace_seconds: 10
  run_cleanup: true
```

Unknown fields and unsupported schema, step, transport, token, or cancellation
types are errors. Identifiers are deliberately restricted. Artifact and `path`
arguments must be relative POSIX paths without traversal.

Argument types are `string`, `integer`, `number`, `boolean`, `string_list`, and
`path`. Numeric arguments may declare `minimum`/`maximum`; lists may declare
`min_items`/`max_items`; any type may declare `enum`. A required argument cannot
also have a default. Secret arguments have no config default and are redacted
from validation output.

## Safe argv tokens

Every `argv` item is a mapping containing exactly one token:

- `literal`: reviewed literal text.
- `arg`: one typed scalar argument, always one argv element.
- `spread`: a `string_list`, expanded to one argv element per item.
- `context`: `workspace`, `execution_dir`, `execution_id`, or `session_id`.
- `artifact`: a declared artifact reference.
- `captured_output`: output captured by an earlier step.

Executables and transports are literals in the profile. There are no shell
commands, interpolation markers, or string templates. Text such as `;`, `$()`,
or whitespace supplied in an argument remains data inside one argv boundary.
The lifecycle service resolves context/artifact/capture nodes and executes the
normalized argv directly.

Profile files themselves are not environment-interpolated. Keep secrets out of
them; declare a `secret: true` operation argument and supply its value when the
operation is compiled. Secret defaults and enums are rejected.

`transport: local` cannot name a resource slot. `transport: resource` must name
a declared slot. An operation may select only a pool listed in that slot's
`allowed_pools`; otherwise compilation fails.

## Discovery and validation

The MCP surface is progressive:

- `execution_kind_list` returns compact summaries.
- `execution_kind_describe(kind)` returns only that kind's argument schema and
  resource/result policies.
- `execution_kind_validate(kind, arguments, resources)` returns a redacted
  compiled plan without starting it.
- `execution_kind_start(...)` compiles and hands the immutable plan to the
  separately installed execution lifecycle service.

Equivalent REST endpoints are:

- `GET /api/execution-kinds`
- `GET /api/execution-kinds/{kind}`
- `POST /api/execution-kinds/{kind}/validate`
- `POST /api/execution-kinds/{kind}/start`

The start surfaces return `503`/an MCP error until the lifecycle service is
installed; catalog discovery and compilation remain available independently.

## Web lifecycle and resource view

The chat UI keeps agent streaming and detached work as separate state. A
session is busy when either its agent turn is running or it owns active work,
but the agent `is_running` flag is never rewritten to mean both. Session Stop
therefore remains available after the initiating turn ends and asks the
lifecycle service to cancel owned work.

Execution cards show the requested pool separately from the selected physical
host, queue position, duration, lease/revocation state, terminal result, and
assistant-continuation state. `queued`, `running`, `cancelling`, `failed`,
`cancelled`, and waiting-for-continuation are visually distinct. A failed
execution is also distinct from a successful execution whose later assistant
continuation failed.

Logs are opt-in: Load/Refresh requests only the recent bounded tail. Follow is
off by default and, when enabled, repeats the same bounded request; it never
downloads the complete build log. Lifecycle WebSocket messages update the open
view quickly, then the store reconciles from REST so reloads, reconnects, and
overlapping events converge on durable state.

The Resources drawer shows pool availability, physical hosts, current leases,
and the FIFO queue. Draining and quarantine recovery are authenticated,
confirmation-gated actions. A revoking or quarantined host stays visibly
unavailable until the backend confirms remote quiescence; the UI cannot release
a lease or clear quarantine merely because a heartbeat/TTL expired.

Run `nerve config validate --workspace <workspace> --portable-only --strict-keys`
before review. Copy the non-destructive examples from
`examples/execution-kinds/` into the workspace catalog directory to try the
contract. Apply a reviewed change with `nerve reload` or workspace sync.
