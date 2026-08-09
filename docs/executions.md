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
- `execution_kind_start(...)` compiles, persists, and queues the immutable plan.
  It waits by default; `detached: true` returns the id immediately and keeps a
  durable completion watch.
- `execution_join` waits for a detached execution. `execution_forget` removes
  the completion watch without cancelling the work.
- `execution_status`, `execution_tail`, `execution_cancel`, and
  `execution_list` expose only work owned by the calling ToolContext session.

Equivalent REST endpoints are:

- `GET /api/execution-kinds`
- `GET /api/execution-kinds/{kind}`
- `POST /api/execution-kinds/{kind}/validate`
- `POST /api/execution-kinds/{kind}/start`

The built-in local backend executes literal argv with no shell. Resource
transport requires a configured inventory/backend implementation; lease
acquisition and release remain service-owned and are never model-managed.

## Durable lifecycle and recovery

Executions use the persistent states `queued`, `starting`, `running`,
`cancelling`, `succeeded`, `failed`, `cancelled`, and `lost`. The row pins the
owner Nerve session, profile version/hash and snapshot, normalized unredacted
plan, resource requests, selected leases, backend handle, result, cancellation
marker, and continuation outbox state. Secret plan values stay in the local
database and are never returned by the public projection.

`join` and `forget` atomically suppress automatic delivery before waiting or
returning. Thus completion is delivered either through the blocked tool call or
through the preserved session continuation, never both.

Completion and cancellation are compare-and-set transitions. An accepted Stop
atomically moves active work to `cancelling`, suppresses pending or claimed
continuations, and cancels an in-process claimed continuation task. A backend
completion can create a `pending` continuation only from `starting`/`running`
with no cancellation marker. The outbox claimant then delivers at most one
`engine.run(..., internal=True, source="execution")` on the same Nerve session;
the stored native thread ID provides backend resume. Only execution ID,
terminal metadata, duration, and a 32 KiB bounded tail enter that prompt.

On daemon startup, queued rows are dispatched again. Each backend classifies
starting/running handles as reattachable, finished, missing, or orphaned.
Reattachable work is drained again; finished evidence is settled; missing or
orphaned work becomes `lost`, with selected resource leases quarantined when
remote quiescence is unknown. The local backend deliberately reports old
handles orphaned because pipe ownership cannot survive restart and it never
signals a persisted PID that may have been reused. Unclaimed continuations are
recovered after the Codex MCP loopback is ready. A claim interrupted by restart
is marked failed rather than dispatched twice.

Graceful shutdown terminates volatile local children but leaves non-terminal
rows for the same restart reconciliation. Session Stop, archive, and delete
cancel active rows and suppress pending continuations before clearing session
state.

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

## SSH resource inventory and leases

`resources` defines opaque `connection_ref` names, hosts, and named pools.
Operations request only a declared pool through their resource slot; raw host,
user, port, and SSH options are rejected. A pool can list members and/or select
them by labels. Overlap is safe because a partial-unique database index covers
the physical host, independent of the pool through which it was selected.

A lease carries a monotonically increasing fencing token. Release and heartbeat
compare the execution ID and token, so a stale owner cannot affect a later
lease. Cancellation or restart uncertainty quarantines the host. TTL/heartbeat
loss changes no host to available: an administrator must confirm remote
quiescence through the guarded recovery endpoint before quarantined leases are
retired and scheduling resumes.

### Remote supervisor

For a resource command Nerve resolves only the selected host's named
`connection_ref` through `resources.ssh_connections`. Each connection has a
dedicated `known_hosts` file, strict host-key verification, optional CIDRs and
one or more allowed remote roots. SSH always invokes the fixed
`nerve remote-supervisor rpc` argv; the structured request is JSON on stdin,
never remote shell text. The worker stores a fenced job record and holds a
host-level `flock` while the process group exists. A cancellation reply is
accepted only when the worker reports that process group quiescent. Transport
ambiguity therefore quarantines the lease rather than releasing the host.

The shipped `ydb.build` and `ydb.test` examples select only `ydb-build` and
`ydb-test` pools. The test profile requires `GOOD` and final `Ok`, and rejects
known build/test failure markers; a zero exit status alone is not success.

Run `nerve config validate --workspace <workspace> --portable-only --strict-keys`
before review. Copy the non-destructive examples from
`examples/execution-kinds/` into the workspace catalog directory to try the
contract. Apply a reviewed change with `nerve reload` or workspace sync.
