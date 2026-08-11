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
  durable completion watch. Once the initiating session ends, Nerve restores it
  automatically when the execution reaches terminal state.
- `execution_forget` removes the completion watch without cancelling the work.
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

## Approval-gated resource commands

Use the retained-handle flow for remote commands: inspect `resource_inventory`,
call `resource_handle_acquire(requests)`, then call
`resource_command(handle_id, executable, args)` and finally
`resource_handle_release(handle_id)` when no operation uses it. A command never
accepts a pool, host, session id, lease, or fence. The executable and every
argument are separate process arguments; Nerve does not add a shell. The
command uses the normal durable status, tail, cancellation, fencing, and
uncertain-cleanup behavior.

This capability permits arbitrary code execution within the remote worker
account. Keep its MCP approval mode at `prompt`; do not add a per-tool `approve`
exception. Operators can still explicitly run a shell executable, but the full
shell invocation remains visible in the approval request.

## Remote artifacts

The SSH supervisor exposes a fixed `artifact_put` RPC for copying a verified
local control-host file to a leased resource host. The handle-bound operation
provides a local source and relative destination below the connection's
configured artifact root; Nerve supplies and verifies the lease id/fencing
token internally. The supervisor checks the fence, rejects traversal and symlink escapes,
writes a private temporary sibling, verifies size and SHA-256 again, then
atomically renames it into place and removes temporary state on failure.

Resource requests that need more than one host are acquired as a single durable
bundle: either every slot is fenced and assigned in one transaction or no slot
is assigned. Queue ordering is durable and deterministic across restarts, so a
two-host operation cannot hold one pool while waiting on another.

`artifact_transfer` accepts tagged endpoints. A remote endpoint is
`{handle_id, artifact_root, path}`: acquire the retained handles first and
release them after the transfer settles. A
localhost endpoint is exactly `{host: localhost, artifact_root: <configured
local root id>, path: <relative path>}`. localhost-to-localhost is rejected.
Plans retain stable pool, host, and root ids with relative paths, never
connection coordinates. Remote-to-remote creates
`source` and `destination` slots; local-to-remote creates only `destination`,
and remote-to-local creates only `source`. Connection coordinates are never
tool input. The destination
worker generates a one-time client key, then connects directly to the source
worker's ephemeral sshd. The control plane relays only public keys and transfer
metadata, never artifact bytes. sshd uses a fresh host key, private state below
the reviewed remote root, a forced fixed `artifact-send` helper, and disables
PTY, shell, forwarding, agent/X11 forwarding, and tunnels. The destination
writes SSH stdout to a private temporary sibling, verifies the source size and
SHA-256, and atomically renames it. Success removes transfer key/server state;
cancellation ambiguity quarantines both leases.

Remote-to-local downloads are limited to 32 KiB while the deployed NRS1
response frame is JSON-only. Larger copies fail explicitly rather than being
silently buffered in the control process.

Local-to-remote uploads use the existing authenticated supervisor channel and
are capped by the 4 GiB NRS1 binary-frame limit; the control process buffers
that frame. Upload timeouts scale with frame size at a conservative 1 MiB/s
floor. Large benchmark artifacts should therefore use direct
remote-to-remote transfer through the control host: Nerve invokes `scp` from
the control host to download the source into a temporary local file and then
invokes `scp` again to upload it to the destination. The remote hosts do not
need network access to one another.

## Session-sticky YDB builders

When `resources.ydb_worktree_root` is configured, the owner-session tools
`ydb_make(worktree, args)` and `ydb_test(worktree, args)` are available. They
only accept a Git top-level below that local allowlist; neither accepts a host,
remote path, connection reference, or shell command. Both use the fixed
`ydb-builders` pool and retain one fenced host reservation for the session and
worktree.

Before each command, Nerve snapshots `HEAD`, tracked changes, and non-ignored
untracked files without touching the index or worktree. The named SSH
supervisor receives a thin object pack and, when its cache lacks that `HEAD`,
an automatically transferred Git base containing the source tree but not
unrelated ancestor history.
It records that base in its bare cache, so every builder self-provisions on
first use without a manual cache seed. The supervisor then
atomically switches a session-specific checkout under its configured root,
preserves ignored build cache, and removes stale non-ignored files. It then
executes literal argv:
`./ya make --build relwithdebinfo …` (and adds `-tA` for `ydb_test`). Test
success also requires `GOOD` and `Ok` output and rejects common failure
markers, even when the exit status is zero.

The owner session can inspect that pinned checkout with `ydb_file_list`,
`ydb_file_find`, and `ydb_file_read`; these accept only relative checkout
paths and return bounded deterministic results. `ydb_host_release()` releases
an idle reservation so the next YDB command reserves again; active or uncertain
remote work is quarantined rather than released.

## Durable lifecycle and recovery

Executions use the persistent states `queued`, `starting`, `running`,
`cancelling`, `succeeded`, `failed`, `cancelled`, and `lost`. The row pins the
owner Nerve session, profile version/hash and snapshot, normalized unredacted
plan, resource requests, selected leases, backend handle, result, cancellation
marker, and continuation outbox state. Secret plan values stay in the local
database and are never returned by the public projection.

Synchronous starts suppress automatic delivery while the tool call waits.
Detached starts retain it, so completion restores the owner session exactly
once. `forget` atomically removes that pending delivery without cancelling the
execution.

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
Profiles declare the pools their slots permit, while callers bind those slots
only with retained handles; raw host, user, port, lease, fence, and SSH options
are rejected. A pool can list members and/or select
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
one or more allowed remote roots. SSH starts one short-lived process per
operation and invokes only the configured absolute `supervisor_path` (default
`/usr/local/libexec/nerve-remote-supervisor`); it never opens a daemon port or
passes remote shell text.

Install the standalone, stdlib-only worker artifact without installing Nerve:
copy `nerve/executions/remote_supervisor.py` from the reviewed release to that
path and mark it executable. The transport sends exactly one `NRS1` frame on
stdin: four-byte magic, 32-bit bounded JSON-header length, UTF-8 JSON header
with `version: 1`, 64-bit raw-pack length, then raw pack bytes. Only `sync`
and the legacy one-way `artifact_put` accept a pack. The direct transfer RPCs
are metadata-only. The worker requires exact EOF after the declared payload and
writes exactly one framed response to stdout; diagnostics and job output never
share stdout. The worker stores a fenced job record and holds a
host-level `flock` while the process group exists. A cancellation reply is
accepted only when the worker reports that process group quiescent. Transport
ambiguity therefore quarantines the lease rather than releasing the host.

The protocol does not negotiate individual operations: the installed worker
artifact must be from a release that accepts every enabled backend RPC. In
particular, remote SPIN uses the metadata-only `spin_prepare` RPC before the
normal fenced `start`; install the reviewed supervisor artifact before enabling
remote SPIN on a host.

Before its first RPC to a named SSH connection, Nerve probes the supervisor's
fixed `capabilities` operation. An older worker that rejects that operation is
bootstrapped automatically: Nerve uploads only its bundled standalone artifact
through pinned SCP and atomically replaces only the configured
`supervisor_path` with fixed `chmod` and `mv` argv. It never sends shell text or
model-provided remote commands. Known pre-start validation rejections (for
example an invalid snapshot identity) fail the Operation without quarantining
its host; all other rejection, lost, or malformed transport remains ambiguous
and is quarantined.

The shipped `ydb.build` and `ydb.test` examples select only `ydb-build` and
`ydb-test` pools. The test profile requires `GOOD` and final `Ok`, and rejects
known build/test failure markers; a zero exit status alone is not success.

Run `nerve config validate --workspace <workspace> --portable-only --strict-keys`
before review. Copy the non-destructive examples from
`examples/execution-kinds/` into the workspace catalog directory to try the
contract. Apply a reviewed change with `nerve reload` or workspace sync.
