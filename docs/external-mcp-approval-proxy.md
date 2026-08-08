# External MCP approval proxy

## Decision

Nerve should put approval enforcement in a **session-bound MCP transport
proxy**, owned by Nerve, between `codex app-server` and each configured
external MCP server.  The proxy is the MCP server visible to Codex; it owns
the upstream stdio process or HTTP/SSE connection.  It evaluates policy and,
only after a durable user decision, forwards `tools/call` upstream.

Do not implement this as an app-server adapter.  The tested app-server schema
(`codex-cli 0.144.1`) has server requests for command/file approvals and MCP
elicitation, but none for an MCP tool approval.  With Codex 0.145.0 installed
locally, the current behavior is still that `prompt` is rejected by Codex
before Nerve receives a call.  An adapter cannot reconstruct a call that was
never emitted.  The proxy must therefore configure its Codex-facing server as
`default_tools_approval_mode = "approve"`; that setting is safe only because
the proxy, not Codex, remains the enforcement point.

The built-in `nerve` HTTP MCP bridge is a useful precedent, but not the
implementation: it authorizes after a Nerve tool call reaches the server.  An
external proxy must also preserve and relay the complete MCP session and
upstream transport.

## Current facts and constraints

* `CodexBackend._translate_mcp_server()` currently gives Codex external
  servers directly, for stdio and HTTP URLs.  It maps credentials through
  synthetic environment variables, so they do not enter argv or config.
* `mcp_stdio_wrapper.py` only remaps environment names then `exec`s the
  upstream program; it cannot inspect JSON-RPC traffic.
* Codex emits `mcpToolCall` lifecycle items with server, tool and arguments,
  but these are observational and occur too late to approve a blocked call.
* `InteractiveToolHandler` is in-memory, web-session scoped and times out in
  one hour.  It is appropriate for a live card, but cannot recover a pending
  decision after a Nerve restart or fan out to Telegram/Discord.
* Nerve's `NotificationService` already persists cross-channel actionable
  approvals, but its existing dispatcher is for mechanical actions; do not
  overload that dispatcher with a live RPC continuation.
* Current config permits `stdio`, `sse` and `http`.  Codex's current translator
  treats URL servers uniformly; the proxy must retain their actual upstream
  transport semantics.

The locally installed `codex-cli 0.145.0` is outside Nerve's checked-in tested
range (`>=0.144.1, <0.145.0`).  Any prototype must regenerate and review the
app-server schema before claiming a behavior change for that version.

## Placement options

| Placement | Coverage | Session/audit fidelity | Main problem | Verdict |
| --- | --- | --- | --- | --- |
| Nerve transport gateway | stdio, HTTP, SSE; one policy path | Proxy assigns session/call IDs before forwarding | New MCP client/server relay | **Recommended** |
| stdio-only wrapper | stdio only | Good after a custom JSON-RPC relay | Does not cover HTTP/SSE; current wrapper cannot intercept | Reuse as one proxy transport adapter |
| HTTP reverse proxy | HTTP/SSE only | Good with signed session credentials | Cannot launch/manage stdio | Reuse as one proxy transport adapter |
| Codex app-server adapter | No reliable external MCP prompt coverage | Only post-hoc item observation | `prompt` is rejected before a server request | Reject |

The gateway is a logical component, not necessarily a public HTTP listener.
For stdio, Codex launches `python -m nerve.mcp_proxy.stdio` and communicates
with its stdio endpoint.  For URL transports, Codex receives a loopback,
per-session proxy URL and a short-lived bearer token.  Both endpoints call the
same `ApprovalProxySession` service.

## Protocol and lifecycle

### Identity and binding

When `CodexBackend` materializes a proxy configuration it creates a random
`proxy_session_id`, binds it to the immutable tuple below, and mints a
short-lived, audience-scoped credential:

```
(nerve_session_id, backend="codex", configured_server_name,
 upstream_config_revision, proxy_session_id)
```

The stdio launcher receives the opaque id and credential through environment,
never argv.  HTTP/SSE uses a loopback route with the credential in an HTTP
header.  The gateway rejects a connection whose credential, target server,
or session binding does not match.  It never accepts a client-provided Nerve
session id.

The proxy owns MCP `initialize`, `notifications/*`, `tools/list`,
`tools/call`, cancellation, and response correlation.  It may cache a
sanitized `tools/list` result for policy/UI rendering, but forwards schemas
unchanged.  Every JSON-RPC request id is scoped to the proxy connection;
upstream ids are independently allocated, preventing a client from completing
or cancelling another connection's call.

### Canonical approval request

Before forwarding a `tools/call`, the proxy creates a durable record:

```json
{
  "id": "mapr_…",
  "idempotency_key": "sha256(proxy_session_id, client_request_id, call_nonce)",
  "nerve_session_id": "impl-…",
  "backend": "codex",
  "source": "web",
  "server": "spin",
  "tool": "verify",
  "arguments": {"…": "redacted audit copy"},
  "arguments_digest": "sha256(canonical JSON)",
  "schema_digest": "sha256(tool inputSchema)",
  "config_revision": 17,
  "risk": {"level": "prompt", "reasons": ["external MCP"]},
  "deadline_at": "…",
  "state": "pending"
}
```

The encrypted/full arguments needed for forwarding are held only in the
runtime secret store (or encrypted DB column with a dedicated key); UI and
audit use a schema-aware redacted projection.  The record is inserted before
the approval is published.  Its state machine is:

```
pending -> approved -> dispatched -> completed | failed
pending -> declined | timed_out | cancelled | lost
approved -> cancelled | lost       dispatched -> cancelled | lost
```

Only a `pending` record can transition to `approved`; the update is conditional
on id, state, session binding, decision nonce and deadline.  A duplicate
decision is a successful no-op for the original result, never a second
upstream call.  On deny/timeout/cancel the proxy returns an MCP error with a
stable Nerve code (`-32041` declined, `-32042` expired, `-32043` cancelled,
`-32044` unavailable/lost) and no upstream request.

Approval is for exactly one immutable tuple `(server, tool, arguments_digest,
schema_digest, config_revision)`.  A changed tool schema, config reload, or
arguments invalidates the pending request and requires a new approval.

### Long-running calls and cancellation

After approval the transport relays progress notifications and the terminal
result without buffering unbounded output.  A client cancellation while
`pending` atomically marks the record `cancelled`; while `dispatched` it also
forwards MCP cancellation upstream when supported, then returns cancellation.
The upstream worker/task remains registered until it exits so a late response
is discarded and audited rather than misrouted.

An HTTP/SSE reconnect is tied to the same proxy session only with the original
credential and MCP resume token.  A stdio EOF cancels all not-yet-dispatched
calls; a dispatched call follows the configured upstream cancellation policy.
No implicit retry is allowed for a non-idempotent `tools/call`.

## Policy model

Policy is evaluated by Nerve before publishing an approval and uses the first
matching, most-specific rule:

1. hard deny / integrity gates (unknown server, disabled config, bad binding,
   changed schema, invalid arguments, source forbidden);
2. session-scoped one-shot decision for the exact digest;
3. configured server+tool rule;
4. configured server default;
5. global external-MCP default (`prompt`).

Modes are `deny`, `prompt`, and `approve`.  `approve` is explicit and only
valid for a known configured tool with a stable schema digest; it is not a
wildcard for a newly discovered tool.  UI choices are deliberately distinct:

* **Approve once** creates only the exact one-shot decision.
* **Always allow this tool** creates/requires an explicit persistent
  server+tool rule, displayed with schema digest and configuration revision.
* **Always allow server** is a separate privileged configuration change and
  cannot override a hard deny.
* **Decline** terminates the one call; it does not create a deny rule unless
  the user explicitly edits policy.

Cron, workflow legs, and other non-interactive sources default to `deny` for
external MCP proxy calls.  They can use only an explicit pre-approved,
stable per-tool policy; they never create a background notification waiting
for a human.  This replaces the current pressure to use a broad Codex
allowlist.

## Interaction contract

Add a backend-neutral persisted `mcp_approval_requests` domain and an
`external-mcp` approval dispatcher.  The dispatcher resolves the conditional
state transition and wakes only the owning proxy waiter.  The request is
published through the existing notification fanout plus a session WebSocket
event, so web, Telegram and Discord operate on the same record.

The web card and channel messages receive only:

```json
{
  "approval_id": "mapr_…",
  "session_id": "…",
  "server": "spin",
  "tool": "verify",
  "arguments_preview": {"model": "…"},
  "risk": {"level": "prompt", "reasons": ["external MCP"]},
  "deadline_at": "…",
  "choices": ["approve_once", "decline"]
}
```

Policy-changing choices are shown only after an authenticated policy-edit
flow, not as a broad button on a normal card.  The answer endpoint supplies
the approval id and server-issued decision nonce.  It is idempotent and
returns the settled state, not an inferred success.  Cards for a different
active chat session remain actionable but visibly name their originating
session.

On restart, Nerve reloads pending records.  Because live stdio connections
cannot be reattached safely, their pending records become `lost` and fail
closed; their clients receive a transport failure/closed pipe.  For HTTP/SSE,
the proxy may resume only if both the upstream transport and session-resume
contract support it; otherwise it also marks `lost`.  A pending record whose
deadline expires becomes `timed_out`; absence of every UI is not approval.

## Threat model

| Threat | Control |
| --- | --- |
| Malicious agent names another session/server | Per-session signed proxy credential and immutable binding; lookup by configured server only |
| Arguments changed after the card is displayed | Canonical digest bound to conditional approval; forward exactly stored payload |
| Tool schema changes behind a stable name | Capture schema digest; invalidate cached policy and pending approval |
| Replay/double-click/duplicate delivery | Unique idempotency key, conditional DB transition, one upstream dispatch lease |
| Config reload turns a prompt into allow | Snapshot config revision; re-evaluate and fail closed before dispatch |
| Secret leakage in UI/audit/logs | Schema-aware redaction; argument digest; credentials remain upstream-only synthetic env/header values |
| Confused deputy via loopback proxy | Audience-bound short-lived credential, no client session ids, independent upstream JSON-RPC ids |
| Nerve/proxy crash | Durable state; no automatic replay; unresolved calls lost/expired and audited |
| DoS by many pending calls | Per-session/server pending limits, deadlines, bounded previews/logs, queue backpressure |

The proxy is an authorization point, not a sandbox.  A pre-approved external
server is still trusted with the credentials and network access its config
gives it; least-privilege credentials and command allowlists remain necessary.

## Implementation sequence

1. Add `mcp_approval_requests` migration and DB methods: create, conditional
   decide, claim-dispatch, settle, expire, recover-lost, and audit queries.
2. Extract persisted cross-channel approval dispatch from the existing
   notification dispatcher, then add `external-mcp` without changing the
   mechanical-action contract.
3. Implement the policy compiler and schema cache.  Extend config with an
   explicit external-MCP policy section; update `config.example.yaml` and
   `docs/config.md`.
4. Implement `nerve/mcp_proxy/` core JSON-RPC relay and stdio adapter.  Change
   Codex config generation to point each external stdio server at it and force
   Codex's own mode to `approve` only for proxy endpoints.
5. Add loopback HTTP/SSE adapter with session credential minting.  Keep direct
   URL transport behind a temporary explicit feature flag until parity tests
   pass.
6. Render the expanded approval card and add Telegram/Discord actions via the
   common notification record.  Do not offer persistent policy mutation from
   ordinary chat buttons.
7. Roll out with `external_mcp_approval_proxy.enabled: false`, first for one
   stdio server and one HTTP server in shadow/audit mode, then enable prompt
   enforcement.  Remove the temporary direct-server `prompt` configuration
   path only after migration.

## Test matrix and acceptance criteria

| Layer | Cases |
| --- | --- |
| Policy/unit | precedence; unknown tool/schema deny; canonical JSON digest; redaction; config revision; one-shot scope |
| DB/dispatcher | conditional approval; duplicate answer; expiry; cancel race; restart recovery; dispatch lease exactly once |
| stdio relay | initialize/list/call forwarding; approved and declined call; EOF; cancellation; credentials absent from argv/logs |
| HTTP/SSE relay | header isolation; session credential rejection; reconnect/resume policy; streaming progress and large result backpressure |
| Integration | Codex receives proxy config; `prompt` reaches Nerve; web/Telegram/Discord answer same request; noninteractive default deny |
| Security/regression | cross-session token misuse; changed arguments/schema; replayed response; upstream late result; no dispatch after timeout |

Acceptance requires one stdio and one HTTP server to complete a prompted
approve/decline round trip, no upstream `tools/call` before approval, exactly
one upstream call after approval, and fail-closed behavior for every timeout,
disconnect, invalid binding, and restart path.
