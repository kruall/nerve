# Observability — Langfuse

Nerve has two independent optional Langfuse exporters: the Python exporter for
the Claude agent loop and Anthropic/Bedrock calls in memU, and the official
Langfuse Codex plugin for native Codex sessions. Python SDK calls become spans
tagged with
`session_id`, `source` (`web` / `cron` / `telegram` / `hook`), `model`, and
`channel`. The Codex plugin groups traces by the native Codex thread id stored
as `sdk_session_id`.

When the keys aren't set, the integration is a complete no-op — Nerve
runs identically with zero observability overhead.

## What gets captured

| Surface                       | Source                                               | Tags                                              |
|-------------------------------|------------------------------------------------------|---------------------------------------------------|
| Agent turns + tool calls      | `claude_agent_sdk` via LangSmith integration         | `source:*`, `model:*`, `channel:*` (when present) |
| memU Anthropic/Bedrock chat   | `anthropic` SDK via `AnthropicInstrumentor`          | `component:memu`, `purpose:summarize`             |
| Codex turns + tool calls      | official `codex-observability-plugin` Stop hook      | native Codex thread/session id                    |

Trace-level attributes (`session_id`, `metadata.parent_session_id`,
`metadata.fork_from`) are propagated to every span emitted inside a turn
via OpenTelemetry Baggage.

## Setup

### 1. Get a Langfuse project

Two options:

- **Langfuse Cloud** — sign up at <https://cloud.langfuse.com> and create
  a project. Region picks: `https://cloud.langfuse.com` (EU, default),
  `https://us.cloud.langfuse.com` (US),
  `https://jp.cloud.langfuse.com` (JP).
- **Self-hosted** — follow the upstream deployment guide at
  <https://langfuse.com/self-hosting/deployment/docker-compose>, then
  point Nerve at the resulting host URL.

### 2. Get API keys

In the Langfuse UI: *Project Settings → API Keys → Create new API keys*.
Copy the public (`pk-lf-...`) and secret (`sk-lf-...`) keys.

### 3. Configure Nerve

Put credentials in `<config_dir>/.env` (gitignored):

```dotenv
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=https://cloud.langfuse.com
TRACE_TO_LANGFUSE=false
```

`config.local.yaml` remains a compatible fallback:

```yaml
langfuse:
  public_key: pk-lf-...
  secret_key: sk-lf-...
  base_url: https://cloud.langfuse.com
```

Resolution is process environment → `<config_dir>/.env` → YAML → defaults.
`LANGFUSE_HOST` remains a legacy fallback for `LANGFUSE_BASE_URL`.

### 4. Optional Codex transcript export

Codex export requires credentials and explicit `TRACE_TO_LANGFUSE=true`.
Nerve ships a reviewed full commit SHA; override it only after reviewing a
different upstream revision:

```yaml
langfuse:
  codex:
    enabled: false              # overridden by TRACE_TO_LANGFUSE
    auto_install: true
    version: 0.1.0
    revision: 33bc50ba75ef82ed1f3718df6fdd06cdbfc7c02e
    max_chars: 20000
```

Nerve creates the marketplace under isolated `~/.nerve/codex`, installs only
that revision, disables floating updates, and enables Codex hooks only after
the installed version and revision match. The pinned `0.1.0` artifact reports
Codex's inclusive input/output counters as flat Langfuse usage buckets. Nerve
therefore applies the deterministic `exclusive-usage-v1` compatibility patch:
cached input and reasoning output are subtracted from their inclusive parent
buckets before export. Both source and bundled hook must match the reviewed
artifact exactly; the patched bytes and patch id are bound into the managed
install receipt. A mismatch disables Codex tracing without blocking Codex.

In headless app-server mode Nerve also
bypasses the interactive hook-trust prompt only after that verification. The
bypass is supplied both to the app-server process and to each fresh or resumed
thread's runtime config because process-level app-server flags alone do not
become thread config overrides. The isolated Codex home must therefore contain
only managed, reviewed hooks.
Credentials are supplied only in the child Codex process environment. Missing
credentials, an invalid pin, installation/network failure, or exporter failure
leaves Codex running without transcript tracing and appears as a separate
diagnostics error.

Restart Nerve. On startup you should see one of:

- `Langfuse: enabled (host=...)` — keys valid, tracing active.
- `Langfuse: disabled (no public_key/secret_key in config)` — keys absent.
- `Langfuse: auth_check failed against ...` — keys present but rejected.

Visit the diagnostics page (`/diagnostics`) to confirm the live status.

## Configuration reference

| Field             | Default                          | Notes                                                           |
|-------------------|----------------------------------|-----------------------------------------------------------------|
| `public_key`      | `""`                             | `pk-lf-...` — required to activate.                             |
| `secret_key`      | `""`                             | `sk-lf-...` — required to activate.                             |
| `base_url`        | `https://cloud.langfuse.com`     | Region endpoint or self-hosted URL (`host` is legacy).          |
| `redact_patterns` | (built-in secret regexes)        | List of regexes — matched substrings are replaced with `[REDACTED]`. |
| `codex.enabled`   | `false`                          | Explicit opt-in; `TRACE_TO_LANGFUSE` has priority.              |
| `codex.auto_install` | `true`                       | Install the pinned plugin when the first Codex client starts.   |
| `codex.version`   | `0.1.0`                          | Expected plugin manifest version.                               |
| `codex.revision`  | `33bc50ba...c7c02e`             | Reviewed full git SHA; override only for an explicit upgrade.   |
| `codex.max_chars` | `20000`                          | Maximum captured characters per large input/output field.       |

The default `redact_patterns` strip common secret formats: Anthropic API
keys, Langfuse keys, and bcrypt hashes. Add more for any project-specific
secret formats you don't want to leave the host.

## Privacy note

When enabled, **prompt content, tool inputs, and model outputs leave the
host** to whichever Langfuse instance you point at. The `base_url` field is
the boundary — make sure it points where you want the data to go. For
strict data residency, self-host Langfuse on infrastructure you control.

`redact_patterns` is a defensive layer — useful even with trusted
endpoints in case a secret leaks into a prompt accidentally. It protects the
Python exporter only. The official JavaScript Codex plugin reads completed
rollouts and can upload prompts, assistant messages, reasoning summaries,
tool inputs/outputs, model metadata, and usage. `codex.max_chars` bounds large
fields but is not secret redaction; do not opt in for sessions whose transcript
must remain local.

## Disabling

Set `TRACE_TO_LANGFUSE=false` to disable Codex transcript export. Remove or
empty credentials to disable both exporters. A restart is required because
instrumentation and Codex child environments are assembled at process start.

## Cost cross-check

Langfuse computes its own cost based on token counts and a price model
maintained by Langfuse. Nerve's `db/usage.py` computes cost in-process
via a hardcoded `MODEL_PRICING` dict and the SDK's
`ResultMessage.total_cost_usd`. Expect minor mismatches between the two —
they're independent calculations. Treat Langfuse as a second source of
truth for catching local cost-tracking bugs.

### Prompt-cache pricing (usage rewriter)

The LangSmith `claude-agent-sdk` integration reports usage in
LangSmith's canonical format: `input_tokens` *includes* prompt-cache
reads and writes, with the breakdown only in `input_token_details`.
Langfuse's OTEL ingestion doesn't read that detail field, so without
correction it prices every cached token at the full uncached input rate
— a ~5-10x cost overcount on agent sessions, which are typically >95%
cache reads billed at 10% of the input price.

Nerve fixes this at export time: `init_langfuse` wraps the Langfuse
OTLP exporter with `nerve/observability/usage_rewrite.py`, which
rewrites each agent span's `gen_ai.usage.*` attributes from the
accurate `langsmith.metadata.usage_metadata` payload into the same
shape `opentelemetry-instrumentation-anthropic` emits (uncached input +
explicit cache-read / cache-creation counts). Langfuse then applies its
managed per-model cache prices.

The diagnostics status block reports this as `usage_rewriter: true`.
If it's `false` while `enabled` is `true`, the SDK layout probably
changed underneath the installer — agent costs in Langfuse will be
inflated until it's fixed. Known approximation: 1-hour cache writes are
priced at the 5-minute rate (Langfuse's OTEL mapping has no separate 1h
key).

## Troubleshooting

- **Spans aren't appearing.** Check `/api/observability/status` —
  if `auth_ok: false`, the keys are wrong. If `enabled: false` despite
  keys being set, look at startup logs for an `ImportError` on the
  `langfuse` package itself (run `uv sync` to refresh).
- **Claude tracing is green but Codex is absent.** Check the separate
  `codex_plugin` block for requested/installed/ready, expected version and
  revision, auth, and the last safe error. Python status does not prove the
  transcript plugin ran.
- **Spans are tagged but session_id is missing.** That can happen if the
  installed Langfuse SDK doesn't accept `session_id=` kwarg in
  `propagate_attributes`. Upgrade to a newer Langfuse Python SDK.
- **The host runs out of memory under heavy load.** The Langfuse SDK
  buffers spans and ships them async. If memory is tight you can drop
  the Anthropic instrumentation by editing `init_langfuse`, or deploy
  Langfuse self-hosted on a separate machine.
