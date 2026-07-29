# Configuration Reference

Nerve uses two YAML config files:
- `config.yaml` — Template settings (version controlled)
- `config.local.yaml` — Secrets and personal overrides (gitignored)

Values in `config.local.yaml` are deep-merged on top of `config.yaml`.
Unknown keys are ignored but logged as warnings at startup (and shown by
`nerve doctor`) so typos don't fail silently.

## Config Directory Resolution

`nerve` commands locate the config directory via a waterfall, so they work
from any working directory:

1. `--config-dir` / `-c` flag
2. `NERVE_CONFIG_DIR` environment variable
3. The current directory, if it contains `config.yaml` or `config.local.yaml`
4. The pointer file `~/.nerve/config_dir` (written by `nerve init` and on
   daemon start)
5. The current directory (fresh-install fallback)

`nerve doctor` reports which directory was used and how it was found.

## Core

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `workspace` | path | `~/nerve-workspace` | Path to workspace directory |
| `timezone` | string | `America/New_York` | Local timezone for scheduling |
| `deployment` | string | `server` | `server` (bare metal) or `docker`. Set during `nerve init`; determines whether CLI commands run directly or proxy to `docker compose`. |

> **Note:** The _mode_ (personal vs worker) is not a config field — it's determined at `nerve init` time and expressed through which workspace templates, cron jobs, and memory categories are active. There's no `mode` key in config.

## Agent

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `agent.model` | string | `claude-opus-4-8` | Primary model for conversations |
| `agent.cron_model` | string | `claude-sonnet-4-6` | Claude backend model for cron jobs (cheaper) |
| `agent.max_turns` | int | `50` | Max agentic turns per request |
| `agent.max_concurrent` | int | `4` | Max concurrent agent sessions |
| `agent.cache_ttl` | string | `"5m"` | Prompt-cache write TTL policy: `5m` (status quo), `1h` (always request the 1-hour TTL), or `auto` (per session at client-build time: sparse-cadence sessions — persistent crons, wakeup loops, spaced chats — get `1h`; dense sessions stay on `5m`). Per-cron-job override via `cache_ttl` in jobs.yaml. See `nerve/agent/cache_policy.py` |
| `agent.cache_ttl_excluded_models` | list | `[]` | Model-name substrings that never request the 1h TTL |
| `agent.prompt_rewrite.enabled` | bool | `true` | Offer the first-prompt rewrite feature in the web UI (per-user toggle lives in the composer) |
| `agent.prompt_rewrite.model` | string | `""` | Model for prompt rewriting (empty = `agent.model`, the chat model) |
| `agent.prompt_rewrite.max_tokens` | int | `1024` | Max tokens for the rewritten prompt |
| `agent.prompt_rewrite.timeout_seconds` | float | `45.0` | Rewrite API call timeout |

**Prompt rewrite:** when the ✨ toggle in the composer is on, the first prompt of a new chat is rewritten by a fast model to better express intent. The result is previewed (editable) and only sent after explicit approval — the user can always send the original instead. Trivial or already-clear prompts are sent unchanged without a preview.

**Note:** The engine uses a `can_use_tool` callback (not `bypassPermissions`) so that interactive tools (`AskUserQuestion`, `ExitPlanMode`, `EnterPlanMode`) can pause mid-turn for user input. All other tools are auto-approved. See [sdk-sessions.md](sdk-sessions.md#permissions--interactive-tools) for details.

## Agent Backends (claude / codex)

Nerve can run sessions on two agent runtimes. The backend is selected per
NEW session and is **sticky**: it's stamped into `sessions.backend` at first
client build and always wins over config afterwards, so flipping the
defaults never crosses an existing conversation (or its wakeups) onto a
runtime that can't resume it. See `docs/plans/codex-backend.md`.

```yaml
agent:
  backend: claude          # claude | codex — new interactive sessions
  cron_backend: null       # null → backend; new cron/hook sessions only

codex:                     # active when a codex backend is selected
  bin_path: codex          # tested: >= 0.144.1 and < 0.145.0
  min_version: 0.144.1
  max_version: 0.145.0
  home_dir: ~/.nerve/codex # isolated CODEX_HOME (auth, config, sessions)
  model: gpt-5.6-sol
  cron_model: null         # null → model
  auth: chatgpt            # chatgpt | api_key
  api_key: null            # config.local.yaml; or api_key_env: OPENAI_API_KEY
  sandbox: danger-full-access   # read-only | workspace-write | danger-full-access
  approval_policy: never        # never | on-request | untrusted
  web_search: true
  tool_timeout_sec: 3600        # nerve MCP calls may block on ask_user
  turn_idle_timeout_seconds: null  # null → agent.cli_idle_timeout_seconds
  pricing:                      # $/1M tokens — cost is None for unlisted models
    gpt-5.6-sol: {input: 5.0, cached_input: 0.5, output: 30.0}
  extra_config: {}              # arbitrary codex -c key=value passthrough
  ultracode:                    # optional managed third-party orchestrator
    enabled: false
    auto_install: true
    repository: https://github.com/just-every/plugin-ultracode.git
    revision: 9dde0086e983413016bf62ab96ba6bb17b599fae
    version: 0.3.0+codex.20260601143116
    dashboard: false            # authenticated read-only Nerve UI
    ui: false                   # detached upstream server; keep disabled
    default_transport: exec
    max_concurrency: 2          # hard cap, even if a workflow asks for more
    default_token_budget: 250000 # default and maximum per workflow
    max_agents: 8               # lifetime worker cap per workflow
```

`codex.ultracode.dashboard` exposes run journals in Nerve's authenticated UI.
It does not start Ultracode's detached dashboard process. Keep
`codex.ultracode.ui: false`: the upstream process serves unauthenticated
mutation and execution endpoints and is not safe to expose through Nerve.

Setup for `auth: chatgpt`: run `CODEX_HOME=~/.nerve/codex codex login` once,
then `nerve codex doctor` to verify CLI version, authentication, the live
model list, protocol, and managed plugin state before flipping any backend
default. Codex sessions reach Nerve tools through a dedicated plaintext ASGI
listener bound to an ephemeral `127.0.0.1` port. Its bearer token is scoped to
the owning session, expires after eight hours, and exists only in the spawned
process environment. `mcp_endpoint.enabled` must stay on; the public gateway
mount and the loopback listener share the same authenticated MCP manager.

External/user-launched Codex uses `bearer_token_env_var = "NERVE_MCP_TOKEN"`
instead of storing a credential in TOML. Refresh it with:

```bash
export NERVE_MCP_TOKEN="$(nerve codex token)"
```

Billing follows the effective account reported by `account/read` (and preflight
flags a mismatch with `codex.auth`). With ChatGPT authentication, token counts and rate/credit events are retained,
but `cost_usd` is null: any API-price calculation is stored separately as an
`api_equivalent_estimate`. API-key sessions use `cost_basis: api_billed` when a
known price is available.

Ultracode is installed only into the isolated Nerve Codex home at the exact
configured revision. A hash-verified Nerve policy overlay hard-enforces the
configured concurrency, token, lifetime-agent, and dashboard caps even when a
workflow requests looser values. Autonomous marketplace updates and its
dashboard are off by default. Workers inherit stable MCP definitions through a Nerve wrapper,
exchange the parent credential for two-hour worker-scoped tokens, report usage
back into the parent turn, and run read-only unless a workflow explicitly asks
for a writable sandbox. `GET /api/codex/status` exposes preflight state and
non-terminal journals available for recovery.

Notes: prompt-cache TTL policy, Claude Code plugins, and Langfuse tracing are
claude-only. PDF attachments are surfaced to Codex as explicit path/context
notes rather than silently dropped. With the
default `approval_policy: never` + full-access sandbox, codex sessions
behave like claude's auto-approved tools; tightening the policy surfaces
Approve/Decline cards in the web UI.

## Gateway

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `gateway.host` | string | `0.0.0.0` | Bind address |
| `gateway.port` | int | `8900` | Port number |
| `gateway.ssl.cert` | path | - | SSL certificate path |
| `gateway.ssl.key` | path | - | SSL private key path |

## Telegram

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `telegram.enabled` | bool | `true` | Enable Telegram bot |
| `telegram.bot_token` | string | - | Bot token from @BotFather |
| `telegram.dm_policy` | string | `pairing` | `pairing` (allowlist + one-time pairing codes) or `open` (anyone — dangerous) |
| `telegram.allowed_users` | list[int] | `[]` | Telegram user IDs allowed to DM the bot |
| `telegram.stream_mode` | string | `partial` | `partial` (edit msgs) or `full` |

### Pairing

With `dm_policy: pairing` (the default), the bot only talks to users in
`allowed_users` and rejects everyone else. To authorize a user without
editing config files:

1. Run `nerve pair` on the server — it prints a one-time 6-digit code
   (valid 1 hour). On a fresh install with no `allowed_users`, a code is
   also generated automatically at startup and printed to the log.
2. Send the bot `/pair <code>` from the Telegram account to authorize.
3. The user ID is appended to `telegram.allowed_users` in
   `config.local.yaml` and takes effect immediately.

An unauthorized `/start` gets a reply with the sender's numeric ID and
pairing instructions (rate-limited); all other messages from unauthorized
users are ignored.

## Discord

Discord is disabled by default and connects through an outbound Gateway
WebSocket. It does not require an inbound listener or a public Nerve endpoint.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `discord.enabled` | bool | `false` | Enable the Discord adapter |
| `discord.bot_token` | string | - | Bot token; keep only in `config.local.yaml` |
| `discord.bot_token_file` | path | - | Preferred mode-0600 file containing one bot token |
| `discord.guild_id` | int | `0` | The only server the Nerve instance accepts |
| `discord.channel_ids` | list[int] | `[]` | Ordinary text channels where a mention starts a conversation thread |
| `discord.task_forums` | map[string, int] | `{}` | Project name to Discord forum-channel ID |
| `discord.audit_forum_id` | int | `0` | Outbound-only forum where Nerve mirrors one thread per session |
| `discord.audit_batch_window_seconds` | float | `60` | Window used to combine adjacent audit activity before updating Discord; terminal events flush immediately |
| `discord.allowed_author_ids` | list[int] | `[]` | Human and peer-bot IDs allowed to invoke Nerve |
| `discord.require_mention` | bool | `true` | Require a direct mention or a reply to the bot in ordinary channels and project-forum threads |

The adapter is fail-closed: enabling it without a guild, at least one inbound
channel/project forum or an outbound audit forum, and a token fails startup.
An author allowlist is additionally required whenever inbound targets are
configured. A forum post is a thread, so the forum's parent ID belongs in
`task_forums`. In an ordinary text channel, a direct mention starts a Discord
thread from that message and
the initial agent turn is routed there. Further messages by allowed authors in
ordinary-channel threads do not need another mention; they start agent turns,
but the agent may deliberately choose not to publish a reply. Project-forum
threads require either a direct mention or a reply to one of the bot's messages
for each agent turn. When a Discord reply is resolved, the agent input includes
its author and up to 500 characters of the referenced text. Replies are always
sent to the individual thread.

Allowed participant messages in project-forum threads are also kept as a
durable, bounded thread context even when they do not invoke the agent. On the
next mention or bot reply, Nerve prepends the earlier summary and recent
messages to the agent input. The full tail is capped by both message count and
character size; older messages are incrementally summarized through the
provider-neutral memory fast model. If that model is unavailable, delivery
continues with a bounded recent tail and the unsummarized messages remain
stored for a later compaction attempt. Messages from authors outside
`allowed_author_ids` are never added to this context.

On first connection Nerve primes each configured target without replaying old
history. Later reconnects use durable cursors to process messages missed while
Nerve was offline, including active conversation threads and active or recently
archived project-forum threads. The first invoked turn in an existing
project-forum thread separately bootstraps its bounded context from Discord
history, so enabling this behavior does not lose messages that predate its
database checkpoint.

The session stream and final answer remain internal. An agent publishes a
deliberate Discord-visible reply only through the session-bound `discord_send`
MCP tool. The tool cannot select a destination: it sends only to the
conversation or project-forum thread that drove the active Discord turn.

When `discord.audit_forum_id` is configured, Nerve creates one outbound-only
forum thread for every session created while the mirror is enabled, regardless
of whether the session came from Discord, web, Telegram, cron, a planner, or
another internal source. The thread contains the persisted user/assistant
messages, tool calls and results, lifecycle events, and errors. Active turns
are edited in place after the configured batching window and replaced with
their persisted form when complete. Adjacent persisted items within the same
window are packed into one Discord message when they fit; oversized content is
split only at Discord's message-size boundary. Completion, stop, and error
events flush without waiting for the window. Hidden model reasoning is never
mirrored.

The session-to-thread mapping and per-item Discord message IDs are stored in
the Nerve database. On restart, mapped threads are reconciled and incomplete
live placeholders are repaired without duplicating persisted content. Existing
historical sessions are not bulk-backfilled when the option is first enabled;
they acquire a mirror when they next become active. The audit forum must belong
to `discord.guild_id` and must not also appear in `discord.channel_ids` or
`discord.task_forums`, so messages in mirror threads cannot recursively invoke
the agent. Mirror threads can contain the same sensitive tool inputs and
outputs as the Nerve session UI, so restrict the audit forum's Discord
permissions to the intended reviewers.

The audit forum also contains three pinned notification inbox threads:
`Notifications`, `Questions`, and `Approvals`. All three notification kinds
are delivered to their Discord inbox whenever `discord.audit_forum_id` is
configured, even when `discord` is not listed under
`notifications.channels`.

Informational notifications have a persistent **Dismiss** control. Questions
render suggested-answer buttons plus **Write answer**, which opens a Discord
modal for free-form input. Approval cards keep their dispatcher-specific
controls: Approve executes directly, while Decline and Request changes open a
modal for written feedback. Pending controls are restored after a Nerve
restart. Only users listed in `discord.allowed_author_ids` can use them.

Enable the **Message Content Intent** for the bot in Discord's Developer
Portal. Grant only the channel permissions required by the deployment:
View Channels, Read Message History, Send Messages, Create Public Threads,
and Send Messages in Threads. Pinning the notification inboxes and assigning
forum tags additionally require Manage Threads; creating, updating, or
deleting the forum's available tags requires Manage Channels. Administrator
is not required.

```yaml
discord:
  enabled: true
  bot_token_file: ~/.config/nerve/discord-bot-token
  guild_id: 123456789012345678
  channel_ids: [123456789012345679]
  task_forums:
    YDB: 123456789012345680
  audit_forum_id: 123456789012345682
  audit_batch_window_seconds: 60
  allowed_author_ids: [123456789012345681]
  require_mention: true
```

### Discord forum-tag tools

Two agent tools expose tag management only for forums listed in
`discord.task_forums`:

- `discord_forum_tags` reads a project's available tags and, when a thread ID
  is supplied or inferred from the active Discord turn, its applied tags.
- `discord_forum_tag_action` prepares create/update/delete and
  add/remove/replace operations. It never mutates Discord during the tool
  call. Instead it creates a high-priority approval notification with
  **Approve** and **Decline** options.

On approval, a server-side dispatcher re-fetches the forum and thread,
verifies the guild and current `discord.task_forums` mapping, validates the
20-tag forum limit and 5-tag thread limit, and only then calls Discord's
Modify Channel endpoint. Decline and expired approvals perform no Discord
request. Approval cards use the configured notification channels (normally
the web UI and/or Telegram) and are additionally delivered to the pinned
`Approvals` thread whenever `discord.audit_forum_id` is configured.

Discord API reference:
[Channel and Forum Tag objects](https://docs.discord.com/developers/resources/channel)
and [forum/thread behavior](https://docs.discord.com/developers/topics/threads).

## Quiet Hours

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `quiet_start` | string | `02:00` | HH:MM — start of quiet period (local timezone) |
| `quiet_end` | string | `08:00` | HH:MM — end of quiet period (local timezone) |

## Sources (sync)

Sources pull data from external services on a schedule. See [sources.md](sources.md) for full details.

**Common fields** (available on all sources):

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.<source>.enabled` | bool | `true` | Enable/disable this source |
| `sync.<source>.schedule` | cron/interval | varies | Fetch frequency (crontab or interval like `2h`) |
| `sync.<source>.processor` | string | `agent` | `agent` (LLM review), `memorize` (direct memU), `notify` (channel forward), `none` |
| `sync.<source>.batch_size` | int | `50` | Max records per fetch cycle |
| `sync.<source>.prompt_hint` | string | `""` | Extra instructions for the agent prompt |
| `sync.<source>.model` | string | `""` | Override model (empty = `agent.cron_model`) |
| `sync.<source>.condense` | bool | `false` | LLM-condense long records via `memory.fast_model` before processing |

**Telegram-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.telegram.api_id` | int | - | Telethon API ID (from my.telegram.org) |
| `sync.telegram.api_hash` | string | - | Telethon API hash |
| `sync.telegram.schedule` | cron | `*/5 * * * *` | Fetch frequency |
| `sync.telegram.exclude_chats` | list[int] | `[]` | Chat IDs to skip |
| `sync.telegram.monitored_folders` | list | `[]` | Telegram folder names to filter |

**Gmail-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.gmail.accounts` | list | `[]` | Gmail accounts to sync |
| `sync.gmail.schedule` | cron | `*/15 * * * *` | Fetch frequency |
| `sync.gmail.keyring_password` | string | - | gog keyring password |

**GitHub-specific:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sync.github.schedule` | cron | `*/15 * * * *` | Fetch frequency |

## Memory (memU)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `memory.provider` | string | `inherit` | Chat provider for memU: `inherit`, `anthropic`, `bedrock`, or `codex`. `inherit` follows the top-level `provider.type` and preserves the previous behavior. |
| `memory.recall_model` | string | `claude-sonnet-4-6` | Model for LLM recall routing/ranking when embeddings are disabled |
| `memory.memorize_model` | string | `claude-sonnet-4-6` | Model for memory extraction when embeddings are enabled. Empty reuses `recall_model`. |
| `memory.fast_model` | string | `claude-haiku-4-5-20251001` | Model for preprocessing, categorization, category summaries, date resolution, and knowledge filtering. Also used for extraction and recall ranking when embeddings are disabled. Empty reuses `recall_model`. |
| `memory.embed_model` | string | *(empty)* | Independent OpenAI embedding model. Requires top-level `openai_api_key` (for example, `text-embedding-3-small`). |
| `memory.codex_workers` | int | `2` | Number of isolated Codex app-server workers. Values are clamped to `1`–`4`; each worker handles one memory turn at a time. |
| `memory.codex_effort` | string | `low` | Codex reasoning effort for memory writes and, without embeddings, LLM recall: `low`, `medium`, `high`, `xhigh`, `max`, or `ultra`. |
| `memory.semantic_dedup_threshold` | float | `0.85` | Cosine similarity threshold for semantic deduplication (0 to disable) |
| `memory.knowledge_filter` | bool | `false` | Post-extraction LLM filter that deletes generic knowledge items (one extra `fast_model` call per memorize) |
| `memory.categories` | list | `[]` | Seed categories — each entry has `name` and `description` fields. Used for semantic routing when memorizing and recalling facts. `nerve init` populates mode-appropriate defaults (personal: relationships, finances, health, etc.; worker: patterns, procedures, approvals, etc.). |

Provider behavior:

- `inherit` resolves to `bedrock` when `provider.type: bedrock`, otherwise to
  `anthropic`.
- `anthropic` uses the configured Anthropic-compatible API or CLIProxyAPI.
- `bedrock` requires the top-level `provider.type: bedrock` configuration.
- `codex` sends chat work through isolated, ephemeral `codex app-server`
  sessions. It uses `codex.home_dir`, `codex.auth`, and the existing ChatGPT
  login or explicitly configured `codex.api_key`; the top-level
  `openai_api_key` is never reused as Codex chat authentication. Native
  command/file, app, browser, computer-use, plugin, collaboration, MCP, and
  dynamic tool paths are disabled for memory workers. Effective config and
  managed feature requirements are verified before a thread starts; enabled
  persistent MCP servers or forced forbidden features fail closed.

With `memory.provider: codex`, `memory.recall_model` must contain a Codex model
ID available to the authenticated account. `memorize_model` and `fast_model`
may be empty to reuse it:

```yaml
memory:
  provider: codex
  recall_model: gpt-5.6-terra
  memorize_model: gpt-5.6-terra
  fast_model: gpt-5.6-terra
  codex_workers: 2
  codex_effort: low
  embed_model: text-embedding-3-small
```

When both `openai_api_key` and `memory.embed_model` are configured, memU uses
OpenAI to produce vectors and performs retrieval/ranking against its local
SQLite-backed vector index. Normal recall therefore does not spend a Codex
turn; Codex handles the write-side extraction, preprocessing, categorization,
date resolution, and optional knowledge filtering. Without embeddings, recall
falls back to LLM ranking through the selected memory provider.

## xmemory (optional, alongside memU)

[xmemory.ai](https://xmemory.ai) is an optional schema-backed memory layer that runs **alongside** memU — it never replaces it. Activated only when both `xmemory.api_key` and `xmemory.instance_id` are set (put them in `config.local.yaml`); otherwise it is completely inert (no SDK calls, zero overhead). The instance and its schema are created out of band on xmemory's side.

When active:
- The `memorize` tool **dual-writes**: memU (as always) plus an async `write_async` to xmemory. Failures on the xmemory side never fail the tool.
- `memory_recall` appends xmemory's read result (serialized as JSON) to memU's N items, run concurrently so the dual lookup is one round-trip. Read behavior is controlled via `xmemory.read_mode` (defaults to `single-answer`).
- The memorization **sweep** (session-close / cron) stays memU-only — it does not go through the `memorize` tool handler.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `xmemory.api_key` | string | *(empty)* | xmemory bearer token (invite-only). Secret → `config.local.yaml`. |
| `xmemory.instance_id` | string | *(empty)* | The xmemory instance to bind. Both this and `api_key` are required to activate. |
| `xmemory.api_url` | string | `https://api.xmemory.ai` | API base URL. |
| `xmemory.extraction_logic` | string | `deep` | Write extraction mode: `deep` (accurate) or `fast` (high-volume). |
| `xmemory.read_mode` | string | `single-answer` | Read mode for recall, whose result is appended as JSON: `single-answer` (synthesized answer envelope), `raw-tables` (table columns + rows), or `xresponse` (objects + relations). |
| `xmemory.timeout` | float | `60.0` | Per-request timeout in seconds. |

## Docker

Configuration for Docker deployment. Only relevant when `deployment: docker`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `docker.extra_mounts` | list[string] | `[]` | Additional host:container mount pairs to add to `docker-compose.yml`. Example: `["~/code:/code", "~/projects:/projects"]` |

The core Docker mounts (source code, `~/.nerve`, workspace) are always included. GitHub CLI (`~/.config/gh`) and Gmail CLI (`~/.config/gog`) auth directories are mounted automatically if they exist on the host.

## MCP Servers

External MCP servers can be added via config without code changes or restarts. The agent picks up new servers on the next session creation, or immediately via the "Reload" button in the UI / `mcp_reload` tool.

Config uses a **dict format** so `_deep_merge` correctly overlays secrets from `config.local.yaml`:

```yaml
# config.yaml — server definitions
mcp_servers:
  filesystem:
    type: stdio
    command: npx
    args: ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

  remote-api:
    type: http
    url: https://mcp.example.com/v1
```

```yaml
# config.local.yaml — secrets merge on top
mcp_servers:
  remote-api:
    headers:
      Authorization: "Bearer sk-secret-token"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `mcp_servers.<name>.type` | string | `stdio` | Transport: `stdio`, `sse`, or `http` |
| `mcp_servers.<name>.enabled` | bool | `true` | Enable/disable this server |
| `mcp_servers.<name>.command` | string | - | Command to run (stdio only) |
| `mcp_servers.<name>.args` | list | `[]` | Command arguments (stdio only) |
| `mcp_servers.<name>.env` | dict | `{}` | Environment variables (stdio only) |
| `mcp_servers.<name>.url` | string | - | Server URL (sse/http only) |
| `mcp_servers.<name>.headers` | dict | `{}` | HTTP headers (sse/http only) |

The built-in `nerve` server (SDK type, in-process) is always present and cannot be overridden.

### Claude Code Plugins

Nerve automatically discovers MCP servers from Claude Code's enabled plugins. Any plugin enabled in `~/.claude/settings.json` is loaded via the SDK's `--plugin-dir` flag, so the CLI handles OAuth, credentials, and plugin lifecycle natively.

- **No config needed** — just enable a plugin in Claude Code and restart Nerve.
- **OAuth works** — the CLI uses cached tokens from `~/.claude/.credentials.json`.
- **Auto-registered in UI** — plugin MCP servers appear in the MCP Servers page on first tool invocation (type: `plugin`).
- **No conflicts** — Nerve-configured MCPs (from `config.yaml`) and Claude Code plugin MCPs coexist; they use separate mechanisms (`--mcp-config` vs `--plugin-dir`).

## Auth

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `auth.password_hash` | string | - | bcrypt hash for login |
| `auth.jwt_secret` | string | - | JWT signing secret |

## API Keys (config.local.yaml)

| Key | Type | Description |
|-----|------|-------------|
| `anthropic_api_key` | string | Anthropic API key for Claude agent sessions and `anthropic` memory chat. Not required for paths using CLIProxyAPI, Bedrock, or Codex. |
| `openai_api_key` | string | OpenAI API key for memU embeddings. Independent of `memory.provider` and never implicitly used for Codex chat authentication. Requires `memory.embed_model`; without both settings, LLM-based recall is used. |
| `brave_search_api_key` | string | Brave Search API key (optional) |

## Proxy (CLIProxyAPI)

Optional local proxy that routes Anthropic API calls through Claude Code's OAuth authentication instead of a direct API key. When enabled, the API key is not required — all API calls go through the proxy at `localhost`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `proxy.enabled` | bool | `false` | Enable CLIProxyAPI proxy |
| `proxy.port` | int | `8317` | Proxy listen port |
| `proxy.host` | string | `127.0.0.1` | Proxy bind address |
| `proxy.binary_path` | path | `~/.nerve/bin/cli-proxy-api` | Path to CLIProxyAPI binary (auto-downloaded if missing) |
| `proxy.auth_dir` | path | `~/.nerve/cli-proxy-auth` | Directory for OAuth token storage |
| `proxy.api_key` | string | `sk-nerve-local-proxy` | Local auth key between Nerve and the proxy |
| `proxy.log_file` | path | `~/.nerve/proxy.log` | Proxy log file |

**Setup:**
```bash
# During nerve init, choose "Claude Code proxy" at the API configuration step.
# Or enable manually:
```

```yaml
# config.yaml
proxy:
  enabled: true
  port: 8317
```

```bash
# Authenticate with Claude (one-time):
~/.nerve/bin/cli-proxy-api --claude-login --no-browser \
  --config ~/.nerve/cli-proxy-config.yaml
```

The proxy binary is automatically downloaded from [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) on first start if not present. OAuth tokens are refreshed automatically.

## Sessions

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sessions.archive_after_days` | int | `30` | Auto-archive idle/stopped sessions older than this |
| `sessions.interactive_archive_after_hours` | int | `0` | Auto-close interactive (web/telegram/…) sessions after this many idle hours (`0` = disabled; opt-in). Cron/persistent sessions are unaffected. |
| `sessions.max_sessions` | int | `500` | Max active (non-archived) sessions before cleanup |
| `sessions.cron_session_mode` | string | `per_run` | `per_run` (unique session per cron run) or `reuse` (shared session per job) |

**Starred sessions are exempt from all auto-archival.** A session starred via
the star toggle (web sidebar, or the Telegram `/sessions` list / `/star`) is
never auto-closed: it is skipped by the idle cutoff and the
`archive_after_days` backstop, and is off-budget for `max_sessions` — neither
counted toward the cap nor evicted. It stays resumable until explicitly
unstarred, archived, or deleted.

## Retention

Opt-in `nerve.db` maintenance. Disabled by default. When enabled, a background
pass every `interval_hours` drops the verbose `blocks`/`thinking` JSON of old,
already-memorized messages (keeping the rendered `content`), prunes append-only
telemetry and file snapshots older than `retention_days`, and checkpoints the
WAL. This frees space inside the database but does not shrink the file on disk;
run `nerve db vacuum` once (with the daemon stopped) to reclaim it.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `retention.enabled` | bool | `false` | Master switch for the background retention pass |
| `retention.retention_full_days` | int | `30` | Compact `blocks`/`thinking` of memorized messages older than this |
| `retention.retention_days` | int | `90` | Prune telemetry and file snapshots older than this |
| `retention.interval_hours` | int | `24` | How often the background pass runs |

Manual commands (run regardless of `enabled`):

- `nerve db prune [--dry-run]` runs one pass immediately. `--dry-run` reports
  what would change without mutating.
- `nerve db vacuum` rewrites the file to reclaim freed pages. It takes a write
  lock, so stop the daemon first.

## Cron

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `cron.system_file` | path | `~/.nerve/cron/system.yaml` | System cron jobs (managed by `nerve init`) |
| `cron.jobs_file` | path | `~/.nerve/cron/jobs.yaml` | User-defined custom cron jobs |
