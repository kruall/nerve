---
name: mcp-server-development
description: Decide whether an MCP capability belongs in Nerve or a standalone server, then build and verify a safe external MCP server.
metadata:
  nerve:
    version: 1.0.0
    context: domain
    dependencies:
      suggested:
        - skill: nerve-dev
          when: The placement changes Nerve-owned state, lifecycle, UI/API, or registration.
---

# MCP server development

## Gate before repository or worktree selection

Write this decision record before any code change:

| Question | Decision and evidence |
| --- | --- |
| State owner | |
| Session/lifecycle owner (start, cancellation, continuation) | |
| Useful to clients other than Nerve? | |
| Dependency and failure isolation required? | |
| Transport, credentials, and secret injection | |
| Nerve UI/API coupling | |
| Placement and version-control owner | |

Default generic local capabilities to a standalone stdio MCP server. Embed only
when Nerve owns concrete state or lifecycle semantics that cannot be an external
contract. Consumption through Nerve alone is insufficient. SPIN and
project-worktrees are external reference cases; their shared Nerve virtualenv
plus `PYTHONPATH` launch setup is migration debt, not a pattern. Detached
executions are embedded because Nerve owns session association, cancellation,
continuations, persistence, and UI state.

## External-server sequence

1. Assign a repository and version-control owner independent from Nerve.
2. Start from `assets/python-stdio-scaffold`; keep its independent package
   metadata, runtime, and lockfile. Do not use Nerve's shared virtualenv or a
   workspace `PYTHONPATH` shortcut.
3. Define strict schemas and bounded output before handler code. Reject unknown
   fields; use structured, non-secret errors; log only to stderr.
4. Bound execution with timeouts, honour `notifications/cancelled`, and keep
   protocol stdout exclusively for newline-delimited JSON-RPC.
5. Put credentials in the process environment or secret manager, never argv,
   source, lockfile, fixtures, or tool responses.
6. Run the scaffold protocol smoke test and the server's own tests.

Read `references/stdio-contract.md` for the transport contract and
`references/nerve-integration.md` when configuring Nerve. Use
`references/forward-tests.md` to check that the placement gate generalizes.
