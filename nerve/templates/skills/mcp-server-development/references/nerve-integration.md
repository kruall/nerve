# Nerve registration and verification

Register an external stdio server in ignored/local configuration with its own
launcher and only required environment variables:

```yaml
mcp_servers:
  example:
    type: stdio
    command: /opt/example-mcp/.venv/bin/example-mcp
```

Keep secrets in the local secret source. New servers default to manual approval;
per-tool approval exceptions need reviewed guardrails. Run `mcp_reload` after a
configuration change, then create a new session: reload changes discovery only
for new sessions. Verify discovery, a safe fixture call, stderr-only diagnostics,
and containment of timeout/failure. The built-in `nerve` MCP is an in-process
Nerve-owned interface; a configured stdio entry is a separate child process.
