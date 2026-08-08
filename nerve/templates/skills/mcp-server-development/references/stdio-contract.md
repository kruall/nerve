# Standalone stdio contract

Each server owns a separate repository, package metadata, reproducible lockfile,
and runtime. Nerve's development environment and `PYTHONPATH` are not a runtime
contract. Put only newline-delimited JSON-RPC on stdout; logs and tracebacks go
to stderr. Advertise schemas with `additionalProperties: false`, validate every
request, bound input and output, and return structured non-secret errors.

Every invocation needs a deadline. Cancel promptly on `notifications/cancelled`
and clean up child work. Credentials are injected by environment or a secret
manager, never argv, source, fixtures, logs, or tool responses.
