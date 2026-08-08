# Resource lease protocol models

These are finite safety abstractions with two executions and one physical
host. They do not establish correctness for unbounded populations or prove
liveness.

- `revoking-safe.pml` models TTL expiry entering `REVOKING`, followed by
  confirmed stop or quarantine. A host becomes `FREE` only after `running` is
  false. Delayed releases are fenced.
- `unsafe-ttl-reuse.pml` preserves the old regression: TTL expiry immediately
  makes a still-running host reusable and must produce a counterexample.

The selected property is `exclusive`: at most one execution is physically
running on the host.
