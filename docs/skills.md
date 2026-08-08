# Skills

Nerve discovers skill packages at `workspace/skills/<skill-id>/SKILL.md`. A
skill's Markdown body contains instructions; YAML frontmatter contains portable
fields and Nerve-specific metadata.

## Canonical package schema

New packages must use this Codex-compatible shape. `name` is a lowercase,
hyphenated identifier and must equal the directory name. `description` is
required and is limited to 1024 characters. Nerve owns only the
`metadata.nerve` namespace; portable top-level fields remain available to Codex.
The Markdown instruction body is required. When the create API receives no
body, it generates a minimal heading and description so the resulting package
remains structurally valid for Codex.

```yaml
---
name: deploy-service
description: Deploy a service safely.
metadata:
  nerve:
    version: 1.0.0
    context: domain
    dependencies:
      required: [release-checklist]
      suggested:
        - skill: kubernetes
          when: The service runs on Kubernetes.
---
```

`version`, `context`, and `dependencies` are Nerve metadata. `agents/openai.yaml`
is ignored for Nerve-only packages. Set `metadata.nerve.codex: true` only for a
dual-use package; then Nerve also checks that this optional file is valid YAML.
References, scripts, assets, and agent metadata must resolve inside the skill
directory; symlinks outside it are rejected.

The shared validator is used by discovery, tool and HTTP create/update, and
plan-driven skill installation. It validates a full replacement before writing,
uses atomic file replacement, and refuses a create when either the target
directory or a registry entry already exists. Invalid writes leave the existing
file and registry unchanged.

## Dependency declarations

Put new dependency declarations in the canonical Nerve metadata namespace:

```yaml
---
name: deploy-service
description: Deploy a service safely.
metadata:
  nerve:
    dependencies:
      required:
        - incident-response
        - skill: release-checklist
      suggested:
        - skill: kubernetes
          when: The target service is deployed to Kubernetes.
---
```

`required` and `suggested` are lists. An entry is either a skill ID or a mapping
with `skill`; suggested mappings may also contain `when`. The condition is
human-readable advisory text. Nerve displays it but does not evaluate it.

Version constraints are deferred for the MVP. Dependency entries do not accept
`version` or other constraint fields. A dependency declaration also grants no
tools, permissions, scripts, or resource access; each runtime continues to apply
its normal controls.

### Required composition

`skill_get` resolves required edges transitively and fail-closed. Loading is
blocked if the root skill or any required dependency is:

- missing or disabled;
- not model-invocable for a model-initiated load;
- part of a required cycle;
- invalidly declared; or
- beyond the graph limits of 5 dependency levels or 20 unique required skills.

Successful bundles are deterministic. Nerve visits sibling requirements in
lexical skill-ID order, emits each dependency after its own requirements, emits a
shared transitive skill once, and places the requested skill last. Required
skills should therefore contain instructions that are indispensable to every use
of the main skill.

### Suggested dependencies

Suggested edges are collected from a successful bundle, deduplicated by skill
ID, and displayed with their declared condition and source skill. Nerve never
loads, enables, validates the availability of, or evaluates conditions for a
suggested skill automatically. Put only optional, situational guidance in this
group.

## Migrating legacy declarations

Existing top-level declarations remain readable during the migration period:

```yaml
dependencies:
  requires: [incident-response, release-checklist]
  suggests:
    - skill: kubernetes
      when: The target service is deployed to Kubernetes.
```

The legacy reader also preserves the old single-string and single-entry group
shorthands. Canonical declarations deliberately require lists.

Move the value under `metadata.nerve.dependencies`, rename `requires` to
`required` and `suggests` to `suggested`, and remove the old top-level field.
Do not keep both forms: simultaneous canonical and legacy declarations are an
explicit validation error rather than a precedence rule. Canonical and migrated
legacy declarations otherwise produce the same sorted bundle.

Other old flat Nerve fields (`version`, `context`, and `agent`) are also loaded
with a `legacy_schema` diagnostic. Create always emits canonical packages;
updates retain the compatibility path so installed skills keep working while
they are migrated. Migrate fields into `metadata.nerve` and make `name` match
the directory on the next update. Discovery never rewrites user or third-party
skills; use its diagnostics as the migration report.

Every loaded skill exposes `skill_revision`, the full SHA-256 digest of the
exact installed `SKILL.md` bytes. Resources do not participate in this token.
Every replacement must pass that token as `expected_skill_revision`; stale
tokens are rejected before filesystem or registry changes. Consolidation also
passes the independent `amendments_revision`, so new append-only notes cannot
be cleared by an older review.

Versions use strict SemVer (`MAJOR.MINOR.PATCH`, with optional prerelease/build
metadata). A content-changing replacement must compare greater than the
installed version. An exact-content replacement is a no-op: it preserves the
file, amendments, registry timestamps, and version. Updates use a durable local
journal; interrupted transitions are rolled back before discovery or the next
update. For emergency repair, edit `SKILL.md` manually under explicit operator
authorization and run skill sync. That repair invalidates every previously
issued revision token; normal API and MCP writes never bypass CAS.

Invalid IDs, duplicate edges, required/suggested conflicts, self-dependencies,
conditional required edges, unsupported fields, and malformed group types are
reported in `skill_get`, the skill HTTP detail response, and the web skill page.
Failed `skill_get` calls return no partial instructions and are recorded as
unsuccessful usage.
