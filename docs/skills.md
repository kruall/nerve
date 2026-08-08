# Skills

Nerve discovers skill packages at `workspace/skills/<skill-id>/SKILL.md`. A
skill's Markdown body contains instructions; YAML frontmatter contains portable
fields and Nerve-specific metadata.

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

Invalid IDs, duplicate edges, required/suggested conflicts, self-dependencies,
conditional required edges, unsupported fields, and malformed group types are
reported in `skill_get`, the skill HTTP detail response, and the web skill page.
Failed `skill_get` calls return no partial instructions and are recorded as
unsuccessful usage.
