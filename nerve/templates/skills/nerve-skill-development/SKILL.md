---
name: nerve-skill-development
description: >
  Create, revise, validate, or install Nerve workspace skills. Use when authoring
  a SKILL.md package, deciding between skill_create, skill_amend, skill_update,
  or a reviewed config PR, defining dependencies or resources, or preparing
  skill-extractor and skill-reviser proposals.
metadata:
  nerve:
    version: 1.0.0
    context: domain
---

# Nerve skill development

Use the system `skill-creator` for concise instruction design, progressive disclosure, and realistic forward-testing. This runbook overrides its package location, frontmatter, validation, and installation lifecycle for Nerve skills.

## Choose the mutation path

| Need | Use |
| --- | --- |
| Simple new Nerve-only skill without custom metadata or resources | `skill_create` |
| Verified reusable correction found while applying a skill | `skill_amend` |
| Reviewed full replacement of an existing `SKILL.md` | `skill_update` |
| New package with resources, dependencies, or a dual-use sidecar; any locked workspace | reviewed config PR |

Do not use an amendment to change policy speculatively or store secrets, transient failures, or task-local state. Do not use `skill_update` without a fresh `skill_get` revision token. Direct creation emits Nerve's default canonical metadata; it cannot create resource files or arbitrary metadata.

## Author the package

Read `references/package-schema.md` before authoring frontmatter and `references/dependencies.md` before declaring a dependency. Keep the body short and imperative; put detailed material in one-level-deep `references/`, deterministic helpers in `scripts/`, and output material in `assets/`.

Use `agents/openai.yaml` only when `metadata.nerve.codex: true`: it is a dual-use Codex package, and the sidecar must be valid YAML. Omit it for a Nerve-only skill. Preserve every existing resource and sidecar during a revision unless the reviewed change explicitly changes it.

## Validate and forward-test

Validate the complete package before proposing or installing it. Read `references/validation.md` for required checks. Forward-test a non-trivial skill using a realistic request and raw task artifact; do not leak the expected answer. Rework the package if it only succeeds because the test saw hidden implementation context.

## Proposals and revisions

Extractor and reviser proposals must contain the complete canonical `SKILL.md`, target skill ID, semantic version, declared dependencies, and validation/forward-test report. A revision proposal must also record the exact `skill_revision` from `skill_get`, the `amendments_revision` when amendments are consolidated, resources retained or changed, and each rejected amendment with its reason. Bump the version monotonically for a changed replacement.

For an approved update, reload with `skill_get` immediately before installation. Pass the current `expected_skill_revision`; when clearing amendments, also pass the current `amendments_revision`. A mismatch makes the proposal stale: do not retry by substituting a new token without review.

## Installation and lockdown

The filesystem package is the source of truth; Nerve indexes it in SQLite. Unlocked instances may use the mutation tools for their supported paths. A locked instance rejects them: submit the full reviewed package through the config-PR path and let the normal reviewed sync install it. Git provenance is available only for a verified config repository; it is not automatic history.

Plan approval always starts an implementation session. A skill plan is carried out by that session, which validates and applies the chosen path; approval does not directly install the plan text.
