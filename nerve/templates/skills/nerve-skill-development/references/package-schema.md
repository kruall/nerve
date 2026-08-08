# Canonical package schema

Every new Nerve package lives at `workspace/skills/<name>/SKILL.md`; its name and directory match. Use only the canonical namespace for Nerve metadata.

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

`name` is lowercase hyphenated; `description` is required and at most 1024 characters; `version` is strict SemVer. Do not emit legacy top-level Nerve fields (`version`, `context`, `dependencies`, `agent`) or legacy `requires`/`suggests` dependency keys. Keep only portable top-level fields plus `metadata.nerve`.
