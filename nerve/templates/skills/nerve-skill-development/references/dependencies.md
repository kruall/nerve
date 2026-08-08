# Dependencies and resources

Put indispensable instructions in `required`; Nerve resolves these transitively, deterministically, and fail-closed. Put conditional optional help in `suggested`; Nerve displays but never loads or evaluates it. Lists contain a skill ID or a mapping with `skill`; suggested mappings may add a human-readable `when`. Do not use version constraints, duplicate edges, self-dependencies, or an edge in both groups.

Resources must stay inside the package: `references/`, `scripts/`, `assets/`, and optional `agents/openai.yaml`. Do not replace or delete existing resources merely because `skill_update` changes only `SKILL.md`; report their preservation explicitly in the proposal.
