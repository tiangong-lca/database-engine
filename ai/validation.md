---
title: database-engine Validation Guide
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when a database-engine change is ready for local validation
  - when deciding the minimum proof required for migration, config, workflow, script, or docs changes
  - when writing PR validation notes for database-engine work
whenToUpdate:
  - when the repo gains a new canonical validation command or wrapper
  - when change categories require different minimum checks
  - when schema-workspace tooling or branch operations change
checkPaths:
  - ai/validation.md
  - ai/task-router.md
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/seed.sql
  - supabase/seeds/**
  - scripts/**
  - .github/workflows/supabase-dev.yml
lastReviewedAt: 2026-04-18
lastReviewedCommit: 0ffe436a3bc80671d68c3f2ff37b248146bc6af2
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./task-router.md
  - ./architecture.md
  - ../docs/agents/supabase-branching.md
  - ../scripts/README.md
---

# database-engine Validation Guide

## Default Baseline

Unless the change is doc-only, the baseline local commands are:

```bash
supabase start
supabase db reset
supabase migration list
```

These commands confirm that the local stack starts, the migration history can be replayed, and the local migration state is inspectable.

## Validation Matrix

| Change type | Minimum local proof | Additional proof when risk is higher | Notes |
| --- | --- | --- | --- |
| `supabase/migrations/**` | `supabase db reset` succeeds | run the relevant SQL assertions under `supabase/tests/**`; inspect affected workspace objects if the migration was authored from workspace files | Record which migration and which test files were exercised. |
| `supabase/tests/**` only | run the relevant SQL assertion files against a reset local DB | add a nearby migration or policy smoke check if the new assertions expose a gap | This repo stores PGTAP-style SQL assertions, not a single canonical runner wrapper. |
| `supabase/seed.sql` or `supabase/seeds/dev.sql` | `supabase db reset` succeeds with expected seed behavior | rerun targeted SQL assertions that depend on the seeded rows | Keep shared seed and dev-only seed expectations separate. |
| `supabase/config.toml` | `supabase start` and `supabase db reset` still work locally | verify the changed branch-binding or auth assumption against `docs/agents/supabase-branching.md` | Config changes can affect preview, persistent dev, and local behavior together. |
| `.github/workflows/supabase-dev.yml` | inspect YAML changes and confirm referenced secrets/vars still exist in docs | verify the intended deploy path in a PR note because the real push occurs only on Git `dev` | Local dry-run for GitHub-hosted execution is limited; document the expected remote proof. |
| `scripts/**` | run the touched script with `--help` when possible, or execute the narrowest safe non-destructive path | if a script changes generated workspace behavior, refresh the workspace in a safe environment and inspect git diff | Avoid remote-destructive script runs unless the task explicitly requires them. |
| `supabase/workspace/**` | prove whether the touched file is generated or stable | if stable manual overlay files changed, explain how they feed migration generation | Generated files alone are not sufficient evidence of a durable schema change. |
| AI docs only | run repo-local `ai-doc-lint` against touched files or the equivalent local PR check | perform scenario-based routing checks from root into this repo | Refresh review metadata even when prose stays unchanged. |

## SQL Assertion Notes

The repo currently stores SQL assertions under `supabase/tests/**`.

Facts that matter:

- they use PGTAP-style SQL assertions
- no single checked-in wrapper defines the only valid invocation
- PR validation notes should therefore record the exact command or SQL runner used

If you add or change assertions:

1. reset the local DB first
2. run the relevant assertion files against that reset state
3. record the exact invocation in the PR

## Schema Workspace Tooling Checks

If the task touches `scripts/**` or `supabase/workspace/**`, first decide whether you changed:

- stable documentation or overlay paths
- generated inspection output
- helper scripts that transform one into the other

Useful low-risk checks:

```bash
python scripts/build_schema_workspace.py --help
python scripts/copy_workspace_file_to_changes.py --help
python scripts/new_migration.py --help
python scripts/export_remote_schema.py --help
```

If you actually refresh the workspace, make the validation note explicit about which generated paths changed and whether the change is intended to be committed.

## Preview, Persistent Dev, and Root Integration

For branch-oriented changes:

- preview-branch proof usually happens on the repo PR
- persistent remote `dev` proof happens after merge into Git `dev`
- root workspace proof happens later in `lca-workspace`

Do not collapse those three phases into one validation note.

## Minimum PR Note Quality

A good PR note for this repo should say:

1. which local commands ran
2. which SQL assertion files or migration paths were exercised
3. whether any proof is deferred to preview branch, persistent `dev`, or root integration
