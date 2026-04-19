---
title: database-engine Task Router
docType: router
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when you already know the task belongs in database-engine but need the correct next file or next validation doc
  - when deciding whether a change belongs in migrations, config, tests, generated workspace files, or another repo
  - when investigating preview-branch, persistent-dev, or root-integration follow-up questions
whenToUpdate:
  - when new high-frequency task categories appear
  - when source-of-truth paths or generated-workspace workflows change
  - when cross-repo boundaries change
checkPaths:
  - AGENTS.md
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/workspace/**
  - scripts/**
  - .github/workflows/supabase-dev.yml
lastReviewedAt: 2026-04-18
lastReviewedCommit: 0ffe436a3bc80671d68c3f2ff37b248146bc6af2
related:
  - ../AGENTS.md
  - ./repo.yaml
  - ./validation.md
  - ./architecture.md
  - ../docs/agents/supabase-branching.md
  - ../supabase/workspace/README.md
  - ../scripts/README.md
---

# database-engine Task Router

## Repo Load Order

When working inside `database-engine`, load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. this file
4. `ai/validation.md` or `ai/architecture.md`
5. `docs/agents/supabase-branching.md` for deeper branch operations
6. `supabase/workspace/README.md` or `scripts/README.md` only for schema-workspace tooling

## High-Frequency Task Routing

| Task intent | First code paths to inspect | Next docs to load | Notes |
| --- | --- | --- | --- |
| Add or edit a migration, policy, trigger, or RPC | `supabase/migrations/**` | `ai/validation.md`, `ai/architecture.md` | Migration history is the checked-in schema truth. |
| Change shared or dev-only seed data | `supabase/seed.sql`, `supabase/seeds/dev.sql` | `ai/validation.md` | Keep shared and dev-only seed responsibilities separate. |
| Update branch bindings, redirects, or local Supabase behavior | `supabase/config.toml` | `ai/repo.yaml`, `docs/agents/supabase-branching.md` | Config changes often affect preview and persistent branch behavior together. |
| Change the persistent remote dev deployment path | `.github/workflows/supabase-dev.yml` | `ai/validation.md`, `docs/agents/supabase-branching.md` | This repo should keep only one checked-in push path for the persistent Supabase `dev` branch. |
| Investigate preview branch failure, drift, or `MIGRATIONS_FAILED` | failing file under `supabase/migrations/**` plus relevant config | `ai/validation.md`, `docs/agents/supabase-branching.md` | Prefer fixing the Git truth and recreating the branch over hand-editing remote state. |
| Browse remote schema or generate a migration from workspace files | `supabase/workspace/**`, `scripts/build_schema_workspace.py`, `scripts/new_migration.py` | `ai/architecture.md`, `supabase/workspace/README.md`, `scripts/README.md` | `supabase/workspace/global/**` and `schemas/**` are generated inspection views. |
| Change the stable manual overlay used for workspace-based migration authoring | `supabase/workspace/changes/**` | `ai/architecture.md`, `scripts/README.md` | Keep the relative structure aligned with `supabase/workspace/schemas/**`. |
| Update operator branch-binding templates | `.env.supabase.dev.local.example`, `.env.supabase.main.local.example` | `ai/repo.yaml`, `docs/agents/supabase-branching.md` | These are operator templates, not app runtime env files. |
| Decide whether a task belongs in frontend envs or app-side Supabase clients | `tiangong-lca-next` instead of this repo | root `ai/task-router.md` | This repo does not own frontend runtime env selection. |
| Decide whether a task belongs in Edge Function runtime code | `tiangong-lca-edge-functions` instead of this repo | root `ai/task-router.md` | Database-side SQL may call Edge Functions, but runtime code still belongs in the edge repo. |
| Change repo-local AI-doc maintenance only | `AGENTS.md`, `ai/**`, `.github/workflows/ai-doc-lint.yml`, `.github/scripts/ai-doc-lint.*` | `ai/validation.md` when present, otherwise `ai/repo.yaml` | Keep the repo-local maintenance gate aligned with root `ai/ci-lint-spec.md` and `ai/review-matrix.md`. |
| Decide whether work is delivery-complete after merge | root workspace docs, not repo code paths | root `AGENTS.md`, `_docs/workspace-branch-policy-contract.md` | Root integration remains a separate phase. |

## Wrong Turns To Avoid

### Editing generated schema workspace files as durable truth

Do not hand-maintain:

- `supabase/workspace/remote_schema.sql`
- `supabase/workspace/global/**`
- `supabase/workspace/schemas/**`

Use them for inspection. If you want to preserve edits, copy into `supabase/workspace/changes/**` first or write the migration directly under `supabase/migrations/**`.

### Treating GitHub default branch as the daily trunk

`database-engine` is an M2 repo:

- GitHub default branch: `main`
- true daily trunk: `dev`
- routine PR base: `dev`

### Moving app code into the database repo

Do not implement these here:

- app-side Supabase client code
- frontend runtime env selection
- Edge Function runtime logic

## Cross-Repo Handoffs

Use these handoffs when the work crosses boundaries:

1. database schema or SQL contract change plus frontend impact
   - start here
   - then notify `tiangong-lca-next`
2. database-triggered webhook or RPC change plus edge runtime impact
   - start here for SQL and Vault contract
   - then notify `tiangong-lca-edge-functions`
3. merged repo PR that still must ship through the workspace
   - return to `lca-workspace`
   - do the submodule pointer bump there

## If You Still Need More Context

Load:

1. `ai/architecture.md` for stable versus generated path rules
2. `ai/validation.md` for minimum checks
3. `docs/agents/supabase-branching.md` for branch-operation edge cases
