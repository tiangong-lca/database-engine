---
title: database-engine AI Working Guide
docType: contract
scope: repo
status: active
authoritative: true
owner: database-engine
language: en
whenToUse:
  - when a task may change database schema, migrations, seeds, Supabase branch config, or database-side SQL tests
  - when routing work from the workspace root into the database-engine repo
  - when deciding whether a change belongs here, in tiangong-lca-next, in tiangong-lca-edge-functions, or in lca-workspace
whenToUpdate:
  - when repo ownership or source-of-truth paths change
  - when branch policy or workspace integration rules change
  - when the repo-local AI bootstrap docs under ai/ change
checkPaths:
  - AGENTS.md
  - README.md
  - ai/**/*.md
  - ai/**/*.yaml
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/seed.sql
  - supabase/seeds/**
  - supabase/workspace/**
  - scripts/**
  - docs/agents/**
  - .github/workflows/**
  - .env.supabase*.example
lastReviewedAt: 2026-04-18
lastReviewedCommit: 0ffe436a3bc80671d68c3f2ff37b248146bc6af2
related:
  - ai/repo.yaml
  - ai/task-router.md
  - ai/validation.md
  - ai/architecture.md
  - docs/agents/supabase-branching.md
---

# AGENTS.md — database-engine AI Working Guide

`database-engine` owns the checked-in Supabase database contract for the TianGong LCA workspace. Start here when the task may change schema truth, migration history, database-side tests, branch bindings, or the automation that deploys the persistent Supabase `dev` branch.

## AI Load Order

Load docs in this order:

1. `AGENTS.md`
2. `ai/repo.yaml`
3. `ai/task-router.md`
4. `ai/validation.md` for change verification
5. `ai/architecture.md` for schema-workspace or hotspot context
6. `docs/agents/supabase-branching.md` for deeper branch-operation rules
7. `supabase/workspace/README.md` and `scripts/README.md` only when working with generated schema workspace helpers

Do not start with the generated schema workspace, long migration files, or GitHub UI branch defaults.

## Repo Ownership

This repo owns:

- `supabase/config.toml`
- `supabase/migrations/**`
- `supabase/seed.sql`
- `supabase/seeds/**`
- `supabase/tests/**`
- `scripts/**` for schema export, workspace refresh, change copying, and migration generation
- `.github/workflows/supabase-dev.yml`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- branching and database-operations docs under `docs/agents/**`

This repo does not own:

- frontend runtime env selection or app-side Supabase clients
- Edge Function runtime code
- workspace submodule pointer bumps or delivery completion

Route those tasks to:

- `tiangong-lca-next` for frontend envs and app-side Supabase integration
- `tiangong-lca-edge-functions` for Edge Function runtime behavior
- `lca-workspace` for root integration after merge

## Branch Facts

- GitHub default branch: `main`
- True daily trunk: `dev`
- Routine branch base: `dev`
- Routine PR base: `dev`
- Promote path: `dev -> main`
- Hotfix path: branch from `main`, merge back into `main`, then back-merge `main -> dev`

Do not infer the working trunk from GitHub default-branch UI.

## Stable Vs Generated Paths

Treat these as stable manual-edit locations:

- `supabase/migrations/**`
- `supabase/seed.sql`
- `supabase/seeds/**`
- `supabase/tests/**`
- `supabase/workspace/changes/**`
- `scripts/**`
- docs and workflow files

Treat these as generated inspection views, not hand-maintained truth:

- `supabase/workspace/remote_schema.sql`
- `supabase/workspace/global/**`
- `supabase/workspace/schemas/**`

If you need to author a migration from generated workspace content, copy the target object into `supabase/workspace/changes/**` first and then generate the migration.

## Quick Routes

| If the task is about... | Load next |
| --- | --- |
| adding or editing a migration, policy, RPC, trigger, or seed | `ai/task-router.md`, then `ai/validation.md` |
| changing branch config, auth redirect settings, or the persistent dev workflow | `ai/repo.yaml`, then `docs/agents/supabase-branching.md` |
| investigating preview-branch failure or migration drift | `ai/validation.md`, then `docs/agents/supabase-branching.md` |
| browsing remote schema snapshots or generating a migration from workspace files | `ai/architecture.md`, then `supabase/workspace/README.md` and `scripts/README.md` |
| deciding whether the change belongs in frontend or edge runtime code instead | `ai/task-router.md` |
| deciding whether root workspace integration is still pending after merge | root `AGENTS.md` and `_docs/workspace-branch-policy-contract.md` in `lca-workspace` |

## Hard Boundaries

- Do not treat `supabase/workspace/global/**` or `supabase/workspace/schemas/**` as stable edit locations.
- Do not create a second workflow that pushes to the persistent Supabase `dev` branch without updating the repo contract docs in the same change.
- Do not move frontend `.env` or app-side client logic into this repo.
- Do not treat a merged PR here as delivery-complete when the workspace still needs a submodule bump.

## Workspace Integration

A merged PR in `database-engine` is repo-complete, not delivery-complete.

If the change must ship through the workspace:

1. merge the child PR into `database-engine`
2. make sure the intended SHA is eligible for root integration
3. update the `lca-workspace` submodule pointer deliberately

For normal root `main` integration, `lca-workspace/main` should point only at commits already promoted onto `database-engine/main`.
