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
  - when deciding which document owns a rule, command, or path boundary in this repo
whenToUpdate:
  - when repo ownership or source-of-truth paths change
  - when branch policy or workspace integration rules change
  - when the current documentation system becomes redundant or ambiguous
checkPaths:
  - AGENTS.md
  - README.md
  - README.zh-CN.md
  - .docpact/**/*.yaml
  - docs/agents/**
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
lastReviewedAt: 2026-04-24
lastReviewedCommit: 9cbf95369a7786d2b15e2787b46462ebbee42cc1
related:
  - .docpact/config.yaml
  - docs/agents/repo-validation.md
  - docs/agents/repo-architecture.md
  - docs/agents/supabase-branching.md
---

## Repo Contract

`database-engine` owns the checked-in Supabase database contract for the TianGong LCA workspace: schema truth, migration history, operator branch bindings, database-side tests, and the automation that deploys committed migrations to the persistent Supabase `dev` branch.

Start here when the task may change schema truth, branch bindings, generated schema-workspace tooling, repo validation rules, or documentation ownership inside this repo.

## Documentation System Principles

This repository treats documentation as an information system, not as narrative writing.

Required principles:

- single source of truth: one rule has one owning document
- one document, one job: each document solves one problem clearly
- conclusion first: put purpose, rules, steps, and boundaries before background
- no redundant prose: keep facts, rules, commands, exceptions, and validation; remove filler
- no ambiguity: prefer explicit conditions and exact actions over vague guidance
- executable commands: any documented command must run as written
- verifiable rules: readers must be able to tell whether they followed the rule correctly
- rules before explanation: operational content comes before rationale
- stable structure: same document type uses the same section order where practical
- reference instead of duplication: when a rule already has an owner, link to it instead of restating it

## Documentation Roles

| Document | Owns | Does not own |
| --- | --- | --- |
| `AGENTS.md` | repo contract, documentation principles, branch and delivery rules, hard boundaries | deep implementation details or large reference material |
| `.docpact/config.yaml` | machine-readable repo facts, routing intents, lint rules, governed-doc inventory | prose explanations and narrative summaries |
| `docs/agents/repo-validation.md` | minimum proof by change type and PR validation note shape | branch rationale or schema-workspace mental model |
| `docs/agents/repo-architecture.md` | compact repo mental model and stable-versus-generated path map | execution checklist details |
| `docs/agents/supabase-branching.md` and `docs/agents/supabase-branching_CN.md` | branch-specific database operations and branch-binding workflow | repo-wide validation matrix or generated-path map |
| `scripts/README.md` and `scripts/README.zh-CN.md` | helper-script usage and supported migration-generation flows | repo contract or branch-policy truth |
| `supabase/workspace/README.md` and `supabase/workspace/README.zh-CN.md` | generated-workspace contract and refresh warnings | schema source-of-truth ownership |

Additional governed source docs, not part of the default first-load surface:

| Document | Owns | Does not own |
| --- | --- | --- |
| `README.md` and `README.zh-CN.md` | repo landing context and high-level purpose | repo contract, proof bar, or branch-policy truth |

## Load Order

Read in this order:

1. `AGENTS.md`
2. `.docpact/config.yaml`
3. `docs/agents/repo-validation.md` or `docs/agents/repo-architecture.md`
4. `docs/agents/supabase-branching.md`
5. `supabase/workspace/README.md` or `scripts/README.md` only when the task touches schema-workspace tooling

Do not start from generated schema workspace files, long migration history, or GitHub default-branch UI.

## Operational Pointers

- path-level ownership, routing intents, governed-doc inventory, and lint rules live in `.docpact/config.yaml`
- minimum proof and PR validation note shape live in `docs/agents/repo-validation.md`
- stable path ownership and generated-workspace rules live in `docs/agents/repo-architecture.md`
- deeper branch-operation rules live in `docs/agents/supabase-branching.md`
- repo-local documentation maintenance is enforced by `.github/workflows/ai-doc-lint.yml` with `docpact lint`

## Minimal Execution Facts

Keep these entry-level facts in `AGENTS.md`. Use `docs/agents/repo-validation.md` and the narrow source docs for the full details.

- local baseline: `supabase start`, `supabase db reset`, `supabase migration list`
- migration authoring starts from Git `dev`, not GitHub default-branch UI
- preview-branch proof belongs to the repo PR
- persistent `dev` proof belongs after merge into Git `dev`
- root workspace proof belongs later in `lca-workspace`
- generated workspace helpers are low-risk to inspect with `python scripts/<name>.py --help`

## Ownership Boundaries

The authoritative path-level ownership map lives in `.docpact/config.yaml`.

At a human-readable level, this repo owns:

- `supabase/config.toml`
- `supabase/migrations/**`
- `supabase/seed.sql`
- `supabase/seeds/**`
- `supabase/tests/**`
- `scripts/**` for schema export, workspace refresh, change copying, and migration generation
- `.github/workflows/supabase-dev.yml`
- `.env.supabase.dev.local.example`
- `.env.supabase.main.local.example`
- repo-local governance and branching docs

This repo does not own:

- frontend runtime env selection or app-side Supabase clients
- Edge Function runtime code
- workspace submodule pointer bumps or delivery completion

Route those tasks to:

- `tiangong-lca-next` for frontend envs and app-side Supabase integration
- `tiangong-lca-edge-functions` for Edge Function runtime behavior
- `lca-workspace` for root integration after merge

## Branch And Delivery Facts

- GitHub default branch: `main`
- true daily trunk: `dev`
- routine branch base: `dev`
- routine PR base: `dev`
- promote path: `dev -> main`
- hotfix path: branch from `main`, merge back into `main`, then back-merge `main -> dev`

Do not infer the working trunk from GitHub default-branch UI alone.

## Documentation Update Rules

Use the role table in this file as the update map.

- if a machine-readable repo fact or governed-doc rule changes, update `.docpact/config.yaml` in the same change
- if a human-readable repo contract, branch rule, or hard boundary changes, update `AGENTS.md`
- if proof, architecture, or branch-operation guidance changes, update only the document that owns that subject
- if a document is governed but not in the default first-load surface, route to it on demand instead of duplicating its rules into `AGENTS.md`
- do not copy the same rule into multiple docs just to make it easier to find

## Hard Boundaries

- do not treat `supabase/workspace/remote_schema.sql`, `global/**`, or `schemas/**` as stable edit locations
- do not create a second workflow that pushes to the persistent Supabase `dev` branch without updating the repo contract docs in the same change
- do not move frontend `.env` or app-side client logic into this repo
- do not treat a merged PR here as delivery-complete when the workspace still needs a submodule bump

## Workspace Integration

A merged PR in `database-engine` is repo-complete, not delivery-complete.

If the change must ship through the workspace:

1. merge the child PR into `database-engine`
2. make sure the intended SHA is eligible for root integration
3. update the `lca-workspace` submodule pointer deliberately

For normal root `main` integration, `lca-workspace/main` should point only at commits already promoted onto `database-engine/main`.
