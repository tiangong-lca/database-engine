---
title: database-engine Repo Architecture Notes
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when you need a compact mental model before editing SQL, config, or schema-workspace tooling
  - when deciding whether a path is stable source of truth or generated inspection output
  - when the task mentions workspace-based migration authoring, remote schema export, or branch-specific database behavior
whenToUpdate:
  - when major repo paths or authoring workflows change
  - when the generated schema workspace contract changes
  - when new hotspot areas make the current map misleading
checkPaths:
  - docs/agents/repo-architecture.md
  - .docpact/config.yaml
  - supabase/config.toml
  - supabase/migrations/**
  - supabase/tests/**
  - supabase/workspace/**
  - scripts/**
  - .github/workflows/supabase-dev.yml
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-06-13
lastReviewedCommit: 5ce13e2d325eb3fc8272871c3a5784d4d64a90fc
related:
  - ../../AGENTS.md
  - ../../.docpact/config.yaml
  - ./repo-validation.md
  - ./supabase-branching.md
---

## Repo Shape

This repo is organized around one checked-in Supabase project plus a generated schema-inspection workspace.

## Stable Path Map

| Path group | Role |
| --- | --- |
| `supabase/config.toml` | shared local baseline plus branch-specific remote bindings |
| `supabase/migrations/**` | authoritative migration history and durable schema changes |
| `supabase/seed.sql` | shared seed data |
| `supabase/seeds/dev.sql` | persistent dev-only seed overlay |
| `supabase/tests/**` | PGTAP-style regression and access-control assertions |
| `.env.supabase.dev.local.example`, `.env.supabase.main.local.example` | operator branch-binding templates |
| `scripts/**` | export, refresh, change-copy, and migration-generation helpers |
| `.github/workflows/supabase-dev.yml` | only checked-in GitHub Actions automation for pushing committed migrations to the persistent remote `dev` branch |
| `supabase/workspace/changes/**` | manual overlay area used when generating migrations from workspace files |
| `supabase/workspace/remote_schema.sql` | generated full raw dump from the remote database |
| `supabase/workspace/global/**` | generated split-out global objects rebuilt on workspace refresh |
| `supabase/workspace/schemas/**` | generated human-browsable split schema objects rebuilt on workspace refresh |

## Branch Model In Practice

`database-engine` is an M2 repo:

- Git `dev` is the daily integration trunk
- Git `main` is the promoted release line
- PR branches map to Supabase preview branches
- `.github/workflows/supabase-dev.yml` pushes committed migrations to the persistent remote `dev` branch on Git `dev`
- the production Supabase project is migrated automatically by the Supabase GitHub integration when Git `main` advances

This means branch behavior is part of the repo architecture, not just delivery process.

## Current Hotspot Themes

The current migration and test history clusters around these themes:

1. access control and policy hardening
2. review workflow command/query RPCs
3. dataset lifecycle and publish/delete flows
4. notification and membership query boundaries
5. lifecycle bundle cleanup and embedding-related compatibility
6. remote schema reconciliation and preview-branch validation
7. review-submit gate persistence, `worker_jobs` queue state, final submit-review assertions, and retired legacy job-table archives
8. worker-produced domain artifact/state contracts for retained `lca_package_*`, LCA result/cache/projection, and review-submit report/coordinator tables

If the task touches one of those areas, expect both schema truth and regression assertions to matter.

## Worker Jobs And Domain State

`worker_jobs` is the canonical lifecycle and queue-control table for work that cannot be safely carried by Edge Function request/response execution.

Retained domain tables such as `lca_package_artifacts`, `lca_package_export_items`, `lca_package_request_cache`, `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, `lca_network_snapshots`, `dataset_review_submit_requests`, and `dataset_review_submit_gate_runs` are not replacement job tables. They store worker-produced artifacts, caches, projections, reports, or coordinator domain state. Post-cutover rows should be traceable back to `worker_jobs` through the appropriate worker job reference columns, except for explicitly documented exceptions such as snapshot identity rows that are traced through downstream worker-linked records.

Use `public.worker_domain_traceability_cutoffs` and `public.worker_domain_traceability_violations` for DB-side audit checks when validating that new worker-produced domain rows remain traceable.

## Generated Workspace Workflow

The generated schema workspace exists to inspect and transform remote schema objects without hand-maintaining the generated tree.

Use it like this:

1. refresh the generated workspace from the target remote database
2. inspect `remote_schema.sql`, `global/**`, or `schemas/**`
3. copy the object you want to modify into `supabase/workspace/changes/**`
4. generate a migration from the stable overlay file or write the migration directly

Do not leave durable manual edits only inside generated paths.

## Script Responsibilities

| Script | Job |
| --- | --- |
| `scripts/export_remote_schema.py` | export the target remote schema into `supabase/workspace/remote_schema.sql` |
| `scripts/build_schema_workspace.py` | rebuild the generated workspace tree from the exported schema |
| `scripts/copy_workspace_file_to_changes.py` | copy generated workspace files into the stable `changes/**` overlay |
| `scripts/new_migration.py` | generate a migration file from a supported overlay object path |
| `scripts/_db_workflow.py` | shared internal module for the helpers above |

## Cross-Repo Boundaries

This repo owns database truth, but not every runtime consequence:

- `database-engine` owns persisted review-submit gate runs, `worker_jobs` lifecycle schema/RPCs, review-submit job coordinator state, access checks, idempotent gate lookup, result recording, legacy lifecycle cutover cleanup, retired legacy job-table archives under `archive.worker_legacy_job_table_rows`, and the final submit-review assertion
- `tiangong-lca-worker` owns numeric-stability checks and the calculator report payload semantics
- `tiangong-lca-next` owns frontend env selection and app-side Supabase clients
- `tiangong-lca-edge-functions` owns Edge Function runtime orchestration, worker invocation, and API response shape
- `lca-workspace` owns root delivery completion after a child PR merges

If a task changes both schema and app behavior, the SQL truth still starts here.

## Common Misreads

- generated workspace files are not the durable schema source of truth
- GitHub default branch does not define the daily trunk
- a merged child PR does not finish workspace delivery
- `public.lca_jobs`, `public.lca_package_jobs`, and `public.dataset_review_submit_jobs` are not active or retained task surfaces after the `worker_jobs` cutover; use `worker_jobs`, retained domain result/artifact tables, and the archive table instead
- `lca_package_*`, LCA result/cache, and review-submit gate/coordinator tables are retained domain state, not leftover legacy job tables; clean them through domain retention contracts instead of dropping them as lifecycle tables

## Local Docpact Push Gate

This repository has a versioned local `pre-push` hook under `.githooks/pre-push` that delegates to `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`, so local agent shells do not need bare `docpact` on `PATH`. The hook is a local developer guard for docpact config validation and enforced doc-governance linting; ordinary PRs and pushes rely on the local gate; `.github/workflows/ai-doc-lint.yml` is manual-dispatch fallback for remote reproduction.
