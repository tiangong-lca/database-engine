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
lastReviewedAt: 2026-07-22
lastReviewedCommit: a5395caa3e13819035286dac332e3a9cadd811a3
lastReviewedNote: "Reviewed issue #283 numerical snapshot certificate boundary through the authoritative migration path; the path map remains accurate."
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
| `supabase/seed.sql` | shared seed data; when no rows are needed, retain an executable no-op statement instead of a comments-only file so hosted Preview seeding has a valid SQL batch |
| `supabase/seeds/dev.sql` | persistent dev-only seed overlay |
| `supabase/tests/**` | PGTAP-style database assertions plus narrow offline Node contracts for test-runner control flow |
| `supabase/tests/preview/**` | exact-ref-bound disposable Hosted Preview mutation fixtures, cleanup, rollback-only fault assertions, and offline transport/lifecycle contracts; test-only and excluded from migrations, seeds, Dev data rehearsal, and production execution |
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
- `.github/workflows/supabase-dev.yml` pushes every committed migration missing from remote history to the persistent remote `dev` branch on Git `dev`, including an older-timestamped migration introduced by a governed `main -> dev` backmerge
- the production Supabase project is migrated automatically by the Supabase GitHub integration when Git `main` advances

This means branch behavior is part of the repo architecture, not just delivery process.

## Test Proof Layers

SQL assertions own database semantics and ACL regressions. Offline Node contracts own runner-only control flow, including outer-frozen request/namespace selectors, deterministic role emails, an outer-created exact empty mode-0700 private temp directory, fsync-before-ACK secret-free recovery checkpoints, exact filtered metadata recovery, global logout, hard DELETE followed by GET-404 plus a fresh empty filtered census, in-connection application-name binding, and fail-closed rendering/parsing of the 39-surface read-only residue proof. The inner runner may not begin actor sign-in or fixture mutation until the outer process has durably acknowledged the exact actor/selectors checkpoint. Cleanup shares the derivative coordinator advisory lock and is allowed only before either exact child crosses external dispatch; otherwise it fails into separately authorized durable recovery. A missing or ambiguous global logout always retains the actor and forbids hard DELETE. Those offline contracts use no Hosted database authority and do not replace the later exact-head Hosted mutation proof or independent Auth/SQL readback execution.

## Current Hotspot Themes

The current migration and test history clusters around these themes:

1. access control and policy hardening
2. review workflow command/query RPCs
3. dataset lifecycle, protected one-shot private owner-draft FP/UG alias rewrites, durable process-atomic Step 3 public-flow identity rewrites, guarded flow/process derivative rebuild coordination with dynamic 1..50 and retained fixed 23+27 closure proofs, and publish/delete flows
4. notification and membership query boundaries
5. lifecycle bundle cleanup and embedding-related compatibility
6. remote schema reconciliation and preview-branch validation
7. review-submit gate persistence, `worker_jobs` queue state, final submit-review assertions, and retired legacy job-table archives
8. worker-produced domain artifact/state contracts for retained `lca_package_*`, LCA result/cache/projection, and review-submit report/coordinator tables
9. canonical LCI/LCIA release runs, exact dataset-version indexes, immutable four-package artifact refs, durable approval, publication, and readback

If the task touches one of those areas, expect both schema truth and regression assertions to matter.

## Worker Jobs And Domain State

`worker_jobs` is the canonical lifecycle and queue-control table for work that cannot be safely carried by Edge Function request/response execution.

Retained domain tables such as `lca_package_artifacts`, `lca_package_export_items`, `lca_package_request_cache`, `lca_results`, `lca_result_cache`, `lca_latest_all_unit_results`, `lca_network_snapshots`, `dataset_review_submit_requests`, and `dataset_review_submit_gate_runs` are not replacement job tables. They store worker-produced artifacts, caches, projections, reports, or coordinator domain state. Post-cutover rows should be traceable back to `worker_jobs` through the appropriate worker job reference columns, except for explicitly documented exceptions such as snapshot identity rows that are traced through downstream worker-linked records.

Use `public.worker_domain_traceability_cutoffs` and `public.worker_domain_traceability_violations` for DB-side audit checks when validating that new worker-produced domain rows remain traceable.

## LCI/LCIA Release Control Plane

`lca_release_runs` is the durable release state machine; `lca_release_dataset_versions`, `lca_release_artifacts`, `lca_release_approvals`, and `lca_release_publications` are immutable/indexed release facts. The dataset index binds every generated identity to its exact source Process and requires exactly one Unit Process, LifecycleModel, and Result Process per source identity; the Unit Process mapping must point to itself. Generated LifecycleModel and Result Process documents are referenced from canonical object artifacts and never inserted into editable `lifecyclemodels` or `processes` authoring rows.

Authenticated prepare, approve, publish, readback, and unpublish commands re-check `auth.uid()` against the live `data_product_manager` platform role. The separate service-only finalize command binds four uploaded package refs to the exact prepared plan and validated release manifest, but service identity has neither direct table writes nor approval/publication function grants. Public and private read/download projections remain RPC-owned so Edge can issue signed downloads without exposing database mutation capability.

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
- `database-engine` owns durable LCI/LCIA release facts and final authorization: exact plan/artifact hashes, manager approval/publication, service-only artifact finalization, immutable pinning, and readback; it does not materialize TIDAS/ILCD bytes or place generated datasets in authoring tables
- `database-engine` owns the protected one-shot owner-draft FP/UG alias execution contract. Authenticated callers may only run the guarded preflight, three ordered live gates, one admission, and read-only status polling; a nonce-bound service executor alone can invoke the private replay-capable whole-plan and per-dimension primitives. The sealed `dataset-alias-plan.v1` request keeps time followed by length-time, one plan hash and operation ID, `target_visibility=owner_draft`, 52 distinct action rows, 59 exchange mutations, 55 immutable alias audits, and atomic admission of all 23-flow plus 27-process derivative children. Preflight and execution independently enforce actor-owned `state_code=0`, unchanged support, embedded identity, canonical exchange hashes, no public/foreign/non-draft parent, exact closure, stable row locks, table-specific allowed paths, and exact factors; indexed `json_ordered` subtrees provide candidates, while legacy `json` is never evidence. A timeout or any primary, audit, or derivative-admission mismatch rolls back every business effect, and the sealed approval permits no redispatch or replay. Production owner-draft data execution is allowed only against a freshly frozen production state with exact human approval; Preview/Dev validate the toolchain rather than replaying that production mutation. Status polling defers the full 50-target causal proof until terminal evidence is available and returns an explicit read-only conflict if the parent ledger changes while evidence is assembled.
- `database-engine` owns the guarded Step 3 public-flow identity rewrite contract. Preflight seals exact source/public/support guards, compatibility policy/evidence, ordered process templates, five-field rewrite locators, collision rows, derivative baselines, and exact pending/blocker occurrences. Initial and recovery approval artifacts are actor-wide non-reusable across request/text/identity hash domains. Each fresh preflight creates exactly one wrapper invocation and returns one memory-only rotating permit; the database persists only its generation and token hash, every successful process or finalize rotates it atomically, and exact preflight replay returns no permit. The public process/finalize RPCs require this authorization as their third argument. Scope read remains read-only status/resume evidence, while an exact read-only scope lookup resolves a lost preflight response without minting or disclosing a permit. If the wrapper loses its permit or exits after an ambiguous/domain-rejected call, continuation requires a fresh exact human-approved recovery artifact bound to observed scope state and whole-scope proof; recovery supersedes the old invocation and permit and never constitutes automatic retry. Each authenticated process call acquires the scope advisory lock, revalidates the next owner-draft process and every used mapping, reconstructs the desired JSON from live data, changes only `@refObjectId`, `@type`, `@uri`, `@version`, and `common:shortDescription`, records one unique audit, and admits one protected derivative child in the same transaction. An authenticated cancel request is actor/receipt/operation/plan/scope-proof bound and may release active fences only for an untouched `sealed` scope whose ledger, primary audits, derivative references, and mutation permits all prove zero writes; exact replay is read-only, while any post-primary scope is immutable to cancel. A terminal failed/stale derivative exposes the exact current single-target snapshot for a distinct derivative-only plan/freeze/approval; it never replays primary or auto-admits compensation. Finalize may consume only the newest exact approved-compensation request and retains active fences until all desired primaries, zero approved-source residue, unchanged source/public/support and protected occurrences, dynamic causal derivative proofs, and the completed final wrapper invocation/generation proof are current. The CLI/foundry own semantic review, canonical approval artifacts, raw in-memory permit custody, live plan/freeze/approval, and process-schema evidence; this repo never turns a historical oracle into execution authority.
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
