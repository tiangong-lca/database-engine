---
title: Scripts
docType: guide
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when the task touches schema-workspace helper scripts
  - when you need the supported command surface for workspace refresh or migration generation
whenToUpdate:
  - when script entrypoints, supported source shapes, or workspace refresh behavior changes
checkPaths:
  - scripts/README.md
  - scripts/**
  - supabase/workspace/**
  - docs/agents/repo-architecture.md
  - .githooks/pre-push
  - scripts/docpact
  - scripts/docpact-gate.sh
  - scripts/install-git-hooks.sh
lastReviewedAt: 2026-08-26
lastReviewedCommit: 5d8e7dd
lastReviewedNote: "Reviewed for the Portal projection manifest checker and named release/sparse benchmark profiles; schema-workspace helper behavior is unchanged."
related:
  - ../AGENTS.md
  - ../.docpact/config.yaml
  - ../docs/agents/repo-architecture.md
  - ../docs/agents/repo-validation.md
  - README.zh-CN.md
---

# Scripts

This directory contains the command-line helpers used for remote schema export, workspace refresh, change-copying, migration generation, and controlled data migrations.

## Layout

Durable helper entry points remain at the top level of this directory.
One-off or dated data remediation runners live under:

- `scripts/data_migrations/<topic>_<yyyymm>/`

Those runners should keep their own `README.md` with dry-run, apply, and validation examples.
Local migration outputs and audit JSONL files should be written under `_artifacts/`, which is intentionally ignored by Git.

## Script List

### `test_full_schema_cutover_upgrade.sh`

Rebuilds the local database to the migration immediately before the full
Schema cutover, adds a representative business row, snapshots relation and
routine identities plus trigger, RLS-policy, constraint, and exact row-count
state, reproduces the production ledger where the later reuse-binding hotfix
was already applied, runs the fail-closed bridge, applies the cutover and
contract-closure migrations, and verifies
complete preservation, capability-manifest installation, idempotency-index
creation, Data Product consumer-facade installation, and removal of PostgreSQL
`PUBLIC` execution from the API surface. It also applies the Issue #422 runtime
ACL repair and verifies the exact authenticated RLS-helper and Edge release
façade grants after a populated upgrade, then proves the bridge is a strict
no-op on an already-cut-over database.

Usage:

```bash
scripts/test_full_schema_cutover_upgrade.sh
```

This script is local-only and resets the local Supabase database.

### `test_search_text_array_upgrade.sh`

Rebuilds the local database to `20260810200000`, exercises the exact
production `search_text` migration, and proves that all seven table OIDs and
heap relfilenodes remain stable while an unrelated business row is preserved.
It then rebuilds the pre-migration state with one populated scalar and proves
the migration fails before changing any column or discarding the value. The
script finishes with a clean reset to the checked-out migration head.

```bash
scripts/test_search_text_array_upgrade.sh
```

### `test_portal_projection_upgrade_recovery.sh`

Exercises the Issue 531 Portal projection rollout against an explicitly
attested, isolated local Supabase project. It uses live concurrent connections
to prove valid-update, delete, state-invalidation, key-change, and
embedding-only races; forces reconcile lock-timeout and cutover-guard failures;
and verifies same-history retry, controlled same-name concurrent-index cleanup,
and no-op repeat without index rebuild. See
`docs/agents/portal-projection-migration-recovery.md` for the required
environment and recovery boundaries. Formal evidence additionally requires
clean HEAD, Supabase CLI `2.109.1`, and byte equality plus aggregate SHA-256 for
the complete 257-file migration tree.

### `run_portal_projection_benchmark.sh`

Runs the Issue 531 representative Process/Flow Search, Hybrid, Facets, writer,
fence, plan, and ANN-recall benchmark only against an explicitly attested
Issue-531 local Supabase project. The runner byte-compares every Issue 531
migration with the repository, writes into a new operator-selected private
directory, and resets the isolated database before and after the run so rolled
back HNSW pages cannot accumulate. Its environment contract mirrors the
recovery runner and additionally requires
`PORTAL_PROJECTION_BENCHMARK_OUTPUT_DIR`.

`PORTAL_PROJECTION_BENCHMARK_PROFILE` selects a fail-closed named profile:

- `release` uses representative rows/vectors plus the 21,000-old-Flow pressure,
  records the natural raw-ANN branch, directly gates both full-cardinality
  exact helpers, and captures the production 5,000-to-200 ANN phase;
- `sparse-zero` uses representative rows with zero embeddings;
- `sparse-199` uses representative rows with 199 embeddings per dataset;
- `diagnostic` permits explicitly supplied smaller counts and is not release
  evidence; `auto` recognizes an exact named profile from its counts.

All named gates require a clean exact HEAD. They cover the complete public
request shapes, retain Search/Facets p95 <= 2 seconds and Hybrid p95 <= 6
seconds with every Hybrid call below 8 seconds. Formal semantic plans must
include parseable shared-buffer evidence, remain below 750,000 total and
250,000 read blocks, finish exact execution within 5 seconds and formal
ANN-plus-exact phases within 6 seconds, and show no temp/disk spill. Use a new
mode-0700 output directory for every run.

### `check_portal_projection_manifest.py`

Checks that the committed Portal projection-v1 digest and exact eleven-function
closure remain present, that no later migration creates, replaces, drops, or
alters a v1 closure/control function, and that reconcile/Search-Hybrid/Facets
retain their required runtime guards.

```bash
python3 scripts/check_portal_projection_manifest.py
```

### `resolve_migration_head.py`

Prints the latest valid migration version from the checked-out
`supabase/migrations` directory. It rejects an empty directory, malformed SQL
migration names, and duplicate 14-digit versions so hosted verification cannot
silently select an ambiguous head.

```bash
python scripts/resolve_migration_head.py
```

Run its regression suite with:

```bash
python scripts/test_resolve_migration_head.py
```

### `test_supabase_dev_workflow_contract.py`

Fails closed unless `.github/workflows/supabase-dev.yml` keeps two isolated
hosted paths. The push-only persistent-Dev job must link the configured Dev
project, run exactly one `supabase db push --include-all`, derive its migration
head, and apply exactly one three-field PostgREST PATCH. The pull-request-only
Preview job must skip forks but fail a same-repository PR when its access token,
main-parent ref, or persistent-Dev ref is absent. It binds one successful check
from the exact official Supabase App/head to a unique non-default,
non-persistent `branches list` row for the same Git branch, PR number, and
parent; the check ref and BranchResponse ref must match and differ from both
main and Dev before the Preview's one identical PATCH/readback.

The contract also requires one separate no-reveal Management API key read using
the raw `disabled` state and exact public-key shape. Only a masked enabled
publishable or legacy anon key crosses into the next step; raw JSON and PAT are
cleared first. The anonymous Hybrid step itself may contain no PAT,
`Authorization`, `Cookie`, or service credential. Across the workflow, the only
Management API mutations are the one persistent-Dev and one Preview PostgREST
PATCH; Functions commands, broad `config push`, pinned migration heads, and any
other mutation remain rejected.

```bash
python scripts/test_supabase_dev_workflow_contract.py
```

### `data_migrations/tidas_schema_202606/runner.py`

Plans, applies, and validates the TIDAS schema 2026-06 JSON data remediation for remote database rows.

Usage:

```bash
python scripts/data_migrations/tidas_schema_202606/runner.py plan --environment dev --run-id tidas-schema-202606-dev --out _artifacts/tidas-schema-202606/dev-plan.jsonl --dry-run
```

See `scripts/data_migrations/tidas_schema_202606/README.md` for the complete command surface and safety notes.

### `data_migrations/root_reference_review_v2_local.sh`

Creates the local encrypted backup and performs the operator-controlled legacy
review migration for the Root/Reference Review v2 cutover.

The backup includes a full custom-format dump, a review-table dump, and only
affected rows from the seven business tables. It must be stored outside the Git
worktree. `apply` remains locked until the operator has completed an independent
restore check, verified a second local copy, and generated the dry-run manifest.

Usage:

```bash
DATABASE_URL='postgresql://...' \
REVIEW_BACKUP_PASSWORD_FILE='<absolute-path-to-local-password-file>' \
scripts/data_migrations/root_reference_review_v2_local.sh backup \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh dry-run \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh verify \
  '/absolute/path/review-v2-backup'

DATABASE_URL='postgresql://...' \
scripts/data_migrations/root_reference_review_v2_local.sh apply \
  '/absolute/path/review-v2-backup'
```

The operator must create `RESTORE_VERIFIED` and
`SECOND_LOCAL_COPY_VERIFIED` only after those checks actually pass. Never
commit the password file, encrypted backup, markers, or migration manifest.

### `export_remote_schema.py`

Exports the target remote database schema to:

- `supabase/workspace/remote_schema.sql`

Usage:

```bash
python scripts/export_remote_schema.py --environment dev
```

Notes:

- Default environment: `dev`
- Default schema list: `public`
- You can override the destination with `--schema-file`
- `--environment local` without `--db-url` uses Supabase CLI `db dump --local`; an explicit `--db-url` still uses that URL

### `build_schema_workspace.py`

Refreshes the human-readable schema workspace under:

- `supabase/workspace/remote_schema.sql`
- `supabase/workspace/global/`
- `supabase/workspace/schemas/`

Usage:

```bash
python scripts/build_schema_workspace.py --environment dev
```

For an exact local migration-state reconstruction:

```bash
python scripts/build_schema_workspace.py --environment local
```

Behavior:

- Exports the latest remote schema first
- Canonicalizes trailing whitespace in the generated SQL view for deterministic review diffs
- Rebuilds `global/` and `schemas/`
- Preserves `supabase/workspace/README.md`
- Preserves `supabase/workspace/README.zh-CN.md`
- Preserves `supabase/workspace/changes/`

Warnings:

- Manual edits inside `remote_schema.sql`, `global/`, and `schemas/` are not stable
- Refresh can overwrite uncommitted Git changes in generated workspace files
- If you want `--git-changes` to reflect only later hand edits, commit the refreshed `supabase/workspace/schemas` to Git after syncing the remote database and before editing files.
- Remote `dev` remains the canonical generated-schema target. A schema-changing PR may commit an exact-local review snapshot after a blank migration rebuild, targeted contract tests, and a second deterministic regeneration show no drift. After merge, the database-only Dev deployment must reach the exact head, hosted catalog checks must pass, and a remote-Dev refresh must be compared with the review snapshot; commit any resulting drift as a follow-up.

### `build_database_types.py`

Generates the checked-in TypeScript contract for the two schemas exposed through the Data API: `public` and `api`.

```bash
python scripts/build_database_types.py --environment local
```

Use `--environment linked` only when the linked Supabase target is intentionally the source. CI regenerates the local contract and fails when `supabase/workspace/database.types.ts` drifts.

### `check_generated_workspace_legacy_tables.py`

Checks that generated schema workspace output no longer advertises retired public legacy job tables:

- `public.lca_jobs`
- `public.lca_package_jobs`
- `public.dataset_review_submit_jobs`

Usage:

```bash
python scripts/check_generated_workspace_legacy_tables.py
```

Use this after refreshing `supabase/workspace/**` from a remote branch where the `worker_jobs` cutover and legacy table retirement migrations have applied.

### `copy_workspace_file_to_changes.py`

Copies files from generated workspace content into the stable manual-edit area:

- from `supabase/workspace/schemas/...`
- to `supabase/workspace/changes/...`

Usage:

```bash
python scripts/copy_workspace_file_to_changes.py --source-path "supabase/workspace/schemas/public/tables/comments/table.sql"
```

```bash
python scripts/copy_workspace_file_to_changes.py --source-path "supabase/workspace/schemas/public/tables/comments"
```

```bash
python scripts/copy_workspace_file_to_changes.py --git-changes
```

Behavior:

- Preserves relative paths
- Supports a single file or a directory
- `--git-changes` copies every uncommitted file currently detected under `supabase/workspace/schemas`
- Recommended workflow: refresh the workspace, commit the generated `supabase/workspace/schemas` state to Git, then edit files and use `--git-changes`

### `new_migration.py`

Generates a migration SQL file from a supported schema object file under:

- `supabase/model/schemas/...`
- `supabase/workspace/changes/...`

Usage:

```bash
python scripts/new_migration.py --name "update policy roles update" --source-path "supabase/workspace/changes/public/functions/policy_roles_update/definition.sql"
```

Output:

- `supabase/migrations/<timestamp>_<slug>.sql`

Currently supported source path shapes:

- `functions/<name>/definition.sql`
- `views/<name>/definition.sql`
- `materialized_views/<name>/definition.sql`
- `tables/<table>/policies/<name>.sql`
- `tables/<table>/triggers/<name>.sql`

Not currently supported:

- `table.sql`
- indexes
- sequences
- schema-level SQL
- other generated workspace files

### `_db_workflow.py`

Internal shared module used by the scripts above.

It is not intended to be the primary entry point for routine command-line use.

### `test_scope_closure_staged_write_set_v2_fixture.sh`

Validates the byte-shared Worker/database staged write-set v2 fixture, including
canonical JSON, descriptor-set SHA-256, bounded batch limits, status-field
opacity, state transitions, and the retained one-shot compatibility window.

Usage:

```bash
scripts/test_scope_closure_staged_write_set_v2_fixture.sh
```

This is a read-only local contract check. It does not refresh generated schema
workspace files or connect to a remote database.

### Scope-closure provider qualification adapters

`run_scope_closure_database_qualification.sh` and
`run_scope_closure_storage_qualification.sh` emit the exact
`lcia.scope-closure-provider-owned-result.v1` records consumed by the Worker
provider aggregator. Both adapters require a Worker-supplied `--run-id`, bind
`componentSha` to the checked-out database-engine commit, accept either
loopback targets or positively allowlisted non-production target fingerprints,
reject production or ambiguous targets, and emit `productionMutation=false`.

The database adapter runs the #308/#316 pgTAP contracts against the explicit
`QUALIFICATION_DATABASE_URL`. The storage adapter uses an explicit
S3-compatible endpoint, bounded generated files, live and expired signed
HEAD/range requests, multipart boundaries, retries, and exact-prefix garbage
collection.
Neither adapter writes credentials, object locators, signed URLs, or payload
contents to its result.

Under exact Worker commit `e5a7f769`, loopback execution is protocol,
fault-injection, and adapter evidence only. It is not the final
provider-specific non-production qualification. Run that later with the same
owner adapters after Worker #188 supplies verified non-production target
classification; ambiguous or production targets must still fail closed.

Usage:

```bash
scripts/run_scope_closure_database_qualification.sh \
  --output <new-result-path> \
  --run-id <worker-supplied-uuid>

scripts/run_scope_closure_storage_qualification.sh \
  --output <new-result-path> \
  --run-id <same-worker-supplied-uuid>
```

Required environment variables are intentionally qualification-scoped:

- both adapters: `QUALIFICATION_NON_PRODUCTION_CONFIRMATION`
- database: `QUALIFICATION_DATABASE_URL`, `QUALIFICATION_SUPABASE_URL`,
  `QUALIFICATION_SUPABASE_SERVICE_ROLE_KEY`
- storage: `QUALIFICATION_DATABASE_URL`, `QUALIFICATION_S3_ENDPOINT`,
  `QUALIFICATION_S3_ACCESS_KEY_ID`,
  `QUALIFICATION_S3_SECRET_ACCESS_KEY`, `QUALIFICATION_S3_BUCKET`, and optional
  `QUALIFICATION_S3_REGION`
- non-loopback targets: `QUALIFICATION_VERIFIED_NON_PRODUCTION_FINGERPRINTS`,
  containing the exact comma-separated SHA-256 target identities approved by
  the qualification coordinator; remote database and provider endpoints must
  use TLS

Feed both records to the exact Worker compatibility verifier without adapting
their schema:

```bash
scripts/verify_scope_closure_worker_aggregator.py \
  --worker-repo <worker-checkout-containing-e5a7f769> \
  <database-result> <storage-result>
```

Run the offline control-flow and safety regressions with:

```bash
python3 -m unittest scripts/test_scope_closure_provider_qualification.py
```

## Local Docpact Push Gate

The repository now includes a local pre-push docpact gate in `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`. It is documentation-governance tooling and does not change database schema workspace behavior.
