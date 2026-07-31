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
lastReviewedAt: 2026-07-31
lastReviewedCommit: be5b5db38fd34649524c1b18b2e582ad84b4f6bc
lastReviewedNote: "Reviewed through Issues #323 and #329: document the local Root/Reference Review backup/cutover runner and exact Worker-compatible isolated database/storage qualification adapters without changing schema-workspace behavior."
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
- Rebuilds `global/` and `schemas/`
- Preserves `supabase/workspace/README.md`
- Preserves `supabase/workspace/README.zh-CN.md`
- Preserves `supabase/workspace/changes/`

Warnings:

- Manual edits inside `remote_schema.sql`, `global/`, and `schemas/` are not stable
- Refresh can overwrite uncommitted Git changes in generated workspace files
- If you want `--git-changes` to reflect only later hand edits, commit the refreshed `supabase/workspace/schemas` to Git after syncing the remote database and before editing files.
- Remote `dev` remains the canonical generated-schema target. Commit local reconstruction output only after proving applied migration parity and running targeted hosted catalog checks for the affected contract.

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

### `run_database_contract.py`

Runs the checked-in fail-closed local database manifest. It rejects unclassified
test assets, keeps Preview/upgrade/fixture/benchmark assets out of the canonical
pgTAP suite, checks the exact reviewed lint-error baseline, and verifies the
stable catalog hash and generated-workspace cleanliness.

```bash
python scripts/run_database_contract.py --suite canonical-local
python scripts/run_database_contract.py --suite worker-control-plane
```

### `test_worker_control_plane_upgrade.py`

Runs the populated `20260731124000` base-to-head Worker Expand proof, including
in-transaction failure injection, rollback residue checks, and idempotent retry.
It is local-only and resets the currently selected local Supabase project.

```bash
python scripts/test_worker_control_plane_upgrade.py
```

### `test_worker_control_plane_data_api.py`

After a clean local reset, sends a post-ready schema reload, polls until
PostgREST exposes both additive Worker RPCs, verifies service-role calls return
the expected empty envelope, and proves anon calls fail closed. It reads local
keys from `supabase status` and never prints them. Isolated stacks can instead
set `DATABASE_URL`, `SUPABASE_REST_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and
`SUPABASE_ANON_KEY` without changing tracked config.

```bash
python scripts/test_worker_control_plane_data_api.py
```

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
