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
lastReviewedAt: 2026-07-30
lastReviewedCommit: 4c7e52d315d02444372d6e1978af33e4ede470c7
lastReviewedNote: "Reviewed for Issue #310 generated-workspace closure: local export and rebuild now use the Supabase CLI-native local path; remote dev remains canonical and local output requires hosted parity evidence before commit."
related:
  - ../AGENTS.md
  - ../.docpact/config.yaml
  - ../docs/agents/repo-architecture.md
  - ../docs/agents/repo-validation.md
  - README.zh-CN.md
---

# Scripts

This directory contains the command-line helpers used for remote schema export, workspace refresh, change-copying, and migration generation.

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

## Local Docpact Push Gate

The repository now includes a local pre-push docpact gate in `scripts/docpact-gate.sh`. The gate resolves the CLI through `scripts/docpact`. It is documentation-governance tooling and does not change database schema workspace behavior.
