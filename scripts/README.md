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
lastReviewedAt: 2026-08-01
lastReviewedCommit: a1be848fefc88d68c1073f98c9e3ecf866095399
lastReviewedNote: "Reviewed through Issues #353/#354 and for #333: retain immutable provenance and five-schema qualification entrypoints while documenting deterministic SECURITY DEFINER audit generation and fail-closed #352/#358 boundaries."
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

### `apply_postgrest_config.py`

Reconciles only the reviewed PostgREST fields for one exact persistent Supabase branch. `--check` is read-only; `--apply` PATCHes only drifted `db_schema`, `db_extra_search_path`, and `max_rows`, then GET-verifies the result. The target project ref must match exactly one checked-in `[remotes.*].project_id`.

```bash
SUPABASE_ACCESS_TOKEN='<injected-secret>' \
python scripts/apply_postgrest_config.py \
  --project-ref fotofiyqnuyvgtotswie \
  --check
```

Run `python -m unittest scripts/test_apply_postgrest_config.py` before changing the gate. A failed post-PATCH readback restores and verifies the prior allowlisted snapshot. Never replace the gate with unconditional `supabase config push` or print Management API response bodies.

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
- Default schema list: `public`, `api`, `private`, `util`, `archive`
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
- Includes `public`, `api`, `private`, `util`, and `archive` by default; use `--schemas` only for an intentional narrower inspection
- Rebuilds `global/` and `schemas/`
- Preserves `supabase/workspace/README.md`
- Preserves `supabase/workspace/README.zh-CN.md`
- Preserves `supabase/workspace/changes/`

Warnings:

- Manual edits inside `remote_schema.sql`, `global/`, and `schemas/` are not stable
- Refresh can overwrite uncommitted Git changes in generated workspace files
- If you want `--git-changes` to reflect only later hand edits, commit the refreshed `supabase/workspace/schemas` to Git after syncing the remote database and before editing files.
- Remote `dev` remains the canonical generated-schema target. Commit local reconstruction output only after proving applied migration parity and running targeted hosted catalog checks for the affected contract.

### `schema_boundary_phase.py`

Checks the versioned Expand/Contract boundary contract against the live catalog and committed public-object inventory. Expand requires the nine core public tables and a non-public target for every other inventoried public table; Contract enforces the final exact public allowlist.

```bash
DATABASE_URL='postgresql://...' python scripts/schema_boundary_phase.py
```

The checker is read-only and is part of `run_database_contract.py`. Issue #354 also provides `test_schema_boundary_data_api.py` for local PostgREST profile/role proof and `test_schema_boundary_rollback.py` for local operator rollback/roll-forward OID proof.

The owner-only rollback fails closed unless retained pre-deployment ACL evidence is supplied explicitly. Use exactly one of:

```bash
psql "$DATABASE_URL" -v source_service_role_maintain=false -f supabase/operator/issue_354_restore_schema_boundary.sql
psql "$DATABASE_URL" -v source_service_role_maintain=true -f supabase/operator/issue_354_restore_schema_boundary.sql
```

Choose `true` only when the retained source readback proves the explicit `service_role MAINTAIN` grant. Missing or any other value is rejected before canonical views move.

Catalog export also rejects rollback- or blank-replay-contaminated Issue #339/#354 state before writing: the 14 reviewed PostgreSQL-17 replay relations plus the five canonical/compatibility view names must have no `service_role MAINTAIN`; internal helpers and the four reviewed public helper facades must retain their browser-role boundary; and the two lifecycle bundle RPCs must remain denied to anon/authenticated. This guard is intentionally issue-scoped; it does not assert that every unrelated application relation has zero `service_role MAINTAIN`. Byte equality with the reviewed catalog artifact remains a separate gate.

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

The canonical contract also runs `public_inventory_closure.py --check` and
`security_definer_audit.py --check`. The inventory
gate joins the imported workspace #533 per-object ledger to the live catalog at
the database #337 merge base, records tables/views/materialized views/functions/
procedures, exact routine identity arguments, ACL/RLS/default privileges, and
FK/rewrite/trigger/policy/composite/body dependencies, then fails if any live
`public` object is unmapped. The deterministic output includes SCC-aware Expand
and reverse Contract order plus exact-SHA static-consumer evidence and explicit
runtime/owner residue.

```bash
python scripts/run_database_contract.py --suite canonical-local
python scripts/run_database_contract.py --suite worker-control-plane
```

For an independently named local stack, export its direct `DATABASE_URL` and
set `SUPABASE_WORKDIR` to that stack's parent directory, then use
`--skip-reset --skip-data-api`; SQL, lint, catalog, and public-inventory checks
bind to that explicit stack instead of the repo-default stack.
Run Data API proof separately against the matching explicit REST URL/keys.

### `public_inventory_closure.py`

Refresh consumer evidence from the exact commits pinned in the checked-in
consumer manifest without switching those repositories:

```bash
python scripts/public_inventory_closure.py --scan-consumers <lca-workspace-root>
python scripts/public_inventory_closure.py --verify-provenance <lca-workspace-root>
```

After a local reset, write or verify the deterministic database contract:

```bash
python scripts/public_inventory_closure.py --write
python scripts/public_inventory_closure.py --check
python -m unittest scripts/test_public_inventory_closure.py
```

`contractReady=false` is expected while the emitted residue still contains
dynamic-SQL or runtime/owner-confirmation blockers. A missing mapping, invalid
target, non-exact repository SHA, duplicate key, or live/ledger count drift is
always a hard failure; an unknown consumer is retained as an explicit blocker
and is never silently converted to `retire`.

The source block separates the reviewed workspace baseline, its exact
`database-engine` gitlink, and historical review/source/merge-base lineage from
`databaseSchemaSha`, the sole migration/catalog replay input. The prior #338
artifact hash is lineage, not the current artifact hash. `--verify-provenance`
replays those relationships
without consulting a moving remote. `--check` first verifies canonical JSON
bytes and the committed SHA-256, then performs committed-vs-generated comparison.

### `security_definer_audit.py`

Verify the frozen public SECURITY DEFINER baseline artifact, or deliberately
compare it with the exact genesis schema stack:

```bash
python scripts/security_definer_audit.py --write
python scripts/security_definer_audit.py --check
DATABASE_URL=<genesis-loopback-url> python scripts/security_definer_audit.py --check-live-baseline
python -m unittest scripts/test_security_definer_audit.py
```

`--check` is artifact-only and does not describe current catalog state;
`--check-live-baseline` is valid only for the exact genesis schema. Current
catalog state is gated by the live v2 audit. The v1 artifact retains all 241 signatures. It distinguishes 129 unresolved #333
owner/runtime signatures (90 api, 39 private), 14 #339 RLS-bound facades, and 98
inventory static closures. Per-signature fields deliberately separate observed
catalog evidence, inferred signals, required Contract proof, and confirmed
facts. Static signals never prove runtime authorization; #352 remains blocked,
#358 owns physical migration, and the gate always keeps `contractReady=false`.

### `security_definer_audit_v2.py`

Preserve the immutable v1 public baseline while generating the cross-schema
privileged-routine lineage and observed endpoint audit:

```bash
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --bootstrap-write
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --write
DATABASE_URL=<loopback-url> python scripts/security_definer_audit_v2.py --check
python scripts/security_definer_audit_v2.py --plan-transition-advance \
  --batch issue-356-worker-control-plane --database-schema-sha <exact-40-hex-commit>
python -m unittest scripts/test_security_definer_audit_v2.py
ISSUE333_DATABASE_URL=<loopback-url> \
  python -m unittest scripts/test_security_definer_audit_v2_postgrest_conformance.py
```

`--bootstrap-write` is only for the reviewed exact baseline schema. Normal
transition batches edit the lineage mapping deliberately, then use `--write`
after an exact-SHA clean reset. The audit scans `public`, `api`, `private`,
`util`, and `archive`; every SECURITY DEFINER endpoint must be claimed exactly
once as the canonical endpoint of one active lineage. Compatibility aliases
must be SECURITY INVOKER; a future privileged exception requires a reviewed
new lineage/version, never a privileged alias. Invoker wrappers remain aliases,
never replacement canonicals. The role matrix records
schema USAGE, effective EXECUTE, effective callability, and Data API exposure
separately. Data API evidence preserves the configured exposed-schema order,
separates PostgREST schema-cache eligibility, request resolution, and direct SQL
invocability, and proves that `authenticator` can `SET ROLE` to each supported
transport role. The v14.7 conformance test compares anonymous OpenAPI routes,
the default first-schema route set, and negative internal-schema profiles with
a disposable loopback-only PostgREST container. Database credentials are passed
to `psql` only through `PGPASSWORD`, never argv. Reviewed transition paths must
be canonical repository-relative Git regular files and are read without
following symlinks. The committed #356 fixture proves 315 lineage identities survive
11 Worker moves and one composite-signature move while 23 invoker aliases do
not increase the privileged endpoint count.
The transition-advance plan deterministically freezes the current v2 bytes,
builds the immutable predecessor/produced artifact receipt, settles sequence 0,
opens sequence 1, and emits the exact reviewed-code constants. A migration PR
must materialize every planned file and validate the hashes; it must not hand-edit
`completedTransitions` or advance from the mutable `security_definer_audit_v2.json`.

The #356 PR gate must additionally run the real two-stack integration harness;
there is no successful skip mode:

```bash
python scripts/run_database_contract.py --suite canonical-local \
  --security-definer-transition-workdir <clean-stack-a> \
  --security-definer-transition-workdir <clean-stack-b> \
  --security-definer-transition-migration <issue-356-migration.sql> \
  --security-definer-transition-rollback <issue-356-operator-rollback.sql> \
  --security-definer-transition-migration-sha256 <exact-sha256> \
  --security-definer-transition-rollback-sha256 <exact-sha256> \
  --security-definer-transition-base 1b94c1ce7c132e5481c4a2594d6d9a957d7dc683
```

Both workdirs must be independent loopback stacks at the exact base. The gate
performs baseline audit, migration, live transition audit, operator rollback,
baseline byte restoration, rollforward, and a second-stack byte/SHA comparison.
Missing inputs, changed SQL bytes, reset failure, or any audit drift fails closed.

### `test_worker_control_plane_upgrade.py`

Runs the populated `20260731124000` base-to-head Worker Expand proof, including
in-transaction failure injection, rollback residue checks, and idempotent retry.
It is local-only and resets the currently selected local Supabase project.

```bash
python scripts/test_worker_control_plane_upgrade.py
```

### `test_worker_control_plane_physical_upgrade.py` and rollback

Issue #356's mandatory physical-boundary qualification is selected through:

```bash
python scripts/run_database_contract.py --suite worker-control-plane
```

The suite runs both physical harnesses before its focused pgTAP/reset phase.
The upgrade harness loads one million jobs by default, injects a failed
transaction, measures lock wait and WAL, preserves OIDs/catalog/data, and
proves exact retry. The rollback harness first proves a maliciously drifted
adapter fails preflight without mutation, then proves exact baseline restore
and roll-forward. Both harnesses poll real PostgREST through baseline and
Expand phases; together they cover baseline → Expand → rollback → roll-forward.
`private` remains unexposed, `service_role` retains the compatibility surface,
and anonymous access fails closed at every phase. Database passwords are passed
through `PGPASSWORD`, never process arguments or output.

### `test_worker_control_plane_data_api.py`

After a clean local reset, sends a post-ready schema reload, polls until
PostgREST exposes both additive Worker RPCs through an explicit `public`
profile (independent of the reviewed `api`-first exposed-schema order), verifies service-role calls return
the expected empty envelope, and proves anon calls fail closed. It reads local
keys from `supabase status` and never prints them. Isolated stacks can instead
set `DATABASE_URL`, `SUPABASE_REST_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and
`SUPABASE_ANON_KEY` without changing tracked config. Legacy JWT keys are also
sent as bearer tokens; opaque `sb_publishable_` and `sb_secret_` keys are sent
only as gateway `apikey` values. Reload failures never print the database URL.

```bash
python scripts/test_worker_control_plane_data_api.py
python -m unittest scripts/test_worker_control_plane_data_api_unit.py
```

### Security ACL Expand qualification

`test_security_acl_upgrade.py` rehearses the Issue #339 migration from the
populated canonical base, injects an in-transaction failure, retries the
migration, proves row parity, restores the environment-specific ACL snapshot,
and reapplies the migration.

```bash
python scripts/test_security_acl_upgrade.py
```

`hosted_security_acl.py` is the fail-closed hosted operator gate. It combines
the database posture view, Management API `db_schema` readback, and real anon
REST negative probes. It never prints credentials. The hosted gate is read-only;
apply or reconcile hosted PostgREST configuration separately
through `scripts/apply_postgrest_config.py`, which owns the reviewed diff,
readback, and rollback contract. `supabase/operator/issue_339_supabase_admin_default_privileges.sql`
must be executed separately by an authorized `supabase_admin` owner session.

```bash
SECURITY_ACL_DATABASE_URL='postgresql://...' \
SECURITY_ACL_PROJECT_REF='<20-letter-ref>' \
SECURITY_ACL_SUPABASE_URL='https://<20-letter-ref>.supabase.co' \
SECURITY_ACL_ANON_KEY='<anon-or-publishable-key>' \
SUPABASE_ACCESS_TOKEN='<management-token>' \
python scripts/hosted_security_acl.py --evidence /new/private/evidence.json
```

The required set, in reviewed order, is `api,public,graphql_public`; `private`,
`util`, and `archive` must remain absent. Opaque `sb_publishable_` credentials
are sent only as `apikey`; legacy JWT-shaped anon keys are also sent as Bearer.
Run `python -m unittest scripts/test_hosted_security_acl.py` for the offline
argument and normalization contract.

### `test_production_equivalent_upgrade.py`

Runs the local-only global populated-upgrade qualification from the reviewed
`20260731124000` base through the exact checked-in head. It generates the
representative identity/review/notification/audit, Worker lifecycle, package,
cache, release, closure, and million-row package-evidence fixture; captures
whole-database row/primary-key/content-hash oracles (normalizing only generic
`created_at`/`updated_at`/`modified_at` reset timestamps and migration-evidence
`captured_at` timestamps); fault-injects and rehearses
every pending migration; resets and deterministically reloads the populated
base before the real CLI roll-forward, using the contract-pinned Issue #339
operator rollback to remove its database-global rehearsal role; proves the five-second lock timeout and
a compatible concurrent reader; then proves every base relation oracle is
preserved while any new physical evidence relation matches the rehearsed head,
and reconciles constraints, ACL/RLS, policies, triggers, and publication
membership against that head together with WAL, retry, and expected objects.

The runner resets only the currently selected local Supabase project. It never
connects to a linked or hosted project, never retains credentials or row
contents, redacts both `postgres://` and `postgresql://` URLs from command and
failure output, and requires evidence output outside the worktree. Evidence is
created as a new mode-0600 regular file with exclusive/no-follow semantics and
is file- and directory-fsynced before success; existing targets, including
symlinks, are refused. A qualification run requires a clean commit and at least
one million representative rows; `--allow-dirty` and smaller counts are
development-only. `--db-url` may select an isolated local stack explicitly,
but rejects every non-loopback host. The no-op migration retry must emit exactly
zero WAL bytes.

```bash
python scripts/test_production_equivalent_upgrade.py \
  --evidence-out /tmp/database-engine-upgrade-evidence.json
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
