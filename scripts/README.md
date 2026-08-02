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
lastReviewedAt: 2026-08-03
lastReviewedCommit: c5356d2b0d340f9c5c31a645479be5f3d19a52db
lastReviewedNote: "Reviewed for Issue #405: exact-head inventory refresh and comparison require explicit loopback URLs, emit no DDL, and cannot authorize Contract."
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

### `test_issue_398_result_gc_runtime.py`

Runs the destructive result-GC real-login and multi-session qualification only
against an explicit loopback database URL. It requires
`--confirm-isolated-destructive-test`, creates unique temporary login roles,
exercises renew/fail/takeover/finalize and concurrency races, and performs exact
database/role zero-residue cleanup. Use the fixed unique project ID and ports in
`docs/agents/lca-result-gc-contract.md`; never point it at a linked or persistent
database.

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

Bare `supabase test db` is not a repository gate: recursive discovery also
selects benchmark, fixture, Preview, upgrade, and transition-migration support
SQL. Inspect the exact manifest-owned selection without touching a database:

```bash
python scripts/run_database_contract.py --suite canonical-local --validate-manifest-only
python scripts/run_database_contract.py --suite canonical-local --list
```

The current derived baseline is 64 selected top-level pgTAP files plus 19
metadata-bearing exclusions. The list evidence records the exact commit,
migration head, CLI version, manifest/file-list hashes, and whether the worktree
was dirty, so local development output cannot masquerade as clean exact-commit
evidence. `lca-private-expand` additionally requires the
canonical hashed Issue #357 freeze and delegates its versioned physical-object,
exposure-surface, fingerprint, and receipt semantics to the official #357
freezer. The official `check-delivery` contract must validate both versioned
JSON Schemas, require phase authorization from the receipt, regenerate API
pre-expand and physical-cut SQL, and reject any byte drift before the focused
suite runs. The #357 freezer/generator/exposure unit modules are mandatory in
the activated gate. CI uses `--if-activated`: it skips only when
no #357 generator, contract, or migration activation path is tracked. Any partial
or ambiguous activation requires exactly one version-matched freeze/receipt,
sidecar and schema, the capture and generator scripts, plus both reviewed
migration phases, and fails before SQL if that closure is incomplete.

### `test_lca_snapshot_family_upgrade.py`

Runs the destructive, local-only Issue #376 migration qualification against an
explicit disposable loopback Supabase database:

```bash
python scripts/test_lca_snapshot_family_upgrade.py \
  --db-url "$ISSUE_376_DB_URL"
```

The checked contract fixes an ancestor database commit with an exact predecessor
migration head, the only permitted committed migration delta, a 10,000-row
network/artifact fixture, and lock/time/WAL budgets. The runner proves OID plus
full-row/primary-key/content parity, failure atomicity at the
second `ALTER TABLE`, clean upgrade, direct migration retry, private-state drift
rejection, and committed rollback/roll-forward. It refuses non-loopback URLs;
the selected database is reset destructively.

### `test_issue_390_pre_ddl_gate.py`

Runs the offline, non-mutating pre-DDL authorization gate for the LCA
result/cache/latest/factorization family:

```bash
python -m pip install --disable-pip-version-check "jsonschema==4.23.0" "pglast==8.4"
python -m unittest scripts.test_issue_390_pre_ddl_gate
```

The checked contract binds the exact `dev` base and migration head, seven target
objects plus their recursive application-object dependency closure, a digest-bound repository catalog plus hosted owner receipt, active
consumer canonical/candidate tuples, a reproducible non-authorizing runtime
receipt, advisor baseline, and owner-signoff state.
While `ddlAuthorized=false`, committed migration history remains append-only. New
target-neutral static migrations are allowed. A separately tested additive
service-only `api` facade must match one exact reviewed path/blob/classification
entry in its first commit and pass fixed-version `pglast` PostgreSQL-AST semantic
checks. A later commit cannot retroactively authorize it. Opaque or
dynamic execution, relation-moving DDL, historical authenticated-access removal,
and browser grants on `private` remain hard denied and cannot be overridden by an
allowlist. Static exclusions include, but are not limited to, top-level DML,
CTAS/SELECT, index or exclusion-index builds, validated constraints and
partitions, column type or storage rewrites, non-metadata-only column additions,
`SET NOT NULL`, custom types/access methods, trigger/rule state changes, and
migration identity/owner switches because existing triggers, views, FDWs,
operators, casts, constraints, or access methods can execute unproven code.
Moves into exposed `api`/`public` are denied; newly created exposed views must be
security-invoker, and exposed routines may neither be security-definer nor
reference internal objects. Reviewed facade signatures use explicitly qualified
`pg_catalog` types so migration-session type shadowing cannot change identities.
Created `api.lca_*` and `api.cmd_lca_*` facades
remain protected from later replacement, privilege, and security-mode changes.
HEAD, index, and worktree each use their own contract revision. The gate
never treats one zero-match query as burn-in. The canonical manifest-contract
module imports this test case, so existing CI runs it without a second workflow.

The Issue #323 Review-progress migration does not weaken those generic hard
denies. Its exact first-appearance path and Git blob may enter the dedicated
`issue_323_review_progress_semantic_gate.py` qualification only through the
`review-progress-least-privilege-reviewed` contract classification. That
qualifier binds the normalized AST and exact statement sequence; proves the
non-login/non-inheriting executor attributes, two-relation read-only ACL, five
helper EXECUTE grants, trusted search path, RPC signature and browser ACL; and
requires the temporary `postgres` ownership-transfer membership to be revoked
by its grantor. Any source, AST, role, ACL, owner, search-path, relation, or
procedural-body drift fails closed while the original hard-deny signals remain
unchanged for every other migration.

The companion `issue_323_review_notification_semantic_gate.py` qualifier binds
the exact notification-event identity migration without suppressing its five
generic hard-deny signals. It permits only one legacy-only partial unique index
replacement and one byte/AST-pinned replacement of the pre-existing validation
notification command. The verifier freezes the predecessor security envelope,
signature, authorization/error contract, exact public relation/helper set,
legacy conflict predicate, owner/ACL non-change, and command-audit write; any
additional statement, relation, privilege, dynamic execution, or event-key
predicate drift fails closed.

### `issue_390_physical_qualification.py`

Defines the non-authorizing physical-move qualification harness before any
Issue #390 relation-moving DDL exists:

```bash
python scripts/issue_390_physical_qualification.py --check
python scripts/issue_390_physical_qualification.py --print-run-plan
python -m unittest scripts.test_issue_390_physical_qualification
```

The default suite keeps the live database case skipped. Exercise the complete
SQL query and receipt validator against an exact predecessor database with:

```bash
ISSUE_390_BASELINE_DB_URL='<explicit-loopback-url>' \
  python -m unittest \
  scripts.test_issue_390_physical_qualification.Issue390PhysicalQualificationLiveIntegrationTest
```

The exact v1 plan is bound to `database-engine/dev@a29f26a9` and migration head
`20260803090000`. It contains no migration, rollback, or populated-fixture
binding and keeps `ddlAuthorized`, `relationMovingDdlAllowed`,
`historicalAuthenticatedSelectRemovalAllowed`, and destructive qualification
execution false. `--qualify` therefore fails closed.

On an explicit loopback database, `--capture-baseline --db-url ... --output
...` may emit a read-only repeatable-read receipt only as `postgres` or
`supabase_admin`, after proving superuser, `BYPASSRLS`, or ownership-based full
row visibility across the four exact ordinary relations and three exact
functions. Loopback and read-only prove neither database disposability nor
independent-instance isolation. The requested client endpoint must be
loopback; a containerized PostgreSQL server can truthfully report its bridge
interface from `inet_server_addr()`. The database receipt independently binds the
database name/OID, server address/port, cluster system identifier, and exact
applied migration set; the Git-derived repository plan is recorded separately.
The receipt freezes OID, owner, ACL/column ACL,
RLS/policy, inbound/outbound FK semantics, indexes, triggers, publications,
row count, sorted canonical SHA-256 PK/full-row digests, routine
properties/definition hashes, recursive
`pg_depend`, view/`pg_rewrite`, composite/rowtype, dynamic-SQL, and regclass
candidates. Capture fails above 100,000 rows per target or after the 120-second
statement budget; a successor needs a separately reviewed bounded/streaming
design for larger tables. It stores no row payload and makes no authorization
claim.

The printed future run plan reserves independent fresh and populated upgrades
whose database receipts must have distinct cluster system identifiers,
failure atomicity, lock timeout, WAL/time budgets, retry, rollback, and
roll-forward receipts and requires distinct `--fresh-db-url` /
`--populated-db-url` loopback identities plus an external `--receipt-dir`.
Activating those steps requires a reviewed successor
contract with exact candidate blobs and independently completed pre-DDL gates;
v1 cannot be edited into an execution authorization.

### `issue_390_external_git_tree.py`

Builds the non-authorizing Issue #397 consumer ledger directly from eight exact
external Git commits. It validates each canonical origin, walks every regular
blob with Git object commands (including the active Next database snapshot),
records only blob/line hashes and semantic classifications, and fails closed on
unsupported entries, unresolved active-runtime tokens, or dynamic selectors.
The Next Edge mirror receipt and exact source tree are checked independently;
staleness and content parity are separate blockers. Recognized direct-token
occurrences are not an exhaustive consumer count.

```bash
python scripts/issue_390_external_git_tree.py --check
python scripts/issue_390_external_git_tree.py --verify-external /absolute/path/to/lca-workspace
python -m unittest scripts.test_issue_390_external_git_tree
```

`--scan-external` rewrites the canonical JSON artifact and SHA sidecar and is
only for a reviewed evidence refresh. These commands do not authorize DDL or
contact Supabase Hosted projects.

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
python scripts/run_database_contract.py --suite lca-private-expand --if-activated
python scripts/run_database_contract.py --suite worker-control-plane
```

For an independently named local stack, export its direct `DATABASE_URL` and
set `SUPABASE_WORKDIR` to that stack's parent directory, then use
`--skip-reset --skip-data-api`; SQL, lint, catalog, and public-inventory checks
bind to that explicit stack instead of the repo-default stack.
Run Data API proof separately against the matching explicit REST URL/keys.

### `public_inventory_closure.py`

`public_inventory_closure.py --check` dispatches to the current Issue #405
offline exact-head verifier. The immutable Issue #338 generator and provenance
helpers remain available for genesis lineage and exact-SHA consumer evidence:

```bash
python scripts/public_inventory_closure.py --scan-consumers <lca-workspace-root>
python scripts/public_inventory_closure.py --verify-provenance <lca-workspace-root>
```

Verify current committed bytes without a database, or explicitly compare a
loopback exact-head catalog:

```bash
python scripts/public_inventory_closure.py --check
python scripts/public_inventory_exact_head.py --check
python scripts/public_inventory_exact_head.py --check-live --db-url postgresql://...
python scripts/public_inventory_exact_head.py --compare-catalogs \
  --db-url postgresql://... --other-db-url postgresql://...
python -m unittest scripts.test_public_inventory_exact_head scripts.test_public_inventory_closure
```

Only an explicit disposable loopback database may regenerate current files:

```bash
python scripts/public_inventory_exact_head.py --refresh --db-url postgresql://...
```

The v2 artifact binds exact source `c5356d2`, migration head
`20260803090000`, 397 live identities, partition `9+37+117+230+4`, and the full
388-residue Contract DROP identity checklist. The checklist is non-executable
inventory: every entry remains `blocked`, no migration is produced, and
`contractReady=false` is invariant. Missing, unknown, duplicate, count, schema,
hash, counterpart, or live-ledger drift is a hard failure.

The prior #338 artifact and hash remain immutable in
`public_object_inventory.genesis.*`; security lineage continues to reference
that genesis instead of treating the v2 refresh as a historical rewrite.

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

The qualification receipt itself must be a reviewed regular file in the clean
source HEAD. To avoid the impossible Git hash self-reference that would result
from embedding that same HEAD in the receipt, `source.commitSha` names the
reviewed ancestor commit containing the exact migration and rollback bytes. The
runner replays those immutable Git blobs and proves the base → source → receipt
HEAD ancestry chain; current fixture and receipt bytes remain independently
bound by exact SHA-256.

The #356 PR gate must additionally run the real two-stack integration harness;
there is no successful skip mode:

```bash
python scripts/run_database_contract.py --suite canonical-local \
  --security-definer-transition-workdir <clean-stack-a> \
  --security-definer-transition-workdir <clean-stack-b> \
  --security-definer-transition-source-workdir <clean-source-worktree> \
  --security-definer-transition-migration <issue-356-migration.sql> \
  --security-definer-transition-rollback <issue-356-operator-rollback.sql> \
  --security-definer-transition-qualification-receipt \
    supabase/tests/contracts/security_definer_transition_qualification_receipt.issue-356.json \
  --security-definer-transition-qualification-receipt-sha256 <exact-sha256> \
  --security-definer-transition-migration-sha256 <exact-sha256> \
  --security-definer-transition-rollback-sha256 <exact-sha256> \
  --security-definer-transition-base 597072ca34a62cdc93df9bf0896a9d361901852c
```

Both workdirs must be independent loopback stacks at the exact base. The gate
performs baseline audit, migration, live transition audit, operator rollback,
baseline byte restoration, rollforward, and a second-stack byte/SHA comparison.
The baseline lineage and v2 audit are immutable sequence-0 artifacts; the gate
never substitutes the mutable current lineage after a transition is settled.
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

`test_security_acl_global_function_defaults_upgrade.py` starts on PostgreSQL
17 before the follow-up migration, proves the built-in global `PUBLIC EXECUTE`
on a real newly-created function, then qualifies the global revoke across the
entire database, including a scratch non-application schema. It also covers
explicit global rows with grant option, owner execution, effective
built-in/global/per-schema catalog evaluation, current-object ACL and
application row-count parity, failure atomicity, retry, and exact restore of
both the global and five additive per-schema layers. A custom per-schema grant
option proves layering and grantability survive restore. A custom table-default
role also proves the snapshot dynamically removes every non-owner grantee while
retaining the `postgres` owner. A million-row fixture is not applicable because this migration
does not scan or rewrite application relations; cardinality cannot change its
lock, WAL, or execution behavior. Set `SUPABASE_WORKDIR` to the root of an
independent disposable project when the shared local stack is not clean.

```bash
python scripts/test_security_acl_upgrade.py
python scripts/test_security_acl_global_function_defaults_upgrade.py
```

`hosted_security_acl.py` is the fail-closed hosted operator gate. It combines
the database posture view, Management API `db_schema` readback, and real anon
REST negative probes. It never prints credentials. The hosted gate is read-only;
apply or reconcile hosted PostgREST configuration separately
through `scripts/apply_postgrest_config.py`, which owns the reviewed diff,
readback, and rollback contract. The posture computes built-in, global, and
per-schema effective defaults. The repo-owned global revoke affects every
future function created by `postgres` in the database, while the gate evaluates
the five application schemas as its deployment target. Platform-owned
`supabase_admin` residue remains a fail-closed #352 blocker and is never claimed
as repaired by this migration.

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

### Identity/collaboration Expand qualification

`test_identity_collaboration_data_api.py` proves the versioned Issue #355 DTOs,
notification RPC-only browser contract, authenticated/service/anonymous role
split, PostgREST schema-cache reload, and negative internal-schema profiles.
`test_identity_collaboration_concurrency.py` runs 800 checksum parity calls over
eight sessions. The static test binds the exact 16-object inventory batch,
consumer SHAs, versioned DTO names, transaction/timeouts, and Contract gates.
`test_identity_collaboration_policy_variants.py` is disposable-local only. It
rehearses the exact blank/repository and persistent Dev/Production users-policy
fingerprints, proves Expand preserves either predecessor, bounds every
security-invoker projection by its source grants, exercises self, same-team,
cross-tenant, public-team, owner, global-admin, review-admin, and review-member
RLS visibility through both the source table and projection, and rejects an unknown third
variant before mutation. The live legacy variant is compatibility evidence,
not the approved security target; retirement remains tracked in Next #753 and
database-engine #358.

```bash
python -m unittest scripts/test_identity_collaboration_expand_static.py
python scripts/test_identity_collaboration_policy_variants.py
python scripts/test_identity_collaboration_data_api.py
python scripts/test_identity_collaboration_concurrency.py
```

### Issue #390/#395 result API facade runtime

`test_issue_390_result_api_facade_runtime.py` is a loopback-only qualification
for the eight service-only `api` routines. It freezes the positive DTO values,
exact anonymous/authenticated/private-profile denials, one eight-request HTTP
admission race, one eight-backend PostgreSQL race, same-binding replay, and
post-cleanup zero residue. The Issue #395 extension also runs eight concurrent
reconciliations of one cancelled Worker job, proves every call contributes
exactly one hit while preserving all identities, then admits a retry that
atomically clears the old result/error binding. `canonical-local` runs it
automatically whenever the Issue #390 facade pgTAP contract is selected.

```bash
python scripts/test_issue_390_result_api_facade_runtime.py
```

The canonical runner resolves one explicit loopback stack once, verifies its
database identity against `SUPABASE_WORKDIR`, then overwrites and passes the
same `DATABASE_URL`, REST credentials, and workdir to every SQL, Data API,
catalog, schema-phase, inventory, lint, and SECURITY DEFINER gate. Offline
two-stack negatives live in `test_database_contract_targeting.py`. The runner
does not execute destructive identity DDL by default. On a disposable local
stack, opt in with `--run-destructive-identity-qualification`. That mandatory
gate runs the dual exact-hash preservation/retry, unknown atomic-rejection, and
actor RLS matrix harness before the rollback/roll-forward/lock-failure harness;
it resets the selected stack to exact Issue #355 head `20260801061000` for that
rehearsal and restores the current repository head afterwards. Qualification
and restoration failures both terminate the canonical runner. `--skip-data-api` skips only
HTTP probes and does not select or redirect the destructive target. The
runner-level control-flow contract is
`test_database_contract_identity_qualification.py`.

The operator rollback is
`supabase/operator/issue_355_restore_identity_collaboration_expand.sql`. It is
repeatable and removes only the additive API/private Expand objects; the audited
public physical routines and their OIDs remain untouched. The rollback harness
also proves reviewed-predecessor routine and extra-overload rejection, arbitrary
custom ACL/owner convergence, exact migration-head plus complete target
fingerprints before deletion, tamper/partial-state rejection, complete target
absence after two rollbacks, and exact roll-forward.

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
