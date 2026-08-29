---
lastReviewedAt: 2026-08-29
lastReviewedCommit: 2425798
lastReviewedNote: "Reviewed for Issues #551/#552: the 290-file tree retains exact-version recovery, forced-RLS CAS indexability, and the helper-only one-code-point Search repair without changing writer recovery ownership."
title: Portal Projection Migration Recovery
docType: runbook
scope: repo
status: active
authoritative: false
owner: database-engine
language: en
whenToUse:
  - when an Issue 531 Portal projection migration stops before cutover completes
  - when a concurrent Portal projection index is INVALID or exists without migration history
  - when validating retry safety for the Portal projection rollout
  - when an Issue 533 Portal catalog-summary eligibility index stops before its guard or façade migration completes
  - when an Issue 539 Portal sitemap exact-version child rollout stops before its public façade migration completes
whenToUpdate:
  - when the Portal projection migration sequence or recovery test changes
checkPaths:
  - docs/agents/portal-projection-migration-recovery.md
  - scripts/test_portal_projection_upgrade_recovery.sh
  - scripts/test_portal_facet_projection_populated_upgrade.sh
  - scripts/check_portal_projection_manifest.py
  - supabase/migrations/20260826060422_portal_candidate_first_search.sql
  - supabase/migrations/20260826080257_portal_projection_backfill_0.sql
  - supabase/migrations/20260826080342_portal_projection_backfill_f.sql
  - supabase/migrations/20260826080345_portal_projection_reconcile.sql
  - supabase/migrations/20260826080348_portal_projection_process_pgroonga.sql
  - supabase/migrations/20260826080351_portal_projection_flow_pgroonga.sql
  - supabase/migrations/20260826080400_portal_projection_candidate_cutover.sql
  - supabase/migrations/20260826080403_portal_projection_facets.sql
  - supabase/migrations/20260827010000_portal_flow_embedding_eligibility_index.sql
  - supabase/migrations/20260827010003_portal_flow_embedding_eligibility_guard.sql
  - supabase/migrations/20260827020000_portal_facet_projection_expand.sql
  - supabase/migrations/20260827020001_portal_facet_projection_backfill_00_3f.sql
  - supabase/migrations/20260827020002_portal_facet_projection_backfill_40_7f.sql
  - supabase/migrations/20260827020003_portal_facet_projection_backfill_80_bf.sql
  - supabase/migrations/20260827020004_portal_facet_projection_backfill_c0_ff.sql
  - supabase/migrations/20260827020005_portal_facet_projection_reconcile.sql
  - supabase/migrations/20260827020006_portal_facet_projection_cutover.sql
  - supabase/migrations/20260827021441_portal_card_context_decorator.sql
  - supabase/migrations/20260827134100_optimize_portal_flow_geography_search.sql
  - supabase/migrations/20260827134101_portal_sitemap_latest_projection.sql
  - supabase/migrations/20260827134102_portal_sitemap_shard_contract.sql
  - supabase/migrations/20260827134103_portal_sitemap_concurrency_repair.sql
  - supabase/migrations/20260827050000_portal_catalog_summary_eligibility_index.sql
  - supabase/migrations/20260827050001_portal_catalog_summary_eligibility_guard.sql
  - supabase/migrations/20260827050002_portal_catalog_summary_v1.sql
  - supabase/tests/benchmarks/20260827_portal_catalog_summary_cardinality.sql
  - supabase/tests/20260827_portal_sitemap_shards_v1.sql
  - supabase/tests/upgrade/20260827_portal_sitemap_preview_winner_fixture.sql
  - supabase/tests/benchmarks/20260827_portal_sitemap_shards_cardinality.sql
related:
  - ../../AGENTS.md
  - repo-validation.md
  - supabase-branching.md
  - ../../scripts/README.md
---

# Portal Projection Migration Recovery

This runbook covers only the additive Issue 531 Portal projection rollout, the
narrow Issue 533 catalog-summary eligibility index, and the Issue 539 sitemap
exact-version child layered on top of the same synchronized facet projection.
It does not authorize production mutation, migration-history edits, or
deletion of an applied migration. Use the repository's normal tracked-delivery
and Supabase branch controls before any hosted action.

## Safe rollout states

The rollout has nine observable boundaries:

1. `20260826060422` installs the immutable v1 derivation-contract registry,
   private projection, dormant projection Hybrid kernel, and source-table sync
   triggers in one explicit transaction. Every row binds contract version 1.
2. `20260826080257` through `20260826080342` backfill bounded UUID ranges. Old
   Search, Hybrid, and Facets wrappers remain authoritative throughout this
   state. A failed or paused rollout therefore adds write-only projection work
   but does not expose partial projection reads. Every batch sets an outer
   five-second lock timeout and 120-second statement timeout before invoking
   the private helper; function proconfig alone is not the statement timer.
3. `20260826080345` acquires a five-second source-write fence in one explicit
   transaction, inserts genuinely missing rows, removes stale rows, verifies
   key/state/modified parity, and removes the backfill helper.
4. Two standalone projection `CREATE INDEX CONCURRENTLY` migrations reuse the
   existing Process/Flow source HNSW indexes and precede the
   transactional Search/Hybrid cutover at `20260826080400` and the transactional
   Facets cutover at `20260826080403`.
5. `20260827010000` adds one narrow concurrent Flow source B-tree on
   `state_code`, restricted to state-100/200 rows with a non-null embedding.
   `20260827010003` transactionally verifies its exact table, keys, opclasses,
   predicate, owner, validity, and lack of INCLUDE/expression/options drift.
   This low-selectivity membership index accelerates the exact 0..199
   embedding-universe probe without becoming a covering id/version join path;
   it does not store vectors, rank semantic candidates, or alter API semantics.
6. `20260827020000` creates a separate narrow facet contract/table, adds
   `json_ordered` to both existing source projection trigger event lists, and
   installs one parent-to-facet trigger without changing a read path. Four
   UUID-quarter files through `20260827020004` insert child facts with the old
   Facets implementation still authoritative. `20260827020005` takes a
   five-second parent-first write fence, fills only genuinely missing children,
   and fails unless the validated child-FK subset has exact per-kind cardinality.
   `20260827020006` then dispatches only normalized
   empty-query/empty-filter requests to the 32-MB bounded narrow helper. Every
   query or filter retains the unchanged card implementation.
7. `20260827050000` builds one standalone concurrent combined eligibility
   B-tree over the existing card projection. It keys only dataset kind,
   identity, version, timestamp, and state, and its predicate admits
   regex-shaped Flow CAS or nonempty classifications without storing card or
   example values. `20260827050001` transactionally verifies the exact
   relation, owner, access method, keys, opclasses, predicate, and validity.
   `20260827050002` then adds the bounded anonymous catalog-summary façade and
   its two ACL-closed validation helpers. Counts and latest timestamp continue
   reading the narrow facet projection; no existing card/facet manifest,
   source trigger, table, or index changes.
8. `20260827134101` transactionally creates
   `private.portal_sitemap_rows_v1` with one row per public facet version, the
   primary key `(dataset_kind,id,version)`, and an exact-key FK to the facet
   child with `ON UPDATE RESTRICT` and `ON DELETE CASCADE`. Its sole
   `AFTER INSERT OR UPDATE` trigger upserts only the affected exact version;
   DELETE requires no sitemap trigger because the FK cascade removes that exact
   child. One set-based backfill proves exact row parity. The ordered index
   `(shard_no,contract_version,dataset_kind,id,version DESC,modified_at DESC)`
   supplies both the physical bucket filter and current-version selection.
   `20260827134102` then exposes the constant 64-row opaque manifest and the
   output/timeout-bounded shard reader. The reader uses
   `DISTINCT ON (dataset_kind,id)` in index order, so scan work grows with
   retained version history even though the response remains capped at 4,096
   identities and 2 MiB. The retained `portal_sitemap_entries_v1` function
   remains byte-identical.
9. `20260827134103_portal_sitemap_concurrency_repair.sql` is the atomic forward
   repair for the first PR Preview winner-table shape. It locks the sole facet
   writer, creates and fully backfills an exact-version shadow child, rebinds
   the assertion and public shard reader, then drops the obsolete winner table,
   delete/upsert helpers, and triggers before committing. Fresh databases
   already receive the final exact-version child from `20260827134101` and take
   the no-drift path. The rebound reader also compares cursor bytes with a fresh
   encoding of the exact expected object, rejecting JSONB-equivalent
   numeric-scale variants.

The card expand/reconcile/cutovers and all facet expand/backfill/reconcile/
cutover files are explicit transactions. A statement or guard failure rolls
back the entire file. Each pre-existing concurrent index file contains exactly one
non-transactional `CREATE INDEX CONCURRENTLY` statement. Source vectors,
embedding triggers, and duplicate projection HNSW indexes are intentionally
absent. The sitemap covering index is created normally while its new table is
empty, so it has no INVALID live-index recovery state.

Search, Hybrid, and Facets call the v1 manifest guard once per request. The
guard compares the committed registry SHA-256 with the live definitions,
owners, language, volatility, parallel/security settings, and function config
of the exact eleven-function card/document closure. Contract drift fails closed
before projection rows are read.

Issue #532 does not add another projection rollout or recovery boundary. The
card-context migration preserves every Issue #531 table, index, trigger, stored
card, and Facet fact. After Search or Hybrid has ordered and limited candidates,
one shared exact-key decorator reads at most 50 Process/Flow source rows and
derives only the exhaustive public card context. Its later query-only Flow
geography Search repair reuses the already synchronized facet child for
latest/filter/order/limit before hydrating 51 parent cards; it adds no relation,
index, Trigger, or writer work. Every matching request validates the independent
Facet manifest before reading the child, so helper/trigger drift fails through
the public Search error contract rather than returning stale derived facts. The
decorator has its own live transitive
manifest because it stores no historical rows; drift or an exact source miss
fails closed. There is no backfill, reconcile, concurrent index, COMMIT/history
cleanup, or writer recovery action for either layer.

The query-only geography Search migration accepts only the exact reviewed
pre-image or its exact canonical post-image digest. A database COMMIT followed
by a missing migration-history write may therefore use the unchanged normal
retry; reapplying the same `CREATE OR REPLACE FUNCTION` is byte-idempotent.
Any third definition, owner/config/ACL drift, or manifest failure remains a
hard stop and requires a separately reviewed forward repair.

## Read-only diagnosis

First identify the exact ledger and object state. Do not infer it from a CLI
error alone.

```sql
select version
from supabase_migrations.schema_migrations
where version between '20260826060422' and '20260827020006'
order by version;

select contract_version,
  manifest_schema,
  function_identities,
  manifest_sha256,
  created_by_migration
from private.portal_catalog_projection_contract_v1;

select private.portal_catalog_projection_manifest_sha256_v1()
  as live_manifest_sha256;

select projection_contract_version,
  count(*)
from private.portal_catalog_search_rows_v1
group by projection_contract_version
order by projection_contract_version;

select private.assert_portal_catalog_projection_contract_v1();

select contract_version,
  manifest_schema,
  function_identities,
  manifest_sha256,
  created_by_migration
from private.portal_catalog_facet_contract_v1;

select private.portal_catalog_facet_manifest_sha256_v1()
  as live_facet_manifest_sha256;

select facet_contract_version,
  count(*)
from private.portal_catalog_facet_rows_v1
group by facet_contract_version
order by facet_contract_version;

select private.assert_portal_catalog_facet_contract_v1();

select index_relation.oid::regclass as index_name,
  index_catalog.indisvalid,
  index_catalog.indisready,
  index_catalog.indislive,
  pg_catalog.pg_get_indexdef(index_relation.oid) as definition
from pg_catalog.pg_class as index_relation
join pg_catalog.pg_namespace as namespace
  on namespace.oid = index_relation.relnamespace
left join pg_catalog.pg_index as index_catalog
  on index_catalog.indexrelid = index_relation.oid
where (
    namespace.nspname = 'private'
    and index_relation.relname in (
      'portal_catalog_search_process_document_v1_pgroonga',
      'portal_catalog_search_flow_document_v1_pgroonga',
      'portal_catalog_facet_rows_v1_pkey',
      'portal_catalog_facet_rows_latest_v1_idx',
      'portal_sitemap_rows_shard_v1_idx'
    )
  ) or (
    namespace.nspname = 'public'
    and index_relation.relname in (
      'processes_embedding_ft_hnsw_idx',
      'flows_embedding_ft_hnsw_idx',
      'flows_portal_embedding_eligible_v1_idx'
    )
  )
order by index_relation.relname;

select trigger.tgrelid::regclass as source_table,
  trigger.tgname,
  trigger.tgenabled,
  trigger.tgtype,
  trigger.tgattr,
  pg_catalog.pg_get_triggerdef(trigger.oid) as definition
from pg_catalog.pg_trigger as trigger
where trigger.tgrelid in (
    'public.processes'::regclass,
    'public.flows'::regclass,
    'private.portal_catalog_search_rows_v1'::regclass,
    'private.portal_catalog_facet_rows_v1'::regclass
  )
  and trigger.tgname in (
    'portal_catalog_projection_content_sync_v1',
    'portal_catalog_facet_sync_v1',
    'portal_sitemap_rows_sync_v1'
  )
order by source_table, trigger.tgname;

select routine.oid::regprocedure as routine_identity,
  owner_role.rolname as owner_name,
  routine.prosecdef,
  routine.proconfig,
  routine.proacl,
  routine.prosrc
from pg_catalog.pg_proc as routine
join pg_catalog.pg_roles as owner_role on owner_role.oid = routine.proowner
where routine.oid in (
  pg_catalog.to_regprocedure(
    'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
  ),
  pg_catalog.to_regprocedure(
    'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
  ),
  pg_catalog.to_regprocedure(
    'private.portal_public_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
  ),
  pg_catalog.to_regprocedure(
    'api.portal_facets_v1(text,text,jsonb)'
  ),
  pg_catalog.to_regprocedure(
    'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'
  ),
  pg_catalog.to_regprocedure(
    'private.catalog_portal_facets_empty_v1_impl(text,text)'
  ),
  pg_catalog.to_regprocedure(
    'private.portal_catalog_facet_facts_v1(text,jsonb)'
  ),
  pg_catalog.to_regprocedure(
    'private.portal_catalog_facet_manifest_sha256_v1()'
  ),
  pg_catalog.to_regprocedure(
    'private.assert_portal_catalog_facet_contract_v1()'
  ),
  pg_catalog.to_regprocedure(
    'private.sync_portal_catalog_facet_row_v1()'
  )
);

select dependency.classid::regclass,
  dependency.objid,
  dependency.refclassid::regclass,
  dependency.refobjid,
  dependency.deptype
from pg_catalog.pg_depend as dependency
where dependency.objid = any (array_remove(array[
    pg_catalog.to_regprocedure(
      'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
    )::oid,
    pg_catalog.to_regprocedure(
      'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
    )::oid,
    pg_catalog.to_regprocedure(
      'api.portal_facets_v1(text,text,jsonb)'
    )::oid,
    pg_catalog.to_regprocedure(
      'private.catalog_portal_facets_empty_v1_impl(text,text)'
    )::oid,
    pg_catalog.to_regprocedure(
      'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'
    )::oid
  ], null))
   or dependency.refobjid = any (array_remove(array[
     pg_catalog.to_regprocedure(
       'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
     )::oid,
     pg_catalog.to_regprocedure(
       'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
     )::oid,
     pg_catalog.to_regprocedure(
       'api.portal_facets_v1(text,text,jsonb)'
     )::oid,
     pg_catalog.to_regprocedure(
       'private.catalog_portal_facets_empty_v1_impl(text,text)'
     )::oid,
     pg_catalog.to_regprocedure(
       'private.catalog_portal_facets_v1_impl(text,text,uuid,text,jsonb,text)'
     )::oid
   ], null));
```

If the failed migration has no ledger row and its explicit transaction rolled
back, repeat the normal migration command. A reconcile lock timeout is expected
to recover this way after the conflicting writer finishes.

If the registry row, live digest, child version, FK/check, or assertion differs,
do not retry, clean up a concurrent index, edit migration history, update the
registry digest, or rebuild v1 rows in place. Preserve the evidence and repair
the helper change through a new versioned migration path.

```bash
supabase db push --include-all
```

Never delete or rewrite a migration-history row to force a retry.

## Concurrent index recovery

A canceled concurrent build can leave an INVALID same-name relation while its
migration remains absent from history. `CREATE INDEX CONCURRENTLY` then fails
closed on retry. Confirm all of the following before cleanup:

- the exact index migration version is absent from migration history;
- `20260826080400` and `20260826080403` are absent;
- the v1 derivation-contract assertion succeeds;
- the same-name relation is one of the two private projection PGroonga indexes;
- it is INVALID, definition-drifted, or a canonical-valid build whose COMMIT
  preceded its missing migration-history row.

Run only the matching cleanup statement as a standalone database command. Do
not combine it with `CREATE INDEX CONCURRENTLY`, `BEGIN`, a `DO` block, or other
SQL in the same command.

```sql
drop index concurrently if exists
  private.portal_catalog_search_process_document_v1_pgroonga;

drop index concurrently if exists
  private.portal_catalog_search_flow_document_v1_pgroonga;
```

Execute only the one statement for the failed migration, then repeat the normal
migration command. The following cutover guard verifies the exact access
method, indexed column, `extensions` opclass, partial predicate, validity state,
and PGroonga options before any wrapper changes.

The post-cutover Flow eligibility index uses the same controlled pattern, but
its preconditions are different: `20260827010000` and `20260827010003` must both
be absent, while the cutover and Facets migrations must already be recorded and
their runtime probes healthy. Removing an unrecorded invalid, definition-drifted,
or canonical-valid commit-gap copy restores only the pre-hotfix slow sparse
probe; it does not revert API semantics. Run only this standalone cleanup:

```sql
drop index concurrently if exists
  public.flows_portal_embedding_eligible_v1_idx;
```

Then rerun the normal migration command. Never drop the index when either
post-cutover migration is recorded; diagnose or repair that state as separately
tracked work.

The Portal catalog-summary eligibility index has its own three-file boundary.
Cleanup is allowed only when `20260827050000`, `20260827050001`, and
`20260827050002` are all absent from migration history, both immutable Portal
projection assertions succeed, the facet cutover through `20260827020006` is
recorded, and the same-name relation is an INVALID, definition-drifted, or
canonical-valid commit-gap copy on
`private.portal_catalog_search_rows_v1`. Run only this standalone statement:

```sql
drop index concurrently if exists
  private.portal_catalog_summary_eligibility_v1_idx;
```

Then repeat the normal migration command. If the guard or façade migration is
already recorded, do not drop or rename the index and do not edit migration
history. Preserve the exact ledger/index/function evidence and repair through
a separately reviewed forward migration.

The Portal sitemap rollout has a three-file transactional boundary. The expand
file creates the exact-version child, composite primary/FK keys, ordered shard
index, one exact-key `AFTER INSERT OR UPDATE` upsert trigger, full backfill, and
row-parity proof in one transaction. Exact-version DELETE cleanup is owned by
the FK cascade. Ordinary failure rolls all of them back.
A database COMMIT followed by a missing `20260827134101` ledger row is not
blind-idempotent: normal retry must fail on the existing table/function. On a
hosted branch, do not delete a subset or edit history; retain the exact ledger,
table/index/FK, sole trigger/helper, row parity, and public-RPC absence evidence,
then use a separately reviewed forward repair. Only the explicitly disposable
isolated recovery project may reset to `20260827134100` and repeat the unchanged
expand.

The `20260827134102` public cutover is also transactional. A missing or disabled
exact-key trigger, missing/invalid history-order index, PK/FK/helper drift, or an
initial shard above 4,096 fails before exposing either RPC. The shard function
must retain exact canonical cursor re-encoding, `DISTINCT ON` current-version
selection, the 4,096-item and 2-MiB response limits, and its four-second timeout.
Restoring an unrecorded metadata rename inside the isolated recovery fixture and
repeating the unchanged migration is valid; an applied hosted expand with real
definition/data drift requires forward repair. Capacity overflow after cutover
is never migration or index corruption and never authorizes cleanup: facet and
sitemap-child writes continue, only the affected shard remains fail closed, and
expansion requires a separately versioned reshard contract.

`20260827134103_portal_sitemap_concurrency_repair.sql` is that checked-in
Preview forward repair, not permission for manual cleanup. While holding the
sole facet-writer fence, it creates the shadow exact-version child and index,
fully backfills it, installs the sole exact-key upsert trigger, rebinds the
assertion and reader, and drops the obsolete winner table/helpers before one
atomic commit. Preserve and escalate any prerequisite or convergence failure;
do not expose a partially backfilled child, retain an obsolete helper, split the
reader swap from old-object retirement, or edit history to force the repair.

`20260827193451_repair_portal_classification_example.sql` changes only the
existing summary function's optional example selection. It filters broad
classification codes, prefers Process evidence, preserves the function owner,
ACL, RLS and two-second summary budget, and adds no projection row, index,
Trigger, or writer path. An uncertain commit follows the normal unchanged
migration retry: `CREATE OR REPLACE FUNCTION` is idempotent after its strict
prerequisite/definition checks, and no data cleanup or projection recovery is
authorized.

`20260827210000_optimize_portal_flow_cas_search.sql` adds one partial expression
B-tree over valid public Flow CAS card values and one exact-CAS candidate branch.
It preserves the existing PGroonga path for ordinary lexical, UUID, and invalid
CAS-shaped text, rechecks the exact latest version before returning a candidate,
and changes no projection row, Trigger, RLS policy, cursor, or timeout. A failed
index build, function prerequisite, public-example smoke, or contract assertion
rolls back atomically; do not retain a manual index or bypass the forward
migration after an uncertain hosted outcome.

`20260827223000_isolate_portal_flow_cas_index.sql` is the forward repair for a
cold persistent-Dev summary timeout discovered after `210000`. It atomically
recreates the same index with an explicit 7..12 CAS-length predicate and adds
the identical predicate to the exact candidate branch. That discriminator is
redundant for valid CAS values but deliberately prevents the unconstrained
summary selector from choosing the CAS-leading index; summary selection
returns to the retained id-ordered eligibility index. The migration must prove
the exact `210000` function/index predecessor, run the real summary smoke inside
its two-second budget, preserve exact-CAS/latest behavior, and roll back both
index and function on any failure.

`20260828003000_select_selective_portal_cas_example.sql` changes only summary
example selection after a strict Dev probe showed that exhaustive context
hydration for a 13-match CAS could still consume the public eight-second
budget. The summary performs one ordered GroupAggregate over the exact CAS
index, retains at most 64 CAS values that occur exactly once across all stored
projection history, and then joins the chosen row to the shared latest CTE.
This conservative condition guarantees one current match after the latest
recheck without repeated helper calls; Search itself remains complete and
unchanged. If valid public CAS evidence exists but the bounded unique-value
probe cannot produce a current example, the migration fails instead of silently
removing the homepage CAS.
The summary's two-second timeout, three-example order, and 16-KiB cap remain
unchanged, and migration verification runs as the constrained Portal executor.

`20260829130000_make_portal_flow_cas_rls_indexable.sql` changes only the
Portal SELECT policy on the purpose-built card projection and the live
projection assertion. The table remains `postgres`-owned with forced RLS and a
validated CHECK that admits only states 100/200; the row-neutral policy is safe
only while the assertion pins all of those facts and its unique role binding.
This lets the existing non-leakproof JSON CAS equality remain an exact index
condition instead of a parent-wide filter. The migration adds or removes no
relation, index, Trigger, row, writer branch, timeout, or public contract. Any
policy, CHECK, owner, RLS-flag, assertion-hash, candidate-function, or summary-
function prerequisite drift aborts atomically. Recovery is the unchanged
migration retry; do not hand-edit the policy or weaken the table constraint.

`20260829131000_repair_portal_single_character_literal_search.sql` adds two
ACL-closed parallel sequential helpers and replaces only the two private
Process/Flow pattern helpers after the current PGroonga
TokenBigram LIKE path demonstrated a one-code-point false negative. It retains
the fixed `%L` multi-code-point template and adds one exact `strpos` branch for
the `%` + one unescaped code point + `%` shape. Index, index-only, and bitmap
paths are disabled only inside those narrow helpers so their scans can use up
to four parallel workers. Escaped wildcard/backslash
patterns, candidate/latest semantics, wrappers, timeouts, ACL/RLS, indexes,
relations, Triggers, and writers are unchanged. The migration requires exact
predecessor helper hashes plus the `130000` policy/domain state and rolls back
both replacements on any mismatch. Recovery is the normal forward retry; do
not rebuild PGroonga, weaken the test, or add a unigram index without separately
measured storage and writer evidence.

## Uncertain expand commit

The expand migration is transactional, so ordinary SQL failure leaves no Issue
531 object. A process crash after database `COMMIT` but before migration-history
recording is different: the ledger can omit `20260826060422` while its objects
exist, and a blind retry will fail on `CREATE TABLE` or `CREATE TRIGGER`.

Stop and obtain operator review. Cleanup is permitted only when read-only checks
prove that `20260826060422` and every later Issue 531 migration are absent, the
old external wrappers are still installed, and no application reads the private
projection. Under those conditions the projection is fully rebuildable from
the source tables:

```sql
begin;
set local lock_timeout = '5s';
set local statement_timeout = '30s';
lock table public.processes, public.flows in share row exclusive mode;

drop trigger if exists portal_catalog_projection_content_sync_v1
  on public.processes;
drop trigger if exists portal_catalog_projection_content_sync_v1
  on public.flows;

drop function if exists
  private.portal_projection_hybrid_search_v1_impl(
    text, text[], extensions.vector, jsonb, integer, text
  );
drop function if exists
  private.portal_projection_semantic_candidates_v1(
    text, extensions.vector
  );
drop function if exists
  private.portal_projection_semantic_process_v1(extensions.vector);
drop function if exists
  private.portal_projection_semantic_flow_v1(extensions.vector);
drop function if exists
  private.portal_projection_semantic_process_exact_v1(extensions.vector);
drop function if exists
  private.portal_projection_semantic_flow_exact_v1(extensions.vector);
drop function if exists private.assert_portal_catalog_projection_contract_v1();
drop function if exists
  private.portal_catalog_projection_manifest_sha256_v1();
drop function if exists
  private.backfill_portal_catalog_search_range_v1(uuid, uuid);
drop function if exists private.sync_portal_catalog_search_row_v1();
drop table if exists private.portal_catalog_search_rows_v1;
drop table if exists private.portal_catalog_projection_contract_v1;
drop function if exists
  private.catalog_portal_projection_payload_v1(text, integer, jsonb);
commit;
```

If any precondition is false, do not run cleanup. Escalate with the exact ledger,
index, trigger, and wrapper evidence.

## Facet projection recovery

The facet expand is transactional but intentionally not blind-idempotent. If
its database COMMIT succeeds before the migration ledger is recorded, normal
retry fails on the existing function/table. Stop and obtain operator review;
do not edit history or drop any subset of the contract. This runbook authorizes
only a full reset of the explicitly disposable isolated recovery project. It
does not authorize cleanup on Preview, persistent Dev, or production. For any
hosted COMMIT/history gap, retain the ledger/object/wrapper evidence and
escalate for a separately reviewed forward-repair migration.

Each of the four facet backfills is idempotent. It uses `ON CONFLICT DO NOTHING`
and verifies exact per-kind/range cardinality before recording history. The
validated composite child FK proves the child key set is a subset of the parent
key set, so equal cardinality proves exact key coverage. A COMMIT/history gap
may therefore use the normal unchanged migration retry. The shard files never
update or delete `private.portal_catalog_search_rows_v1`.

The reconcile migration locks the parent before the child with a five-second
timeout. Lock failure leaves both ledger and API unchanged; retry only after the
conflicting writer finishes. It inserts missing children only and treats any
existing mismatch as contract drift. The cutover separately rechecks full
parity before replacing the wrapper. A cutover guard failure must leave
`20260827020006` absent and the old wrapper byte-identical. Do not repair drift
by editing the literal facet digest or silently updating child facts.

The migration-time guards deliberately do not detoast every wide parent card or
repeat state/timestamp comparisons while holding deployment transactions or the
reconcile fence. The immutable helper, validated FK/checks, and only two
governed writers establish row provenance; the pgTAP suite and populated-upgrade
runner perform the complete key/state/timestamp/five-fact comparison outside
the short migration fence.

## Local recovery regression

The checked-in recovery regression requires an explicitly attested, isolated
Issue 531 Supabase project. It resets that local project and must never target a
shared checkout, Preview, persistent Dev, or production.

Formal recovery evidence requires clean HEAD, the reviewed Supabase CLI
`2.109.1`, and byte equality plus one aggregate SHA-256 across all 290 migration
files in the repository and isolated project. Comparing only Issue 531 files is
not sufficient because an earlier baseline change can alter recovery behavior.

```bash
PORTAL_PROJECTION_RECOVERY_TARGET=local-isolated \
PORTAL_PROJECTION_SUPABASE_WORKDIR=/absolute/path/to/database-engine-531-project \
SUPABASE_CLI=/absolute/path/to/supabase \
scripts/test_portal_projection_upgrade_recovery.sh
```

The regression uses two live database connections to cover valid-update,
delete, state `100 -> 20`, id/version key-change, and embedding-only/missing-row
races. Embedding-only changes remain source-HNSW owned and the final fence fills
any independently missing card row. The regression also proves reconcile
lock-timeout rollback, reconcile COMMIT/history retry, wrong and canonical-valid
same-name index cleanup without history edits, cutover guard rollback,
post-cutover Flow eligibility build/guard recovery, facet expand COMMIT/history
failure, facet shard idempotent retry, facet reconcile lock rollback, facet
cutover parity rollback, parent-trigger/FK-cascade convergence, successful
retry, and no-op repeat without rebuilding the eight recorded indexes.

The same runner also resets to `20260827134100` and proves the sitemap expand
COMMIT/history gap requires an explicit disposable-project reset. It then
renames the recorded covering index to force public-cutover rollback, proves
both RPCs remain absent, restores the unrecorded metadata name, applies the
unchanged cutover plus `20260827134103` forward repair, and validates the exact
exact-version child PK/FK/index, sole trigger/helper, absence of obsolete winner
objects, and final 64-descriptor manifest. It then exercises exact concurrent
inserts, updates, and deletes across versions of the same identity. Every writer
must commit through the parent/facet transaction and FK cascade without a
writer-side retry, while the sitemap child remains exactly equal to the
committed public facet-version set. Its no-op pass includes the history index
OID so a recorded retry cannot rebuild it. Canonical cursor evidence also
rejects alternate numeric scales such as `1.0` and `64.0`.
It separately loads the SHA-pinned exact `343b7a1` two-migration Preview fixture
over a populated v1/v2 identity, records only those two isolated ledger rows,
and proves `20260827134103` alone preserves both exact versions while switching
the public reader to v2 and retiring every old winner object.

The separate populated-upgrade runner resets the same kind of isolated project
to `20260827010003`, inserts 17,299 Process plus 108,947 Flow parent cards, and
executes the seven facet files verbatim. Every statement in a UUID-quarter file
must finish within 60 seconds, preserving at least 2x headroom under its
authored 120-second statement timeout, and each complete file must finish
within 120 seconds. Reconcile must complete within five seconds, and final
key coverage, deterministic sampled fact parity, and aggregate DTO counts must
be exact. The runner then applies `20260827134101`, `20260827134102`, and
`20260827134103` over all 126,246 rows with 60/15/15-second evidence budgets and
120/30/30-second outer timeouts. It requires exact facet/sitemap version-row
parity, the composite PK and exact `ON UPDATE RESTRICT`/`ON DELETE CASCADE` FK,
the ordered history index, the sole exact-key trigger, the 4,096-identity shard
cap, and both public sitemap RPCs. The pgTAP suite remains the exhaustive
semantic equality oracle.

The representative single-version benchmark retains 126,246 exact rows, a
largest shard of 2,066 identities, and roughly 11 ms shard-read p95. Its
history-density probe materializes 2,048 identities with 64 versions each
(131,072 rows). The natural `DISTINCT ON` plan must use the history-order index
as an index-only path, contain no `Sort` or `Incremental Sort`, spill no temp
data, and finish below four seconds. These gates make the retained-history scan
cost explicit rather than describing the reader as constant-cost.

```bash
PORTAL_FACET_UPGRADE_TARGET=local-isolated \
PORTAL_FACET_UPGRADE_SUPABASE_WORKDIR=/absolute/path/to/database-engine-531-project \
SUPABASE_CLI=/absolute/path/to/supabase \
scripts/test_portal_facet_projection_populated_upgrade.sh
```

The pre-cutover raw-source
`private.portal_public_hybrid_search_v1_impl(...)` is a rollback asset only
until `20260826080400` replaces the API wrapper. The same cutover transaction
drops it with `IF EXISTS`; a failed transaction restores it, while a committed
or commit/history-gap retry leaves it absent. The final guard requires the API
wrapper to call only `private.portal_projection_hybrid_search_v1_impl(...)`.

## Derived-semantics changes

The registry row, its digest, the eleven-function v1 closure, and every v1 row
label are immutable. A card/document semantic change must create a new helper
closure and shadow projection, then use bounded backfill, a short source-write
reconcile fence, concurrent lexical indexes, and an atomic read cutover. Never
update the v1 registry row or perform a long in-place mixed-semantics backfill.

The two-function facet manifest is independently immutable. Its v1 facts are
derived only from the already public-safe card, and its storage is a child
projection rather than a rewrite of card/document rows. A facet-fact semantic
change requires a new literal facet contract and an additive child rollout;
never modify the v1 digest or mix fact versions in the current child table.

Even a claimed output-equivalent bug fix requires full source-card byte
equivalence proof. Without that proof, treat it as v2. The static manifest check
rejects later migrations that replace a v1 closure member; the runtime guard is
the fail-closed defense if live catalog definitions drift.

The supported safety model assumes privileged DDL runs only through governed
migrations and CI. The live digest has no historical memory: an out-of-band
change followed by source writes/backfill and restoration of the old definition
can leave mixed rows while the current digest matches. If that sequence may
have occurred, do not re-enable or retry v1, even after restoring identical
function bytes. Treat the full v1 projection as untrusted and recover through a
shadow v2 rebuild and cutover.

The recovery harness holds the conflicting writer lock for 60 seconds so CLI
startup cannot let it expire before the migration reaches `LOCK`. After the
expected five-second lock timeout is captured, the harness terminates exactly
that application-name-bound backend and verifies a 5–30 second wall window,
unchanged migration ledger/helper state, and successful retry. Formal evidence
uses Supabase CLI `2.109.1`.
