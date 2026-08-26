---
lastReviewedAt: 2026-08-26
lastReviewedCommit: 7a38910
lastReviewedNote: "Reviewed for immutable v1 manifest diagnosis, fail-closed drift handling, and the required shadow-v2 semantic-change path."
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
whenToUpdate:
  - when the Portal projection migration sequence or recovery test changes
checkPaths:
  - docs/agents/portal-projection-migration-recovery.md
  - scripts/test_portal_projection_upgrade_recovery.sh
  - scripts/check_portal_projection_manifest.py
  - supabase/migrations/20260826060422_portal_candidate_first_search.sql
  - supabase/migrations/20260826080257_portal_projection_backfill_0.sql
  - supabase/migrations/20260826080342_portal_projection_backfill_f.sql
  - supabase/migrations/20260826080345_portal_projection_reconcile.sql
  - supabase/migrations/20260826080348_portal_projection_process_pgroonga.sql
  - supabase/migrations/20260826080351_portal_projection_flow_pgroonga.sql
  - supabase/migrations/20260826080400_portal_projection_candidate_cutover.sql
  - supabase/migrations/20260826080403_portal_projection_facets.sql
related:
  - ../../AGENTS.md
  - repo-validation.md
  - supabase-branching.md
  - ../../scripts/README.md
---

# Portal Projection Migration Recovery

This runbook covers only the additive Issue 531 Portal projection rollout. It
does not authorize production mutation, migration-history edits, or deletion of
an applied migration. Use the repository's normal tracked-delivery and Supabase
branch controls before any hosted action.

## Safe rollout states

The rollout has four observable boundaries:

1. `20260826060422` installs the immutable v1 derivation-contract registry,
   private projection, dormant projection Hybrid kernel, and source-table sync
   triggers in one explicit transaction. Every row binds contract version 1.
2. `20260826080257` through `20260826080342` backfill bounded UUID ranges. Old
   Search, Hybrid, and Facets wrappers remain authoritative throughout this
   state. A failed or paused rollout therefore adds write-only projection work
   but does not expose partial projection reads.
3. `20260826080345` acquires a five-second source-write fence in one explicit
   transaction, inserts genuinely missing rows, removes stale rows, verifies
   key/state/modified parity, and removes the backfill helper.
4. Two standalone projection `CREATE INDEX CONCURRENTLY` migrations reuse the
   existing Process/Flow source HNSW indexes and precede the
   transactional Search/Hybrid cutover at `20260826080400` and the transactional
   Facets cutover at `20260826080403`.

The expand, reconcile, Search/Hybrid cutover, and Facets cutover files are
explicit transactions. A statement or guard failure rolls back the entire
file. Each concurrent index file contains exactly one non-transactional
`CREATE INDEX CONCURRENTLY` statement. Source vectors, embedding triggers, and
duplicate projection HNSW indexes are intentionally absent.

Search, Hybrid, and Facets call the v1 manifest guard once per request. The
guard compares the committed registry SHA-256 with the live definitions,
owners, language, volatility, parallel/security settings, and function config
of the exact eleven-function card/document closure. Contract drift fails closed
before projection rows are read.

## Read-only diagnosis

First identify the exact ledger and object state. Do not infer it from a CLI
error alone.

```sql
select version
from supabase_migrations.schema_migrations
where version between '20260826060422' and '20260826080403'
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
      'portal_catalog_search_flow_document_v1_pgroonga'
    )
  ) or (
    namespace.nspname = 'public'
    and index_relation.relname in (
      'processes_embedding_ft_hnsw_idx',
      'flows_embedding_ft_hnsw_idx'
    )
  )
order by index_relation.relname;

select trigger.tgrelid::regclass as source_table,
  trigger.tgname,
  trigger.tgenabled
from pg_catalog.pg_trigger as trigger
where trigger.tgrelid in (
    'public.processes'::regclass,
    'public.flows'::regclass
  )
  and trigger.tgname in (
    'portal_catalog_projection_content_sync_v1'
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
    )::oid
  ], null))
   or dependency.refobjid = any (array_remove(array[
     pg_catalog.to_regprocedure(
       'api.portal_hybrid_search_v1(text,text[],text,jsonb,integer)'
     )::oid,
     pg_catalog.to_regprocedure(
       'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'
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

## Local recovery regression

The checked-in recovery regression requires an explicitly attested, isolated
Issue 531 Supabase project. It resets that local project and must never target a
shared checkout, Preview, persistent Dev, or production.

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
successful retry, and no-op repeat without rebuilding recorded indexes.

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

Supabase CLI `2.109.1` was used for the local Issue 531 evidence. A real
five-second reconcile lock timeout returned after eight wall-clock seconds
including CLI connection/migration overhead; the migration ledger and helper
state remained unchanged before retry.
