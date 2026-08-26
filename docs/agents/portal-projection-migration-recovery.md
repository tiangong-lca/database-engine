---
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
  - supabase/migrations/20260826060422_portal_candidate_first_search.sql
  - supabase/migrations/20260826080257_portal_projection_backfill_0.sql
  - supabase/migrations/20260826080342_portal_projection_backfill_f.sql
  - supabase/migrations/20260826080345_portal_projection_reconcile.sql
  - supabase/migrations/20260826080348_portal_projection_process_pgroonga.sql
  - supabase/migrations/20260826080351_portal_projection_flow_pgroonga.sql
  - supabase/migrations/20260826080354_portal_projection_process_hnsw.sql
  - supabase/migrations/20260826080357_portal_projection_flow_hnsw.sql
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

1. `20260826060422` installs the private projection, dormant projection Hybrid
   kernel, and source-table sync triggers in one explicit transaction.
2. `20260826080257` through `20260826080342` backfill bounded UUID ranges. Old
   Search, Hybrid, and Facets wrappers remain authoritative throughout this
   state. A failed or paused rollout therefore adds write-only projection work
   but does not expose partial projection reads.
3. `20260826080345` acquires a five-second source-write fence in one explicit
   transaction, inserts genuinely missing rows, removes stale rows, verifies
   key/state/modified/vector parity, and removes the backfill helper.
4. Four standalone `CREATE INDEX CONCURRENTLY` migrations precede the
   transactional Search/Hybrid cutover at `20260826080400` and the transactional
   Facets cutover at `20260826080403`.

The expand, reconcile, Search/Hybrid cutover, and Facets cutover files are
explicit transactions. A statement or guard failure rolls back the entire
file. Each concurrent index file contains exactly one non-transactional
`CREATE INDEX CONCURRENTLY` statement.

## Read-only diagnosis

First identify the exact ledger and object state. Do not infer it from a CLI
error alone.

```sql
select version
from supabase_migrations.schema_migrations
where version between '20260826060422' and '20260826080403'
order by version;

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
where namespace.nspname = 'private'
  and index_relation.relname in (
    'portal_catalog_search_process_document_v1_pgroonga',
    'portal_catalog_search_flow_document_v1_pgroonga',
    'portal_catalog_search_process_embedding_v1_hnsw',
    'portal_catalog_search_flow_embedding_v1_hnsw'
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
    'portal_catalog_projection_content_sync_v1',
    'portal_catalog_projection_embedding_sync_v1'
  )
order by source_table, trigger.tgname;
```

If the failed migration has no ledger row and its explicit transaction rolled
back, repeat the normal migration command. A reconcile lock timeout is expected
to recover this way after the conflicting writer finishes.

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
- the same-name relation is one of the four private projection indexes above;
- its definition or validity differs from the guarded canonical definition.

Run only the matching cleanup statement as a standalone database command. Do
not combine it with `CREATE INDEX CONCURRENTLY`, `BEGIN`, a `DO` block, or other
SQL in the same command.

```sql
drop index concurrently if exists
  private.portal_catalog_search_process_document_v1_pgroonga;

drop index concurrently if exists
  private.portal_catalog_search_flow_document_v1_pgroonga;

drop index concurrently if exists
  private.portal_catalog_search_process_embedding_v1_hnsw;

drop index concurrently if exists
  private.portal_catalog_search_flow_embedding_v1_hnsw;
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
drop trigger if exists portal_catalog_projection_embedding_sync_v1
  on public.processes;
drop trigger if exists portal_catalog_projection_content_sync_v1
  on public.flows;
drop trigger if exists portal_catalog_projection_embedding_sync_v1
  on public.flows;

drop table if exists private.portal_catalog_search_rows_v1;
drop function if exists private.sync_portal_catalog_search_row_v1();
drop function if exists
  private.backfill_portal_catalog_search_range_v1(uuid, uuid);
drop function if exists
  private.portal_projection_hybrid_search_v1_impl(
    text, text[], extensions.vector, jsonb, integer, text
  );
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
races. It also proves reconcile lock-timeout rollback, controlled same-name
index cleanup without history edits, cutover guard rollback, successful retry,
and no-op repeat without rebuilding recorded indexes.

Supabase CLI `2.109.1` was used for the local Issue 531 evidence. A real
five-second reconcile lock timeout returned after eight wall-clock seconds
including CLI connection/migration overhead; the migration ledger and helper
state remained unchanged before retry.
