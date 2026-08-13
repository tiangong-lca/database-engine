#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
migration="$repo_root/supabase/migrations/20260810210000_search_text_array_and_review_guard.sql"
database_url="$(
  supabase status --output env \
    | sed -n 's/^DB_URL="\([^"]*\)"$/\1/p'
)"

if [[ -z "$database_url" ]]; then
  echo "unable to resolve the local Supabase DB_URL" >&2
  exit 1
fi

cd "$repo_root"

# Positive path: preserve unrelated rows and every table heap while replacing
# the proven-empty scalar columns with nullable text[] catalog entries.
supabase db reset --version 20260810200000 --no-seed

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
set session_replication_role = replica;

insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, rule_verification
)
values (
  '42221000-0000-4000-8000-000000000001',
  '01.00.000',
  '{"name":"metadata-only sentinel"}'::jsonb,
  '{"name":"metadata-only sentinel"}'::json,
  '42221000-0000-4000-8000-000000000010',
  0,
  true
);

reset session_replication_role;

drop table if exists archive.issue_422_search_text_heap_snapshot;
create table archive.issue_422_search_text_heap_snapshot as
select
  class.oid as relation_oid,
  namespace.nspname as schema_name,
  class.relname as relation_name,
  class.relfilenode
from pg_catalog.pg_class as class
join pg_catalog.pg_namespace as namespace
  on namespace.oid = class.relnamespace
where namespace.nspname = 'public'
  and class.relname in (
    'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
    'processes', 'sources', 'unitgroups'
  );
SQL

psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_metadata_only_upgrade$
declare
  mismatch_count bigint;
begin
  select pg_catalog.count(*)
  into mismatch_count
  from archive.issue_422_search_text_heap_snapshot as snapshot
  join pg_catalog.pg_class as class
    on class.oid = snapshot.relation_oid
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = class.relnamespace
  where namespace.nspname is distinct from snapshot.schema_name
     or class.relname is distinct from snapshot.relation_name
     or class.relfilenode is distinct from snapshot.relfilenode;

  if mismatch_count <> 0
     or (select pg_catalog.count(*)
         from archive.issue_422_search_text_heap_snapshot) <> 7 then
    raise exception
      'metadata-only search_text conversion changed one or more table heaps';
  end if;

  if (
    select pg_catalog.count(*)
    from pg_catalog.pg_attribute as attribute
    join pg_catalog.pg_class as class on class.oid = attribute.attrelid
    join pg_catalog.pg_namespace as namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relname in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and attribute.attname = 'search_text'
      and attribute.atttypid = 'text[]'::regtype
      and not attribute.attnotnull
      and not attribute.atthasdef
      and not attribute.attisdropped
  ) <> 7 then
    raise exception 'not all seven search_text columns became nullable text[]';
  end if;

  if not exists (
    select 1
    from public.contacts
    where id = '42221000-0000-4000-8000-000000000001'::uuid
      and version = '01.00.000'
      and json = '{"name":"metadata-only sentinel"}'::jsonb
      and search_text is null
  ) then
    raise exception 'representative business row changed during conversion';
  end if;
end
$verify_metadata_only_upgrade$;

set session_replication_role = replica;
delete from public.contacts
where id = '42221000-0000-4000-8000-000000000001'::uuid;
reset session_replication_role;
drop table archive.issue_422_search_text_heap_snapshot;
SQL

# Negative path: a single populated legacy value must abort before any of the
# seven column definitions change.
supabase db reset --version 20260810200000 --no-seed

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
set session_replication_role = replica;
insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, rule_verification,
  search_text
)
values (
  '42221000-0000-4000-8000-000000000002',
  '01.00.000',
  '{"name":"fail-closed sentinel"}'::jsonb,
  '{"name":"fail-closed sentinel"}'::json,
  '42221000-0000-4000-8000-000000000010',
  0,
  true,
  'must not be discarded'
);
reset session_replication_role;
SQL

if psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration"; then
  echo "search_text migration unexpectedly accepted a populated scalar" >&2
  exit 1
fi

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_fail_closed_upgrade$
begin
  if (
    select pg_catalog.count(*)
    from pg_catalog.pg_attribute as attribute
    join pg_catalog.pg_class as class on class.oid = attribute.attrelid
    join pg_catalog.pg_namespace as namespace on namespace.oid = class.relnamespace
    where namespace.nspname = 'public'
      and class.relname in (
        'contacts', 'flowproperties', 'flows', 'lifecyclemodels',
        'processes', 'sources', 'unitgroups'
      )
      and attribute.attname = 'search_text'
      and attribute.atttypid = 'text'::regtype
      and not attribute.attisdropped
  ) <> 7 then
    raise exception 'failed conversion changed one or more column definitions';
  end if;

  if not exists (
    select 1
    from public.contacts
    where id = '42221000-0000-4000-8000-000000000002'::uuid
      and search_text = 'must not be discarded'
  ) then
    raise exception 'failed conversion discarded the populated scalar';
  end if;
end
$verify_fail_closed_upgrade$;
SQL

# Leave the local database at the checked-out migration head for subsequent
# pgTAP and generated-contract validation.
supabase db reset --no-seed

echo "search_text metadata-only upgrade validation passed"
