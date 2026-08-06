#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cutover_migration="$repo_root/supabase/migrations/20260805130000_full_schema_cutover.sql"
contract_migrations=(
  "$repo_root/supabase/migrations/20260806160000_api_contract_closure.sql"
  "$repo_root/supabase/migrations/20260806161000_lca_package_capability_facades.sql"
)
database_url="$(
  supabase status --output env \
    | sed -n 's/^DB_URL="\([^"]*\)"$/\1/p'
)"

if [[ -z "$database_url" ]]; then
  echo "unable to resolve the local Supabase DB_URL" >&2
  exit 1
fi

cd "$repo_root"

supabase db reset --version 20260804090000 --no-seed

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
insert into public.identity_center_processed_events (event_id, event_type)
values ('issue-422-populated-upgrade', 'schema-cutover-test')
on conflict (event_id) do update
set event_type = excluded.event_type;

drop table if exists archive.issue_422_relation_snapshot;
create table archive.issue_422_relation_snapshot as
select
  class.oid as object_oid,
  class.relname as object_name,
  class.relkind as object_kind,
  case
    when class.relname in (
      'contacts', 'flowproperties', 'flows', 'ilcd', 'lciamethods',
      'lifecyclemodels', 'processes', 'sources', 'unitgroups'
    ) then 'public'
    when class.relname = 'worker_job_domain_refs' then 'api'
    when class.relname in (
      'worker_domain_traceability_violations',
      'worker_legacy_lifecycle_audit',
      'worker_legacy_table_retirement_blockers'
    ) then 'util'
    else 'private'
  end::name as target_schema,
  (
    (xpath(
      '/row/n/text()',
      query_to_xml(
        format('select count(*) n from %s', class.oid::regclass),
        false,
        true,
        ''
      )
    ))[1]::text
  )::bigint as exact_rows
from pg_class class
join pg_namespace namespace on namespace.oid = class.relnamespace
where namespace.nspname = 'public'
  and class.relkind in ('r', 'p', 'v');

drop table if exists archive.issue_422_routine_snapshot;
create table archive.issue_422_routine_snapshot as
select routine.oid as object_oid, routine.proname as object_name
from pg_proc routine
join pg_namespace namespace on namespace.oid = routine.pronamespace
where namespace.nspname = 'public'
  and routine.prokind = 'f';

drop table if exists archive.issue_422_trigger_snapshot;
create table archive.issue_422_trigger_snapshot as
select
  trigger_record.oid,
  trigger_record.tgrelid,
  trigger_record.tgfoid,
  trigger_record.tgenabled
from pg_trigger trigger_record
where not trigger_record.tgisinternal;

drop table if exists archive.issue_422_policy_snapshot;
create table archive.issue_422_policy_snapshot as
select
  policy.oid,
  policy.polrelid,
  policy.polname,
  policy.polcmd,
  policy.polpermissive,
  policy.polroles,
  policy.polqual,
  policy.polwithcheck
from pg_policy policy;

drop table if exists archive.issue_422_constraint_snapshot;
create table archive.issue_422_constraint_snapshot as
select
  constraint_record.oid,
  constraint_record.conrelid,
  constraint_record.confrelid,
  constraint_record.contype,
  constraint_record.convalidated
from pg_constraint constraint_record
where constraint_record.connamespace in (
  'public'::regnamespace,
  'private'::regnamespace,
  'util'::regnamespace
);
SQL

psql "$database_url" -v ON_ERROR_STOP=1 -f "$cutover_migration"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify$
declare
  mismatch_count bigint;
begin
  select count(*)
  into mismatch_count
  from archive.issue_422_relation_snapshot snapshot
  left join pg_namespace namespace
    on namespace.nspname = snapshot.target_schema
  left join pg_class class
    on class.relnamespace = namespace.oid
   and class.relname = snapshot.object_name
   and class.oid = snapshot.object_oid
   and class.relkind = snapshot.object_kind
  where class.oid is null;

  if mismatch_count <> 0 then
    raise exception
      'relation OID/schema/kind preservation failed for % objects',
      mismatch_count;
  end if;

  select count(*)
  into mismatch_count
  from archive.issue_422_relation_snapshot snapshot
  join pg_class class on class.oid = snapshot.object_oid
  where (
    (xpath(
      '/row/n/text()',
      query_to_xml(
        format('select count(*) n from %s', class.oid::regclass),
        false,
        true,
        ''
      )
    ))[1]::text
  )::bigint <> snapshot.exact_rows;

  if mismatch_count <> 0 then
    raise exception
      'exact relation row-count preservation failed for % objects',
      mismatch_count;
  end if;

  select count(*)
  into mismatch_count
  from archive.issue_422_routine_snapshot snapshot
  left join pg_proc routine
    on routine.oid = snapshot.object_oid
   and routine.proname = snapshot.object_name
  left join pg_namespace namespace on namespace.oid = routine.pronamespace
  where routine.oid is null
     or namespace.nspname not in ('api', 'private');

  if mismatch_count <> 0 then
    raise exception
      'routine OID/name preservation failed for % functions',
      mismatch_count;
  end if;

  select count(*)
  into mismatch_count
  from archive.issue_422_trigger_snapshot snapshot
  left join pg_trigger trigger_record
    on trigger_record.oid = snapshot.oid
   and trigger_record.tgrelid = snapshot.tgrelid
   and trigger_record.tgfoid = snapshot.tgfoid
   and trigger_record.tgenabled = snapshot.tgenabled
  where trigger_record.oid is null;

  if mismatch_count <> 0 then
    raise exception
      'trigger OID/dependency preservation failed for % triggers',
      mismatch_count;
  end if;

  select count(*)
  into mismatch_count
  from archive.issue_422_policy_snapshot snapshot
  left join pg_policy policy
    on policy.oid = snapshot.oid
   and policy.polrelid = snapshot.polrelid
   and policy.polname = snapshot.polname
   and policy.polcmd = snapshot.polcmd
   and policy.polpermissive = snapshot.polpermissive
   and policy.polroles = snapshot.polroles
   and policy.polqual is not distinct from snapshot.polqual
   and policy.polwithcheck is not distinct from snapshot.polwithcheck
  where policy.oid is null;

  if mismatch_count <> 0 then
    raise exception
      'RLS policy OID/expression preservation failed for % policies',
      mismatch_count;
  end if;

  select count(*)
  into mismatch_count
  from archive.issue_422_constraint_snapshot snapshot
  left join pg_constraint constraint_record
    on constraint_record.oid = snapshot.oid
   and constraint_record.conrelid = snapshot.conrelid
   and constraint_record.confrelid = snapshot.confrelid
   and constraint_record.contype = snapshot.contype
   and constraint_record.convalidated = snapshot.convalidated
  where constraint_record.oid is null;

  if mismatch_count <> 0 then
    raise exception
      'constraint OID/dependency preservation failed for % constraints',
      mismatch_count;
  end if;

  if not exists (
    select 1
    from private.identity_center_processed_events
    where event_id = 'issue-422-populated-upgrade'
      and event_type = 'schema-cutover-test'
  ) then
    raise exception 'representative pre-cutover business row was not preserved';
  end if;
end
$verify$;

select
  (select count(*) from archive.issue_422_relation_snapshot) as preserved_relations,
  (select count(*) from archive.issue_422_routine_snapshot) as preserved_routines,
  (select count(*) from archive.issue_422_trigger_snapshot) as preserved_triggers,
  (select count(*) from archive.issue_422_policy_snapshot) as preserved_policies,
  (select count(*) from archive.issue_422_constraint_snapshot) as preserved_constraints;
SQL

for contract_migration in "${contract_migrations[@]}"; do
  psql "$database_url" -v ON_ERROR_STOP=1 -f "$contract_migration"
done

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_contract_closure_upgrade$
begin
  if not exists (
    select 1
    from private.identity_center_processed_events
    where event_id = 'issue-422-populated-upgrade'
      and event_type = 'schema-cutover-test'
  ) then
    raise exception 'representative business row was lost during contract-closure upgrade';
  end if;

  if to_regclass('private.api_capability_grants') is null then
    raise exception 'API capability manifest is missing after upgrade';
  end if;

  if to_regclass('private.lca_package_import_prepare_idempotency_uk') is null then
    raise exception 'package import idempotency index is missing after upgrade';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
    cross join lateral pg_catalog.aclexplode(
      coalesce(routine.proacl, pg_catalog.acldefault('f', routine.proowner))
    ) as acl
    where namespace.nspname = 'api'
      and acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  ) then
    raise exception 'an API routine remains executable by PUBLIC after upgrade';
  end if;
end
$verify_contract_closure_upgrade$;
SQL
