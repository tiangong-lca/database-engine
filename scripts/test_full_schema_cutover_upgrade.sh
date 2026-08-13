#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
production_reuse_binding_hotfix="$repo_root/supabase/migrations/20260810170000_fix_completed_progress_and_reused_closure_binding.sql"
production_cutover_bridge="$repo_root/supabase/migrations/20260805120000_bridge_production_reuse_binding_cutover.sql"
cutover_migration="$repo_root/supabase/migrations/20260805130000_full_schema_cutover.sql"
contract_migrations=(
  "$repo_root/supabase/migrations/20260806160000_api_contract_closure.sql"
  "$repo_root/supabase/migrations/20260806161000_lca_package_capability_facades.sql"
)
drift_cleanup_migration="$repo_root/supabase/migrations/20260806230500_remove_recreated_public_routines.sql"
consumer_facade_migration="$repo_root/supabase/migrations/20260807103000_data_product_consumer_facades.sql"
acl_runtime_repair_migration="$repo_root/supabase/migrations/20260807233000_issue_422_acl_runtime_repair.sql"
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

# Reproduce the production ledger order from Issue #422: the later main-only
# hotfix is already recorded before the older full-schema cutover is applied.
psql "$database_url" -v ON_ERROR_STOP=1 -f "$production_reuse_binding_hotfix"

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

psql "$database_url" -v ON_ERROR_STOP=1 -f "$production_cutover_bridge"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify_production_cutover_bridge$
declare
  actual_public_functions bigint;
  actual_private_callers text[];
  expected_private_callers constant text[] := array[
    'cmd_lcia_result_build_request_v2_without_expiry',
    'lcia_result_package_bind_closure_certificate',
    'lcia_scope_closure_certificate_validity_guard',
    'lcia_scope_closure_evidence_usable',
    'svc_lcia_scope_closure_build_binding_without_expiry'
  ];
begin
  select count(*)
  into actual_public_functions
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'public'
    and routine.prokind = 'f';

  if actual_public_functions <> 333 then
    raise exception
      'production cutover bridge expected 333 public functions, found %',
      actual_public_functions;
  end if;

  if pg_catalog.to_regprocedure(
       'public.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
     ) is not null
     or pg_catalog.to_regprocedure(
       'private.lcia_scope_closure_bundle_binding_matches(public.lcia_scope_closure_checks,public.worker_job_artifacts)'
     ) is null then
    raise exception 'production cutover bridge did not move the helper to private';
  end if;

  actual_private_callers := array(
    select routine.proname::text
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.prosrc
        like '%private.lcia_scope_closure_bundle_binding_matches%'
    order by routine.proname
  );

  if actual_private_callers is distinct from expected_private_callers then
    raise exception
      'production cutover bridge caller manifest drifted: expected %, found %',
      expected_private_callers,
      actual_private_callers;
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.prosrc
        like '%public.lcia_scope_closure_bundle_binding_matches%'
  ) then
    raise exception 'production cutover bridge left a public helper reference';
  end if;
end
$verify_production_cutover_bridge$;
SQL

psql "$database_url" -v ON_ERROR_STOP=1 -f "$cutover_migration"

psql "$database_url" -v ON_ERROR_STOP=1 <<'SQL'
do $verify$
declare
  mismatch_count bigint;
begin
  if (select count(*) from archive.issue_422_routine_snapshot) <> 334 then
    raise exception
      'production-shaped routine snapshot expected 334 functions, found %',
      (select count(*) from archive.issue_422_routine_snapshot);
  end if;

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

  if pg_catalog.to_regprocedure(
       'private.lcia_scope_closure_bundle_binding_matches(private.lcia_scope_closure_checks,private.worker_job_artifacts)'
     ) is null then
    raise exception 'production bridge helper was not preserved through the cutover';
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
create function public.policy_roles_delete(uuid, uuid, text)
returns boolean language sql stable security definer
set search_path = public
as 'select false';

create function public.policy_roles_insert(uuid, uuid, text)
returns boolean language sql stable security definer
set search_path = public
as 'select false';

create function public.policy_roles_select(uuid, text)
returns boolean language sql stable security definer
set search_path = public
as 'select false';

create function public.policy_roles_update(uuid, uuid, text)
returns boolean language sql stable security definer
set search_path = public
as 'select false';

create function public.update_modified_at()
returns trigger language plpgsql
set search_path = ''
as $function$
begin
  new.modified_at = pg_catalog.now();
  return new;
end
$function$;

grant execute on function public.policy_roles_delete(uuid, uuid, text)
  to public, anon, authenticated, service_role;
grant execute on function public.policy_roles_insert(uuid, uuid, text)
  to public, anon, authenticated, service_role;
grant execute on function public.policy_roles_select(uuid, text)
  to public, anon, authenticated, service_role;
grant execute on function public.policy_roles_update(uuid, uuid, text)
  to public, anon, authenticated, service_role;
grant execute on function public.update_modified_at()
  to public, anon, authenticated, service_role;

do $verify_persistent_dev_drift_fixture$
declare
  fixture_count bigint;
begin
  select count(*)
  into fixture_count
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'public';

  if fixture_count <> 5 then
    raise exception 'expected five recreated public routines, found %', fixture_count;
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    cross join lateral (values
      ('anon'::name),
      ('authenticated'::name),
      ('service_role'::name)
    ) as expected_role(role_name)
    where namespace.nspname = 'public'
      and not pg_catalog.has_function_privilege(
        expected_role.role_name,
        routine.oid,
        'EXECUTE'
      )
  ) then
    raise exception 'persistent Dev drift fixture is missing an external EXECUTE grant';
  end if;
end
$verify_persistent_dev_drift_fixture$;
SQL

psql "$database_url" -v ON_ERROR_STOP=1 -f "$drift_cleanup_migration"
psql "$database_url" -v ON_ERROR_STOP=1 -f "$consumer_facade_migration"
psql "$database_url" -v ON_ERROR_STOP=1 -f "$acl_runtime_repair_migration"

# Reapplying the older bridge after the private-schema cutover proves the dev
# back-merge path is a strict no-op rather than a second state transition.
psql "$database_url" -v ON_ERROR_STOP=1 -f "$production_cutover_bridge"

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

  if pg_catalog.to_regprocedure('api.svc_data_product_publication_list(integer)') is null
     or pg_catalog.to_regprocedure('api.svc_data_product_worker_metadata(uuid[])') is null
     or pg_catalog.to_regprocedure('api.svc_data_product_current_public_package()') is null
     or pg_catalog.to_regprocedure('api.svc_membership_is_review_admin(uuid)') is null then
    raise exception 'an Edge consumer facade is missing after upgrade';
  end if;

  if not exists (
    select 1
    from archive.issue_422_routine_snapshot as snapshot
    join pg_catalog.pg_proc as routine on routine.oid = snapshot.object_oid
    join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
    where snapshot.object_name = 'lcia_scope_closure_bundle_binding_matches'
      and routine.proname = 'lcia_scope_closure_bundle_binding_matches'
      and namespace.nspname = 'private'
  ) then
    raise exception 'production bridge helper identity drifted after cutover or no-op replay';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.prosrc like '%public.lcia_scope_closure_bundle_binding_matches%'
  ) then
    raise exception 'a public production bridge helper reference survived cutover';
  end if;

  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
  ) then
    raise exception 'a recreated public routine remains after drift cleanup';
  end if;

  if pg_catalog.to_regprocedure('api.policy_roles_delete(uuid,uuid,text)') is null
     or pg_catalog.to_regprocedure('api.policy_roles_insert(uuid,uuid,text)') is null
     or pg_catalog.to_regprocedure('api.policy_roles_select(uuid,text)') is null
     or pg_catalog.to_regprocedure('api.policy_roles_update(uuid,uuid,text)') is null
     or pg_catalog.to_regprocedure('private.update_modified_at()') is null then
    raise exception 'a canonical routine was lost during public drift cleanup';
  end if;

  if not pg_catalog.has_function_privilege(
       'authenticated', 'api.policy_roles_select(uuid,text)', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon', 'api.policy_roles_select(uuid,text)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'
     ) then
    raise exception 'authenticated RLS helper ACLs drifted after populated upgrade';
  end if;

  if not pg_catalog.has_function_privilege(
       'authenticated', 'api.assert_lca_release_manager()', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'api.assert_lca_release_manager()', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role', 'api.get_current_lca_release()', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role', 'api.get_current_lca_release_process(uuid,text)', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role', 'api.get_lca_release_artifact_download(uuid)', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'service_role', 'api.get_lca_release_run(uuid)', 'EXECUTE'
     ) then
    raise exception 'Edge release ACLs drifted after populated upgrade';
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

drop table if exists archive.issue_422_constraint_snapshot;
drop table if exists archive.issue_422_policy_snapshot;
drop table if exists archive.issue_422_relation_snapshot;
drop table if exists archive.issue_422_routine_snapshot;
drop table if exists archive.issue_422_trigger_snapshot;
SQL
