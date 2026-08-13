begin;

set local lock_timeout = '10s';
set local statement_timeout = '2min';

-- Issue #422 runtime repair. The contract-closure migration revoked every
-- external api EXECUTE grant before rebuilding ACLs from this manifest. Keep
-- the repair manifest-first so later exact-ACL checks cannot silently drift
-- from the privileges required by Next RLS evaluation and Edge release reads.
do $preflight$
declare
  missing_routines text[];
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using
      errcode = '42501',
      message = 'Issue #422 ACL repair requires the postgres owner session';
  end if;

  if pg_catalog.to_regclass('private.api_capability_grants') is null then
    raise exception using
      errcode = '42P01',
      message = 'Issue #422 capability grant manifest is missing';
  end if;

  select pg_catalog.array_agg(identity order by identity)
  into missing_routines
  from pg_catalog.unnest(array[
    'api.policy_roles_select(uuid,text)',
    'api.policy_review_can_read(uuid,uuid)',
    'api.assert_lca_release_manager()',
    'api.get_current_lca_release()',
    'api.get_current_lca_release_process(uuid,text)',
    'api.get_lca_release_artifact_download(uuid)',
    'api.get_lca_release_run(uuid)'
  ]::text[]) as required(identity)
  where pg_catalog.to_regprocedure(identity) is null;

  if missing_routines is not null then
    raise exception using
      errcode = '42883',
      message = 'Issue #422 ACL repair routines are missing',
      detail = pg_catalog.array_to_string(missing_routines, ', ');
  end if;
end
$preflight$;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
select
  pg_catalog.format(
    '%I.%I(%s)',
    namespace.nspname,
    routine.proname,
    pg_catalog.oidvectortypes(routine.proargtypes)
  ),
  required.capability_id,
  required.allow_anon,
  required.allow_authenticated,
  required.allow_service_role
from (
  values
    ('api.policy_roles_select(uuid,text)', 'NX-CORE-01', false, true, false),
    ('api.policy_review_can_read(uuid,uuid)', 'NX-REV-01', false, true, false),
    ('api.assert_lca_release_manager()', 'EDGE-REL-01', false, true, false),
    ('api.get_current_lca_release()', 'EDGE-REL-01', true, true, true),
    ('api.get_current_lca_release_process(uuid,text)', 'EDGE-REL-01', true, true, true),
    ('api.get_lca_release_artifact_download(uuid)', 'EDGE-REL-01', false, true, true),
    ('api.get_lca_release_run(uuid)', 'EDGE-REL-01', false, true, true)
) as required(identity, capability_id, allow_anon, allow_authenticated, allow_service_role)
join pg_catalog.pg_proc as routine
  on routine.oid = pg_catalog.to_regprocedure(required.identity)
join pg_catalog.pg_namespace as namespace
  on namespace.oid = routine.pronamespace
on conflict (routine_identity) do update set
  capability_id = excluded.capability_id,
  allow_anon = excluded.allow_anon,
  allow_authenticated = excluded.allow_authenticated,
  allow_service_role = excluded.allow_service_role;

-- Rebuild these seven ACLs exactly instead of only layering grants over an
-- unknown state. PUBLIC remains closed and every role gets only its declared
-- capability.
revoke execute on function api.policy_roles_select(uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function api.policy_roles_select(uuid, text)
  to authenticated;

revoke execute on function api.policy_review_can_read(uuid, uuid)
  from public, anon, authenticated, service_role;
grant execute on function api.policy_review_can_read(uuid, uuid)
  to authenticated;

revoke execute on function api.assert_lca_release_manager()
  from public, anon, authenticated, service_role;
grant execute on function api.assert_lca_release_manager()
  to authenticated;

revoke execute on function api.get_current_lca_release()
  from public, anon, authenticated, service_role;
grant execute on function api.get_current_lca_release()
  to anon, authenticated, service_role;

revoke execute on function api.get_current_lca_release_process(uuid, text)
  from public, anon, authenticated, service_role;
grant execute on function api.get_current_lca_release_process(uuid, text)
  to anon, authenticated, service_role;

revoke execute on function api.get_lca_release_artifact_download(uuid)
  from public, anon, authenticated, service_role;
grant execute on function api.get_lca_release_artifact_download(uuid)
  to authenticated, service_role;

revoke execute on function api.get_lca_release_run(uuid)
  from public, anon, authenticated, service_role;
grant execute on function api.get_lca_release_run(uuid)
  to authenticated, service_role;

do $postcondition$
declare
  mismatch_count integer;
begin
  select pg_catalog.count(*)
  into mismatch_count
  from (
    values
      ('api.policy_roles_select(uuid,text)', 'NX-CORE-01', false, true, false),
      ('api.policy_review_can_read(uuid,uuid)', 'NX-REV-01', false, true, false),
      ('api.assert_lca_release_manager()', 'EDGE-REL-01', false, true, false),
      ('api.get_current_lca_release()', 'EDGE-REL-01', true, true, true),
      ('api.get_current_lca_release_process(uuid,text)', 'EDGE-REL-01', true, true, true),
      ('api.get_lca_release_artifact_download(uuid)', 'EDGE-REL-01', false, true, true),
      ('api.get_lca_release_run(uuid)', 'EDGE-REL-01', false, true, true)
  ) as expected(identity, capability_id, allow_anon, allow_authenticated, allow_service_role)
  join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(expected.identity)
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  left join private.api_capability_grants as manifest
    on manifest.routine_identity = pg_catalog.format(
      '%I.%I(%s)',
      namespace.nspname,
      routine.proname,
      pg_catalog.oidvectortypes(routine.proargtypes)
    )
  where manifest.capability_id is distinct from expected.capability_id
     or manifest.allow_anon is distinct from expected.allow_anon
     or manifest.allow_authenticated is distinct from expected.allow_authenticated
     or manifest.allow_service_role is distinct from expected.allow_service_role;

  if mismatch_count <> 0 then
    raise exception 'Issue #422 capability manifest postcondition failed: % rows', mismatch_count;
  end if;

  if not pg_catalog.has_function_privilege(
       'authenticated', 'api.policy_roles_select(uuid,text)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon', 'api.policy_roles_select(uuid,text)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'api.policy_roles_select(uuid,text)', 'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'authenticated', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'api.policy_review_can_read(uuid,uuid)', 'EXECUTE'
     ) then
    raise exception 'Issue #422 RLS helper ACL postcondition failed';
  end if;

  if not pg_catalog.has_function_privilege(
       'authenticated', 'api.assert_lca_release_manager()', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon', 'api.assert_lca_release_manager()', 'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role', 'api.assert_lca_release_manager()', 'EXECUTE'
     ) then
    raise exception 'Issue #422 release manager assertion ACL postcondition failed';
  end if;

  if not pg_catalog.has_function_privilege(
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
    raise exception 'Issue #422 Edge release ACL postcondition failed';
  end if;
end
$postcondition$;

notify pgrst, 'reload schema';

commit;
