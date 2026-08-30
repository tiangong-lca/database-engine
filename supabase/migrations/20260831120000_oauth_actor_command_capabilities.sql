begin;

-- Direct core-table DML stays ACL-closed after command cutover. OAuth CLI and
-- MCP mutations use the existing actor-bound command RPCs instead. Classify
-- the three CRUD commands separately so the MCP broker never needs the wider
-- CLI command capability.
update private.api_capability_grants as manifest
set capability_id = 'DB-CORE-WRITE-01'
from pg_catalog.pg_proc as routine
join pg_catalog.pg_namespace as namespace
  on namespace.oid = routine.pronamespace
where namespace.nspname = 'api'
  and routine.proname in (
    'cmd_dataset_create',
    'cmd_dataset_save_draft',
    'cmd_dataset_delete'
  )
  and pg_catalog.to_regprocedure(manifest.routine_identity) = routine.oid;

-- Repair any authenticated API-manifest drift without changing function ACLs.
-- Existing overload families retain their one capability. A previously
-- unclassified authenticated function is admitted only to the first-party CLI
-- capability, except for the three narrow CRUD commands above.
insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
with authenticated_routines as (
  select
    routine.oid,
    routine.proname,
    format(
      '%I.%I(%s)',
      namespace.nspname,
      routine.proname,
      pg_catalog.oidvectortypes(routine.proargtypes)
    ) as routine_identity
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
    and pg_catalog.has_function_privilege(
      'authenticated',
      routine.oid,
      'execute'
    )
), existing_name_capability as (
  select
    routine.proname,
    min(manifest.capability_id) as capability_id,
    count(distinct manifest.capability_id) as capability_count
  from private.api_capability_grants as manifest
  join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(manifest.routine_identity)
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
  group by routine.proname
)
select
  candidate.routine_identity,
  case
    when candidate.proname in (
      'cmd_dataset_create',
      'cmd_dataset_save_draft',
      'cmd_dataset_delete'
    ) then 'DB-CORE-WRITE-01'
    when existing.capability_count = 1 then existing.capability_id
    else 'CLI-RPC-01'
  end,
  false,
  true,
  false
from authenticated_routines as candidate
left join existing_name_capability as existing
  on existing.proname = candidate.proname
where not exists (
  select 1
  from private.api_capability_grants as manifest
  where manifest.routine_identity = candidate.routine_identity
)
on conflict (routine_identity) do nothing;

do $oauth_actor_command_invariants$
declare
  drifted record;
begin
  select
    routine.proname,
    array_agg(distinct manifest.capability_id order by manifest.capability_id) as capabilities
  into drifted
  from private.api_capability_grants as manifest
  join pg_catalog.pg_proc as routine
    on routine.oid = pg_catalog.to_regprocedure(manifest.routine_identity)
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
  group by routine.proname
  having count(distinct manifest.capability_id) <> 1
  limit 1;

  if found then
    raise exception 'OAuth API route has ambiguous capabilities: % -> %',
      drifted.proname,
      drifted.capabilities;
  end if;

  select routine.oid::regprocedure as routine_identity
  into drifted
  from pg_catalog.pg_proc as routine
  join pg_catalog.pg_namespace as namespace
    on namespace.oid = routine.pronamespace
  where namespace.nspname = 'api'
    and pg_catalog.has_function_privilege('authenticated', routine.oid, 'execute')
    and not exists (
      select 1
      from private.api_capability_grants as manifest
      where pg_catalog.to_regprocedure(manifest.routine_identity) = routine.oid
        and manifest.allow_authenticated
    )
  limit 1;

  if found then
    raise exception 'Authenticated API routine is missing OAuth capability: %',
      drifted.routine_identity;
  end if;
end
$oauth_actor_command_invariants$;

commit;
