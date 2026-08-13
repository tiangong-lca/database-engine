begin;

set local lock_timeout = '5s';
set local statement_timeout = '30s';

do $public_routine_drift_preflight$
declare
  unexpected_public_routines text[];
  missing_canonical_routines text[];
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using
      errcode = '42501',
      message = 'Issue #422 public-routine cleanup requires the postgres owner session';
  end if;

  select pg_catalog.array_agg(identity order by identity)
  into unexpected_public_routines
  from (
    select pg_catalog.format(
      '%I.%I(%s)',
      namespace.nspname,
      routine.proname,
      pg_catalog.replace(pg_catalog.oidvectortypes(routine.proargtypes), ' ', '')
    ) as identity
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
  ) as public_routine
  where identity <> all (array[
    'public.policy_roles_delete(uuid,uuid,text)',
    'public.policy_roles_insert(uuid,uuid,text)',
    'public.policy_roles_select(uuid,text)',
    'public.policy_roles_update(uuid,uuid,text)',
    'public.update_modified_at()'
  ]::text[]);

  if unexpected_public_routines is not null then
    raise exception using
      errcode = '55000',
      message = 'unexpected public routines block Issue #422 drift cleanup',
      detail = pg_catalog.array_to_string(unexpected_public_routines, ', ');
  end if;

  select pg_catalog.array_agg(identity order by identity)
  into missing_canonical_routines
  from pg_catalog.unnest(array[
    'api.policy_roles_delete(uuid,uuid,text)',
    'api.policy_roles_insert(uuid,uuid,text)',
    'api.policy_roles_select(uuid,text)',
    'api.policy_roles_update(uuid,uuid,text)',
    'private.update_modified_at()'
  ]::text[]) as required(identity)
  where pg_catalog.to_regprocedure(identity) is null;

  if missing_canonical_routines is not null then
    raise exception using
      errcode = '55000',
      message = 'canonical routines must exist before public drift cleanup',
      detail = pg_catalog.array_to_string(missing_canonical_routines, ', ');
  end if;
end
$public_routine_drift_preflight$;

drop function if exists public.policy_roles_delete(uuid, uuid, text) restrict;
drop function if exists public.policy_roles_insert(uuid, uuid, text) restrict;
drop function if exists public.policy_roles_select(uuid, text) restrict;
drop function if exists public.policy_roles_update(uuid, uuid, text) restrict;
drop function if exists public.update_modified_at() restrict;

do $public_routine_drift_postcondition$
begin
  if exists (
    select 1
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
  ) then
    raise exception using
      errcode = '55000',
      message = 'public routines remain after Issue #422 drift cleanup';
  end if;
end
$public_routine_drift_postcondition$;

notify pgrst, 'reload schema';

commit;
