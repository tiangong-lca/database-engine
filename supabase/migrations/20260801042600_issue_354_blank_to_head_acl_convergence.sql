begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

do $preflight$
declare
  relation_name text;
  function_signature text;
begin
  if session_user <> 'postgres' or current_user <> 'postgres' then
    raise exception using errcode = '42501', message = 'Issue #354 ACL convergence requires the postgres owner session';
  end if;

  foreach relation_name in array array[
    'comments','contacts','flowproperties','flows','ilcd','lciamethods','lifecyclemodels',
    'processes','reviews','roles','sources','teams','unitgroups','users'
  ] loop
    if to_regclass(format('public.%I', relation_name)) is null then
      raise exception 'required relation is missing: public.%', relation_name;
    end if;
  end loop;

  foreach function_signature in array array[
    'public.delete_lifecycle_model_bundle(uuid,text)',
    'public.ilcd_classification_get(text,text,text[])',
    'public.ilcd_flow_categorization_get(text,text[])',
    'public.ilcd_location_get(text,text[])',
    'public.policy_is_current_user_in_roles(uuid,text[])',
    'public.save_lifecycle_model_bundle(jsonb)'
  ] loop
    if to_regprocedure(function_signature) is null then
      raise exception 'required function is missing: %', function_signature;
    end if;
  end loop;
end
$preflight$;

-- PostgreSQL 17 includes MAINTAIN in historical ALL grants. Converge only the
-- exact blank-replay delta proven against the persistent-dev catalog.
revoke maintain on table
  public.comments,
  public.contacts,
  public.flowproperties,
  public.flows,
  public.ilcd,
  public.lciamethods,
  public.lifecyclemodels,
  public.processes,
  public.reviews,
  public.roles,
  public.sources,
  public.teams,
  public.unitgroups,
  public.users
from service_role;

revoke execute on function
  public.ilcd_classification_get(text,text,text[]),
  public.ilcd_flow_categorization_get(text,text[]),
  public.ilcd_location_get(text,text[]),
  public.policy_is_current_user_in_roles(uuid,text[])
from anon;

revoke execute on function
  public.save_lifecycle_model_bundle(jsonb),
  public.delete_lifecycle_model_bundle(uuid,text)
from public, anon, authenticated;

do $readback$
declare
  relation_name text;
begin
  foreach relation_name in array array[
    'comments','contacts','flowproperties','flows','ilcd','lciamethods','lifecyclemodels',
    'processes','reviews','roles','sources','teams','unitgroups','users'
  ] loop
    if has_table_privilege('service_role', format('public.%I', relation_name), 'MAINTAIN') then
      raise exception 'service_role MAINTAIN convergence failed for public.%', relation_name;
    end if;
  end loop;

  if exists (
       select 1
       from pg_proc procedure
       join pg_namespace namespace on namespace.oid=procedure.pronamespace
       cross join lateral aclexplode(coalesce(procedure.proacl,acldefault('f',procedure.proowner))) acl
       join pg_roles grantee on grantee.oid=acl.grantee
       where namespace.nspname='public'
         and procedure.proname in (
           'ilcd_classification_get','ilcd_flow_categorization_get',
           'ilcd_location_get','policy_is_current_user_in_roles'
         )
         and grantee.rolname='anon' and acl.privilege_type='EXECUTE'
     )
     or has_function_privilege('anon','public.save_lifecycle_model_bundle(jsonb)','EXECUTE')
     or has_function_privilege('authenticated','public.save_lifecycle_model_bundle(jsonb)','EXECUTE')
     or has_function_privilege('anon','public.delete_lifecycle_model_bundle(uuid,text)','EXECUTE')
     or has_function_privilege('authenticated','public.delete_lifecycle_model_bundle(uuid,text)','EXECUTE') then
    raise exception 'Issue #354 function ACL convergence failed';
  end if;
end
$readback$;

commit;
