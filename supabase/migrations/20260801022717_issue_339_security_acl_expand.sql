begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

-- Expand keeps the existing public Data API surface while introducing the
-- dedicated API namespace.  Contract removal of public compatibility objects
-- is deliberately out of scope for this migration.
create schema if not exists api authorization postgres;

comment on schema api is
  'Explicit Data API surface. Objects require individual grants and RLS or an equivalent security-invoker boundary.';

revoke all on schema api from public;
grant usage on schema api to anon, authenticated, service_role;

-- Existing public search/query facades were security-invoker functions that
-- called private helpers.  A non-login, non-BYPASSRLS owner lets those facades
-- keep caller JWT/RLS semantics without granting API roles access to private.
do $role$
begin
  if not exists (select 1 from pg_roles where rolname = 'api_internal_executor') then
    create role api_internal_executor nologin inherit nobypassrls;
  end if;
end
$role$;
alter role api_internal_executor nologin inherit nobypassrls;
grant api_internal_executor to postgres;
grant authenticated to api_internal_executor;
grant usage on schema public, private to api_internal_executor;
grant create on schema public to api_internal_executor;
grant select on all tables in schema public to api_internal_executor;
grant execute on all functions in schema public, private to api_internal_executor;

do $wrappers$
declare
  signature text;
  function_oid regprocedure;
begin
  for signature in select column1 from (values
    ('public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
    ('public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
    ('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'),
    ('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
    ('public.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
    ('public.search_processes_latest_v2(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'),
    ('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'),
    ('public.semantic_search_contacts_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_flowproperties_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_sources_v1(text,text,double precision,integer,text,integer,uuid)'),
    ('public.semantic_search_unitgroups_v1(text,text,double precision,integer,text,integer,uuid)')
  ) signatures
  loop
    function_oid := to_regprocedure(signature);
    if function_oid is null then
      raise exception 'required private facade is missing: %', signature;
    end if;
    execute format('alter function %s security definer', function_oid);
    execute format('alter function %s owner to api_internal_executor', function_oid);
  end loop;
end
$wrappers$;
revoke create on schema public from api_internal_executor;
revoke api_internal_executor from postgres;

-- Capture the exact environment-specific ACLs before convergence.  The
-- operator rollback file restores these catalog grants without touching data.
create table if not exists archive.security_acl_expand_20260801_snapshot (
  object_class text not null,
  object_schema text not null,
  object_identity text not null,
  grantee text not null,
  privilege_type text not null,
  captured_at timestamptz not null default statement_timestamp(),
  primary key (object_class, object_schema, object_identity, grantee, privilege_type)
);

revoke all on archive.security_acl_expand_20260801_snapshot from public, anon, authenticated;
grant select on archive.security_acl_expand_20260801_snapshot to service_role;

insert into archive.security_acl_expand_20260801_snapshot
  (object_class, object_schema, object_identity, grantee, privilege_type)
select 'schema', n.nspname, n.nspname,
  case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  a.privilege_type
from pg_namespace n
cross join lateral aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
left join pg_roles grantee on grantee.oid = a.grantee
where n.nspname in ('private', 'util', 'archive')
  and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated'))
on conflict do nothing;

insert into archive.security_acl_expand_20260801_snapshot
  (object_class, object_schema, object_identity, grantee, privilege_type)
select 'relation', n.nspname, c.relname,
  case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  a.privilege_type
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
cross join lateral aclexplode(coalesce(c.relacl, acldefault(
  case when c.relkind = 'S' then 'S'::"char" else 'r'::"char" end,
  c.relowner
))) a
left join pg_roles grantee on grantee.oid = a.grantee
where c.relkind in ('r', 'p', 'v', 'm', 'S')
  and n.nspname in ('public', 'api', 'private', 'util', 'archive')
  and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated'))
  and (
    a.privilege_type = 'MAINTAIN'
    or (
      n.nspname = 'public'
      and c.relname in (
        'lca_active_snapshots',
        'lca_factorization_registry',
        'lca_latest_all_unit_results',
        'lca_network_snapshots',
        'lca_package_artifacts',
        'lca_package_export_items',
        'lca_package_request_cache',
        'lca_result_cache',
        'lca_results',
        'lca_snapshot_artifacts'
      )
    )
  )
on conflict do nothing;

insert into archive.security_acl_expand_20260801_snapshot
  (object_class, object_schema, object_identity, grantee, privilege_type)
select 'function', n.nspname,
  format('%I.%I(%s)', n.nspname, p.proname, pg_get_function_identity_arguments(p.oid)),
  case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  a.privilege_type
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
left join pg_roles grantee on grantee.oid = a.grantee
where p.prokind = 'f'
  and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
  and (
    n.nspname in ('private', 'util', 'archive')
    or (
      n.nspname = 'public'
      and p.proname in ('save_lifecycle_model_bundle', 'delete_lifecycle_model_bundle')
    )
  )
on conflict do nothing;

insert into archive.security_acl_expand_20260801_snapshot
  (object_class, object_schema, object_identity, grantee, privilege_type)
select 'default_' || d.defaclobjtype::text, n.nspname, owner_role.rolname,
  case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  a.privilege_type
from pg_default_acl d
join pg_namespace n on n.oid = d.defaclnamespace
join pg_roles owner_role on owner_role.oid = d.defaclrole
cross join lateral aclexplode(d.defaclacl) a
left join pg_roles grantee on grantee.oid = a.grantee
where owner_role.rolname = 'postgres'
  and n.nspname in ('public', 'api', 'private', 'util', 'archive')
  and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
on conflict do nothing;

-- Internal schemas are never browser/API-role namespaces.  Public facades use
-- the RLS-bound non-login executor above to reach their private helpers.
revoke all on schema private, util, archive from public, anon, authenticated;
revoke execute on all functions in schema private, util, archive
  from public, anon, authenticated;
revoke all on all tables in schema private, util, archive
  from public, anon, authenticated;
revoke all on all sequences in schema private, util, archive
  from public, anon, authenticated;

-- Converge the known persistent-dev drift to the production-compatible ACL.
revoke all on table
  public.lca_active_snapshots,
  public.lca_factorization_registry,
  public.lca_latest_all_unit_results,
  public.lca_network_snapshots,
  public.lca_package_artifacts,
  public.lca_package_export_items,
  public.lca_package_request_cache,
  public.lca_result_cache,
  public.lca_results,
  public.lca_snapshot_artifacts
from anon, authenticated;

-- These three SELECT grants are existing public compatibility contracts.  RLS
-- remains the row boundary; writes continue to be command/service owned.
grant select on table
  public.lca_package_artifacts,
  public.lca_package_request_cache,
  public.lca_results
to authenticated;

-- The two lifecycle bundle RPCs remain public compatibility contracts during
-- Expand.  Contract removes them only after their authenticated consumers move.

-- PostgreSQL 17 added MAINTAIN to ALL.  API roles never need VACUUM/ANALYZE or
-- related maintenance authority, including on retained public compatibility.
revoke maintain on all tables in schema public, api, private, util, archive
  from anon, authenticated;

-- Every future object in an application-owned schema is opt-in for all API and
-- service identities.  The supabase_admin-owned defaults require a separate
-- hosted operator session because postgres cannot alter another owner's ACL.
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on sequences from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke execute on functions from public, anon, authenticated, service_role;

-- Machine-readable, business-row-free readback.  Contract remains false while
-- public compatibility is intentionally retained during Expand.
create or replace view util.security_acl_expand_posture
with (security_invoker = true)
as
with forbidden_schema_access as (
  select role_name, schema_name
  from (values ('anon'), ('authenticated')) roles(role_name)
  cross join (values ('private'), ('util'), ('archive')) schemas(schema_name)
  where has_schema_privilege(role_name, schema_name, 'USAGE')
), forbidden_internal_execute as (
  select role_name, n.nspname as schema_name, p.oid::regprocedure::text as function_name
  from (values ('anon'), ('authenticated')) roles(role_name)
  join pg_proc p on has_function_privilege(role_name, p.oid, 'EXECUTE')
  join pg_namespace n on n.oid = p.pronamespace
  where n.nspname in ('private', 'util', 'archive')
), forbidden_maintain as (
  select role_name, n.nspname as schema_name, c.relname as relation_name
  from (values ('anon'), ('authenticated')) roles(role_name)
  join pg_class c on has_table_privilege(role_name, c.oid, 'MAINTAIN')
  join pg_namespace n on n.oid = c.relnamespace
  where c.relkind in ('r', 'p', 'm')
    and n.nspname in ('public', 'api', 'private', 'util', 'archive')
), drift_tables as (
  select * from (values
    ('lca_active_snapshots', false),
    ('lca_factorization_registry', false),
    ('lca_latest_all_unit_results', false),
    ('lca_network_snapshots', false),
    ('lca_package_artifacts', true),
    ('lca_package_export_items', false),
    ('lca_package_request_cache', true),
    ('lca_result_cache', false),
    ('lca_results', true),
    ('lca_snapshot_artifacts', false)
  ) value(relation_name, authenticated_select)
), drift_acl_violations as (
  select role_name, relation_name, privilege_type
  from drift_tables
  cross join (values ('anon'), ('authenticated')) roles(role_name)
  cross join (values ('SELECT'), ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE'), ('REFERENCES'), ('TRIGGER'), ('MAINTAIN')) privileges(privilege_type)
  where has_table_privilege(role_name, format('public.%I', relation_name), privilege_type)
    <> (role_name = 'authenticated' and privilege_type = 'SELECT' and authenticated_select)
), executor_state as (
  select
    not executor.rolcanlogin
    and executor.rolinherit
    and not executor.rolbypassrls
    and (
      select count(*)
      from pg_proc p
      join pg_namespace n on n.oid = p.pronamespace
      where n.nspname = 'public'
        and p.proowner = executor.oid
        and p.prosecdef
    ) = 16 as ready
  from pg_roles executor
  where executor.rolname = 'api_internal_executor'
), repo_default_residue as (
  select n.nspname as schema_name, d.defaclobjtype as object_type,
    case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end as grantee,
    a.privilege_type
  from pg_default_acl d
  join pg_namespace n on n.oid = d.defaclnamespace
  join pg_roles owner_role on owner_role.oid = d.defaclrole
  cross join lateral aclexplode(d.defaclacl) a
  left join pg_roles grantee on grantee.oid = a.grantee
  where owner_role.rolname = 'postgres'
    and n.nspname in ('public', 'api', 'private', 'util', 'archive')
    and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
), platform_default_residue as (
  select n.nspname as schema_name, d.defaclobjtype as object_type,
    case when a.grantee = 0 then 'PUBLIC' else grantee.rolname end as grantee,
    a.privilege_type
  from pg_default_acl d
  join pg_namespace n on n.oid = d.defaclnamespace
  join pg_roles owner_role on owner_role.oid = d.defaclrole
  cross join lateral aclexplode(d.defaclacl) a
  left join pg_roles grantee on grantee.oid = a.grantee
  where owner_role.rolname = 'supabase_admin'
    and n.nspname in ('public', 'api', 'private', 'util', 'archive')
    and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
), migration_state as (
  select
    not exists (select 1 from forbidden_schema_access)
    and not exists (select 1 from forbidden_internal_execute)
    and not exists (select 1 from forbidden_maintain)
    and not exists (select 1 from drift_acl_violations)
    and coalesce((select ready from executor_state), false)
    and not exists (select 1 from repo_default_residue)
    as ready
)
select jsonb_build_object(
  'contractVersion', 'security-acl.expand.v1',
  'migrationVersion', '20260801022717',
  'phase', 'expand',
  'expectedHostedExposedSchemas', jsonb_build_array('public', 'api', 'graphql_public'),
  'migrationReady', (select ready from migration_state),
  'facadeExecutorReady', coalesce((select ready from executor_state), false),
  'hostedOperatorReady',
    (select ready from migration_state) and not exists (select 1 from platform_default_residue),
  'contractReady', false,
  'contractBlocker', 'private helper facades and public lifecycle bundle RPCs remain until consumer-zero and burn-in gates close',
  'forbiddenSchemaAccess',
    (select coalesce(jsonb_agg(to_jsonb(x) order by role_name, schema_name), '[]') from forbidden_schema_access x),
  'forbiddenInternalExecute',
    (select coalesce(jsonb_agg(to_jsonb(x) order by role_name, schema_name, function_name), '[]') from forbidden_internal_execute x),
  'forbiddenMaintain',
    (select coalesce(jsonb_agg(to_jsonb(x) order by role_name, schema_name, relation_name), '[]') from forbidden_maintain x),
  'driftAclViolations',
    (select coalesce(jsonb_agg(to_jsonb(x) order by role_name, relation_name, privilege_type), '[]') from drift_acl_violations x),
  'repoOwnerDefaultPrivilegeResidue',
    (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name, object_type, grantee, privilege_type), '[]') from repo_default_residue x),
  'platformOwnerDefaultPrivilegeResidue',
    (select coalesce(jsonb_agg(to_jsonb(x) order by schema_name, object_type, grantee, privilege_type), '[]') from platform_default_residue x)
) as posture;

revoke all on util.security_acl_expand_posture from public, anon, authenticated;
grant usage on schema util to service_role;
grant select on util.security_acl_expand_posture to service_role;

notify pgrst, 'reload config';
notify pgrst, 'reload schema';

commit;
