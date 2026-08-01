begin;

set local lock_timeout = '5s';
set local statement_timeout = '2min';

-- PostgreSQL grants EXECUTE on newly-created functions to PUBLIC through its
-- built-in global default.  A per-schema REVOKE cannot subtract that global
-- privilege, so preserve the pre-migration global posture before converging it.
create table if not exists archive.security_acl_postgres_global_functions_20260801_snapshot (
  scope_schema text not null,
  grantee text not null,
  privilege_type text not null,
  is_grantable boolean not null,
  captured_at timestamptz not null default statement_timestamp(),
  primary key (scope_schema, grantee, privilege_type),
  check (scope_schema in ('*', 'public', 'api', 'private', 'util', 'archive')),
  check (privilege_type = 'EXECUTE')
);

-- The snapshot may inherit table defaults granted to roles unknown to this
-- migration. Remove every ACL identity other than the table owner rather than
-- relying on a fixed role list, then prove the resulting object is owner-only.
do $snapshot_acl$
declare
  acl_grantee record;
  grant_target text;
begin
  for acl_grantee in
    select distinct acl.grantee, grantee.rolname
    from pg_class relation
    cross join lateral aclexplode(coalesce(
      relation.relacl, acldefault('r', relation.relowner)
    )) acl
    left join pg_roles grantee on grantee.oid = acl.grantee
    where relation.oid = 'archive.security_acl_postgres_global_functions_20260801_snapshot'::regclass
      and acl.grantee <> relation.relowner
  loop
    if acl_grantee.grantee = 0 then
      grant_target := 'PUBLIC';
    elsif acl_grantee.rolname is null then
      raise exception 'snapshot ACL contains unknown role oid %', acl_grantee.grantee;
    else
      grant_target := format('%I', acl_grantee.rolname);
    end if;
    execute format(
      'REVOKE ALL ON archive.security_acl_postgres_global_functions_20260801_snapshot FROM %s',
      grant_target
    );
  end loop;

  if exists (
    select 1
    from pg_class relation
    cross join lateral aclexplode(coalesce(
      relation.relacl, acldefault('r', relation.relowner)
    )) acl
    where relation.oid = 'archive.security_acl_postgres_global_functions_20260801_snapshot'::regclass
      and acl.grantee <> relation.relowner
  ) then
    raise exception 'snapshot ACL convergence left a non-owner grantee';
  end if;

  if not has_table_privilege(
    'postgres',
    'archive.security_acl_postgres_global_functions_20260801_snapshot',
    'SELECT'
  ) then
    raise exception 'postgres owner lost snapshot SELECT';
  end if;
end
$snapshot_acl$;

insert into archive.security_acl_postgres_global_functions_20260801_snapshot
  (scope_schema, grantee, privilege_type, is_grantable)
select
  '*',
  case when acl.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  acl.privilege_type,
  acl.is_grantable
from aclexplode(coalesce(
  (
    select d.defaclacl
    from pg_default_acl d
    where d.defaclrole = 'postgres'::regrole
      and d.defaclnamespace = 0
      and d.defaclobjtype = 'f'
  ),
  acldefault('f', 'postgres'::regrole)
)) acl
left join pg_roles grantee on grantee.oid = acl.grantee
where acl.grantee = 0
   or grantee.rolname in ('anon', 'authenticated', 'service_role')
union all
select
  namespace.nspname,
  case when acl.grantee = 0 then 'PUBLIC' else grantee.rolname end,
  acl.privilege_type,
  acl.is_grantable
from pg_default_acl defaults
join pg_namespace namespace on namespace.oid = defaults.defaclnamespace
cross join lateral aclexplode(defaults.defaclacl) acl
left join pg_roles grantee on grantee.oid = acl.grantee
where defaults.defaclrole = 'postgres'::regrole
  and defaults.defaclobjtype = 'f'
  and namespace.nspname in ('public', 'api', 'private', 'util', 'archive')
  and (acl.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
on conflict do nothing;

-- Functions require both layers: the global revoke removes PostgreSQL's
-- built-in PUBLIC EXECUTE, while the per-schema revoke removes any additive
-- grants retained from the historical baseline.  All four transport
-- identities are explicit so future access is object-by-object opt-in.
alter default privileges for role postgres
  revoke execute on functions from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke execute on functions from public, anon, authenticated, service_role;

-- Tables and sequences have owner-only built-in defaults.  Keep their
-- application-schema convergence scoped per schema rather than broadening it
-- into a database-wide policy.
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on tables from public, anon, authenticated, service_role;
alter default privileges for role postgres in schema public, api, private, util, archive
  revoke all on sequences from public, anon, authenticated, service_role;

-- Compute effective defaults from all three layers:
--   1. PostgreSQL's built-in acldefault when no global row exists;
--   2. the owner's explicit global pg_default_acl row when present; and
--   3. additive per-schema pg_default_acl rows.
-- An empty explicit catalog query is therefore never interpreted as safe.
create or replace view util.security_acl_effective_default_privileges
with (security_invoker = true)
as
with application_schemas as (
  select n.oid as schema_oid, n.nspname as schema_name
  from pg_namespace n
  where n.nspname in ('public', 'api', 'private', 'util', 'archive')
), owners as (
  select r.oid as owner_oid, r.rolname as owner_name
  from pg_roles r
  where r.rolname in ('postgres', 'supabase_admin')
), targets as (
  select owners.*, application_schemas.*, object_type
  from owners
  cross join application_schemas
  cross join (values ('r'::"char"), ('S'::"char"), ('f'::"char")) object_types(object_type)
), effective_acl as (
  select targets.owner_name, targets.schema_name, targets.object_type,
    acl.grantee, acl.privilege_type, acl.is_grantable
  from targets
  cross join lateral (
    select exploded.grantee, exploded.privilege_type, exploded.is_grantable
    from aclexplode(coalesce(
      (
        select global_defaults.defaclacl
        from pg_default_acl global_defaults
        where global_defaults.defaclrole = targets.owner_oid
          and global_defaults.defaclnamespace = 0
          and global_defaults.defaclobjtype = targets.object_type
      ),
      acldefault(targets.object_type, targets.owner_oid)
    )) exploded
    union all
    select exploded.grantee, exploded.privilege_type, exploded.is_grantable
    from pg_default_acl schema_defaults
    cross join lateral aclexplode(schema_defaults.defaclacl) exploded
    where schema_defaults.defaclrole = targets.owner_oid
      and schema_defaults.defaclnamespace = targets.schema_oid
      and schema_defaults.defaclobjtype = targets.object_type
  ) acl
)
select
  effective_acl.owner_name,
  effective_acl.schema_name,
  effective_acl.object_type,
  case when effective_acl.grantee = 0 then 'PUBLIC' else grantee.rolname end as grantee,
  effective_acl.privilege_type,
  bool_or(effective_acl.is_grantable) as is_grantable
from effective_acl
left join pg_roles grantee on grantee.oid = effective_acl.grantee
group by effective_acl.owner_name, effective_acl.schema_name,
  effective_acl.object_type, effective_acl.grantee, grantee.rolname,
  effective_acl.privilege_type;

revoke all on util.security_acl_effective_default_privileges
  from public, anon, authenticated;
grant select on util.security_acl_effective_default_privileges to service_role;

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
  select schema_name, object_type, grantee, privilege_type, is_grantable
  from util.security_acl_effective_default_privileges
  where owner_name = 'postgres'
    and grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
), platform_default_residue as (
  select schema_name, object_type, grantee, privilege_type, is_grantable
  from util.security_acl_effective_default_privileges
  where owner_name = 'supabase_admin'
    and grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
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
  'contractVersion', 'security-acl.expand.v2',
  'migrationVersion', '20260801131918',
  'phase', 'expand',
  'repoOwnerFunctionDefaultScope', 'database-global-all-schemas',
  'evaluatedApplicationSchemas', jsonb_build_array('public', 'api', 'private', 'util', 'archive'),
  'defaultPrivilegeEvaluation', 'built-in+global+per-schema-effective',
  'expectedHostedExposedSchemas', jsonb_build_array('public', 'api', 'graphql_public'),
  'migrationReady', (select ready from migration_state),
  'facadeExecutorReady', coalesce((select ready from executor_state), false),
  'hostedOperatorReady',
    (select ready from migration_state) and not exists (select 1 from platform_default_residue),
  'contractReady', false,
  'contractBlocker', 'private helper facades and public lifecycle bundle RPCs remain until consumer-zero and burn-in gates close',
  'platformOwnerBlocker', 'tiangong-lca/database-engine#352',
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

notify pgrst, 'reload schema';

commit;
