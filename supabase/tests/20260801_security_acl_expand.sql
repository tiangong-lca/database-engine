begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(49);

select has_schema('api', 'dedicated API schema exists during Expand');
select ok(has_schema_privilege('anon', 'api', 'USAGE'), 'anon can resolve the explicit API namespace');
select ok(has_schema_privilege('authenticated', 'api', 'USAGE'), 'authenticated can resolve the explicit API namespace');
select ok(has_schema_privilege('service_role', 'api', 'USAGE'), 'service role can resolve the explicit API namespace');

select ok(not has_schema_privilege('anon', 'private', 'USAGE'), 'anon cannot resolve private');
select ok(not has_schema_privilege('authenticated', 'private', 'USAGE'), 'authenticated cannot resolve private');
select ok(not has_schema_privilege('anon', 'util', 'USAGE'), 'anon cannot resolve util');
select ok(not has_schema_privilege('authenticated', 'util', 'USAGE'), 'authenticated cannot resolve util');
select ok(not has_schema_privilege('anon', 'archive', 'USAGE'), 'anon cannot resolve archive');
select ok(not has_schema_privilege('authenticated', 'archive', 'USAGE'), 'authenticated cannot resolve archive');

select is((
  select count(*)::integer
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  cross join (values ('anon'), ('authenticated')) roles(role_name)
  where n.nspname in ('private', 'util', 'archive')
    and has_function_privilege(role_name, p.oid, 'EXECUTE')
), 0, 'API roles cannot execute internal-schema routines');

select is((
  select count(*)::integer
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  join pg_roles owner_role on owner_role.oid = p.proowner
  where n.nspname = 'public'
    and owner_role.rolname = 'api_internal_executor'
    and p.prosecdef
), 16, 'private facades use the non-login RLS-bound executor');

select is((
  select count(*)::integer
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  cross join (values ('anon'), ('authenticated')) roles(role_name)
  where n.nspname in ('private', 'util', 'archive')
    and c.relkind in ('r', 'p', 'v', 'm')
    and has_table_privilege(role_name, c.oid, 'SELECT,INSERT,UPDATE,DELETE')
), 0, 'API roles have no internal relation CRUD grants');

select is((
  select count(*)::integer
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  cross join (values ('anon'), ('authenticated')) roles(role_name)
  where n.nspname in ('private', 'util', 'archive')
    and c.relkind = 'S'
    and has_sequence_privilege(role_name, c.oid, 'USAGE,SELECT,UPDATE')
), 0, 'API roles have no internal sequence grants');

select is((
  select count(*)::integer
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  cross join (values ('anon'), ('authenticated')) roles(role_name)
  where n.nspname in ('public', 'api', 'private', 'util', 'archive')
    and c.relkind in ('r', 'p', 'm')
    and has_table_privilege(role_name, c.oid, 'MAINTAIN')
), 0, 'API roles have no application-relation MAINTAIN privilege');

with drift_tables(name, authenticated_select) as (values
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
)
select ok(not has_table_privilege('anon', format('public.%I', name), 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN'),
  format('anon has no direct privilege on public.%I', name))
from drift_tables;

with drift_tables(name, authenticated_select) as (values
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
)
select ok(
  has_table_privilege('authenticated', format('public.%I', name), 'SELECT') = authenticated_select
  and not has_table_privilege('authenticated', format('public.%I', name), 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN'),
  format('authenticated has production-compatible ACL on public.%I', name))
from drift_tables;

select ok(has_function_privilege('service_role', 'public.save_lifecycle_model_bundle(jsonb)', 'EXECUTE'),
  'service role retains save bundle execution');
select ok(has_function_privilege('anon', 'public.save_lifecycle_model_bundle(jsonb)', 'EXECUTE'),
  'anon save bundle compatibility remains visible during Expand');
select ok(has_function_privilege('authenticated', 'public.save_lifecycle_model_bundle(jsonb)', 'EXECUTE'),
  'authenticated save bundle compatibility remains during Expand');
select ok(has_function_privilege('service_role', 'public.delete_lifecycle_model_bundle(uuid,text)', 'EXECUTE'),
  'service role retains delete bundle execution');
select ok(has_function_privilege('anon', 'public.delete_lifecycle_model_bundle(uuid,text)', 'EXECUTE'),
  'anon delete bundle compatibility remains visible during Expand');
select ok(has_function_privilege('authenticated', 'public.delete_lifecycle_model_bundle(uuid,text)', 'EXECUTE'),
  'authenticated delete bundle compatibility remains during Expand');

select is((
  select count(*)::integer
  from pg_default_acl d
  join pg_namespace n on n.oid = d.defaclnamespace
  cross join lateral aclexplode(d.defaclacl) a
  left join pg_roles grantee on grantee.oid = a.grantee
  where d.defaclrole = 'postgres'::regrole
    and n.nspname in ('public', 'api', 'private', 'util', 'archive')
    and (a.grantee = 0 or grantee.rolname in ('anon', 'authenticated', 'service_role'))
), 0, 'postgres future application objects require explicit grants');

select ok((select (posture->>'migrationReady')::boolean from util.security_acl_expand_posture),
  'migration-owned security posture is ready');
select ok(not (select (posture->>'hostedOperatorReady')::boolean from util.security_acl_expand_posture),
  'local platform-owner default ACL residue remains a visible hosted operator gate');
select ok(not (select (posture->>'contractReady')::boolean from util.security_acl_expand_posture),
  'Expand does not claim Contract readiness while public compatibility remains');
select is((select posture->'expectedHostedExposedSchemas' from util.security_acl_expand_posture),
  '["public", "api", "graphql_public"]'::jsonb, 'hosted exposure readback publishes the exact expected set');

select ok(has_table_privilege('service_role', 'archive.security_acl_expand_20260801_snapshot', 'SELECT'),
  'service role can read the catalog-only rollback snapshot');
select ok(not has_table_privilege('anon', 'archive.security_acl_expand_20260801_snapshot', 'SELECT'),
  'anon cannot read the rollback snapshot');
select ok(not has_table_privilege('authenticated', 'archive.security_acl_expand_20260801_snapshot', 'SELECT'),
  'authenticated cannot read the rollback snapshot');

select * from finish();
rollback;
