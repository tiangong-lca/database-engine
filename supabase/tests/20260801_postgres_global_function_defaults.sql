begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(45);

select is(
  (select posture->>'contractVersion' from util.security_acl_expand_posture),
  'security-acl.expand.v2',
  'security posture uses the effective-default v2 contract'
);
select is(
  (select posture->>'defaultPrivilegeEvaluation' from util.security_acl_expand_posture),
  'built-in+global+per-schema-effective',
  'posture names every default-privilege layer it evaluates'
);
select ok(
  (select (posture->>'migrationReady')::boolean from util.security_acl_expand_posture),
  'repo-owned migration posture is ready'
);
select ok(
  not (select (posture->>'hostedOperatorReady')::boolean from util.security_acl_expand_posture),
  'supabase_admin remains a fail-closed hosted blocker owned by issue 352'
);
select is(
  (select posture->>'platformOwnerBlocker' from util.security_acl_expand_posture),
  'tiangong-lca/database-engine#352',
  'platform-owner residue is attributed without claiming it was repaired'
);
select is(
  (select jsonb_array_length(posture->'repoOwnerDefaultPrivilegeResidue') from util.security_acl_expand_posture),
  0,
  'repo-owned effective default privilege residue is empty'
);
select ok(
  (select jsonb_array_length(posture->'platformOwnerDefaultPrivilegeResidue') > 0 from util.security_acl_expand_posture),
  'platform-owner effective residue remains visible'
);

select is((
  select count(*)::integer
  from util.security_acl_effective_default_privileges
  where owner_name = 'postgres'
    and grantee in ('PUBLIC', 'anon', 'authenticated', 'service_role')
), 0, 'postgres built-in, global, and per-schema effective defaults are closed');

select is((
  select count(*)::integer
  from util.security_acl_effective_default_privileges
  where owner_name = 'supabase_admin'
    and schema_name in ('public', 'api', 'private', 'util', 'archive')
    and object_type = 'f'
    and grantee = 'PUBLIC'
    and privilege_type = 'EXECUTE'
), 5, 'supabase_admin built-in PUBLIC EXECUTE is not hidden by absent explicit catalog rows');

alter default privileges for role postgres
  grant execute on functions to service_role;
alter default privileges for role postgres in schema api
  grant execute on functions to service_role with grant option;
select is((
  select count(*)::integer
  from util.security_acl_effective_default_privileges
  where owner_name='postgres' and schema_name='api' and object_type='f'
    and grantee='service_role' and privilege_type='EXECUTE'
), 1, 'effective defaults fold global and additive schema ACLs to one identity');
select ok((
  select is_grantable
  from util.security_acl_effective_default_privileges
  where owner_name='postgres' and schema_name='api' and object_type='f'
    and grantee='service_role' and privilege_type='EXECUTE'
), 'effective defaults preserve grant-option precedence with bool_or');
alter default privileges for role postgres in schema api
  revoke execute on functions from service_role;
alter default privileges for role postgres
  revoke execute on functions from service_role;

create schema issue_339_non_application_probe authorization postgres;
create function issue_339_non_application_probe.future_function() returns integer
language sql immutable as $$ select 1 $$;

select ok(not exists (
  select 1
  from pg_proc p
  cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
  where p.oid = 'issue_339_non_application_probe.future_function()'::regprocedure
    and acl.grantee = 0
    and acl.privilege_type = 'EXECUTE'
), 'database-global revoke also closes PUBLIC in a non-application schema');
select ok(
  not has_function_privilege('anon', 'issue_339_non_application_probe.future_function()', 'EXECUTE'),
  'database-global revoke also closes anon in a non-application schema'
);
select ok(
  not has_function_privilege('authenticated', 'issue_339_non_application_probe.future_function()', 'EXECUTE'),
  'database-global revoke also closes authenticated in a non-application schema'
);
select ok(
  not has_function_privilege('service_role', 'issue_339_non_application_probe.future_function()', 'EXECUTE'),
  'database-global revoke also closes service_role in a non-application schema'
);
select ok(
  has_function_privilege('postgres', 'issue_339_non_application_probe.future_function()', 'EXECUTE'),
  'object owner retains function EXECUTE after the global revoke'
);

create function public.__issue_339_future_probe__() returns integer
language sql immutable as $$ select 1 $$;
create function api.__issue_339_future_probe__() returns integer
language sql immutable as $$ select 1 $$;
create function private.__issue_339_future_probe__() returns integer
language sql immutable as $$ select 1 $$;
create function util.__issue_339_future_probe__() returns integer
language sql immutable as $$ select 1 $$;
create function archive.__issue_339_future_probe__() returns integer
language sql immutable as $$ select 1 $$;

select ok(not exists (
  select 1
  from pg_proc p
  cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl
  where p.oid = function_name::regprocedure
    and acl.grantee = 0
    and acl.privilege_type = 'EXECUTE'
), format('PUBLIC has no default EXECUTE on newly-created %s', function_name))
from (values
  ('public.__issue_339_future_probe__()'),
  ('api.__issue_339_future_probe__()'),
  ('private.__issue_339_future_probe__()'),
  ('util.__issue_339_future_probe__()'),
  ('archive.__issue_339_future_probe__()')
) probes(function_name);

select ok(
  not has_function_privilege(role_name, function_name, 'EXECUTE'),
  format('%s has no default EXECUTE on newly-created %s', role_name, function_name)
)
from (values
  ('anon', 'public.__issue_339_future_probe__()'),
  ('authenticated', 'public.__issue_339_future_probe__()'),
  ('service_role', 'public.__issue_339_future_probe__()'),
  ('anon', 'api.__issue_339_future_probe__()'),
  ('authenticated', 'api.__issue_339_future_probe__()'),
  ('service_role', 'api.__issue_339_future_probe__()'),
  ('anon', 'private.__issue_339_future_probe__()'),
  ('authenticated', 'private.__issue_339_future_probe__()'),
  ('service_role', 'private.__issue_339_future_probe__()'),
  ('anon', 'util.__issue_339_future_probe__()'),
  ('authenticated', 'util.__issue_339_future_probe__()'),
  ('service_role', 'util.__issue_339_future_probe__()'),
  ('anon', 'archive.__issue_339_future_probe__()'),
  ('authenticated', 'archive.__issue_339_future_probe__()'),
  ('service_role', 'archive.__issue_339_future_probe__()')
) probes(role_name, function_name);

grant execute on function api.__issue_339_future_probe__() to service_role;
select ok(
  has_function_privilege('service_role', 'api.__issue_339_future_probe__()', 'EXECUTE'),
  'service-only function access remains available through an explicit object grant'
);
select ok(
  not has_function_privilege('anon', 'api.__issue_339_future_probe__()', 'EXECUTE'),
  'explicit service-only grant does not expose the function to anon'
);
select ok(
  not has_function_privilege('authenticated', 'api.__issue_339_future_probe__()', 'EXECUTE'),
  'explicit service-only grant does not expose the function to authenticated'
);

select ok(not exists (
  select 1
  from pg_class c
  cross join lateral aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
  where c.oid = 'archive.security_acl_postgres_global_functions_20260801_snapshot'::regclass
    and acl.grantee = 0
    and acl.privilege_type = 'SELECT'
), 'PUBLIC cannot read the operator-only global-default rollback snapshot');
select ok(not exists (
  select 1
  from pg_class c
  cross join lateral aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) acl
  where c.oid = 'archive.security_acl_postgres_global_functions_20260801_snapshot'::regclass
    and acl.grantee <> c.relowner
), 'snapshot ACL contains no non-owner grantee, including unknown custom roles');
select ok(
  not has_table_privilege('anon', 'archive.security_acl_postgres_global_functions_20260801_snapshot', 'SELECT'),
  'anon cannot read the global-default rollback snapshot'
);
select ok(
  not has_table_privilege('authenticated', 'archive.security_acl_postgres_global_functions_20260801_snapshot', 'SELECT'),
  'authenticated cannot read the global-default rollback snapshot'
);
select ok(
  not has_table_privilege('service_role', 'archive.security_acl_postgres_global_functions_20260801_snapshot', 'SELECT'),
  'service_role cannot read the operator-only global-default rollback snapshot'
);
select ok(
  has_table_privilege('postgres', 'archive.security_acl_postgres_global_functions_20260801_snapshot', 'SELECT'),
  'postgres owner can read the rollback snapshot for an explicit operator restore'
);

select * from finish();
rollback;
