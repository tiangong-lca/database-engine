-- Database #616: execute the Flow V2 semantic leaf as the fixed public Portal
-- role instead of the authenticated-inheriting internal role. RLS remains on;
-- the function body, HNSW settings, indexes, and external API ACLs are unchanged.

begin;

set local lock_timeout = '5s';
set local statement_timeout = '120s';

do $portal_flow_semantic_executor_alignment_guard$
declare
  v_function_sha text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_function_sha;

  if v_function_sha is distinct from
       '534a4e6cead5da4575747d50667e9280e469a672526d0eb97488b99577230b2a'
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'portal_public_executor'
         and not role.rolsuper
         and not role.rolinherit
         and not role.rolcreaterole
         and not role.rolcreatedb
         and not role.rolcanlogin
         and not role.rolreplication
         and not role.rolbypassrls
     )
     or not exists (
       select 1
       from pg_catalog.pg_roles as role
       where role.rolname = 'api_internal_executor'
         and not role.rolsuper
         and role.rolinherit
         and not role.rolcreaterole
         and not role.rolcreatedb
         and not role.rolcanlogin
         and not role.rolreplication
         and not role.rolbypassrls
     )
     or not exists (
       select 1
       from pg_catalog.pg_auth_members as membership
       where membership.member = 'api_internal_executor'::regrole
         and membership.roleid = 'authenticated'::regrole
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
         and routine.proowner = 'api_internal_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 's'
         and routine.proparallel = 'r'
         and routine.proconfig = array[
           'search_path=""',
           'statement_timeout=20s',
           'plan_cache_mode=force_custom_plan',
           'hnsw.iterative_scan=strict_order',
           'hnsw.ef_search=200',
           'hnsw.max_scan_tuples=20000',
           'hnsw.scan_mem_multiplier=2',
           'jit=off',
           'row_security=on'
         ]::text[]
     )
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_projection_semantic_candidates_v2(text,extensions.vector,jsonb)'::regprocedure
         and routine.proowner = 'api_internal_executor'::regrole
         and routine.prosrc ~ 'portal_projection_semantic_flow_v2'
     )
     or not exists (
       select 1
       from pg_catalog.pg_class as relation
       where relation.oid = 'public.flows'::regclass
         and relation.relrowsecurity
         and relation.relowner <> 'portal_public_executor'::regrole
     )
     or not exists (
       select 1
       from pg_catalog.pg_policy as policy
       where policy.polrelid = 'public.flows'::regclass
         and policy.polname = 'portal_public_executor_select_flows_v1'
         and policy.polpermissive
         and policy.polcmd = 'r'
         and policy.polroles =
           array['portal_public_executor'::regrole::oid]::oid[]
         and pg_catalog.pg_get_expr(policy.polqual, policy.polrelid) =
           '(state_code = ANY (ARRAY[100, 200]))'
         and policy.polwithcheck is null
     )
     or not pg_catalog.has_schema_privilege(
       'portal_public_executor', 'public', 'USAGE'
     )
     or not pg_catalog.has_schema_privilege(
       'portal_public_executor', 'private', 'USAGE'
     )
     or not pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'id', 'SELECT'
     )
     or not pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'version', 'SELECT'
     )
     or not pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'state_code', 'SELECT'
     )
     or pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'embedding_ft', 'SELECT'
     )
     or not pg_catalog.has_function_privilege(
       'portal_public_executor',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     ) then
    raise exception 'Portal Flow semantic executor prerequisites drifted'
      using errcode = '55000';
  end if;
end
$portal_flow_semantic_executor_alignment_guard$;

-- Hosted postgres retains ADMIN-only memberships that cannot SET either role.
-- These temporary grants follow the established migration pattern and are
-- revoked after transferring the function between the two NOLOGIN roles.
grant api_internal_executor, portal_public_executor to postgres;
grant create on schema private to portal_public_executor;

grant select (embedding_ft)
  on table public.flows
  to portal_public_executor;

alter function private.portal_projection_semantic_flow_v2(
  extensions.vector,
  jsonb
) owner to portal_public_executor;

revoke all on function private.portal_projection_semantic_flow_v2(
  extensions.vector,
  jsonb
) from public, anon, authenticated, service_role;
grant execute on function private.portal_projection_semantic_flow_v2(
  extensions.vector,
  jsonb
) to api_internal_executor;

comment on function private.portal_projection_semantic_flow_v2(
  extensions.vector,
  jsonb
) is
  'Returns at most 200 exact public Flow versions under portal_public_executor RLS: exact distance over at most 2000 indexed geography/access candidates, otherwise strict iterative HNSW.';

revoke create on schema private from portal_public_executor;
revoke api_internal_executor, portal_public_executor from postgres;

do $verify_portal_flow_semantic_executor_alignment$
declare
  v_function_sha text;
begin
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        pg_catalog.pg_get_functiondef(
          'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
  into v_function_sha;

  if v_function_sha is distinct from
       '534a4e6cead5da4575747d50667e9280e469a672526d0eb97488b99577230b2a'
     or not exists (
       select 1
       from pg_catalog.pg_proc as routine
       where routine.oid =
         'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
         and routine.proowner = 'portal_public_executor'::regrole
         and routine.prosecdef
         and routine.provolatile = 's'
         and routine.proparallel = 'r'
         and routine.proconfig = array[
           'search_path=""',
           'statement_timeout=20s',
           'plan_cache_mode=force_custom_plan',
           'hnsw.iterative_scan=strict_order',
           'hnsw.ef_search=200',
           'hnsw.max_scan_tuples=20000',
           'hnsw.scan_mem_multiplier=2',
           'jit=off',
           'row_security=on'
         ]::text[]
     )
     or not pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'embedding_ft', 'SELECT'
     )
     or pg_catalog.has_table_privilege(
       'portal_public_executor', 'public.flows', 'SELECT'
     )
     or pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'search_text', 'SELECT'
     )
     or pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.flows', 'extracted_md', 'SELECT'
     )
     or pg_catalog.has_column_privilege(
       'portal_public_executor', 'public.processes', 'embedding_ft', 'SELECT'
     )
     or not pg_catalog.has_function_privilege(
       'api_internal_executor',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or not pg_catalog.has_function_privilege(
       'portal_public_executor',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'anon',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'authenticated',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or pg_catalog.has_function_privilege(
       'service_role',
       'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
       'EXECUTE'
     )
     or not exists (
       select 1
       from pg_catalog.pg_class as relation
       where relation.oid = 'public.flows'::regclass
         and relation.relrowsecurity
         and relation.relowner <> 'portal_public_executor'::regrole
     )
     or not exists (
       select 1
       from pg_catalog.pg_policy as policy
       where policy.polrelid = 'public.flows'::regclass
         and policy.polname = 'portal_public_executor_select_flows_v1'
         and policy.polpermissive
         and policy.polcmd = 'r'
         and policy.polroles =
           array['portal_public_executor'::regrole::oid]::oid[]
         and pg_catalog.pg_get_expr(policy.polqual, policy.polrelid) =
           '(state_code = ANY (ARRAY[100, 200]))'
         and policy.polwithcheck is null
     ) then
    raise exception 'Portal Flow semantic executor alignment drifted'
      using errcode = '55000';
  end if;
end
$verify_portal_flow_semantic_executor_alignment$;

notify pgrst, 'reload schema';

commit;
