-- Database #616 catalog, ACL, and RLS proof for the Flow semantic executor.
begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions,public,auth;
select extensions.no_plan();

select extensions.ok(
  exists (
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
  ),
  'Portal executor remains an isolated NOLOGIN NOINHERIT NOBYPASSRLS role'
);

select extensions.is(
  (
    select pg_catalog.pg_get_userbyid(routine.proowner)
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
  ),
  'portal_public_executor'::name,
  'Flow semantic leaf executes as the fixed public Portal role'
);

select extensions.is(
  pg_catalog.encode(
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
  ),
  '534a4e6cead5da4575747d50667e9280e469a672526d0eb97488b99577230b2a'::text,
  'executor alignment leaves the reviewed Flow semantic body byte-identical'
);

select extensions.ok(
  (
    select routine.prosecdef
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
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)'::regprocedure
  ),
  'security definer hardening, timeout, and HNSW settings remain unchanged'
);

select extensions.ok(
  pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'id', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'version', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'state_code', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'embedding_ft', 'SELECT'
  )
  and not pg_catalog.has_table_privilege(
    'portal_public_executor', 'public.flows', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'search_text', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'extracted_md', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'embedding_ft', 'SELECT'
  ),
  'Portal executor gains only the Flow vector column needed by this leaf'
);

select extensions.ok(
  pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'portal_public_executor',
    'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.portal_projection_semantic_flow_v2(extensions.vector,jsonb)',
    'EXECUTE'
  ),
  'internal caller remains admitted while every external role stays denied'
);

select extensions.ok(
  (
    select relation.relrowsecurity
      and relation.relowner <> 'portal_public_executor'::regrole
    from pg_catalog.pg_class as relation
    where relation.oid = 'public.flows'::regclass
  )
  and exists (
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
  ),
  'Flow source keeps RLS with the exact Portal public-state policy'
);

-- Controlled source rows prove that the new vector privilege does not bypass
-- RLS. User triggers are disabled only inside this rollback-only transaction.
alter table public.flows disable trigger user;
alter table private.portal_catalog_search_rows_v1 disable trigger user;
insert into public.flows(id, version, state_code, embedding_ft)
values
  (
    '61600000-0000-4000-8000-000000000001',
    '01.00.000',
    20,
    pg_catalog.array_cat(
      array[1::real,0::real],
      pg_catalog.array_fill(0::real,array[1022])
    )::extensions.vector(1024)
  ),
  (
    '61600000-0000-4000-8000-000000000002',
    '01.00.000',
    100,
    pg_catalog.array_cat(
      array[0.99::real,0.01::real],
      pg_catalog.array_fill(0::real,array[1022])
    )::extensions.vector(1024)
  );

insert into private.portal_catalog_search_rows_v1(
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card,
  document,
  projection_contract_version
)
values(
  'flow',
  '61600000-0000-4000-8000-000000000002',
  '01.00.000',
  100,
  '2026-09-04 00:00:00+00',
  '{"document":""}',
  '',
  1
);

grant portal_public_executor to postgres;
set local role portal_public_executor;

select extensions.is(
  (
    select pg_catalog.count(*)
    from public.flows
    where id in (
      '61600000-0000-4000-8000-000000000001'::uuid,
      '61600000-0000-4000-8000-000000000002'::uuid
    )
  ),
  1::bigint,
  'Portal executor sees the controlled public row and not the state-20 row'
);

select extensions.lives_ok(
  $$select id, version, state_code, embedding_ft
    from public.flows
    where id = '61600000-0000-4000-8000-000000000002'::uuid$$,
  'Portal executor can read exactly the Flow columns needed by the semantic leaf'
);

select extensions.is(
  (
    select pg_catalog.array_agg(candidate.id order by candidate.id)
    from private.portal_projection_semantic_flow_v2(
      pg_catalog.array_cat(
        array[1::real,0::real],
        pg_catalog.array_fill(0::real,array[1022])
      )::extensions.vector(1024),
      '{}'::jsonb
    ) as candidate
    where candidate.id in (
      '61600000-0000-4000-8000-000000000001'::uuid,
      '61600000-0000-4000-8000-000000000002'::uuid
    )
  ),
  array['61600000-0000-4000-8000-000000000002'::uuid],
  'closest nonpublic vector is hidden while the nearby public vector is returned'
);

reset role;
revoke portal_public_executor from postgres;

select * from extensions.finish();
rollback;
