begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select extensions.no_plan();

select extensions.ok(
  pg_catalog.to_regprocedure(
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
  ) is not null,
  'the candidate-first Portal catalog kernel has the exact private signature'
);

select extensions.ok(
  (
    select routine.proowner = 'api_internal_executor'::regrole
      and routine.prosecdef
      and routine.proconfig @> array[
        'search_path=""',
        'statement_timeout=8s',
        'plan_cache_mode=force_custom_plan'
      ]::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
    )
  ),
  'the catalog kernel is pinned to the constrained internal executor and 8-second budget'
);

select extensions.ok(
  pg_catalog.has_function_privilege(
    'portal_public_executor',
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)',
    'EXECUTE'
  ),
  'only the Portal executor can cross into the private candidate kernel'
);

select extensions.ok(
  pg_catalog.to_regprocedure(
    'private.catalog_portal_document_v1(text,jsonb)'
  ) is null
  and (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.proconfig @> array['search_path=""']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_projection_payload_v1(text,integer,jsonb)'
    )
  )
  and pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
    'EXECUTE'
  ),
  'the projection payload has one internal writer edge and no obsolete expression helper'
);

select extensions.ok(
  (
    select relation.relowner = 'postgres'::regrole
      and relation.relrowsecurity
      and relation.relforcerowsecurity
    from pg_catalog.pg_class as relation
    where relation.oid =
      'private.portal_catalog_projection_contract_v1'::regclass
  )
  and (
    select count(*) = 1
      and min(contract_version) = 1
      and min(manifest_schema) =
        'portal.catalog-projection-function-manifest.v1'
      and min(manifest_sha256) =
        'b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc'
      and min(created_by_migration) = '20260826060422'
    from private.portal_catalog_projection_contract_v1
  )
  and (
    select function_identities = array[
      'private.catalog_portal_projection_payload_v1(text,integer,jsonb)',
      'private.portal_catalog_card_v1(text,integer,jsonb)',
      'private.portal_capabilities_v1(text,integer,jsonb)',
      'private.portal_publication_root_v1(text,jsonb)',
      'private.portal_access_restrictions_open_v1(jsonb)',
      'private.portal_scalar_text_v1(jsonb)',
      'private.portal_localized_text_v1(jsonb)',
      'private.portal_json_items_v1(jsonb)',
      'private.portal_classifications_v1(jsonb)',
      'private.portal_safe_year_v1(text)',
      'private.portal_source_v1(text,jsonb)'
    ]::text[]
      and pg_catalog.cardinality(function_identities) = 11
      and (
        select count(distinct identity)
        from pg_catalog.unnest(function_identities) as identity
      ) = 11
    from private.portal_catalog_projection_contract_v1
    where contract_version = 1
  ),
  'the immutable private registry stores the exact 11-function v1 closure and digest literal'
);

select extensions.ok(
  (
    select attribute.atttypid = 'pg_catalog.int2'::regtype
      and attribute.attnotnull
      and not attribute.atthasdef
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid =
        'private.portal_catalog_search_rows_v1'::regclass
      and attribute.attname = 'projection_contract_version'
      and not attribute.attisdropped
  )
  and exists (
    select 1
    from pg_catalog.pg_constraint as contract_check
    where contract_check.conrelid =
        'private.portal_catalog_search_rows_v1'::regclass
      and contract_check.conname =
        'portal_catalog_search_rows_contract_version_v1_chk'
      and contract_check.contype = 'c'
      and contract_check.convalidated
      and pg_catalog.regexp_replace(
        pg_catalog.pg_get_expr(
          contract_check.conbin,
          contract_check.conrelid
        ),
        '[[:space:]]',
        '',
        'g'
      ) = '(projection_contract_version=1)'
  )
  and exists (
    select 1
    from pg_catalog.pg_constraint as contract_fk
    where contract_fk.conrelid =
        'private.portal_catalog_search_rows_v1'::regclass
      and contract_fk.confrelid =
        'private.portal_catalog_projection_contract_v1'::regclass
      and contract_fk.conname =
        'portal_catalog_search_rows_contract_version_v1_fk'
      and contract_fk.contype = 'f'
      and contract_fk.convalidated
      and contract_fk.confupdtype = 'r'
      and contract_fk.confdeltype = 'r'
  ),
  'projection rows require an explicit validated smallint v1 RESTRICT foreign key'
);

select extensions.ok(
  pg_catalog.has_table_privilege(
    'api_internal_executor',
    'private.portal_catalog_projection_contract_v1',
    'SELECT'
  )
  and not pg_catalog.has_table_privilege(
    'portal_public_executor',
    'private.portal_catalog_projection_contract_v1',
    'SELECT'
  )
  and not pg_catalog.has_table_privilege(
    'anon', 'private.portal_catalog_projection_contract_v1', 'SELECT'
  )
  and not pg_catalog.has_table_privilege(
    'authenticated',
    'private.portal_catalog_projection_contract_v1',
    'SELECT'
  )
  and not pg_catalog.has_table_privilege(
    'service_role',
    'private.portal_catalog_projection_contract_v1',
    'SELECT'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.portal_catalog_projection_manifest_sha256_v1()',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.portal_catalog_projection_manifest_sha256_v1()',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.portal_catalog_projection_manifest_sha256_v1()',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.assert_portal_catalog_projection_contract_v1()',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.assert_portal_catalog_projection_contract_v1()',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.assert_portal_catalog_projection_contract_v1()',
    'EXECUTE'
  ),
  'registry and manifest guards expose no external table or function ACL'
);

grant api_internal_executor to postgres;
set local role api_internal_executor;

select extensions.is(
  private.portal_catalog_projection_manifest_sha256_v1(),
  'b5e0aff9abbffcc8d2dacaf559a5d1a8c993c20b647d0c70f0e4fa18eb06d2dc',
  'the live canonical 11-function manifest equals the committed digest literal'
);

select extensions.lives_ok(
  $$select private.assert_portal_catalog_projection_contract_v1()$$,
  'the live projection derivation contract passes before Portal reads'
);

reset role;
revoke api_internal_executor from postgres;

select extensions.ok(
  (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 'i'
      and routine.proconfig @> array['search_path=""']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)'
    )
  )
  and pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_card_facts_v1(jsonb,jsonb,text)',
    'EXECUTE'
  ),
  'stored-card score/filter facts remain an immutable owner-to-internal-only helper'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    join pg_catalog.pg_namespace as namespace
      on namespace.oid = routine.pronamespace
    where namespace.nspname = 'private'
      and routine.proname in (
        'catalog_portal_process_pattern_versions_v1',
        'catalog_portal_flow_pattern_versions_v1'
      )
      and routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.proconfig @> array[
        'search_path=""',
        'statement_timeout=8s',
        'plan_cache_mode=force_custom_plan',
        'row_security=on'
      ]::text[]
      and pg_catalog.strpos(routine.prosrc, 'return query execute pg_catalog.format') > 0
      and pg_catalog.strpos(routine.prosrc, '%L') > 0
      and pg_catalog.strpos(routine.prosrc, '%I') = 0
      and coalesce(routine.proacl::text, '')
        = '{portal_public_executor=X/portal_public_executor}'
  ),
  2::bigint,
  'two owner-only fixed SQL templates render only a literal LIKE pattern and never an identifier'
);

select extensions.ok(
  (
    select routine.proowner = 'portal_public_executor'::regrole
      and not routine.prosecdef
      and routine.proconfig @> array['search_path=""']::text[]
      and routine.prosrc ~ 'catalog_portal_search_v1_impl'
    from pg_catalog.pg_proc as routine
    where routine.oid = 'private.portal_search_v1(text,text,jsonb,text,text,integer)'::regprocedure
  ),
  'the existing catalog coordinator keeps its owner/config and delegates only after validation'
);

select extensions.ok(
  (
    select routine.prosrc ~ 'catalog_portal_candidate_rows_v1\('
      and pg_catalog.strpos(routine.prosrc, 'portal_prefilter')
        < pg_catalog.strpos(routine.prosrc, 'portal_facts')
      and pg_catalog.strpos(routine.prosrc, 'portal_ordered')
        < pg_catalog.strpos(routine.prosrc, 'portal_hydrated')
      and routine.prosrc !~ 'hybrid_search_processes'
      and routine.prosrc !~ 'hybrid_search_flows'
      and routine.prosrc !~ 'semantic_process_candidates'
      and routine.prosrc !~ 'semantic_flow_candidates'
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
    )
  ),
  'catalog candidate reduction uses existing PGroonga sources before public-card projection and calls no legacy Hybrid helper'
);

select extensions.ok(
  (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.proconfig @> array[
        'search_path=""',
        'statement_timeout=8s',
        'plan_cache_mode=force_custom_plan',
        'row_security=on'
      ]::text[]
      and (
        (pg_catalog.length(routine.prosrc)
          - pg_catalog.length(pg_catalog.replace(
            routine.prosrc,
            'return query',
            ''
          ))) / pg_catalog.length('return query')
      ) = 6
      and routine.prosrc ~ 'catalog_portal_process_pattern_versions_v1'
      and routine.prosrc ~ 'catalog_portal_flow_pattern_versions_v1'
      and routine.prosrc !~* '[[:space:]]execute[[:space:]]'
      and (
        pg_catalog.length(routine.prosrc)
        - pg_catalog.length(pg_catalog.replace(
          routine.prosrc,
          'latest_keys as materialized',
          ''
        ))
      ) / pg_catalog.length('latest_keys as materialized') = 4
      and (
        pg_catalog.length(routine.prosrc)
        - pg_catalog.length(pg_catalog.replace(
          routine.prosrc,
          'eligible_keys as materialized',
          ''
        ))
      ) / pg_catalog.length('eligible_keys as materialized') = 4
      and routine.prosrc ~ 'join matched_versions as latest_match'
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)'
    )
  )
  and pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_candidate_rows_v1(text,text,uuid,text)',
    'EXECUTE'
  ),
  'candidate lookup has six static branches and no external or dynamic-SQL edge'
);

select extensions.ok(
  pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'id', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'version', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'json', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'state_code', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'modified_at', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'id', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'version', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'json', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'state_code', 'SELECT'
  )
  and pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'modified_at', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'search_text', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.processes', 'embedding_ft', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'search_text', 'SELECT'
  )
  and not pg_catalog.has_column_privilege(
    'portal_public_executor', 'public.flows', 'embedding_ft', 'SELECT'
  ),
  'candidate owner retains only the original five safe source columns per table'
);

select extensions.ok(
  (
    select routine.prosrc ~ 'portal_catalog_search_rows_v1'
      and routine.prosrc !~ 'public\.processes|public\.flows'
      and routine.prosrc ~ 'portal_projection_semantic_candidates_v1'
      and pg_catalog.strpos(routine.prosrc, 'portal_lexical_matches')
        < pg_catalog.strpos(routine.prosrc, 'portal_fused')
      and pg_catalog.strpos(routine.prosrc, 'portal_fused')
        < pg_catalog.strpos(routine.prosrc, 'portal_fused_decorated')
    from pg_catalog.pg_proc as routine
    where routine.oid = 'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'::regprocedure
  ),
  'Hybrid uses projection lexical/card rows and isolated source-HNSW semantic helpers before filtering'
);

select extensions.is(
  (
    with expected(routine_identity) as (
      values
        ('private.portal_search_v1(text,text,jsonb,text,text,integer)'::text),
        ('private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'),
        ('api.portal_facets_v1(text,text,jsonb)')
    )
    select count(*)
    from expected
    join pg_catalog.pg_proc as routine
      on routine.oid = pg_catalog.to_regprocedure(
        expected.routine_identity
      )
    where (
      pg_catalog.length(routine.prosrc)
      - pg_catalog.length(pg_catalog.replace(
        routine.prosrc,
        'assert_portal_catalog_projection_contract_v1',
        ''
      ))
    ) / pg_catalog.length(
      'assert_portal_catalog_projection_contract_v1'
    ) = 1
  ),
  3::bigint,
  'Search, Hybrid, and Facets each assert the derivation contract exactly once per request'
);

select extensions.ok(
  not exists (
    select 1
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[
        'private.portal_projection_semantic_process_v1(extensions.vector)'::regprocedure::oid,
        'private.portal_projection_semantic_flow_v1(extensions.vector)'::regprocedure::oid
      ])
      and not (
        routine.proowner = 'api_internal_executor'::regrole
        and routine.prosecdef
        and routine.proconfig @> array[
          'search_path=""',
          'statement_timeout=8s',
          'plan_cache_mode=force_custom_plan',
          'hnsw.iterative_scan=relaxed_order',
          'hnsw.ef_search=1000',
          'hnsw.max_scan_tuples=200000',
          'hnsw.scan_mem_multiplier=4',
          'enable_sort=off',
          'row_security=on'
        ]::text[]
        and routine.prosrc ~ 'portal_catalog_search_rows_v1'
        and routine.prosrc ~ 'embedding_ft'
      )
  )
  and (
    select routine.prosrc ~ 'portal_projection_semantic_process_v1'
      and routine.prosrc ~ 'portal_projection_semantic_flow_v1'
      and routine.prosrc ~ 'if p_kind = ''process'''
      and routine.prosrc ~ 'elsif p_kind = ''flow'''
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_projection_semantic_candidates_v1(text,extensions.vector)'::regprocedure
  )
  and not exists (
    select 1
    from pg_catalog.pg_attribute as attribute
    where attribute.attrelid =
      'private.portal_catalog_search_rows_v1'::regclass
      and attribute.attname = 'embedding_ft'
      and not attribute.attisdropped
  ),
  'semantic helpers reuse source HNSW under isolated planner settings without copying vectors'
);

select extensions.is(
  (
    with expected(
      routine_identity,
      definition_md5,
      owner_name,
      security_definer,
      proconfig_text,
      acl_text
    ) as (
      values
        (
          'api.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          'a36294ce86731379f01c88ced5c37ca4', 'api_internal_executor', true,
          '{"search_path=api, private, public, util, extensions, extensions, pg_temp",statement_timeout=60s}',
          '{api_internal_executor=X/api_internal_executor,anon=X/api_internal_executor,authenticated=X/api_internal_executor}'
        ),
        (
          'api.hybrid_search_flows(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          'd6a6eaa2291c53ce4c2301a98146f50b', 'api_internal_executor', true,
          '{"search_path=api, private, public, util, extensions, extensions, pg_temp",statement_timeout=60s}',
          '{api_internal_executor=X/api_internal_executor,anon=X/api_internal_executor,authenticated=X/api_internal_executor}'
        ),
        (
          'api.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          'b4a75c92187f9a88e9db131590c1efd1', 'api_internal_executor', true,
          '{"search_path=api, private, public, util, extensions, extensions, pg_temp",statement_timeout=60s}',
          '{api_internal_executor=X/api_internal_executor,anon=X/api_internal_executor,authenticated=X/api_internal_executor}'
        ),
        (
          'api.hybrid_search_processes(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          'a540312d04fda305d64c0c03e60ed6d3', 'api_internal_executor', true,
          '{"search_path=api, private, public, util, extensions, extensions, pg_temp",statement_timeout=60s}',
          '{api_internal_executor=X/api_internal_executor,anon=X/api_internal_executor,authenticated=X/api_internal_executor}'
        ),
        (
          'private.hybrid_search_flows_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          'ae135bcc2856c90162805cd3d777d5ea', 'postgres', false,
          '{statement_timeout=60s,"search_path=private, api, public, util, extensions, extensions, pg_temp"}',
          '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
        ),
        (
          'private.hybrid_search_processes_v2_impl(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])',
          '51c5e51a4ca22de1167d6b05e8c33b57', 'postgres', false,
          '{statement_timeout=60s,"search_path=private, api, public, util, extensions, extensions, pg_temp"}',
          '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
        ),
        (
          'private.semantic_flow_candidates(text,text,double precision,integer,text)',
          '0f2a7a47c8444e2c6a568c6c7506fc78', 'postgres', true,
          '{"search_path=private, api, public, util, extensions, extensions, pg_temp",statement_timeout=60s,plan_cache_mode=force_custom_plan,hnsw.iterative_scan=strict_order}',
          '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
        ),
        (
          'private.semantic_process_candidates(text,text,double precision,integer,text)',
          'a52a02f9c6daf89d5dc55ed3c580c634', 'postgres', true,
          '{"search_path=private, api, public, util, extensions, extensions, pg_temp",statement_timeout=60s,plan_cache_mode=force_custom_plan,hnsw.iterative_scan=strict_order}',
          '{postgres=X/postgres,service_role=X/postgres,api_internal_executor=X/postgres}'
        )
    ), actual as (
      select expected.routine_identity,
        pg_catalog.md5(pg_catalog.pg_get_functiondef(routine.oid)) as definition_md5,
        owner_role.rolname as owner_name,
        routine.prosecdef as security_definer,
        coalesce(routine.proconfig, '{}'::text[])::text as proconfig_text,
        coalesce(routine.proacl::text, '') as acl_text
      from expected
      join pg_catalog.pg_proc as routine
        on routine.oid = pg_catalog.to_regprocedure(expected.routine_identity)
      join pg_catalog.pg_roles as owner_role
        on owner_role.oid = routine.proowner
    )
    select count(*)
    from (
      (select * from actual except select * from expected)
      union all
      (select * from expected except select * from actual)
    ) as difference
  ),
  0::bigint,
  'all eight legacy raw Hybrid definitions, owners, configs, and ACLs remain byte-stable'
);

-- Prove that synchronized projection maintenance does not become a hidden
-- write-path permission requirement. Fixture admission uses the two actual relation
-- writers (service_role and the postgres-owned authenticated command), and all
-- effects roll back with this suite.
create temporary table portal_candidate_webhook_calls (
  edge_function text not null,
  body jsonb not null,
  timeout_milliseconds integer not null
) on commit drop;

create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
)
returns void
language plpgsql
security definer
set search_path = ''
as $function$
begin
  insert into pg_temp.portal_candidate_webhook_calls (
    edge_function,
    body,
    timeout_milliseconds
  ) values (name, body, timeout_milliseconds);
end
$function$;

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values (
  '00000000-0000-0000-0000-000000000000',
  '53100000-0000-4000-8000-000000000001',
  'authenticated',
  'authenticated',
  'portal-candidate-writer@example.test',
  'test-password-hash',
  pg_catalog.now(),
  '{"provider":"email","providers":["email"]}'::jsonb,
  '{"sub":"53100000-0000-4000-8000-000000000001"}'::jsonb,
  pg_catalog.now(),
  pg_catalog.now(),
  false,
  false
);

create or replace function pg_temp.portal_candidate_process_payload(
  p_name text
)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'processDataSet', pg_catalog.jsonb_build_object(
      'processInformation', pg_catalog.jsonb_build_object(
        'dataSetInformation', pg_catalog.jsonb_build_object(
          'name', pg_catalog.jsonb_build_object(
            'baseName', pg_catalog.jsonb_build_object(
              '@xml:lang', 'en',
              '#text', p_name
            )
          )
        )
      ),
      'administrativeInformation', pg_catalog.jsonb_build_object(
        'publicationAndOwnership', pg_catalog.jsonb_build_object(
          'common:dataSetVersion', '01.00.000',
          'common:licenseType', 'Free of charge for all users and uses'
        )
      )
    )
  )
$function$;

grant execute on function pg_temp.portal_candidate_process_payload(text)
  to service_role;

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;
alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;

set local role service_role;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
values
  (
    '53100000-0000-4000-8000-000000000101',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    0,
    true,
    '2026-08-26 06:30:01+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000103',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate review process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate review process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    20,
    true,
    '2026-08-26 06:30:03+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000102',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate public process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate public process"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:02+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000104',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"alpha"}},"common:generalComment":{"@xml:lang":"en","#text":"beta"}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"alpha"}},"common:generalComment":{"@xml:lang":"en","#text":"beta"}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:04+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000105',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"percent% underscore_ backslash\\ punctuation--- singlea 电力生产"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"percent% underscore_ backslash\\ punctuation--- singlea 电力生产"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:05+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000100',
    '01.00.000',
    pg_temp.portal_candidate_process_payload(
      'prefix candidate public process suffix 53100000-0000-4000-8000-000000000100'
    ),
    pg_temp.portal_candidate_process_payload(
      'prefix candidate public process suffix 53100000-0000-4000-8000-000000000100'
    )::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:06+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000106',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('filterfillneedle open one'),
    pg_temp.portal_candidate_process_payload('filterfillneedle open one')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:07+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000107',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('filterfillneedle open two'),
    pg_temp.portal_candidate_process_payload('filterfillneedle open two')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:08+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000108',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('filterfillneedle open three'),
    pg_temp.portal_candidate_process_payload('filterfillneedle open three')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:09+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000109',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('filterfillneedle metadata one'),
    pg_temp.portal_candidate_process_payload('filterfillneedle metadata one')::json,
    '53100000-0000-4000-8000-000000000001',
    200,
    true,
    '2026-08-26 06:30:10+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000110',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('filterfillneedle metadata two'),
    pg_temp.portal_candidate_process_payload('filterfillneedle metadata two')::json,
    '53100000-0000-4000-8000-000000000001',
    200,
    true,
    '2026-08-26 06:30:11+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000112',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('staleversionneedle process'),
    pg_temp.portal_candidate_process_payload('staleversionneedle process')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:12+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000112',
    '01.00.001',
    pg_temp.portal_candidate_process_payload('latest neutral process'),
    pg_temp.portal_candidate_process_payload('latest neutral process')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:30:13+00',
    null,
    null,
    null
  );

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft
)
values
  (
    '53100000-0000-4000-8000-000000000201',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    0,
    true,
    '2026-08-26 06:31:01+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000202',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate public flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate public flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:31:02+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000203',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate review flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate review flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    20,
    true,
    '2026-08-26 06:31:03+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000204',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"staleversionneedle flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"staleversionneedle flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:31:04+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000204',
    '01.00.001',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"latest neutral flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.001","common:licenseType":"Free of charge for all users and uses"}}}}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"latest neutral flow"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.001","common:licenseType":"Free of charge for all users and uses"}}}}'::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 06:31:05+00',
    null,
    null
  );

reset role;
alter table public.processes enable trigger user;
alter table public.flows enable trigger user;

set local role service_role;

update public.processes
set search_text = array['candidate public process derivative'],
    embedding_ft = (
      '[' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1024]),
        ','
      ) || ']'
    )::extensions.vector(1024),
    embedding_ft_at = '2026-08-26 06:32:00+00'
where id = '53100000-0000-4000-8000-000000000102'
  and version = '01.00.000';

update public.flows
set search_text = array['candidate public flow derivative'],
    embedding_ft = (
      '[' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1024]),
        ','
      ) || ']'
    )::extensions.vector(1024),
    embedding_ft_at = '2026-08-26 06:32:00+00'
where id = '53100000-0000-4000-8000-000000000202'
  and version = '01.00.000';

reset role;

select extensions.ok(
  (
    select modified_at = '2026-08-26 06:30:02+00'::timestamptz
      and json #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}'
        = 'candidate public process'
      and search_text = array['candidate public process derivative']
      and embedding_ft is not null
    from public.processes
    where id = '53100000-0000-4000-8000-000000000102'
      and version = '01.00.000'
  ),
  'service-role Process derivative update retains authored JSON/modified_at and updates only source derivatives'
);

select extensions.ok(
  (
    select modified_at = '2026-08-26 06:31:02+00'::timestamptz
      and json #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,#text}'
        = 'candidate public flow'
      and search_text = array['candidate public flow derivative']
      and embedding_ft is not null
    from public.flows
    where id = '53100000-0000-4000-8000-000000000202'
      and version = '01.00.000'
  ),
  'service-role Flow derivative update retains authored JSON/modified_at and updates only source derivatives'
);

set local enable_sort = on;
set local hnsw.iterative_scan = off;
set local hnsw.ef_search = 41;
set local hnsw.max_scan_tuples = 12345;
set local hnsw.scan_mem_multiplier = 2;

grant api_internal_executor to postgres;
set local role api_internal_executor;

select extensions.lives_ok(
  $$select count(*)
    from private.portal_projection_semantic_candidates_v1(
      'process',
      ('[1,' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1023]),
        ','
      ) || ']')::extensions.vector(1024)
    )$$,
  'the semantic kind dispatcher executes the requested Process helper'
);

select extensions.throws_ok(
  $$select *
    from private.portal_projection_semantic_process_v1(
      null::extensions.vector(1024)
    )$$,
  '22023',
  'invalid portal semantic query',
  'the semantic helper fail-closed path is explicit'
);

reset role;
revoke api_internal_executor from postgres;

select extensions.ok(
  pg_catalog.current_setting('enable_sort') = 'on'
  and pg_catalog.current_setting('hnsw.iterative_scan') = 'off'
  and pg_catalog.current_setting('hnsw.ef_search') = '41'
  and pg_catalog.current_setting('hnsw.max_scan_tuples') = '12345'
  and pg_catalog.current_setting('hnsw.scan_mem_multiplier') = '2',
  'normal and error semantic calls restore every caller planner/HNSW GUC'
);

set local role authenticated;
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '53100000-0000-4000-8000-000000000001',
  true
);
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);

select extensions.is(
  api.cmd_dataset_save_draft(
    'processes',
    '53100000-0000-4000-8000-000000000101',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft process updated"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    null,
    true,
    '{"test":"portal-candidate-index"}'::jsonb
  ) ->> 'ok',
  'true',
  'authenticated Process draft command remains writable with projection triggers'
);

select extensions.is(
  api.cmd_dataset_save_draft(
    'flows',
    '53100000-0000-4000-8000-000000000201',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":{"@xml:lang":"en","#text":"candidate draft flow updated"}}}},"administrativeInformation":{"publicationAndOwnership":{"common:dataSetVersion":"01.00.000"}}}}'::jsonb,
    null,
    true,
    '{"test":"portal-candidate-index"}'::jsonb
  ) ->> 'ok',
  'true',
  'authenticated Flow draft command remains writable with projection triggers'
);

reset role;

select extensions.ok(
  (select json_ordered::jsonb #>> '{processDataSet,processInformation,dataSetInformation,name,baseName,#text}'
     from public.processes
    where id = '53100000-0000-4000-8000-000000000101'
      and version = '01.00.000') = 'candidate draft process updated'
  and
  (select json_ordered::jsonb #>> '{flowDataSet,flowInformation,dataSetInformation,name,baseName,#text}'
     from public.flows
    where id = '53100000-0000-4000-8000-000000000201'
      and version = '01.00.000') = 'candidate draft flow updated',
  'both authenticated command updates persist their exact draft payloads inside the rollback-only proof'
);

set local role anon;

select extensions.is(
  (
    with queries(label, query_text, expected_id) as (
      values
        ('cross_field', 'alpha beta', '53100000-0000-4000-8000-000000000104'::uuid),
        ('percent', '%', '53100000-0000-4000-8000-000000000105'::uuid),
        ('underscore', '_', '53100000-0000-4000-8000-000000000105'::uuid),
        ('backslash', pg_catalog.chr(92), '53100000-0000-4000-8000-000000000105'::uuid),
        ('punctuation', '---', '53100000-0000-4000-8000-000000000105'::uuid),
        ('single_character', 'a', '53100000-0000-4000-8000-000000000105'::uuid),
        ('cjk', '电力生产', '53100000-0000-4000-8000-000000000105'::uuid)
    )
    select count(*)
    from queries
    cross join lateral (
      select api.portal_search_processes_v1(
        queries.query_text,
        '{}'::jsonb,
        'relevance',
        null,
        20
      ) as payload
    ) as response
    where exists (
      select 1
      from pg_catalog.jsonb_array_elements(
        response.payload -> 'items'
      ) as item(value)
      where item.value #>> '{key,id}' = queries.expected_id::text
    )
  ),
  7::bigint,
  'literal LIKE candidates preserve cross-field, wildcard, slash, punctuation, single-character, and CJK matches'
);

select extensions.ok(
  pg_catalog.jsonb_array_length(
    api.portal_search_processes_v1(
      'staleversionneedle', '{}'::jsonb, 'relevance', null, 20
    ) -> 'items'
  ) = 0
  and pg_catalog.jsonb_array_length(
    api.portal_search_flows_v1(
      'staleversionneedle', '{}'::jsonb, 'relevance', null, 20
    ) -> 'items'
  ) = 0
  and pg_catalog.jsonb_array_length(
    api.portal_facets_v1(
      'process', 'staleversionneedle', '{}'::jsonb
    ) -> 'groups'
  ) = 0
  and pg_catalog.jsonb_array_length(
    api.portal_facets_v1(
      'flow', 'staleversionneedle', '{}'::jsonb
    ) -> 'groups'
  ) = 0,
  'older visible lexical hits are excluded when the latest visible version does not match'
);

select extensions.ok(
  api.portal_search_processes_v1(
    'candidate public process',
    '{}'::jsonb,
    'relevance',
    null,
    20
  ) #>> '{items,0,key,id}' = '53100000-0000-4000-8000-000000000102'
  and api.portal_search_processes_v1(
    'candidate public process',
    '{}'::jsonb,
    'relevance',
    null,
    20
  ) #>> '{items,0,match,score}' = '0.95'
  and api.portal_search_processes_v1(
    'candidate public process',
    '{}'::jsonb,
    'relevance',
    null,
    20
  ) #> '{items,0,match,reasonCodes}' = '["name"]'::jsonb,
  'exact name score outranks an earlier-ID generic document match'
);

select extensions.ok(
  (
    with response as (
      select api.portal_search_processes_v1(
        '53100000-0000-4000-8000-000000000100',
        '{}'::jsonb,
        'relevance',
        null,
        20
      ) as payload
    )
    select pg_catalog.jsonb_array_length(response.payload -> 'items') = 1
      and response.payload #>> '{items,0,key,id}'
        = '53100000-0000-4000-8000-000000000100'
    from response
  ),
  'UUID and lexical matches for the same latest row are de-duplicated'
);

select extensions.ok(
  (
    with response as (
      select api.portal_search_processes_v1(
        'filterfillneedle',
        '{"accessLevel":"metadata_only"}'::jsonb,
        'relevance',
        null,
        2
      ) as payload
    )
    select pg_catalog.jsonb_array_length(response.payload -> 'items') = 2
      and not exists (
        select 1
        from pg_catalog.jsonb_array_elements(response.payload -> 'items') as item(value)
        where item.value ->> 'accessLevel' <> 'metadata_only'
      )
    from response
  ),
  'filters run before limit and fill the page after earlier open candidates are removed'
);

select extensions.ok(
  (
    with first_page as (
      select api.portal_search_processes_v1(
        'filterfillneedle',
        '{"accessLevel":"metadata_only"}'::jsonb,
        'relevance',
        null,
        1
      ) as payload
    ), second_page as (
      select api.portal_search_processes_v1(
        'filterfillneedle',
        '{"accessLevel":"metadata_only"}'::jsonb,
        'relevance',
        first_page.payload ->> 'nextCursor',
        1
      ) as payload
      from first_page
    )
    select first_page.payload ->> 'nextCursor' is not null
      and first_page.payload #>> '{items,0,key,id}'
        <> second_page.payload #>> '{items,0,key,id}'
      and pg_catalog.jsonb_array_length(second_page.payload -> 'items') = 1
    from first_page
    cross join second_page
  ),
  'filtered relevance cursor continues without overlap or an underfilled second page'
);

reset role;

grant portal_public_executor to postgres;
set local role portal_public_executor;

select extensions.is(
  (
    select count(*)
    from private.catalog_portal_candidate_rows_v1(
      'process', '', null, null
    )
  ),
  10::bigint,
  'candidate helper and Portal RLS exclude state-0/state-20 Process rows'
);

select extensions.is(
  (
    select count(*)
    from private.catalog_portal_candidate_rows_v1(
      'flow', '', null, null
    )
  ),
  2::bigint,
  'candidate helper and Portal RLS exclude state-0/state-20 Flow rows'
);

select extensions.is(
  (
    select projection.document
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.id = '53100000-0000-4000-8000-000000000102'
      and projection.version = '01.00.000'
  ),
  (
    select private.portal_catalog_card_v1(
      'process', process.state_code, process.json
    ) ->> 'document'
    from public.processes as process
    where process.id = '53100000-0000-4000-8000-000000000102'
      and process.version = '01.00.000'
  ),
  'the synchronized Process document is byte-equivalent to the reviewed public-card document'
);

select extensions.is(
  (
    select projection.document
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.id = '53100000-0000-4000-8000-000000000202'
      and projection.version = '01.00.000'
  ),
  (
    select private.portal_catalog_card_v1(
      'flow', flow.state_code, flow.json
    ) ->> 'document'
    from public.flows as flow
    where flow.id = '53100000-0000-4000-8000-000000000202'
      and flow.version = '01.00.000'
  ),
  'the synchronized Flow document is byte-equivalent to the reviewed public-card document'
);

select extensions.ok(
  (
    select facts.value ->> 'accessLevel' is not distinct from card.value ->> 'accessLevel'
      and facts.value ->> 'nameKey' is not distinct from card.value #>> '{names,0,value}'
      and (facts.value ->> 'nameExact')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'names') as name(item)
        where pg_catalog.lower(pg_catalog.btrim(name.item ->> 'value'))
          = 'candidate public'
      )
      and (facts.value ->> 'nameContains')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'names') as name(item)
        where pg_catalog.strpos(
          pg_catalog.lower(name.item ->> 'value'),
          'candidate public'
        ) > 0
      )
      and (facts.value ->> 'classificationExact')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'classifications') as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
          = 'candidate public'
      )
      and (facts.value ->> 'classificationFilterMatch')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'classifications') as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
          = 'portal'
      )
      and facts.value ->> 'geographyCode' is not distinct from card.value #>> '{geography,code}'
      and facts.value -> 'referenceYear' is not distinct from card.value -> 'referenceYear'
      and facts.value -> 'processSubtype' is not distinct from card.value -> 'processSubtype'
      and facts.value -> 'source' is not distinct from card.value -> 'source'
      and facts.value -> 'casNumber' is not distinct from card.value -> 'casNumber'
    from private.portal_catalog_search_rows_v1 as projection
    cross join lateral (
      select private.catalog_portal_card_facts_v1(
        projection.card,
        '{"accessLevel":"open","geography":"cn","classification":"portal","referenceYearFrom":0,"referenceYearTo":9999,"processSubtype":"unit","source":"provider"}'::jsonb,
        'candidate public'
      ) as value
    ) as facts
    cross join lateral (
      select projection.card as value
    ) as card
    where projection.dataset_kind = 'process'
      and projection.id = '53100000-0000-4000-8000-000000000102'
      and projection.version = '01.00.000'
  ),
  'Process pre-limit score/filter facts are exact projections of the reviewed card'
);

select extensions.ok(
  (
    select facts.value ->> 'accessLevel' is not distinct from card.value ->> 'accessLevel'
      and facts.value ->> 'nameKey' is not distinct from card.value #>> '{names,0,value}'
      and (facts.value ->> 'nameExact')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'names') as name(item)
        where pg_catalog.lower(pg_catalog.btrim(name.item ->> 'value'))
          = 'candidate public'
      )
      and (facts.value ->> 'nameContains')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'names') as name(item)
        where pg_catalog.strpos(
          pg_catalog.lower(name.item ->> 'value'),
          'candidate public'
        ) > 0
      )
      and (facts.value ->> 'classificationExact')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'classifications') as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
          = 'candidate public'
      )
      and (facts.value ->> 'classificationFilterMatch')::boolean = exists (
        select 1
        from pg_catalog.jsonb_array_elements(card.value -> 'classifications') as classification(item)
        where pg_catalog.lower(pg_catalog.btrim(classification.item ->> 'code'))
          = 'portal'
      )
      and facts.value ->> 'geographyCode' is not distinct from card.value #>> '{geography,code}'
      and facts.value -> 'referenceYear' is not distinct from card.value -> 'referenceYear'
      and facts.value -> 'processSubtype' is not distinct from card.value -> 'processSubtype'
      and facts.value -> 'source' is not distinct from card.value -> 'source'
      and facts.value -> 'casNumber' is not distinct from card.value -> 'casNumber'
    from private.portal_catalog_search_rows_v1 as projection
    cross join lateral (
      select private.catalog_portal_card_facts_v1(
        projection.card,
        '{"accessLevel":"open","geography":"cn","classification":"portal","referenceYearFrom":0,"referenceYearTo":9999,"source":"provider"}'::jsonb,
        'candidate public'
      ) as value
    ) as facts
    cross join lateral (
      select projection.card as value
    ) as card
    where projection.dataset_kind = 'flow'
      and projection.id = '53100000-0000-4000-8000-000000000202'
      and projection.version = '01.00.000'
  ),
  'Flow pre-limit score/filter facts are exact projections of the reviewed card'
);

reset role;
revoke portal_public_executor from postgres;

-- Exercise the bounded semantic exits. A two-row Flow set proves that a
-- complete (<200) source-HNSW universe returns exact latest-visible parity
-- without scanning the whole projection. A separate 205-row Process set fills
-- the ANN branch and preserves the raw ANN order byte-for-byte.
create or replace function pg_temp.portal_candidate_vector(p_ordinal integer)
returns extensions.vector(1024)
language sql
immutable
set search_path = ''
as $function$
  select (
    '[1,' || (p_ordinal::numeric / 10000::numeric)::text || ',' ||
    pg_catalog.array_to_string(
      pg_catalog.array_fill('0'::text, array[1022]),
      ','
    ) || ']'
  )::extensions.vector(1024)
$function$;

grant execute on function pg_temp.portal_candidate_vector(integer)
  to service_role, api_internal_executor;

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;
alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;

set local role service_role;

update public.flows
set embedding_ft = pg_temp.portal_candidate_vector(1),
    embedding_ft_at = '2026-08-26 07:00:01+00'
where id = '53100000-0000-4000-8000-000000000202'
  and version = '01.00.000';

update public.flows
set embedding_ft = pg_temp.portal_candidate_vector(2),
    embedding_ft_at = '2026-08-26 07:00:02+00'
where id = '53100000-0000-4000-8000-000000000204'
  and version = '01.00.001';

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
select (
    '53200000-0000-4000-8000-' ||
    pg_catalog.lpad(series.ordinal::text, 12, '0')
  )::uuid,
  '01.00.000',
  pg_temp.portal_candidate_process_payload(
    'healthy semantic process ' || series.ordinal::text
  ),
  pg_temp.portal_candidate_process_payload(
    'healthy semantic process ' || series.ordinal::text
  )::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  100,
  true,
  '2026-08-26 07:01:00+00'::timestamptz
    + series.ordinal * interval '1 millisecond',
  null,
  pg_temp.portal_candidate_vector(series.ordinal),
  null
from pg_catalog.generate_series(1, 205) as series(ordinal);

reset role;

alter table public.processes enable trigger user;
alter table public.flows enable trigger user;

create temporary table portal_semantic_underfill_actual (
  row_position bigint primary key,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null
) on commit drop;

create temporary table portal_semantic_underfill_exact (
  row_position bigint primary key,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null
) on commit drop;

create temporary table portal_semantic_healthy_actual (
  row_position bigint primary key,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null
) on commit drop;

create temporary table portal_semantic_healthy_raw_ann (
  row_position bigint primary key,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null
) on commit drop;

grant insert, select on
  pg_temp.portal_semantic_underfill_actual,
  pg_temp.portal_semantic_underfill_exact,
  pg_temp.portal_semantic_healthy_actual,
  pg_temp.portal_semantic_healthy_raw_ann
to api_internal_executor;

grant api_internal_executor to postgres;
set local role api_internal_executor;

insert into pg_temp.portal_semantic_underfill_actual
select pg_catalog.row_number() over (
    order by candidate.semantic_distance,
      candidate.id,
      candidate.version desc
  ) as row_position,
  candidate.id,
  candidate.version,
  candidate.semantic_distance
from private.portal_projection_semantic_flow_v1(
  pg_temp.portal_candidate_vector(0)
) as candidate;

insert into pg_temp.portal_semantic_underfill_exact
with eligible as materialized (
  select flow.id,
    flow.version::text as version,
    flow.embedding_ft operator(extensions.<=>)
      pg_temp.portal_candidate_vector(0) as semantic_distance
  from public.flows as flow
  where flow.state_code in (100, 200)
    and flow.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = flow.id
        and projection.version = flow.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
), ordered as (
  select eligible.*
  from eligible
  where eligible.semantic_distance is not null
    and eligible.semantic_distance >= 0::double precision
    and eligible.semantic_distance <= 0.5::double precision
  order by eligible.semantic_distance,
    eligible.id,
    eligible.version desc
  limit 200
)
select pg_catalog.row_number() over (
    order by ordered.semantic_distance,
      ordered.id,
      ordered.version desc
  ) as row_position,
  ordered.id,
  ordered.version,
  ordered.semantic_distance
from ordered;

insert into pg_temp.portal_semantic_healthy_actual
select pg_catalog.row_number() over (
    order by candidate.semantic_distance,
      candidate.id,
      candidate.version desc
  ) as row_position,
  candidate.id,
  candidate.version,
  candidate.semantic_distance
from private.portal_projection_semantic_process_v1(
  pg_temp.portal_candidate_vector(0)
) as candidate;

set local enable_sort = off;
set local hnsw.iterative_scan = relaxed_order;
set local hnsw.ef_search = 1000;
set local hnsw.max_scan_tuples = 200000;
set local hnsw.scan_mem_multiplier = 4;

insert into pg_temp.portal_semantic_healthy_raw_ann
with approximate as materialized (
  select process.id,
    process.version::text as version,
    process.embedding_ft operator(extensions.<=>)
      pg_temp.portal_candidate_vector(0) as semantic_distance
  from public.processes as process
  where process.state_code in (100, 200)
    and process.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = process.id
        and projection.version = process.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
  order by process.embedding_ft operator(extensions.<=>)
    pg_temp.portal_candidate_vector(0)
  limit 5000
), raw_bounded as (
  select approximate.*
  from approximate
  where approximate.semantic_distance is not null
    and approximate.semantic_distance >= 0::double precision
  order by approximate.semantic_distance + 0::double precision,
    approximate.id,
    approximate.version desc
  limit 200
), thresholded as (
  select raw_bounded.*
  from raw_bounded
  where raw_bounded.semantic_distance <= 0.5::double precision
  order by raw_bounded.semantic_distance,
    raw_bounded.id,
    raw_bounded.version desc
)
select pg_catalog.row_number() over (
    order by thresholded.semantic_distance,
      thresholded.id,
      thresholded.version desc
  ) as row_position,
  thresholded.id,
  thresholded.version,
  thresholded.semantic_distance
from thresholded;

reset role;
revoke api_internal_executor from postgres;

select extensions.ok(
  (select count(*) from pg_temp.portal_semantic_underfill_actual)
    between 1 and 199,
  'the sparse Flow fixture exercises the complete-source underfill shortcut'
);

select extensions.is(
  (
    select count(*)
    from (
      (
        select id, version, semantic_distance
        from pg_temp.portal_semantic_underfill_actual
        except
        select id, version, semantic_distance
        from pg_temp.portal_semantic_underfill_exact
      )
      union all
      (
        select id, version, semantic_distance
        from pg_temp.portal_semantic_underfill_exact
        except
        select id, version, semantic_distance
        from pg_temp.portal_semantic_underfill_actual
      )
    ) as difference
  ),
  0::bigint,
  'complete-source underfill is a two-way exact latest-visible candidate match'
);

select extensions.is(
  (
    select count(*)
    from pg_temp.portal_semantic_underfill_actual as actual
    full join pg_temp.portal_semantic_underfill_exact as expected
      using (row_position)
    where actual.id is distinct from expected.id
      or actual.version is distinct from expected.version
      or actual.semantic_distance is distinct from expected.semantic_distance
  ),
  0::bigint,
  'complete-source underfill preserves exact distance/id/version order positionally'
);

select extensions.is(
  (select count(*) from pg_temp.portal_semantic_healthy_actual),
  200::bigint,
  'the healthy Process fixture fills the bounded ANN branch'
);

select extensions.is(
  (
    select count(*)
    from pg_temp.portal_semantic_healthy_actual as actual
    full join pg_temp.portal_semantic_healthy_raw_ann as expected
      using (row_position)
    where actual.id is distinct from expected.id
      or actual.version is distinct from expected.version
      or actual.semantic_distance is distinct from expected.semantic_distance
  ),
  0::bigint,
  'healthy semantic output preserves raw bounded ANN rows positionally'
);

select extensions.is(
  (
    select count(*)
    from pg_catalog.pg_proc as routine
    where routine.oid = any (array[
        'private.portal_projection_semantic_process_v1(extensions.vector)'::regprocedure::oid,
        'private.portal_projection_semantic_flow_v1(extensions.vector)'::regprocedure::oid
      ])
      and routine.prosrc ~ 'cardinality\(v_ids\), 0\) >= 200'
      and routine.prosrc ~ 'generate_subscripts\(v_ids, 1\)'
      and pg_catalog.strpos(
        routine.prosrc,
        'from pg_catalog.generate_subscripts(v_ids, 1)'
      ) < pg_catalog.strpos(
        routine.prosrc,
        'select distinct on (projection.id)'
      )
      and pg_catalog.strpos(
        routine.prosrc,
        E'    return;\n  end if;'
      ) < pg_catalog.strpos(
        routine.prosrc,
        'select distinct on (projection.id)'
      )
  ),
  2::bigint,
  'both semantic helpers structurally return the healthy ANN arrays before exact fallback'
);

-- Force the source>=200/latest-eligible<200 branch for both kinds. Process
-- keeps only 50 projected synthetic rows; Flow adds 205 public source vectors
-- with no valid projection payload. The streaming exact path must still equal
-- the original source-driven oracle positionally.
grant api_internal_executor to postgres;
set local role api_internal_executor;

delete from private.portal_catalog_search_rows_v1
where dataset_kind = 'process'
  and id >= '53200000-0000-4000-8000-000000000051'::uuid
  and id <= '53200000-0000-4000-8000-000000000205'::uuid;

reset role;
revoke api_internal_executor from postgres;

alter table public.flows disable trigger user;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;
set local role service_role;

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft
)
select (
    '53300000-0000-4000-8000-' ||
    pg_catalog.lpad(series.ordinal::text, 12, '0')
  )::uuid,
  '01.00.000',
  '{}'::jsonb,
  '{}'::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  100,
  true,
  '2026-08-26 07:05:00+00'::timestamptz
    + series.ordinal * interval '1 millisecond',
  null,
  pg_temp.portal_candidate_vector(series.ordinal)
from pg_catalog.generate_series(1, 205) as series(ordinal);

reset role;
alter table public.flows enable trigger user;

select extensions.ok(
  (
    select count(*) = 200
    from (
      select 1
      from public.processes
      where state_code in (100, 200)
        and embedding_ft is not null
      limit 200
    ) as bounded_source
  )
  and (
    select count(*) = 200
    from (
      select 1
      from public.flows
      where state_code in (100, 200)
        and embedding_ft is not null
      limit 200
    ) as bounded_source
  )
  and (
    select count(*) between 1 and 199
    from (
      select distinct on (projection.dataset_kind, projection.id)
        projection.dataset_kind,
        projection.id,
        projection.version
      from private.portal_catalog_search_rows_v1 as projection
      join public.processes as process
        on projection.dataset_kind = 'process'
       and process.id = projection.id
       and process.version = projection.version::character(9)
       and process.embedding_ft is not null
      order by projection.dataset_kind,
        projection.id,
        projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
    ) as latest
  )
  and (
    select count(*) between 1 and 199
    from (
      select distinct on (projection.dataset_kind, projection.id)
        projection.dataset_kind,
        projection.id,
        projection.version
      from private.portal_catalog_search_rows_v1 as projection
      join public.flows as flow
        on projection.dataset_kind = 'flow'
       and flow.id = projection.id
       and flow.version = projection.version::character(9)
       and flow.embedding_ft is not null
      order by projection.dataset_kind,
        projection.id,
        projection.version desc,
        projection.modified_at desc,
        projection.state_code desc
    ) as latest
  ),
  'both streaming fixtures have at least 200 source vectors but fewer than 200 latest projected vectors'
);

create temporary table portal_semantic_streaming_actual (
  dataset_kind text not null,
  row_position bigint not null,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null,
  primary key (dataset_kind, row_position)
) on commit drop;

create temporary table portal_semantic_streaming_exact (
  dataset_kind text not null,
  row_position bigint not null,
  id uuid not null,
  version text not null,
  semantic_distance double precision not null,
  primary key (dataset_kind, row_position)
) on commit drop;

grant insert, select on pg_temp.portal_semantic_streaming_actual,
  pg_temp.portal_semantic_streaming_exact
to api_internal_executor;

grant api_internal_executor to postgres;
set local role api_internal_executor;

insert into pg_temp.portal_semantic_streaming_actual
select 'process',
  pg_catalog.row_number() over (
    order by candidate.semantic_distance, candidate.id, candidate.version desc
  ),
  candidate.id,
  candidate.version,
  candidate.semantic_distance
from private.portal_projection_semantic_process_v1(
  pg_temp.portal_candidate_vector(0)
) as candidate
union all
select 'flow',
  pg_catalog.row_number() over (
    order by candidate.semantic_distance, candidate.id, candidate.version desc
  ),
  candidate.id,
  candidate.version,
  candidate.semantic_distance
from private.portal_projection_semantic_flow_v1(
  pg_temp.portal_candidate_vector(0)
) as candidate;

insert into pg_temp.portal_semantic_streaming_exact
with source_rows as (
  select 'process'::text as dataset_kind,
    process.id,
    process.version::text as version,
    process.embedding_ft operator(extensions.<=>)
      pg_temp.portal_candidate_vector(0) as semantic_distance
  from public.processes as process
  where process.state_code in (100, 200)
    and process.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = process.id
        and projection.version = process.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
  union all
  select 'flow',
    flow.id,
    flow.version::text,
    flow.embedding_ft operator(extensions.<=>)
      pg_temp.portal_candidate_vector(0)
  from public.flows as flow
  where flow.state_code in (100, 200)
    and flow.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = flow.id
        and projection.version = flow.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
), bounded as (
  select source_rows.*,
    pg_catalog.row_number() over (
      partition by source_rows.dataset_kind
      order by source_rows.semantic_distance,
        source_rows.id,
        source_rows.version desc
    ) as row_position
  from source_rows
  where source_rows.semantic_distance is not null
    and source_rows.semantic_distance >= 0::double precision
    and source_rows.semantic_distance <= 0.5::double precision
)
select bounded.dataset_kind,
  bounded.row_position,
  bounded.id,
  bounded.version,
  bounded.semantic_distance
from bounded
where bounded.row_position <= 200;

reset role;
revoke api_internal_executor from postgres;

select extensions.is(
  (
    select count(*)
    from (
      (
        select dataset_kind, id, version, semantic_distance
        from pg_temp.portal_semantic_streaming_actual
        except
        select dataset_kind, id, version, semantic_distance
        from pg_temp.portal_semantic_streaming_exact
      )
      union all
      (
        select dataset_kind, id, version, semantic_distance
        from pg_temp.portal_semantic_streaming_exact
        except
        select dataset_kind, id, version, semantic_distance
        from pg_temp.portal_semantic_streaming_actual
      )
    ) as difference
  ),
  0::bigint,
  'Process and Flow streaming fallback candidates equal the source exact oracle bidirectionally'
);

select extensions.is(
  (
    select count(*)
    from pg_temp.portal_semantic_streaming_actual as actual
    full join pg_temp.portal_semantic_streaming_exact as expected
      using (dataset_kind, row_position)
    where actual.id is distinct from expected.id
      or actual.version is distinct from expected.version
      or actual.semantic_distance is distinct from expected.semantic_distance
  ),
  0::bigint,
  'Process and Flow streaming fallback preserve exact positional rank order'
);

-- The empty/unfiltered name_asc fast path must preserve the general kernel's
-- validated name key, unnamed fallback, tuple order, and cursor semantics.
create or replace function pg_temp.portal_candidate_flow_payload(p_name text)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'flowDataSet', pg_catalog.jsonb_build_object(
      'flowInformation', pg_catalog.jsonb_build_object(
        'dataSetInformation', pg_catalog.jsonb_build_object(
          'name', pg_catalog.jsonb_build_object(
            'baseName', pg_catalog.jsonb_build_object(
              '@xml:lang', 'en',
              '#text', p_name
            )
          )
        )
      ),
      'administrativeInformation', pg_catalog.jsonb_build_object(
        'publicationAndOwnership', pg_catalog.jsonb_build_object(
          'common:dataSetVersion', '01.00.000',
          'common:licenseType', 'Free of charge for all users and uses'
        )
      )
    )
  )
$function$;

grant execute on function pg_temp.portal_candidate_flow_payload(text)
  to service_role;

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;
alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;

set local role service_role;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
values
  (
    '53100000-0000-4000-8000-000000000310',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('!fast alpha'),
    pg_temp.portal_candidate_process_payload('!fast alpha')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:10:00+00',
    null,
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000311',
    '01.00.000',
    pg_temp.portal_candidate_process_payload('!fast beta'),
    pg_temp.portal_candidate_process_payload('!fast beta')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:10:01+00',
    null,
    null,
    null
  );

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft
)
values
  (
    '53100000-0000-4000-8000-000000000300',
    '01.00.000',
    pg_temp.portal_candidate_flow_payload(''),
    pg_temp.portal_candidate_flow_payload('')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:11:00+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000301',
    '01.00.000',
    pg_temp.portal_candidate_flow_payload(pg_catalog.repeat('x', 501)),
    pg_temp.portal_candidate_flow_payload(pg_catalog.repeat('x', 501))::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:11:01+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000302',
    '01.00.000',
    pg_temp.portal_candidate_flow_payload(E'bad\nname'),
    pg_temp.portal_candidate_flow_payload(E'bad\nname')::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:11:02+00',
    null,
    null
  ),
  (
    '53100000-0000-4000-8000-000000000303',
    '01.00.000',
    pg_temp.portal_candidate_flow_payload(null),
    pg_temp.portal_candidate_flow_payload(null)::json,
    '53100000-0000-4000-8000-000000000001',
    100,
    true,
    '2026-08-26 07:11:03+00',
    null,
    null
  );

reset role;

alter table public.processes enable trigger user;
alter table public.flows enable trigger user;

set local role anon;

select extensions.ok(
  (
    with first_page as (
      select api.portal_search_processes_v1(
        '', '{}'::jsonb, 'name_asc', null, 1
      ) as payload
    ), second_page as (
      select api.portal_search_processes_v1(
        '',
        '{}'::jsonb,
        'name_asc',
        first_page.payload ->> 'nextCursor',
        1
      ) as payload
      from first_page
    )
    select first_page.payload #>> '{items,0,key,id}'
        = '53100000-0000-4000-8000-000000000310'
      and second_page.payload #>> '{items,0,key,id}'
        = '53100000-0000-4000-8000-000000000311'
      and first_page.payload #>> '{items,0,key,id}'
        <> second_page.payload #>> '{items,0,key,id}'
      and first_page.payload ->> 'nextCursor' is not null
    from first_page
    cross join second_page
  ),
  'empty name-ascending fast-path pages preserve exact tuple order without overlap'
);

select extensions.is(
  (
    with response as (
      select api.portal_search_flows_v1(
        '', '{}'::jsonb, 'name_asc', null, 50
      ) as payload
    )
    select pg_catalog.array_agg(
      item.value #>> '{key,id}' order by item.ordinality
    ) filter (
      where item.value #>> '{key,id}' like
        '53100000-0000-4000-8000-0000000003%'
    )
    from response
    cross join lateral pg_catalog.jsonb_array_elements(
      response.payload -> 'items'
    ) with ordinality as item(value, ordinality)
  ),
  array[
    '53100000-0000-4000-8000-000000000300',
    '53100000-0000-4000-8000-000000000301',
    '53100000-0000-4000-8000-000000000302',
    '53100000-0000-4000-8000-000000000303'
  ]::text[],
  'empty, overlong, control, and missing names use stable unnamed-id ordering'
);

reset role;

select extensions.ok(
  exists (
    select 1
    from private.portal_catalog_search_rows_v1
  )
  and not exists (
    select 1
    from private.portal_catalog_search_rows_v1
    where projection_contract_version <> 1
  ),
  'triggered fixtures persist only explicit projection derivation version 1'
);

grant portal_public_executor to postgres;
grant create on schema private to portal_public_executor;
set local role portal_public_executor;

create or replace function private.portal_scalar_text_v1(p_value jsonb)
returns text
language sql
immutable
parallel safe
set search_path = ''
as $function$
  select case
    when pg_catalog.jsonb_typeof(p_value) = 'string'
      then nullif(
        pg_catalog.btrim(p_value #>> '{}'),
        '__portal_projection_manifest_drift__'
      )
    else null
  end
$function$;

reset role;
revoke create on schema private from portal_public_executor;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
set local role api_internal_executor;

select extensions.throws_ok(
  $$select private.assert_portal_catalog_projection_contract_v1()$$,
  '55000',
  'Portal projection derivation contract drifted',
  'a rollback-only leaf projector drift fails the private live manifest guard'
);

reset role;
revoke api_internal_executor from postgres;

set local role anon;

select extensions.throws_ok(
  $$select api.portal_search_processes_v1(
    '', '{}'::jsonb, 'relevance', null, 20
  )$$,
  'P0001',
  'portal catalog unavailable',
  'Search fails closed through its stable public error contract on projector drift'
);

select extensions.throws_ok(
  $$select api.portal_hybrid_search_v1(
    'process',
    array['candidate'],
    ('[1,' || pg_catalog.array_to_string(
      pg_catalog.array_fill('0'::text, array[1023]),
      ','
    ) || ']'),
    '{}'::jsonb,
    20
  )$$,
  'P0001',
  'portal hybrid unavailable',
  'Hybrid fails closed through its stable public error contract on projector drift'
);

select extensions.throws_ok(
  $$select api.portal_facets_v1('process', '', '{}'::jsonb)$$,
  'P0001',
  'portal catalog unavailable',
  'Facets fail closed through its stable public error contract on projector drift'
);

reset role;

select * from extensions.finish();

rollback;
