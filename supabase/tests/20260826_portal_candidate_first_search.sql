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
  pg_catalog.has_function_privilege(
    'postgres',
    'private.catalog_portal_document_v1(text,jsonb)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.catalog_portal_document_v1(text,jsonb)',
    'EXECUTE'
  )
  and pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_document_v1(text,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_document_v1(text,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_document_v1(text,jsonb)',
    'EXECUTE'
  ),
  'only database writer roles can evaluate the Portal index expression'
);

select extensions.ok(
  (
    select routine.proowner = 'portal_public_executor'::regrole
      and routine.prosecdef
      and routine.provolatile = 'i'
      and routine.proconfig @> array['search_path=""']::text[]
    from pg_catalog.pg_proc as routine
    where routine.oid = pg_catalog.to_regprocedure(
      'private.catalog_portal_facts_v1(text,integer,jsonb)'
    )
  )
  and pg_catalog.has_function_privilege(
    'api_internal_executor',
    'private.catalog_portal_facts_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'anon',
    'private.catalog_portal_facts_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'authenticated',
    'private.catalog_portal_facts_v1(text,integer,jsonb)',
    'EXECUTE'
  )
  and not pg_catalog.has_function_privilege(
    'service_role',
    'private.catalog_portal_facts_v1(text,integer,jsonb)',
    'EXECUTE'
  ),
  'exact pre-limit facts remain an immutable owner-to-internal-only helper'
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
    select routine.prosrc ~ 'catalog_portal_document_v1\('
      and routine.prosrc ~ 'operator\(extensions\.\&@\)'
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
    select routine.prosrc ~ 'catalog_portal_document_v1\('
      and routine.prosrc ~ 'operator\(extensions\.\&@\|\)'
      and pg_catalog.strpos(routine.prosrc, 'portal_lexical_prefilter')
        < pg_catalog.strpos(routine.prosrc, 'portal_lexical_documents')
      and pg_catalog.strpos(routine.prosrc, 'portal_fused')
        < pg_catalog.strpos(routine.prosrc, 'portal_fused_decorated')
    from pg_catalog.pg_proc as routine
    where routine.oid = 'private.portal_public_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'::regprocedure
  ),
  'Hybrid reduces lexical and semantic candidates before final card hydration'
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

-- Prove that the expression-index helper does not become a hidden write-path
-- permission requirement.  Fixture admission uses the two actual relation
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

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;

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
  'service-role Process derivative update retains authored JSON and modified_at while maintaining both indexes'
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
  'service-role Flow derivative update retains authored JSON and modified_at while maintaining both indexes'
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
  'authenticated Process draft command remains writable after the expression index'
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
  'authenticated Flow draft command remains writable after the expression index'
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

grant portal_public_executor to postgres;
set local role portal_public_executor;

select extensions.is(
  (
    select private.catalog_portal_document_v1('process', process.json)
    from public.processes as process
    where process.id = '53100000-0000-4000-8000-000000000102'
      and process.version = '01.00.000'
  ),
  (
    select private.portal_catalog_card_v1(
      'process', process.state_code, process.json
    ) ->> 'document'
    from public.processes as process
    where process.id = '53100000-0000-4000-8000-000000000102'
      and process.version = '01.00.000'
  ),
  'the indexed Process document is byte-equivalent to the reviewed public-card document'
);

select extensions.is(
  (
    select private.catalog_portal_document_v1('flow', flow.json)
    from public.flows as flow
    where flow.id = '53100000-0000-4000-8000-000000000202'
      and flow.version = '01.00.000'
  ),
  (
    select private.portal_catalog_card_v1(
      'flow', flow.state_code, flow.json
    ) ->> 'document'
    from public.flows as flow
    where flow.id = '53100000-0000-4000-8000-000000000202'
      and flow.version = '01.00.000'
  ),
  'the indexed Flow document is byte-equivalent to the reviewed public-card document'
);

select extensions.ok(
  (
    select facts.value ->> 'accessLevel' is not distinct from card.value ->> 'accessLevel'
      and facts.value -> 'names' is not distinct from card.value -> 'names'
      and facts.value -> 'classifications' is not distinct from card.value -> 'classifications'
      and facts.value ->> 'geographyCode' is not distinct from card.value #>> '{geography,code}'
      and facts.value -> 'referenceYear' is not distinct from card.value -> 'referenceYear'
      and facts.value -> 'processSubtype' is not distinct from card.value -> 'processSubtype'
      and facts.value -> 'source' is not distinct from card.value -> 'source'
      and facts.value -> 'casNumber' is not distinct from card.value -> 'casNumber'
      and facts.value ->> 'document' is not distinct from card.value ->> 'document'
    from public.processes as process
    cross join lateral (
      select private.catalog_portal_facts_v1(
        'process', process.state_code, process.json
      ) as value
    ) as facts
    cross join lateral (
      select private.portal_catalog_card_v1(
        'process', process.state_code, process.json
      ) as value
    ) as card
    where process.id = '53100000-0000-4000-8000-000000000102'
      and process.version = '01.00.000'
  ),
  'Process pre-limit score/filter facts are exact projections of the reviewed card'
);

select extensions.ok(
  (
    select facts.value ->> 'accessLevel' is not distinct from card.value ->> 'accessLevel'
      and facts.value -> 'names' is not distinct from card.value -> 'names'
      and facts.value -> 'classifications' is not distinct from card.value -> 'classifications'
      and facts.value ->> 'geographyCode' is not distinct from card.value #>> '{geography,code}'
      and facts.value -> 'referenceYear' is not distinct from card.value -> 'referenceYear'
      and facts.value -> 'processSubtype' is not distinct from card.value -> 'processSubtype'
      and facts.value -> 'source' is not distinct from card.value -> 'source'
      and facts.value -> 'casNumber' is not distinct from card.value -> 'casNumber'
      and facts.value ->> 'document' is not distinct from card.value ->> 'document'
    from public.flows as flow
    cross join lateral (
      select private.catalog_portal_facts_v1(
        'flow', flow.state_code, flow.json
      ) as value
    ) as facts
    cross join lateral (
      select private.portal_catalog_card_v1(
        'flow', flow.state_code, flow.json
      ) as value
    ) as card
    where flow.id = '53100000-0000-4000-8000-000000000202'
      and flow.version = '01.00.000'
  ),
  'Flow pre-limit score/filter facts are exact projections of the reviewed card'
);

reset role;
revoke portal_public_executor from postgres;

select * from extensions.finish();

rollback;
