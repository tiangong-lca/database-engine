begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(18);

select is(
  (
    select count(*)::integer
    from pg_class index_relation
    join pg_namespace namespace on namespace.oid = index_relation.relnamespace
    join pg_index index_catalog on index_catalog.indexrelid = index_relation.oid
    join pg_am access_method on access_method.oid = index_relation.relam
    where namespace.nspname = 'public'
      and index_relation.relname in (
        'contacts_extracted_md_pgroonga',
        'flowproperties_extracted_md_pgroonga',
        'flows_extracted_md_pgroonga',
        'lifecyclemodels_extracted_md_pgroonga',
        'processes_extracted_md_pgroonga',
        'sources_extracted_md_pgroonga',
        'unitgroups_extracted_md_pgroonga'
      )
      and access_method.amname = 'pgroonga'
      and index_catalog.indisvalid
      and index_catalog.indisready
      and index_catalog.indislive
  ),
  7,
  'all seven extracted_md PGroonga indexes are valid, ready, and live'
);

select is(
  (
    select count(*)::integer
    from pg_class index_relation
    join pg_namespace namespace on namespace.oid = index_relation.relnamespace
    join pg_index index_catalog on index_catalog.indexrelid = index_relation.oid
    join pg_attribute attribute
      on attribute.attrelid = index_catalog.indrelid
     and attribute.attnum = index_catalog.indkey[0]
    where namespace.nspname = 'public'
      and index_relation.relname in (
        'contacts_extracted_md_pgroonga',
        'flowproperties_extracted_md_pgroonga',
        'flows_extracted_md_pgroonga',
        'lifecyclemodels_extracted_md_pgroonga',
        'processes_extracted_md_pgroonga',
        'sources_extracted_md_pgroonga',
        'unitgroups_extracted_md_pgroonga'
      )
      and attribute.attname = 'extracted_md'
  ),
  7,
  'all seven new lexical indexes target extracted_md'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)')
    ) expected(signature)
    where to_regprocedure(expected.signature) is not null
  ),
  7,
  'all seven Hybrid Search v2 RPC signatures exist'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)')
    ) expected(signature)
    join pg_proc routine on routine.oid = to_regprocedure(expected.signature)
    where 'lexical_weight' = any(routine.proargnames)
      and not ('full_text_weight' = any(routine.proargnames))
      and not ('extracted_text_weight' = any(routine.proargnames))
  ),
  7,
  'v2 exposes one lexical_weight and no legacy text-weight arguments'
);

select is(
  (
    select count(*)::integer
    from (values
      ('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_contacts(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_flowproperties(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_sources(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_unitgroups(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)')
    ) expected(signature)
    where to_regprocedure(expected.signature) is not null
  ),
  7,
  'the seven legacy RPC signatures remain available during Expand'
);

select is(
  (
    select count(*)::integer
    from (values
      ('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
      ('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'),
      ('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])')
    ) expected(signature)
    join pg_proc routine on routine.oid = to_regprocedure(expected.signature)
    where strpos(routine.prosrc, '.extracted_md &@~|') > 0
  ),
  3,
  'all three core latest-search implementations read extracted_md'
);

select is(
  (
    select count(*)::integer
    from (values
      ('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'),
      ('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'),
      ('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])')
    ) expected(signature)
    join pg_proc routine on routine.oid = to_regprocedure(expected.signature)
    where strpos(routine.prosrc, '.extracted_text &@~') = 0
  ),
  3,
  'core latest-search implementations no longer read extracted_text'
);

select ok(
  strpos(
    pg_get_functiondef(
      'public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure
    ),
    'd.extracted_md &@~'
  ) > 0,
  'foundation latest search reads extracted_md'
);

select is(
  strpos(
    pg_get_functiondef(
      'public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure
    ),
    'd.extracted_text &@~'
  ),
  0,
  'foundation latest search no longer reads extracted_text'
);

select ok(
  strpos(
    pg_get_functiondef(
      'private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
    ),
    'd.extracted_md &@~|'
  ) > 0,
  'foundation Hybrid Search reads extracted_md'
);

select is(
  strpos(
    pg_get_functiondef(
      'private.hybrid_search_simple_dataset(regclass,text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'::regprocedure
    ),
    'd.extracted_text &@~|'
  ),
  0,
  'foundation Hybrid Search no longer reads extracted_text'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.proname like 'hybrid_search_%_v2'
      and strpos(routine.prosrc, '0.0::double precision') > 0
  ),
  7,
  'v2 adapters pass no independent extracted-text weight'
);

select is(
  (
    select count(*)::integer
    from (values
      ('anon'),
      ('authenticated'),
      ('service_role')
    ) expected(role_name)
    cross join (values
      ('public.hybrid_search_flows_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_processes_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_lifecyclemodels_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[])'),
      ('public.hybrid_search_contacts_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_flowproperties_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_sources_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)'),
      ('public.hybrid_search_unitgroups_v2(text,text,text,double precision,integer,double precision,double precision,integer,text,integer,integer,text[],integer,uuid)')
    ) function_signature(signature)
    where has_function_privilege(
      expected.role_name,
      function_signature.signature,
      'EXECUTE'
    )
  ),
  21,
  'all API roles can execute all seven v2 RPCs'
);

select is(
  (
    select count(*)::integer
    from pg_proc routine
    join pg_namespace namespace on namespace.oid = routine.pronamespace
    join pg_description description on description.objoid = routine.oid
    where namespace.nspname = 'public'
      and routine.proname like 'hybrid_search_%_v2'
      and description.description like 'Hybrid Search v2: extracted_md lexical candidates%'
  ),
  7,
  'all v2 RPCs document extracted_md as their lexical source'
);

select ok(
  to_regprocedure(
    'public.search_processes_latest_v2(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'
  ) is not null,
  'indexed process latest-search v2 exists'
);

select ok(
  strpos(
    pg_get_functiondef(
      'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure
    ),
    'p.state_code = 0 and p.team_id is null and p.review_id is null'
  ) > 0
  and strpos(
    pg_get_functiondef(
      'private.search_processes_latest_v2_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[],boolean)'::regprocedure
    ),
    'p2.state_code = 0 and p2.team_id is null and p2.review_id is null'
  ) > 0,
  'strict owner-draft search constrains both candidate and latest rows'
);

insert into public.users (id, raw_user_meta_data, contact)
values (
  'a7290000-0000-0000-0000-000000000001',
  '{"email":"lexical-v2-owner@example.com"}'::jsonb,
  null
);

alter table public.processes disable trigger user;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  extracted_md,
  rule_verification,
  created_at,
  modified_at
)
values
  (
    'b7290000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"search":"strict-owner-draft-token"}'::jsonb,
    '{"search":"strict-owner-draft-token"}'::json,
    'a7290000-0000-0000-0000-000000000001',
    0,
    null,
    'strict-owner-draft-token',
    true,
    now(),
    now()
  ),
  (
    'b7290000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"search":"strict-owner-draft-token"}'::jsonb,
    '{"search":"strict-owner-draft-token"}'::json,
    'a7290000-0000-0000-0000-000000000001',
    20,
    null,
    'strict-owner-draft-token',
    true,
    now(),
    now()
  );

select is(
  (
    select array_agg(id::text order by id)
    from public.search_processes_latest_v2(
      'strict-owner-draft-token',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'my',
      'a7290000-0000-0000-0000-000000000001',
      null,
      null,
      'all',
      array['strict-owner-draft-token'],
      true
    )
  ),
  array['b7290000-0000-0000-0000-000000000001']::text[],
  'strict owner-draft v2 returns only an unsubmitted personal draft'
);

select is(
  (
    select count(*)::integer
    from public.search_processes_latest_v2(
      'strict-owner-draft-token',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      'a7290000-0000-0000-0000-000000000001',
      null,
      null,
      'all',
      array['strict-owner-draft-token'],
      true
    )
  ),
  0,
  'strict owner-draft mode fails closed outside my-data scope'
);

select * from finish();

rollback;
