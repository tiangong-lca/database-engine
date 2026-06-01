begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(17);

alter table public.flows disable trigger user;
alter table public.processes disable trigger user;
alter table public.lifecyclemodels disable trigger user;
alter table public.sources disable trigger user;
alter table public.contacts disable trigger user;
alter table public.unitgroups disable trigger user;
alter table public.flowproperties disable trigger user;

select ok(
  strpos(pg_get_functiondef('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), 'private.search_dataset_json_uuid_mentions_impl') > 0,
  'public JSON UUID mention wrapper delegates to private helper'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_dataset_json_uuid_mentions_impl(uuid,text[],text,text,uuid,integer,integer)'::regprocedure),
  'private JSON UUID mention helper is security definer'
);

select ok(
  not (select prosecdef from pg_proc where oid = 'public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'::regprocedure),
  'public JSON UUID mention wrapper remains security invoker'
);

select ok(
  strpos(pg_get_function_result('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), 'total_count') = 0,
  'JSON UUID mention RPC does not expose total_count'
);

select ok(
  strpos(pg_get_functiondef('private.search_dataset_json_uuid_mentions_impl(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), 'least(greatest(coalesce(p_limit, 20), 1), 50)') > 0,
  'JSON UUID mention RPC hard-caps limit at 50'
);

select ok(
  strpos(pg_get_functiondef('private.search_dataset_json_uuid_mentions_impl(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), 'statement_timeout') > 0
    and strpos(pg_get_functiondef('private.search_dataset_json_uuid_mentions_impl(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), '20s') > 0
    and strpos(pg_get_functiondef('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), 'statement_timeout') > 0
    and strpos(pg_get_functiondef('public.search_dataset_json_uuid_mentions(uuid,text[],text,text,uuid,integer,integer)'::regprocedure), '20s') > 0,
  'JSON UUID mention RPC has bounded 20s statement timeout'
);

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
values
  (
    '00000000-0000-0000-0000-000000000000',
    'a1380000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'json-uuid-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a1380000-0000-0000-0000-000000000001","email":"json-uuid-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b1380000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'json-uuid-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b1380000-0000-0000-0000-000000000001","email":"json-uuid-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1380000-0000-0000-0000-000000000001', '{"email":"json-uuid-owner@example.com"}'::jsonb, null),
  ('b1380000-0000-0000-0000-000000000001', '{"email":"json-uuid-outsider@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values
  ('c1380000-0000-0000-0000-000000000001', '{"name":"JSON UUID Mention Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values
  ('a1380000-0000-0000-0000-000000000001', 'c1380000-0000-0000-0000-000000000001', 'owner');

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  (
    'f1380000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Old UUID flow"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Old UUID flow"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
    'a1380000-0000-0000-0000-000000000001',
    100,
    null,
    'old uuid flow',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    'f1380000-0000-0000-0000-000000000001',
    '01.01.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Latest UUID flow"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Latest UUID flow"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
    'a1380000-0000-0000-0000-000000000001',
    100,
    null,
    'latest uuid flow',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  );

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  (
    'e1380000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Public UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000001"}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Public UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000001"}}'::json,
    'a1380000-0000-0000-0000-000000000001',
    100,
    null,
    'public uuid process',
    true,
    now(),
    now()
  ),
  (
    'e1380000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Owner UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000002"}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Owner UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000002"}}'::json,
    'a1380000-0000-0000-0000-000000000001',
    0,
    null,
    'owner uuid process',
    true,
    now(),
    now()
  ),
  (
    'e1380000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Outsider UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000002"}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Outsider UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000002"}}'::json,
    'b1380000-0000-0000-0000-000000000001',
    0,
    null,
    'outsider uuid process',
    true,
    now(),
    now()
  ),
  (
    'e1380000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Team UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000003"}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"Team UUID process"}]}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"nested":{"uuid":"d1380000-0000-0000-0000-000000000003"}}'::json,
    'b1380000-0000-0000-0000-000000000001',
    0,
    'c1380000-0000-0000-0000-000000000001',
    'team uuid process',
    true,
    now(),
    now()
  );

insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values (
  'd1380000-0000-0000-0000-000000000010',
  '01.00.000',
  '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"UUID model"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
  '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"#text":"UUID model"}]}}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
  'a1380000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid model',
  true,
  now(),
  now()
);

insert into public.sources (id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at)
values (
  '51380000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"#text":"UUID source"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
  '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"#text":"UUID source"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
  'a1380000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid source',
  true,
  now(),
  now()
);

insert into public.contacts (id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at)
values (
  'c1380000-0000-0000-0000-000000000010',
  '01.00.000',
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"#text":"UUID contact"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
  '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"#text":"UUID contact"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
  'a1380000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid contact',
  true,
  now(),
  now()
);

insert into public.unitgroups (id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at)
values (
  '71380000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"#text":"UUID unitgroup"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
  '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"#text":"UUID unitgroup"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
  'a1380000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid unitgroup',
  true,
  now(),
  now()
);

insert into public.flowproperties (id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at)
values (
  '81380000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"#text":"UUID flowproperty"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::jsonb,
  '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"#text":"UUID flowproperty"}]}}},"contains":"d1380000-0000-0000-0000-000000000001"}'::json,
  'a1380000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid flowproperty',
  true,
  now(),
  now()
);

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1380000-0000-0000-0000-000000000001', true);

select is(
  (
    select source_version::text
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['flow'],
      'tg',
      '',
      null,
      null,
      20
    )
    where source_id = 'f1380000-0000-0000-0000-000000000001'
  ),
  '01.01.000',
  'flow JSON UUID mention search returns latest visible row'
);

select is(
  (
    select source_name
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['flow'],
      'tg',
      '',
      null,
      null,
      20
    )
    where source_id = 'f1380000-0000-0000-0000-000000000001'
  ),
  'Latest UUID flow',
  'flow JSON UUID mention search exposes display name'
);

select set_eq(
  $$
    select source_entity_kind
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['process'],
      'tg',
      '',
      null,
      null,
      20
    )
  $$,
  $$ values ('process'::text) $$,
  'source entity kind filter restricts JSON UUID deep search'
);

select is(
  (
    select count(*)
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['flow','process','lifecyclemodel','source','contact','unitgroup','flowproperty'],
      'tg',
      '',
      null,
      null,
      2
    )
  ),
  2::bigint,
  'JSON UUID deep search respects caller limit'
);

select is(
  (
    select count(*)
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['flow','process','lifecyclemodel','source','contact','unitgroup','flowproperty'],
      'tg',
      '',
      null,
      null,
      20
    )
  ),
  7::bigint,
  'JSON UUID deep search covers all supported entity branches'
);

select is(
  (
    select matched_by
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['source'],
      'tg',
      '',
      null,
      null,
      20
    )
    limit 1
  ),
  'json_uuid_scan',
  'JSON UUID deep search marks match source'
);

select is(
  (
    select source_id::text
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000002',
      array['process'],
      'my',
      'b1380000-0000-0000-0000-000000000001',
      null,
      null,
      20
    )
  ),
  'e1380000-0000-0000-0000-000000000002',
  'my JSON UUID deep search uses auth.uid instead of spoofable this_user_id'
);

select is(
  (
    select count(*)
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000002',
      array['process'],
      'my',
      'b1380000-0000-0000-0000-000000000001',
      null,
      null,
      20
    )
    where source_id = 'e1380000-0000-0000-0000-000000000003'
  ),
  0::bigint,
  'my JSON UUID deep search does not return outsider data'
);

select is(
  (
    select source_id::text
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000003',
      array['process'],
      'te',
      '',
      'c1380000-0000-0000-0000-000000000001',
      null,
      20
    )
  ),
  'e1380000-0000-0000-0000-000000000004',
  'team JSON UUID deep search returns team data for a member'
);

select set_config('request.jwt.claim.sub', 'b1380000-0000-0000-0000-000000000001', true);

select is(
  (
    select count(*)
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000003',
      array['process'],
      'te',
      '',
      'c1380000-0000-0000-0000-000000000001',
      null,
      20
    )
  ),
  0::bigint,
  'team JSON UUID deep search rejects a non-member'
);

select is(
  (
    select count(*)
    from public.search_dataset_json_uuid_mentions(
      'd1380000-0000-0000-0000-000000000001',
      array['unsupported-kind'],
      'tg',
      '',
      null,
      null,
      20
    )
  ),
  0::bigint,
  'unsupported entity kind returns no JSON UUID deep search rows'
);

select * from finish();

rollback;
