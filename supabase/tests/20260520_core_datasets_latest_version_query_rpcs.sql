begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

select set_config('request.jwt.claim.role', 'authenticated', true);

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
    '16000000-0000-0000-0000-000000000047',
    'authenticated',
    'authenticated',
    'latest-core-datasets-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"16000000-0000-0000-0000-000000000047","email":"latest-core-datasets-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public)
values
  ('26000000-0000-0000-0000-000000000048', '{"name":"Latest Core Dataset Team"}'::jsonb, 1, false);

create temporary table latest_zero_embedding (value text) on commit drop;
insert into latest_zero_embedding (value)
select '[' || string_agg('0', ',') || ']'
from generate_series(1, 1024);

alter table public.flows disable trigger "flows_json_sync_trigger";
alter table public.processes disable trigger "processes_json_sync_trigger";
alter table public.lifecyclemodels disable trigger "lifecyclemodels_json_sync_trigger";

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  created_at,
  modified_at
)
values
  (
    '47000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-old","#text":"Old class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"legacy-flow-token"}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-old","#text":"Old class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"legacy-flow-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '47000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-new","#text":"New class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"latest-flow-token"}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-new","#text":"New class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"latest-flow-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '47000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-single","#text":"Single class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"single-flow-token"}'::jsonb,
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single flow"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"c-single","#text":"Single class"}]}}},"geography":{"locationOfSupply":"GLO"}},"modellingAndValidation":{"LCIMethod":{"typeOfDataSet":"Product flow"}}},"search":"single-flow-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  );

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  created_at,
  modified_at
)
values
  (
    '48000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-old","#text":"Old class"}]}}},"time":{"common:referenceYear":"2020"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"legacy-process-token"}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-old","#text":"Old class"}]}}},"time":{"common:referenceYear":"2020"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"legacy-process-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '48000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-new","#text":"New class"}]}}},"time":{"common:referenceYear":"2021"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"latest-process-token"}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-new","#text":"New class"}]}}},"time":{"common:referenceYear":"2021"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"latest-process-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '48000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-single","#text":"Single class"}]}}},"time":{"common:referenceYear":"2022"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"single-process-token"}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single process"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"p-single","#text":"Single class"}]}}},"time":{"common:referenceYear":"2022"},"geography":{"locationOfOperationSupplyOrProduction":{"@location":"GLO"}}},"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"single-process-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  );

insert into public.lifecyclemodels (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  rule_verification,
  created_at,
  modified_at
)
values
  (
    '49000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-old","#text":"Old class"}]}}}}},"search":"legacy-lifecycle-token"}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Legacy lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-old","#text":"Old class"}]}}}}},"search":"legacy-lifecycle-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '49000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-new","#text":"New class"}]}}}}},"search":"latest-lifecycle-token"}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Latest lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-new","#text":"New class"}]}}}}},"search":"latest-lifecycle-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '49000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-single","#text":"Single class"}]}}}}},"search":"single-lifecycle-token"}'::jsonb,
    '{"lifeCycleModelDataSet":{"lifeCycleModelInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Single lifecycle model"}]},"classificationInformation":{"common:classification":{"common:class":[{"@classId":"l-single","#text":"Single class"}]}}}}},"search":"single-lifecycle-token"}'::json,
    '16000000-0000-0000-0000-000000000047',
    100,
    '26000000-0000-0000-0000-000000000048',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '16000000-0000-0000-0000-000000000047', true);

select is(
  (select version::text from public.get_latest_flow_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '47000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'flow list returns the highest visible version for a UUID'
);

select is(
  (select version_count from public.get_latest_flow_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '47000000-0000-0000-0000-000000000001'),
  2::bigint,
  'flow list reports version_count for the visible UUID group'
);

select is(
  (select max(total_count) from public.get_latest_flow_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047')),
  2::bigint,
  'flow list total_count counts unique UUIDs'
);

select is(
  (select version::text from public.pgroonga_search_flows_latest('legacy-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '47000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'flow PGroonga search can match an older version while returning the highest visible version'
);

select is(
  (select version::text from public.hybrid_search_flows('legacy-flow-token', (select value from latest_zero_embedding), '{}', 0.1, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) where id = '47000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'flow hybrid search returns the highest visible version for a matching UUID'
);

select is(
  (select version::text from public.get_latest_process_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '48000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'process list returns the highest visible version for a UUID'
);

select is(
  (select version_count from public.get_latest_process_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '48000000-0000-0000-0000-000000000001'),
  2::bigint,
  'process list reports version_count for the visible UUID group'
);

select is(
  (select max(total_count) from public.get_latest_process_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047')),
  2::bigint,
  'process list total_count counts unique UUIDs'
);

select is(
  (select version::text from public.pgroonga_search_processes_latest('legacy-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '48000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'process PGroonga search can match an older version while returning the highest visible version'
);

select is(
  (select version::text from public.hybrid_search_processes('legacy-process-token', (select value from latest_zero_embedding), '{}', 0.1, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) where id = '48000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'process hybrid search returns the highest visible version for a matching UUID'
);

select is(
  (select version::text from public.get_latest_lifecyclemodel_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '49000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'lifecyclemodel list returns the highest visible version for a UUID'
);

select is(
  (select version_count from public.get_latest_lifecyclemodel_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '49000000-0000-0000-0000-000000000001'),
  2::bigint,
  'lifecyclemodel list reports version_count for the visible UUID group'
);

select is(
  (select max(total_count) from public.get_latest_lifecyclemodel_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000047')),
  2::bigint,
  'lifecyclemodel list total_count counts unique UUIDs'
);

select is(
  (select version::text from public.pgroonga_search_lifecyclemodels_latest('legacy-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '16000000-0000-0000-0000-000000000047') where id = '49000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'lifecyclemodel PGroonga search can match an older version while returning the highest visible version'
);

select is(
  (select version::text from public.hybrid_search_lifecyclemodels('legacy-lifecycle-token', (select value from latest_zero_embedding), '{}', 0.1, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) where id = '49000000-0000-0000-0000-000000000001'),
  '01.00.002',
  'lifecyclemodel hybrid search returns the highest visible version for a matching UUID'
);

select * from finish();

rollback;
