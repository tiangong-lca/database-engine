begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(21);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'exact_query_id') > 0,
  'flow latest search has an exact UUID fast path'
);

select ok(
  strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'exact_query_id') > 0,
  'process latest search has an exact UUID fast path'
);

select ok(
  strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'exact_query_id') > 0,
  'lifecyclemodel latest search has an exact UUID fast path'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'exact_query_id') > 0,
  'generic simple latest search has an exact UUID fast path'
);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'f.extracted_text &@~ $1') > 0,
  'flow non-UUID search still uses extracted_text PGroonga'
);

select ok(
  util.dataset_json_search_text(
    'flows',
    '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"common:UUID":"f1320000-0000-0000-0000-000000000001","name":{"baseName":[{"@xml:lang":"en","#text":"UUID Fast Path Flow"}]}}}}}'::jsonb
  ) !~* 'f1320000-0000-0000-0000-000000000001',
  'extracted_text generation still filters UUID metadata'
);

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
    'a1320000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'uuid-fast-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a1320000-0000-0000-0000-000000000001","email":"uuid-fast-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b1320000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'uuid-fast-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b1320000-0000-0000-0000-000000000001","email":"uuid-fast-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1320000-0000-0000-0000-000000000001', '{"email":"uuid-fast-owner@example.com"}'::jsonb, null),
  ('b1320000-0000-0000-0000-000000000001', '{"email":"uuid-fast-outsider@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values
  ('c1320000-0000-0000-0000-000000000001', '{"name":"UUID Fast Path Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values
  ('a1320000-0000-0000-0000-000000000001', 'c1320000-0000-0000-0000-000000000001', 'owner');

select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flows_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'flow_dataset_extraction_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.flows'::regclass, 'zz_flows_extracted_text_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'processes_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'zz_processes_extracted_text_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.lifecyclemodels'::regclass, 'lifecyclemodels_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.lifecyclemodels'::regclass, 'lifecyclemodel_extract_md_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.lifecyclemodels'::regclass, 'lifecyclemodels_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.lifecyclemodels'::regclass, 'zz_lifecyclemodels_extracted_text_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.contacts'::regclass, 'contacts_json_sync_trigger');
select pg_temp.disable_trigger_if_exists('public.contacts'::regclass, 'zz_contacts_extracted_text_sync_trigger');

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('f1320000-0000-0000-0000-000000000001', '01.00.000', '{"kind":"legacy","search":"uuid-fast-flow-token"}'::jsonb, '{"kind":"legacy","search":"uuid-fast-flow-token"}'::json, 'a1320000-0000-0000-0000-000000000001', 100, null, 'uuid-fast-flow-token legacy', true, now() - interval '2 days', now() - interval '2 days'),
  ('f1320000-0000-0000-0000-000000000001', '01.00.002', '{"kind":"latest","search":"uuid-fast-flow-token"}'::jsonb, '{"kind":"latest","search":"uuid-fast-flow-token"}'::json, 'a1320000-0000-0000-0000-000000000001', 100, null, 'uuid-fast-flow-token latest', true, now() - interval '1 day', now() - interval '1 day'),
  ('f1320000-0000-0000-0000-000000000002', '01.00.000', '{"kind":"owner","search":"uuid-fast-owner-flow-token"}'::jsonb, '{"kind":"owner","search":"uuid-fast-owner-flow-token"}'::json, 'a1320000-0000-0000-0000-000000000001', 0, null, 'uuid-fast-owner-flow-token', true, now(), now()),
  ('f1320000-0000-0000-0000-000000000003', '01.00.000', '{"kind":"outsider","search":"uuid-fast-outsider-flow-token"}'::jsonb, '{"kind":"outsider","search":"uuid-fast-outsider-flow-token"}'::json, 'b1320000-0000-0000-0000-000000000001', 0, null, 'uuid-fast-outsider-flow-token', true, now(), now()),
  ('f1320000-0000-0000-0000-000000000004', '01.00.000', '{"kind":"team","search":"uuid-fast-team-flow-token"}'::jsonb, '{"kind":"team","search":"uuid-fast-team-flow-token"}'::json, 'b1320000-0000-0000-0000-000000000001', 0, 'c1320000-0000-0000-0000-000000000001', 'uuid-fast-team-flow-token', true, now(), now());

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values (
  'e1320000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"uuid-fast-process-token"}'::jsonb,
  '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"uuid-fast-process-token"}'::json,
  'a1320000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid-fast-process-token',
  true,
  now(),
  now()
);

insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values (
  'd1320000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"search":"uuid-fast-lifecycle-token"}'::jsonb,
  '{"search":"uuid-fast-lifecycle-token"}'::json,
  'a1320000-0000-0000-0000-000000000001',
  100,
  null,
  'uuid-fast-lifecycle-token',
  true,
  now(),
  now()
);

insert into public.contacts (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('c1320000-0000-0000-0000-000000000101', '01.00.000', '{"search":"uuid-fast-contact-token","contactDataSet":{"contactInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy UUID contact"}]}}}}'::jsonb, '{"search":"uuid-fast-contact-token","contactDataSet":{"contactInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy UUID contact"}]}}}}'::json, 'a1320000-0000-0000-0000-000000000001', 100, null, 'uuid-fast-contact-token legacy', true, now() - interval '2 days', now() - interval '2 days'),
  ('c1320000-0000-0000-0000-000000000101', '01.00.002', '{"search":"uuid-fast-contact-token","contactDataSet":{"contactInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest UUID contact"}]}}}}'::jsonb, '{"search":"uuid-fast-contact-token","contactDataSet":{"contactInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest UUID contact"}]}}}}'::json, 'a1320000-0000-0000-0000-000000000001', 100, null, 'uuid-fast-contact-token latest', true, now() - interval '1 day', now() - interval '1 day');

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1320000-0000-0000-0000-000000000001', true);

select is(
  (select version::text from public.search_flows_latest('  F1320000-0000-0000-0000-000000000001  ', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') where id = 'f1320000-0000-0000-0000-000000000001'),
  '01.00.002',
  'flow UUID exact search trims input, accepts uppercase, and returns the latest visible version'
);

select is(
  (select (rank::text || ':' || total_count::text) from public.search_flows_latest('f1320000-0000-0000-0000-000000000001', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') limit 1),
  '1:1',
  'flow UUID exact search returns bounded rank and total_count'
);

select is(
  (select count(*) from public.search_flows_latest('f1320000-0000-0000-0000', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '')),
  0::bigint,
  'flow UUID fragments do not use exact id search and are not in extracted_text'
);

select is(
  (select id::text from public.search_flows_latest('uuid-fast-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') limit 1),
  'f1320000-0000-0000-0000-000000000001',
  'flow non-UUID text search still works'
);

select is(
  (select id::text from public.search_flows_latest('f1320000-0000-0000-0000-000000000002', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b1320000-0000-0000-0000-000000000001') limit 1),
  'f1320000-0000-0000-0000-000000000002',
  'flow UUID my search uses auth.uid instead of spoofable this_user_id'
);

select is(
  (select count(*) from public.search_flows_latest('f1320000-0000-0000-0000-000000000003', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b1320000-0000-0000-0000-000000000001')),
  0::bigint,
  'flow UUID my search cannot read another user row'
);

select is(
  (select id::text from public.search_flows_latest('f1320000-0000-0000-0000-000000000004', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c1320000-0000-0000-0000-000000000001') limit 1),
  'f1320000-0000-0000-0000-000000000004',
  'flow UUID te search returns team data for a member'
);

select set_config('request.jwt.claim.sub', 'b1320000-0000-0000-0000-000000000001', true);

select is(
  (select count(*) from public.search_flows_latest('f1320000-0000-0000-0000-000000000004', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c1320000-0000-0000-0000-000000000001')),
  0::bigint,
  'flow UUID te search rejects a non-member'
);

select set_config('request.jwt.claim.sub', 'a1320000-0000-0000-0000-000000000001', true);

select is(
  (select id::text from public.search_processes_latest('e1320000-0000-0000-0000-000000000001', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '', null, null, 'all') limit 1),
  'e1320000-0000-0000-0000-000000000001',
  'process UUID exact search returns public data'
);

select is(
  (select count(*) from public.search_processes_latest('e1320000-0000-0000-0000-000000000001', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '', null, null, 'LCI result')),
  0::bigint,
  'process UUID exact search keeps type_of_data_set_filter semantics'
);

select is(
  (select id::text from public.search_lifecyclemodels_latest('d1320000-0000-0000-0000-000000000001', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') limit 1),
  'd1320000-0000-0000-0000-000000000001',
  'lifecyclemodel UUID exact search returns public data'
);

select is(
  (select version::text from public.pgroonga_search_contacts_latest('c1320000-0000-0000-0000-000000000101', '{}'::jsonb, 10, 1, 'tg', '') where id = 'c1320000-0000-0000-0000-000000000101'),
  '01.00.002',
  'generic simple UUID exact search returns the latest visible contact version'
);

select is(
  (select count(*) from public.pgroonga_search_contacts_latest('c1320000-0000-0000-0000-000000000101', '{}'::jsonb, 10, 2, 'tg', '')),
  0::bigint,
  'generic simple UUID exact search keeps pagination semantics'
);

select is(
  (select id::text from public.pgroonga_search_contacts_latest('uuid-fast-contact-token', '{}'::jsonb, 10, 1, 'tg', '') limit 1),
  'c1320000-0000-0000-0000-000000000101',
  'generic simple non-UUID text search still works'
);

select ok(
  not exists (
    select 1
    from public.flows
    where id = 'f1320000-0000-0000-0000-000000000001'
      and extracted_text ~* 'f1320000-0000-0000-0000-000000000001'
  ),
  'stored extracted_text fixtures do not need UUIDs for UUID search'
);

select * from finish();

rollback;
