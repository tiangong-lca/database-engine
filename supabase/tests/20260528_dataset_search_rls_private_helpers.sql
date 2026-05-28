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

select plan(26);

select ok(to_regnamespace('private') is not null, 'private schema exists for non-exposed search helpers');

select ok(
  strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'private.search_flows_latest_impl') > 0,
  'flow public search wrapper delegates to private helper'
);

select ok(
  strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure), 'private.search_processes_latest_impl') > 0,
  'process public search wrapper delegates to private helper'
);

select ok(
  strpos(pg_get_functiondef('public.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'private.search_lifecyclemodels_latest_impl') > 0,
  'lifecyclemodel public search wrapper delegates to private helper'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure),
  'flow private helper is security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure),
  'process private helper is security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure),
  'lifecyclemodel private helper is security definer'
);

select ok(
  not (select prosecdef from pg_proc where oid = 'public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure),
  'public search wrapper remains security invoker'
);

select ok(
  not (select prosecdef from pg_proc where oid = 'public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure),
  'generic regclass helper remains security invoker'
);

select ok(
  not has_function_privilege('authenticated', 'private.dataset_search_effective_user_id(text)', 'EXECUTE'),
  'authenticated cannot execute private identity helper directly'
);

select ok(
  has_function_privilege('authenticated', 'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer)', 'EXECUTE'),
  'authenticated can execute the hardcoded flow search helper through the wrapper path'
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
    'a1000000-0000-0000-0000-000000000103',
    'authenticated',
    'authenticated',
    'dataset-search-rls-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a1000000-0000-0000-0000-000000000103","email":"dataset-search-rls-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b2000000-0000-0000-0000-000000000103',
    'authenticated',
    'authenticated',
    'dataset-search-rls-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b2000000-0000-0000-0000-000000000103","email":"dataset-search-rls-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1000000-0000-0000-0000-000000000103', '{"email":"dataset-search-rls-owner@example.com"}'::jsonb, null),
  ('b2000000-0000-0000-0000-000000000103', '{"email":"dataset-search-rls-outsider@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values
  ('c3000000-0000-0000-0000-000000000103', '{"name":"Dataset Search RLS Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values
  ('a1000000-0000-0000-0000-000000000103', 'c3000000-0000-0000-0000-000000000103', 'owner');

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

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('f1000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-public-flow-token"}'::jsonb, '{"search":"rls-public-flow-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 100, null, 'rls-public-flow-token', true, now(), now()),
  ('f2000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-owner-flow-token"}'::jsonb, '{"search":"rls-owner-flow-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 0, null, 'rls-owner-flow-token', true, now(), now()),
  ('f3000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-team-flow-token"}'::jsonb, '{"search":"rls-team-flow-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, 'c3000000-0000-0000-0000-000000000103', 'rls-team-flow-token', true, now(), now()),
  ('f4000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-outsider-flow-token"}'::jsonb, '{"search":"rls-outsider-flow-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, null, 'rls-outsider-flow-token', true, now(), now());

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('e1000000-0000-0000-0000-000000000103', '01.00.000', '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-public-process-token"}'::jsonb, '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-public-process-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 100, null, 'rls-public-process-token', true, now(), now()),
  ('e2000000-0000-0000-0000-000000000103', '01.00.000', '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-owner-process-token"}'::jsonb, '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-owner-process-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 0, null, 'rls-owner-process-token', true, now(), now()),
  ('e3000000-0000-0000-0000-000000000103', '01.00.000', '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-team-process-token"}'::jsonb, '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-team-process-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, 'c3000000-0000-0000-0000-000000000103', 'rls-team-process-token', true, now(), now()),
  ('e4000000-0000-0000-0000-000000000103', '01.00.000', '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-outsider-process-token"}'::jsonb, '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process"}}},"search":"rls-outsider-process-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, null, 'rls-outsider-process-token', true, now(), now());

insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('d1000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-public-lifecycle-token"}'::jsonb, '{"search":"rls-public-lifecycle-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 100, null, 'rls-public-lifecycle-token', true, now(), now()),
  ('d2000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-owner-lifecycle-token"}'::jsonb, '{"search":"rls-owner-lifecycle-token"}'::json, 'a1000000-0000-0000-0000-000000000103', 0, null, 'rls-owner-lifecycle-token', true, now(), now()),
  ('d3000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-team-lifecycle-token"}'::jsonb, '{"search":"rls-team-lifecycle-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, 'c3000000-0000-0000-0000-000000000103', 'rls-team-lifecycle-token', true, now(), now()),
  ('d4000000-0000-0000-0000-000000000103', '01.00.000', '{"search":"rls-outsider-lifecycle-token"}'::jsonb, '{"search":"rls-outsider-lifecycle-token"}'::json, 'b2000000-0000-0000-0000-000000000103', 0, null, 'rls-outsider-lifecycle-token', true, now(), now());

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1000000-0000-0000-0000-000000000103', true);

select is((select id::text from public.search_flows_latest('rls-public-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') limit 1), 'f1000000-0000-0000-0000-000000000103', 'flow tg search returns public data');
select is((select id::text from public.search_processes_latest('rls-public-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '', null, null, 'all') limit 1), 'e1000000-0000-0000-0000-000000000103', 'process tg search returns public data');
select is((select id::text from public.search_lifecyclemodels_latest('rls-public-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'tg', '') limit 1), 'd1000000-0000-0000-0000-000000000103', 'lifecyclemodel tg search returns public data');

select is((select id::text from public.search_flows_latest('rls-owner-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103') limit 1), 'f2000000-0000-0000-0000-000000000103', 'flow my search uses auth.uid instead of spoofable this_user_id');
select is((select id::text from public.search_processes_latest('rls-owner-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103', null, null, 'all') limit 1), 'e2000000-0000-0000-0000-000000000103', 'process my search uses auth.uid instead of spoofable this_user_id');
select is((select id::text from public.search_lifecyclemodels_latest('rls-owner-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103') limit 1), 'd2000000-0000-0000-0000-000000000103', 'lifecyclemodel my search uses auth.uid instead of spoofable this_user_id');

select is((select count(*) from public.search_flows_latest('rls-outsider-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103')), 0::bigint, 'flow my search cannot spoof another user');
select is((select count(*) from public.search_processes_latest('rls-outsider-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103', null, null, 'all')), 0::bigint, 'process my search cannot spoof another user');
select is((select count(*) from public.search_lifecyclemodels_latest('rls-outsider-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'my', 'b2000000-0000-0000-0000-000000000103')), 0::bigint, 'lifecyclemodel my search cannot spoof another user');

select is((select id::text from public.search_flows_latest('rls-team-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103') limit 1), 'f3000000-0000-0000-0000-000000000103', 'flow te search returns team data for a team member');
select is((select id::text from public.search_processes_latest('rls-team-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103', null, 'all') limit 1), 'e3000000-0000-0000-0000-000000000103', 'process te search returns team data for a team member');
select is((select id::text from public.search_lifecyclemodels_latest('rls-team-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103') limit 1), 'd3000000-0000-0000-0000-000000000103', 'lifecyclemodel te search returns team data for a team member');

select set_config('request.jwt.claim.sub', 'b2000000-0000-0000-0000-000000000103', true);

select is((select count(*) from public.search_flows_latest('rls-team-flow-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103')), 0::bigint, 'flow te search rejects a non-member');
select is((select count(*) from public.search_processes_latest('rls-team-process-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103', null, 'all')), 0::bigint, 'process te search rejects a non-member');
select is((select count(*) from public.search_lifecyclemodels_latest('rls-team-lifecycle-token', '{}'::jsonb, '{}'::jsonb, 10, 1, 'te', '', 'c3000000-0000-0000-0000-000000000103')), 0::bigint, 'lifecyclemodel te search rejects a non-member');

select * from finish();

rollback;
