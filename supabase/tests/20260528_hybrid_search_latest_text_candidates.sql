begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(12);

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger_name text)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger_name
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger_name);
  end if;
end;
$$;

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.search_flows_latest') > 0,
  'flow hybrid search uses latest text search for text candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.search_processes_latest') > 0,
  'process hybrid search uses latest text search for text candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.search_lifecyclemodels_latest') > 0,
  'lifecyclemodel hybrid search uses latest text search for text candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'pgroonga_search_processes_v1') = 0
    and strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'pgroonga_search_processes_text_v1') = 0,
  'process hybrid search no longer depends on legacy process PGroonga helpers'
);

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1000000-0000-0000-0000-000000000101', '{"email":"hybrid-search-owner@example.com"}'::jsonb, null),
  ('b2000000-0000-0000-0000-000000000101', '{"email":"hybrid-search-outsider@example.com"}'::jsonb, null);

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
  ('f1000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-public-flow-token"}'::jsonb, '{"search":"hybrid-public-flow-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 100, null, 'hybrid-public-flow-token 电子器件', true, now(), now()),
  ('f2000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-owner-flow-token"}'::jsonb, '{"search":"hybrid-owner-flow-token"}'::json, 'a1000000-0000-0000-0000-000000000101', 0, null, 'hybrid-owner-flow-token 电子器件', true, now(), now()),
  ('f3000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-outsider-flow-token"}'::jsonb, '{"search":"hybrid-outsider-flow-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 0, null, 'hybrid-outsider-flow-token 电子器件', true, now(), now());

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('e1000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-public-process-token"}'::jsonb, '{"search":"hybrid-public-process-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 100, null, 'hybrid-public-process-token 正极材料 cathode material', true, now(), now()),
  ('e2000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-owner-process-token"}'::jsonb, '{"search":"hybrid-owner-process-token"}'::json, 'a1000000-0000-0000-0000-000000000101', 0, null, 'hybrid-owner-process-token 正极材料 cathode material', true, now(), now()),
  ('e3000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-outsider-process-token"}'::jsonb, '{"search":"hybrid-outsider-process-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 0, null, 'hybrid-outsider-process-token 正极材料 cathode material', true, now(), now());

insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  ('d1000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-public-lifecycle-token"}'::jsonb, '{"search":"hybrid-public-lifecycle-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 100, null, 'hybrid-public-lifecycle-token 交流电', true, now(), now()),
  ('d2000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-owner-lifecycle-token"}'::jsonb, '{"search":"hybrid-owner-lifecycle-token"}'::json, 'a1000000-0000-0000-0000-000000000101', 0, null, 'hybrid-owner-lifecycle-token 交流电', true, now(), now()),
  ('d3000000-0000-0000-0000-000000000101', '01.00.000', '{"search":"hybrid-outsider-lifecycle-token"}'::jsonb, '{"search":"hybrid-outsider-lifecycle-token"}'::json, 'b2000000-0000-0000-0000-000000000101', 0, null, 'hybrid-outsider-lifecycle-token 交流电', true, now(), now());

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1000000-0000-0000-0000-000000000101', true);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_flows('(电子器件) OR (electronic component)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) limit 1),
  'f1000000-0000-0000-0000-000000000101',
  'flow hybrid tg search returns a text candidate through latest search'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_flows('(电子器件) OR (electronic component)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1) limit 1),
  'f2000000-0000-0000-0000-000000000101',
  'flow hybrid my search returns the authenticated owner text candidate'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select count(*) from public.hybrid_search_flows('hybrid-outsider-flow-token', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1)),
  0::bigint,
  'flow hybrid my search cannot return another user text candidate'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_processes('(正极材料) OR (cathode material)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) limit 1),
  'e1000000-0000-0000-0000-000000000101',
  'process hybrid tg search returns a text candidate through latest search'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_processes('(正极材料) OR (cathode material)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1) limit 1),
  'e2000000-0000-0000-0000-000000000101',
  'process hybrid my search returns the authenticated owner text candidate'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select count(*) from public.hybrid_search_processes('hybrid-outsider-process-token', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1)),
  0::bigint,
  'process hybrid my search cannot return another user text candidate'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_lifecyclemodels('(交流电) OR (electricity)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'tg', 10, 1) limit 1),
  'd1000000-0000-0000-0000-000000000101',
  'lifecyclemodel hybrid tg search returns a text candidate through latest search'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_lifecyclemodels('(交流电) OR (electricity)', (select value from latest_zero_embedding), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1) limit 1),
  'd2000000-0000-0000-0000-000000000101',
  'lifecyclemodel hybrid my search returns the authenticated owner text candidate'
);

select * from finish();

rollback;
