begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

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
  (select prosecdef from pg_proc where oid = 'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure),
  'flow semantic candidates run as security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure),
  'process semantic candidates run as security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.semantic_lifecyclemodel_candidates(text,text,double precision,integer,text)'::regprocedure),
  'lifecyclemodel semantic candidates run as security definer'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'private.semantic_flow_candidates') > 0
    and strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.semantic_search_flows_v1') = 0,
  'flow hybrid search uses lightweight private semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'private.semantic_process_candidates') > 0
    and strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.semantic_search_processes_v1') = 0,
  'process hybrid search uses lightweight private semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'private.semantic_lifecyclemodel_candidates') > 0
    and strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'public.semantic_search_lifecyclemodels_v1') = 0,
  'lifecyclemodel hybrid search uses lightweight private semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure), 'limit candidate_size') > 0,
  'process semantic candidates apply a real candidate limit'
);

select ok(
  strpos(pg_get_functiondef('private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure), 'p.user_id = effective_user_id') > 0,
  'process my semantic candidates are visibility-first'
);

select ok(
  strpos(pg_get_functiondef('private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure), 'r.user_id = effective_user_id') > 0
    and strpos(pg_get_functiondef('private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure), 'r.team_id = p.team_id') > 0,
  'process team semantic candidates check actor team membership'
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
    'a1000000-0000-0000-0000-000000000111',
    'authenticated',
    'authenticated',
    'semantic-candidate-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"a1000000-0000-0000-0000-000000000111","email":"semantic-candidate-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'b2000000-0000-0000-0000-000000000111',
    'authenticated',
    'authenticated',
    'semantic-candidate-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"b2000000-0000-0000-0000-000000000111","email":"semantic-candidate-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a1000000-0000-0000-0000-000000000111', '{"email":"semantic-candidate-owner@example.com"}'::jsonb, null),
  ('b2000000-0000-0000-0000-000000000111', '{"email":"semantic-candidate-outsider@example.com"}'::jsonb, null);

insert into public.teams (id, json, rank, is_public)
values
  ('c3000000-0000-0000-0000-000000000111', '{"name":"Semantic Candidate Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values
  ('a1000000-0000-0000-0000-000000000111', 'c3000000-0000-0000-0000-000000000111', 'owner');

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

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, embedding_ft, rule_verification, created_at, modified_at
)
select *
from (
  select 'f1000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-owner-flow"}'::jsonb, '{"search":"semantic-owner-flow"}'::json, 'a1000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-owner-flow'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'f2000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-team-flow"}'::jsonb, '{"search":"semantic-team-flow"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, 'c3000000-0000-0000-0000-000000000111'::uuid, 'semantic-team-flow'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'f3000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-outsider-flow"}'::jsonb, '{"search":"semantic-outsider-flow"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-outsider-flow'::text, test_vector.value, true, now(), now() from test_vector
) rows;

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, embedding_ft, rule_verification, created_at, modified_at
)
select *
from (
  select 'e1000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-owner-process"}'::jsonb, '{"search":"semantic-owner-process"}'::json, 'a1000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-owner-process'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'e2000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-team-process"}'::jsonb, '{"search":"semantic-team-process"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, 'c3000000-0000-0000-0000-000000000111'::uuid, 'semantic-team-process'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'e3000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-outsider-process"}'::jsonb, '{"search":"semantic-outsider-process"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-outsider-process'::text, test_vector.value, true, now(), now() from test_vector
) rows;

with test_vector(value) as (
  select ('[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']')::vector(1024)
)
insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, embedding_ft, rule_verification, created_at, modified_at
)
select *
from (
  select 'd1000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-owner-lifecycle"}'::jsonb, '{"search":"semantic-owner-lifecycle"}'::json, 'a1000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-owner-lifecycle'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'd2000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-team-lifecycle"}'::jsonb, '{"search":"semantic-team-lifecycle"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, 'c3000000-0000-0000-0000-000000000111'::uuid, 'semantic-team-lifecycle'::text, test_vector.value, true, now(), now() from test_vector
  union all
  select 'd3000000-0000-0000-0000-000000000111'::uuid, '01.00.000'::character(9), '{"search":"semantic-outsider-lifecycle"}'::jsonb, '{"search":"semantic-outsider-lifecycle"}'::json, 'b2000000-0000-0000-0000-000000000111'::uuid, 0, null::uuid, 'semantic-outsider-lifecycle'::text, test_vector.value, true, now(), now() from test_vector
) rows;

set local role authenticated;
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claim.sub', 'a1000000-0000-0000-0000-000000000111', true);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from private.semantic_flow_candidates((select value from query_vector), '{}', 0.5, 20, 'my')),
  array['f1000000-0000-0000-0000-000000000111']::text[],
  'flow my semantic candidates only include the authenticated owner row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from private.semantic_process_candidates((select value from query_vector), '{}', 0.5, 20, 'my')),
  array['e1000000-0000-0000-0000-000000000111']::text[],
  'process my semantic candidates only include the authenticated owner row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from private.semantic_lifecyclemodel_candidates((select value from query_vector), '{}', 0.5, 20, 'my')),
  array['d1000000-0000-0000-0000-000000000111']::text[],
  'lifecyclemodel my semantic candidates only include the authenticated owner row'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select array_agg(id::text order by id) from private.semantic_process_candidates((select value from query_vector), '{}', 0.5, 20, 'te')),
  array['e2000000-0000-0000-0000-000000000111']::text[],
  'process team semantic candidates only include actor team rows'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_processes('no-text-match-token', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1) limit 1),
  'e1000000-0000-0000-0000-000000000111',
  'process hybrid search can return a semantic-only my candidate'
);

with query_vector(value) as (
  select '[1,' || array_to_string(array_fill('0'::text, array[1023]), ',') || ']'
)
select is(
  (select id::text from public.hybrid_search_processes('semantic-outsider-process', (select value from query_vector), '{}', 0.5, 20, 0.3, 0.2, 0.5, 10, 'my', 10, 1) limit 1),
  'e1000000-0000-0000-0000-000000000111',
  'process hybrid my search does not leak outsider semantic candidates even when text query names outsider row'
);

select * from finish();

rollback;
