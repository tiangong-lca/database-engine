begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(20);

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

select is(
  private.pgroonga_escape_query_terms(array[null, '', '  alternating   current  ', 'AC power']),
  array[
    extensions.pgroonga_query_escape('alternating current'),
    extensions.pgroonga_query_escape('AC power')
  ],
  'query term helper ignores empty values and normalizes whitespace'
);

select is(
  (private.pgroonga_escape_query_terms(array['111479-05-1']))[1],
  extensions.pgroonga_query_escape('111479-05-1'),
  'query term helper delegates PGroonga syntax escaping'
);

select ok(
  'industrial alternating grid current'::text &@~ extensions.pgroonga_query_escape('alternating current'),
  'escaped ordinary multi-word term keeps PGroonga AND-style matching'
);

select ok(
  not ('industrial alternating grid current'::text &@~ '"alternating current"'),
  'quoted phrase search would not match separated ordinary words'
);

select ok(
  'sodium chloride reagent'::text &@~| private.pgroonga_escape_query_terms(array['no-match-token', 'sodium chloride']),
  '&@~| matches when any escaped query term matches'
);

select ok(
  '111479-05-1 quizalofop P tefuryl'::text &@~| private.pgroonga_escape_query_terms(array['111479-05-1']),
  'escaped CAS term with hyphens matches through term array search'
);

with sample(term) as (
  values ('Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-')
)
select ok(
  (select ('USLCI ' || term)::text &@~| private.pgroonga_escape_query_terms(array[term]) from sample),
  'escaped nested chemical term with brackets commas hyphens and parentheses does not throw'
);

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

insert into public.users (id, raw_user_meta_data, contact)
values
  ('a6130000-0000-0000-0000-000000000001', '{"email":"query-terms-owner@example.com"}'::jsonb, null),
  ('b6130000-0000-0000-0000-000000000001', '{"email":"query-terms-outsider@example.com"}'::jsonb, null);

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  (
    'f6130000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"search":"query-terms-public-flow-token"}'::jsonb,
    '{"search":"query-terms-public-flow-token"}'::json,
    'b6130000-0000-0000-0000-000000000001',
    100,
    null,
    'query-terms-public-flow-token 111479-05-1 Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-',
    true,
    now(),
    now()
  ),
  (
    'f6130000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"search":"query-terms-owner-flow-token"}'::jsonb,
    '{"search":"query-terms-owner-flow-token"}'::json,
    'a6130000-0000-0000-0000-000000000001',
    0,
    null,
    'query-terms-owner-flow-token owner chemical term',
    true,
    now(),
    now()
  ),
  (
    'f6130000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"search":"query-terms-outsider-flow-token"}'::jsonb,
    '{"search":"query-terms-outsider-flow-token"}'::json,
    'b6130000-0000-0000-0000-000000000001',
    0,
    null,
    'query-terms-outsider-flow-token outsider chemical term',
    true,
    now(),
    now()
  );

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values (
  'e6130000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"search":"query-terms-process-token"}'::jsonb,
  '{"search":"query-terms-process-token"}'::json,
  'b6130000-0000-0000-0000-000000000001',
  100,
  null,
  'query-terms-process-token industrial alternating grid current with AC grid power',
  true,
  now(),
  now()
);

insert into public.lifecyclemodels (
  id, version, json, json_ordered, user_id, state_code, team_id, extracted_text, rule_verification, created_at, modified_at
)
values
  (
    'd6130000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"search":"query-terms-lifecycle-token"}'::jsonb,
    '{"search":"query-terms-lifecycle-token"}'::json,
    'b6130000-0000-0000-0000-000000000001',
    100,
    null,
    'query-terms-lifecycle-token 硅酸盐水泥 clinker',
    true,
    now(),
    now()
  ),
  (
    'd6130000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"search":"query-terms-legacy-token"}'::jsonb,
    '{"search":"query-terms-legacy-token"}'::json,
    'b6130000-0000-0000-0000-000000000001',
    100,
    null,
    'query-terms-legacy-token legacy fallback term',
    true,
    now(),
    now()
  );

with chemical(term) as (
  values ('Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-')
)
select is(
  (
    select id::text
    from public.search_flows_latest(
      '(111479-05-1) OR (Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-)',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer,
      array['111479-05-1', (select term from chemical)]
    )
    limit 1
  ),
  'f6130000-0000-0000-0000-000000000001',
  'flow latest search uses escaped query_terms and does not evaluate invalid query_text'
);

with latest_zero_embedding(value) as (
  select '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
),
chemical(term) as (
  values ('Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-')
)
select is(
  (
    select id::text
    from public.hybrid_search_flows(
      '(111479-05-1) OR (Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-)',
      (select value from latest_zero_embedding),
      '{}',
      0.5,
      20,
      0.3,
      0.2,
      0.5,
      10,
      'tg',
      10,
      1,
      array['111479-05-1', (select term from chemical)]
    )
    limit 1
  ),
  'f6130000-0000-0000-0000-000000000001',
  'flow hybrid search passes query_terms into latest text candidates'
);

select is(
  (
    select id::text
    from public.search_processes_latest(
      'ignored-invalid-query (alternating current) OR (AC power)',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer,
      'all',
      array['alternating current', 'AC power']
    )
    limit 1
  ),
  'e6130000-0000-0000-0000-000000000001',
  'process latest search query_terms preserve ordinary multi-word matching'
);

select is(
  (
    select id::text
    from public.search_processes_latest(
      'alternating current',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer,
      'all'
    )
    limit 1
  ),
  'e6130000-0000-0000-0000-000000000001',
  'process latest search derives escaped query terms from query_text when query_terms is omitted'
);

select is(
  (
    select id::text
    from public.search_lifecyclemodels_latest(
      'ignored-invalid-query (硅酸盐水泥)',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer,
      array['硅酸盐水泥']
    )
    limit 1
  ),
  'd6130000-0000-0000-0000-000000000001',
  'lifecyclemodel latest search query_terms handle Chinese terms'
);

with chemical(term) as (
  values ('Propanoic acid, 2-[4-[(6-chloro-2-quinoxalinyl)oxy]phenoxy]-, 2-[[(1-methylethylidene)amino]oxy]ethyl ester, (2R)-')
)
select is(
  (
    select id::text
    from public.search_flows_latest(
      (select term from chemical),
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer
    )
    limit 1
  ),
  'f6130000-0000-0000-0000-000000000001',
  'flow latest query_text-only search escapes nested chemical punctuation'
);

select is(
  (
    select id::text
    from public.search_lifecyclemodels_latest(
      'legacy fallback term',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'tg',
      '',
      null::uuid,
      null::integer
    )
    limit 1
  ),
  'd6130000-0000-0000-0000-000000000002',
  'query_text-only search still works when query_terms is omitted'
);

select is(
  (
    select id::text
    from public.search_flows_latest(
      'ignored',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'my',
      'a6130000-0000-0000-0000-000000000001',
      null::uuid,
      null::integer,
      array['owner chemical term']
    )
    limit 1
  ),
  'f6130000-0000-0000-0000-000000000002',
  'my-data visibility still returns the authenticated owner result'
);

select is(
  (
    select count(*)
    from public.search_flows_latest(
      'ignored',
      '{}'::jsonb,
      '{}'::jsonb,
      10,
      1,
      'my',
      'a6130000-0000-0000-0000-000000000001',
      null::uuid,
      null::integer,
      array['outsider chemical term']
    )
  ),
  0::bigint,
  'my-data visibility still excludes another user result'
);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'f.extracted_text &@~| $14') > 0
    and strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'f.extracted_text &@~ $1') = 0,
  'flow latest implementation always uses escaped term-array text search'
);

select ok(
  strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'p.extracted_text &@~| $11') > 0
    and strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'p.extracted_text &@~ $1') = 0,
  'process latest implementation always uses escaped term-array text search'
);

select ok(
  strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'l.extracted_text &@~| $10') > 0
    and strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'l.extracted_text &@~ $1') = 0,
  'lifecyclemodel latest implementation always uses escaped term-array text search'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer,text[])'::regprocedure), 'query_terms') > 0,
  'flow hybrid signature exposes query_terms as the trailing optional argument'
);

select * from finish();

rollback;
