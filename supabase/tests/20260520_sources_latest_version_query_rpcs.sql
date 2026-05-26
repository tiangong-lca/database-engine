begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(11);

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
    '16000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'latest-source-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"16000000-0000-0000-0000-000000000001","email":"latest-source-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public)
values
  ('26000000-0000-0000-0000-000000000001', '{"name":"Latest Source Team A"}'::jsonb, 1, false),
  ('26000000-0000-0000-0000-000000000002', '{"name":"Latest Source Team B"}'::jsonb, 2, false);

alter table public.sources disable trigger "sources_json_sync_trigger";

insert into public.sources (
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
    '36000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Legacy matched source"}]}}},"search":"legacy unique"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Legacy matched source"}]}}},"search":"legacy unique"}'::json,
    '16000000-0000-0000-0000-000000000001',
    100,
    '26000000-0000-0000-0000-000000000001',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '36000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Latest source"}]}}},"search":"current text"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Latest source"}]}}},"search":"current text"}'::json,
    '16000000-0000-0000-0000-000000000001',
    100,
    '26000000-0000-0000-0000-000000000001',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '36000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Single open source"}]}}},"search":"single"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Single open source"}]}}},"search":"single"}'::json,
    '16000000-0000-0000-0000-000000000001',
    100,
    '26000000-0000-0000-0000-000000000001',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    '36000000-0000-0000-0000-000000000003',
    '01.00.001',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Other team source"}]}}},"search":"other team"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Other team source"}]}}},"search":"other team"}'::json,
    '16000000-0000-0000-0000-000000000001',
    100,
    '26000000-0000-0000-0000-000000000002',
    true,
    now() - interval '3 days',
    now() - interval '3 days'
  ),
  (
    '36000000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner draft source"}]}}},"search":"draft"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner draft source"}]}}},"search":"draft"}'::json,
    '16000000-0000-0000-0000-000000000001',
    0,
    '26000000-0000-0000-0000-000000000001',
    true,
    now() - interval '4 days',
    now() - interval '4 days'
  ),
  (
    '36000000-0000-0000-0000-000000000004',
    '01.00.001',
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner review source"}]}}},"search":"review"}'::jsonb,
    '{"sourceDataSet":{"sourceInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner review source"}]}}},"search":"review"}'::json,
    '16000000-0000-0000-0000-000000000001',
    20,
    '26000000-0000-0000-0000-000000000001',
    true,
    now() - interval '1 hour',
    now() - interval '1 hour'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '16000000-0000-0000-0000-000000000001', true);

select is(
  (
    select version::text
    from public.get_latest_source_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000001')
    where id = '36000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'open source list returns the highest visible version for a UUID'
);


select is(
  (
    select max(total_count)
    from public.get_latest_source_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000001')
  ),
  3::bigint,
  'open source list total_count counts unique UUIDs, not version rows'
);

select is(
  (
    select count(*)
    from public.get_latest_source_versions(
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000001',
      '26000000-0000-0000-0000-000000000001'
    )
  ),
  2::bigint,
  'team filter limits open source latest-version rows before pagination'
);



select is(
  (
    select version::text
    from public.pgroonga_search_sources_latest(
      'legacy unique',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000001'
    )
    where id = '36000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'search can match an older version while returning the highest visible version for that UUID'
);

select is(
  (
    select max(total_count)
    from public.pgroonga_search_sources_latest(
      'legacy unique',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000001'
    )
  ),
  1::bigint,
  'search total_count counts matching UUIDs after grouping'
);

select is(
  strpos(pg_get_functiondef('public.pgroonga_search_sources_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'user_id::text = this_user_id'),
  0,
  'source latest search does not cast user_id on the my-data predicate'
);

select ok(
  to_regclass('public.sources_text_pgroonga') is not null,
  'source latest PGroonga search has an extracted_text index'
);

select ok(
  to_regclass('public.sources_json_pgroonga') is null
    and to_regclass('public.sources_public_json_pgroonga_idx') is null
    and to_regclass('public.sources_co_json_pgroonga_idx') is null,
  'source latest search no longer keeps JSON PGroonga indexes'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'join lateral') > 0,
  'source latest PGroonga search fetches latest versions with lateral index lookups'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'd.extracted_text &@~ $1') > 0,
  'source latest PGroonga search matches query_text against extracted_text'
);

select ok(
  exists (
    select 1
    from pg_proc p
    cross join unnest(p.proconfig) cfg
    where p.oid = 'public.pgroonga_search_sources_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure
      and cfg = 'statement_timeout=60s'
  ),
  'source latest PGroonga search has a function-level timeout budget'
);

select * from finish();

rollback;
