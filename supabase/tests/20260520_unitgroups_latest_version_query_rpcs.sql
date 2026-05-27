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
    '16000000-0000-0000-0000-000000000046',
    'authenticated',
    'authenticated',
    'latest-unitgroup-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"16000000-0000-0000-0000-000000000046","email":"latest-unitgroup-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public)
values
  ('26000000-0000-0000-0000-000000000046', '{"name":"Latest Unitgroup Team A"}'::jsonb, 1, false),
  ('26000000-0000-0000-0000-000000000047', '{"name":"Latest Unitgroup Team B"}'::jsonb, 2, false);

alter table public.unitgroups disable trigger "unitgroups_json_sync_trigger";

insert into public.unitgroups (
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
    '46000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy matched unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"legacy unique"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy matched unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"legacy unique"}'::json,
    '16000000-0000-0000-0000-000000000046',
    100,
    '26000000-0000-0000-0000-000000000046',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '46000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"current text"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"current text"}'::json,
    '16000000-0000-0000-0000-000000000046',
    100,
    '26000000-0000-0000-0000-000000000046',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '46000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Single open unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u2"}},"units":{"unit":[{"@dataSetInternalID":"u2","name":"MJ"}]}},"search":"single"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Single open unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u2"}},"units":{"unit":[{"@dataSetInternalID":"u2","name":"MJ"}]}},"search":"single"}'::json,
    '16000000-0000-0000-0000-000000000046',
    100,
    '26000000-0000-0000-0000-000000000046',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    '46000000-0000-0000-0000-000000000003',
    '01.00.001',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Other team unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u3"}},"units":{"unit":[{"@dataSetInternalID":"u3","name":"m"}]}},"search":"other team"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Other team unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u3"}},"units":{"unit":[{"@dataSetInternalID":"u3","name":"m"}]}},"search":"other team"}'::json,
    '16000000-0000-0000-0000-000000000046',
    100,
    '26000000-0000-0000-0000-000000000047',
    true,
    now() - interval '3 days',
    now() - interval '3 days'
  ),
  (
    '46000000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner draft unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"draft"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner draft unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"draft"}'::json,
    '16000000-0000-0000-0000-000000000046',
    0,
    '26000000-0000-0000-0000-000000000046',
    true,
    now() - interval '4 days',
    now() - interval '4 days'
  ),
  (
    '46000000-0000-0000-0000-000000000004',
    '01.00.001',
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner review unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"review"}'::jsonb,
    '{"unitGroupDataSet":{"unitGroupInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner review unit group"}]},"quantitativeReference":{"referenceToReferenceUnit":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"review"}'::json,
    '16000000-0000-0000-0000-000000000046',
    200,
    '26000000-0000-0000-0000-000000000046',
    true,
    now() - interval '1 hour',
    now() - interval '1 hour'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '16000000-0000-0000-0000-000000000046', true);

select is(
  (
    select version::text
    from public.get_latest_unitgroup_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000046')
    where id = '46000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'open unit group list returns the highest visible version for a UUID'
);


select is(
  (
    select max(total_count)
    from public.get_latest_unitgroup_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000046')
  ),
  3::bigint,
  'open unit group list total_count counts unique UUIDs, not version rows'
);

select is(
  (
    select count(*)
    from public.get_latest_unitgroup_versions(
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000046',
      '26000000-0000-0000-0000-000000000046'
    )
  ),
  2::bigint,
  'team filter limits open unit group latest-version rows before pagination'
);



select is(
  (
    select version::text
    from public.pgroonga_search_unitgroups_latest(
      'Legacy matched unit group',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000046'
    )
    where id = '46000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'search can match an older version while returning the highest visible version for that UUID'
);

select is(
  (
    select max(total_count)
    from public.pgroonga_search_unitgroups_latest(
      'Legacy matched unit group',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000046'
    )
  ),
  1::bigint,
  'search total_count counts matching UUIDs after grouping'
);

select is(
  strpos(pg_get_functiondef('public.pgroonga_search_unitgroups_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'user_id::text = this_user_id'),
  0,
  'unit group latest search does not cast user_id on the my-data predicate'
);

select ok(
  to_regclass('public.unitgroups_text_pgroonga') is not null,
  'unit group latest PGroonga search has an extracted_text index'
);

select ok(
  to_regclass('public.unitgroups_json_pgroonga') is null
    and to_regclass('public.unitgroups_public_json_pgroonga_idx') is null
    and to_regclass('public.unitgroups_co_json_pgroonga_idx') is null,
  'unit group latest search no longer keeps JSON PGroonga indexes'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'join lateral') > 0,
  'unit group latest PGroonga search fetches latest versions with lateral index lookups'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'd.extracted_text &@~ $1') > 0,
  'unit group latest PGroonga search matches query_text against extracted_text'
);

select ok(
  exists (
    select 1
    from pg_proc p
    cross join unnest(p.proconfig) cfg
    where p.oid = 'public.pgroonga_search_unitgroups_latest(text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure
      and cfg = 'statement_timeout=60s'
  ),
  'unit group latest PGroonga search has a function-level timeout budget'
);

select * from finish();

rollback;
