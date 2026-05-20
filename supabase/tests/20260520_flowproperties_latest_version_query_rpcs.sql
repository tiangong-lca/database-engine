begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(8);

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
    'latest-flowproperty-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"16000000-0000-0000-0000-000000000046","email":"latest-flowproperty-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public)
values
  ('26000000-0000-0000-0000-000000000046', '{"name":"Latest Flowproperty Team A"}'::jsonb, 1, false),
  ('26000000-0000-0000-0000-000000000047', '{"name":"Latest Flowproperty Team B"}'::jsonb, 2, false);

alter table public.flowproperties disable trigger "flowproperties_json_sync_trigger";

insert into public.flowproperties (
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy matched flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"legacy unique"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Legacy matched flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"legacy unique"}'::json,
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"current text"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Latest flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u1"}},"units":{"unit":[{"@dataSetInternalID":"u1","name":"kg"}]}},"search":"current text"}'::json,
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Single open flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u2"}},"units":{"unit":[{"@dataSetInternalID":"u2","name":"MJ"}]}},"search":"single"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Single open flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u2"}},"units":{"unit":[{"@dataSetInternalID":"u2","name":"MJ"}]}},"search":"single"}'::json,
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Other team flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u3"}},"units":{"unit":[{"@dataSetInternalID":"u3","name":"m"}]}},"search":"other team"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Other team flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u3"}},"units":{"unit":[{"@dataSetInternalID":"u3","name":"m"}]}},"search":"other team"}'::json,
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner draft flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"draft"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner draft flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"draft"}'::json,
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
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner review flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"review"}'::jsonb,
    '{"flowPropertyDataSet":{"flowPropertiesInformation":{"dataSetInformation":{"common:name":[{"@xml:lang":"en","#text":"Owner review flow property"}]},"quantitativeReference":{"referenceToReferenceUnitGroup":"u4"}},"units":{"unit":[{"@dataSetInternalID":"u4","name":"s"}]}},"search":"review"}'::json,
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
    from public.get_latest_flowproperty_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000046')
    where id = '46000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'open flow property list returns the highest visible version for a UUID'
);

select is(
  (
    select version_count
    from public.get_latest_flowproperty_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000046')
    where id = '46000000-0000-0000-0000-000000000001'
  ),
  2::bigint,
  'open flow property list reports version_count for the visible UUID group'
);

select is(
  (
    select max(total_count)
    from public.get_latest_flowproperty_versions(10, 1, 'tg', '16000000-0000-0000-0000-000000000046')
  ),
  3::bigint,
  'open flow property list total_count counts unique UUIDs, not version rows'
);

select is(
  (
    select count(*)
    from public.get_latest_flowproperty_versions(
      10,
      1,
      'tg',
      '16000000-0000-0000-0000-000000000046',
      '26000000-0000-0000-0000-000000000046'
    )
  ),
  2::bigint,
  'team filter limits open flow property latest-version rows before pagination'
);

select is(
  (
    select version_count
    from public.get_latest_flowproperty_versions(
      10,
      1,
      'my',
      '16000000-0000-0000-0000-000000000046'
    )
    where id = '46000000-0000-0000-0000-000000000004'
  ),
  2::bigint,
  'my data without a state filter counts all owner-visible versions'
);

select is(
  (
    select version_count
    from public.get_latest_flowproperty_versions(
      10,
      1,
      'my',
      '16000000-0000-0000-0000-000000000046',
      null,
      200
    )
    where id = '46000000-0000-0000-0000-000000000004'
  ),
  1::bigint,
  'my data state filter is applied before version_count is calculated'
);

select is(
  (
    select version::text
    from public.pgroonga_search_flowproperties_latest(
      'legacy unique',
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
    from public.pgroonga_search_flowproperties_latest(
      'legacy unique',
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

select * from finish();

rollback;
