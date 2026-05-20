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
    '17000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'latest-contact-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"17000000-0000-0000-0000-000000000001","email":"latest-contact-owner@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.teams (id, json, rank, is_public)
values
  ('27000000-0000-0000-0000-000000000001', '{"name":"Latest Contact Team A"}'::jsonb, 1, false),
  ('27000000-0000-0000-0000-000000000002', '{"name":"Latest Contact Team B"}'::jsonb, 2, false);

insert into public.contacts (
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
    '37000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Legacy matched contact"}],"common:name":[{"@xml:lang":"en","#text":"Legacy matched contact"}],"email":"legacy@example.com"}}},"search":"legacy unique"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Legacy matched contact"}],"common:name":[{"@xml:lang":"en","#text":"Legacy matched contact"}],"email":"legacy@example.com"}}},"search":"legacy unique"}'::json,
    '17000000-0000-0000-0000-000000000001',
    100,
    '27000000-0000-0000-0000-000000000001',
    true,
    now() - interval '5 days',
    now() - interval '5 days'
  ),
  (
    '37000000-0000-0000-0000-000000000001',
    '01.00.002',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Latest contact"}],"common:name":[{"@xml:lang":"en","#text":"Latest contact"}],"email":"latest@example.com"}}},"search":"current text"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Latest contact"}],"common:name":[{"@xml:lang":"en","#text":"Latest contact"}],"email":"latest@example.com"}}},"search":"current text"}'::json,
    '17000000-0000-0000-0000-000000000001',
    100,
    '27000000-0000-0000-0000-000000000001',
    true,
    now() - interval '1 day',
    now() - interval '1 day'
  ),
  (
    '37000000-0000-0000-0000-000000000002',
    '01.00.001',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Single open contact"}],"common:name":[{"@xml:lang":"en","#text":"Single open contact"}],"email":"single@example.com"}}},"search":"single"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Single open contact"}],"common:name":[{"@xml:lang":"en","#text":"Single open contact"}],"email":"single@example.com"}}},"search":"single"}'::json,
    '17000000-0000-0000-0000-000000000001',
    100,
    '27000000-0000-0000-0000-000000000001',
    true,
    now() - interval '2 days',
    now() - interval '2 days'
  ),
  (
    '37000000-0000-0000-0000-000000000003',
    '01.00.001',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Other team contact"}],"common:name":[{"@xml:lang":"en","#text":"Other team contact"}],"email":"other-team@example.com"}}},"search":"other team"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Other team contact"}],"common:name":[{"@xml:lang":"en","#text":"Other team contact"}],"email":"other-team@example.com"}}},"search":"other team"}'::json,
    '17000000-0000-0000-0000-000000000001',
    100,
    '27000000-0000-0000-0000-000000000002',
    true,
    now() - interval '3 days',
    now() - interval '3 days'
  ),
  (
    '37000000-0000-0000-0000-000000000004',
    '01.00.000',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner draft contact"}],"common:name":[{"@xml:lang":"en","#text":"Owner draft contact"}],"email":"draft@example.com"}}},"search":"draft"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner draft contact"}],"common:name":[{"@xml:lang":"en","#text":"Owner draft contact"}],"email":"draft@example.com"}}},"search":"draft"}'::json,
    '17000000-0000-0000-0000-000000000001',
    0,
    '27000000-0000-0000-0000-000000000001',
    true,
    now() - interval '4 days',
    now() - interval '4 days'
  ),
  (
    '37000000-0000-0000-0000-000000000004',
    '01.00.001',
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner review contact"}],"common:name":[{"@xml:lang":"en","#text":"Owner review contact"}],"email":"review@example.com"}}},"search":"review"}'::jsonb,
    '{"contactDataSet":{"contactInformation":{"dataSetInformation":{"common:shortName":[{"@xml:lang":"en","#text":"Owner review contact"}],"common:name":[{"@xml:lang":"en","#text":"Owner review contact"}],"email":"review@example.com"}}},"search":"review"}'::json,
    '17000000-0000-0000-0000-000000000001',
    20,
    '27000000-0000-0000-0000-000000000001',
    true,
    now() - interval '1 hour',
    now() - interval '1 hour'
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '17000000-0000-0000-0000-000000000001', true);

select is(
  (
    select version::text
    from public.get_latest_contact_versions(10, 1, 'tg', '17000000-0000-0000-0000-000000000001')
    where id = '37000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'open contact list returns the highest visible version for a UUID'
);

select is(
  (
    select version_count
    from public.get_latest_contact_versions(10, 1, 'tg', '17000000-0000-0000-0000-000000000001')
    where id = '37000000-0000-0000-0000-000000000001'
  ),
  2::bigint,
  'open contact list reports version_count for the visible UUID group'
);

select is(
  (
    select max(total_count)
    from public.get_latest_contact_versions(10, 1, 'tg', '17000000-0000-0000-0000-000000000001')
  ),
  3::bigint,
  'open contact list total_count counts unique UUIDs, not version rows'
);

select is(
  (
    select count(*)
    from public.get_latest_contact_versions(
      10,
      1,
      'tg',
      '17000000-0000-0000-0000-000000000001',
      '27000000-0000-0000-0000-000000000001'
    )
  ),
  2::bigint,
  'team filter limits open contact latest-version rows before pagination'
);

select is(
  (
    select version_count
    from public.get_latest_contact_versions(
      10,
      1,
      'my',
      '17000000-0000-0000-0000-000000000001'
    )
    where id = '37000000-0000-0000-0000-000000000004'
  ),
  2::bigint,
  'my data without a state filter counts all owner-visible versions'
);

select is(
  (
    select version_count
    from public.get_latest_contact_versions(
      10,
      1,
      'my',
      '17000000-0000-0000-0000-000000000001',
      null,
      20
    )
    where id = '37000000-0000-0000-0000-000000000004'
  ),
  1::bigint,
  'my data state filter is applied before version_count is calculated'
);

select is(
  (
    select version::text
    from public.pgroonga_search_contacts_latest(
      'legacy unique',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '17000000-0000-0000-0000-000000000001'
    )
    where id = '37000000-0000-0000-0000-000000000001'
  ),
  '01.00.002',
  'search can match an older version while returning the highest visible version for that UUID'
);

select is(
  (
    select max(total_count)
    from public.pgroonga_search_contacts_latest(
      'legacy unique',
      '{}'::jsonb,
      10,
      1,
      'tg',
      '17000000-0000-0000-0000-000000000001'
    )
  ),
  1::bigint,
  'search total_count counts matching UUIDs after grouping'
);

select * from finish();

rollback;
