begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select no_plan();

select is(
  (
    select count(*)
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'api'
      and routine.proname = 'qry_national_carbon_organization_contributions'
      and routine.prosecdef
      and routine.provolatile = 's'
      and routine.proconfig = array['search_path=""']::text[]
  ),
  1::bigint,
  'organization-contribution RPC is a fixed-path stable SECURITY DEFINER'
);

select ok(
  has_function_privilege(
    'authenticated',
    'api.qry_national_carbon_organization_contributions(integer)',
    'EXECUTE'
  )
    and not has_function_privilege(
      'anon',
      'api.qry_national_carbon_organization_contributions(integer)',
      'EXECUTE'
    )
    and not has_function_privilege(
      'service_role',
      'api.qry_national_carbon_organization_contributions(integer)',
      'EXECUTE'
    ),
  'only authenticated browser sessions receive the RPC ACL'
);

select is(
  (
    select capability_id
    from private.api_capability_grants
    where routine_identity =
      'api.qry_national_carbon_organization_contributions(integer)'
      and allow_authenticated
      and not allow_anon
      and not allow_service_role
  ),
  'NX-DASH-01'::text,
  'the exact RPC signature is registered under the dashboard capability'
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
    '57400000-0000-4000-8000-000000000001',
    'authenticated', 'authenticated', 'dashboard-manager@example.com', 'test', now(),
    '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000002',
    'authenticated', 'authenticated', 'contributor-one@example.com', 'test', now(),
    '{}', '{"organization":"Acme Labs"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000003',
    'authenticated', 'authenticated', 'contributor-two@example.com', 'test', now(),
    '{}', '{"organization":"acme   labs"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000004',
    'authenticated', 'authenticated', 'contributor-three@example.com', 'test', now(),
    '{}', '{"organization":"Beta Institute"}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000005',
    'authenticated', 'authenticated', 'no-organization@example.com', 'test', now(),
    '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000006',
    'authenticated', 'authenticated', 'ordinary-user@example.com', 'test', now(),
    '{}', '{}', now(), now(), false, false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '57400000-0000-4000-8000-000000000007',
    'authenticated', 'authenticated', 'organization-only@example.com', 'test', now(),
    '{}', '{"organization":"Gamma Cooperative"}', now(), now(), false, false
  );

insert into private.users (id, raw_user_meta_data)
values
  ('57400000-0000-4000-8000-000000000001', '{}'),
  ('57400000-0000-4000-8000-000000000002', '{"organization":"Acme Labs"}'),
  ('57400000-0000-4000-8000-000000000003', '{"organization":"acme   labs"}'),
  ('57400000-0000-4000-8000-000000000004', '{"organization":"Beta Institute"}'),
  ('57400000-0000-4000-8000-000000000005', '{}'),
  ('57400000-0000-4000-8000-000000000006', '{}'),
  ('57400000-0000-4000-8000-000000000007', '{"organization":"Gamma Cooperative"}')
on conflict (id) do update set
  raw_user_meta_data = excluded.raw_user_meta_data;

insert into private.roles (user_id, team_id, role)
values (
  '57400000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000',
  'admin'
);

set local session_replication_role = replica;

insert into public.processes (
  id, version, user_id, state_code, created_at, modified_at
)
values
  ('57410000-0000-4000-8000-000000000001', '01.00.000', '57400000-0000-4000-8000-000000000002', 100, now() - interval '80 days', now() - interval '60 days'),
  ('57410000-0000-4000-8000-000000000001', '02.00.000', '57400000-0000-4000-8000-000000000002', 100, now() - interval '10 days', now() - interval '10 days'),
  ('57410000-0000-4000-8000-000000000002', '01.00.000', '57400000-0000-4000-8000-000000000003', 100, now() - interval '8 days', now() - interval '8 days'),
  ('57410000-0000-4000-8000-000000000003', '01.00.000', '57400000-0000-4000-8000-000000000005', 100, now() - interval '5 days', now() - interval '5 days'),
  ('57410000-0000-4000-8000-000000000004', '01.00.000', '57400000-0000-4000-8000-000000000004', 20, now() - interval '4 days', now() - interval '4 days'),
  ('57410000-0000-4000-8000-000000000005', '01.00.000', '57400000-0000-4000-8000-000000000004', 20, now() - interval '6 days', now() - interval '6 days'),
  ('57410000-0000-4000-8000-000000000005', '02.00.000', '57400000-0000-4000-8000-000000000004', 0, now() - interval '2 days', now() - interval '2 days'),
  ('57410000-0000-4000-8000-000000000006', '01.00.000', '57400000-0000-4000-8000-000000000002', 100, now() - interval '20 days', now() - interval '20 days'),
  ('57410000-0000-4000-8000-000000000006', '02.00.000', '57400000-0000-4000-8000-000000000004', 20, now() - interval '1 day', now() - interval '1 day');

insert into public.lifecyclemodels (
  id, version, user_id, state_code, created_at, modified_at
)
values
  ('57410000-0000-4000-8000-000000000001', '01.00.000', '57400000-0000-4000-8000-000000000002', 100, now() - interval '7 days', now() - interval '7 days'),
  ('57420000-0000-4000-8000-000000000002', '01.00.000', '57400000-0000-4000-8000-000000000004', 100, now() - interval '5 days', now() - interval '5 days'),
  ('57420000-0000-4000-8000-000000000003', '01.00.000', '57400000-0000-4000-8000-000000000002', 20, now() - interval '3 days', now() - interval '3 days'),
  ('57420000-0000-4000-8000-000000000004', '01.00.000', '57400000-0000-4000-8000-000000000005', 100, now() - interval '35 days', now() - interval '35 days'),
  ('57420000-0000-4000-8000-000000000005', '01.00.000', '57400000-0000-4000-8000-000000000005', 20, now() - interval '2 days', now() - interval '2 days');

insert into private.reviews (
  id,
  data_id,
  data_version,
  state_code,
  reviewer_id,
  json,
  review_kind,
  target_table,
  submitted_revision_checksum,
  target_owner_id
)
values
  (
    '57430000-0000-4000-8000-000000000001',
    '57410000-0000-4000-8000-000000000004',
    '01.00.000',
    1,
    '["57400000-0000-4000-8000-000000000001"]',
    '{}',
    'root',
    'processes',
    repeat('a', 64),
    '57400000-0000-4000-8000-000000000004'
  ),
  (
    '57430000-0000-4000-8000-000000000002',
    '57410000-0000-4000-8000-000000000006',
    '02.00.000',
    0,
    '[]',
    '{}',
    'root',
    'processes',
    repeat('b', 64),
    '57400000-0000-4000-8000-000000000004'
  ),
  (
    '57430000-0000-4000-8000-000000000003',
    '57420000-0000-4000-8000-000000000003',
    '01.00.000',
    1,
    '["57400000-0000-4000-8000-000000000001"]',
    '{}',
    'root',
    'lifecyclemodels',
    repeat('c', 64),
    '57400000-0000-4000-8000-000000000002'
  ),
  (
    '57430000-0000-4000-8000-000000000004',
    '57420000-0000-4000-8000-000000000005',
    '01.00.000',
    0,
    '[]',
    '{}',
    'root',
    'lifecyclemodels',
    repeat('d', 64),
    '57400000-0000-4000-8000-000000000005'
  ),
  (
    '57430000-0000-4000-8000-000000000005',
    '57410000-0000-4000-8000-000000000005',
    '01.00.000',
    1,
    '["57400000-0000-4000-8000-000000000001"]',
    '{}',
    'root',
    'processes',
    repeat('e', 64),
    '57400000-0000-4000-8000-000000000004'
  );

update public.processes set json_ordered = json_build_object(
  'processDataSet', json_build_object('processInformation', json_build_object(
    'geography', json_build_object('locationOfOperationSupplyOrProduction',
      json_build_object('@location', case
        when id = '57410000-0000-4000-8000-000000000001' and version = '01.00.000' then 'DE'
        when id = '57410000-0000-4000-8000-000000000001' then ' cn '
        when id = '57410000-0000-4000-8000-000000000002' then 'ZZ'
        when id = '57410000-0000-4000-8000-000000000003' then 'GLO'
        else null end)))))
where id::text like '57410000-%';

insert into private.roles (user_id, team_id, role) values
  ('57400000-0000-4000-8000-000000000002', '00000000-0000-0000-0000-000000000000', 'review-member'),
  ('57400000-0000-4000-8000-000000000003', '00000000-0000-0000-0000-000000000000', 'review-member'),
  ('57400000-0000-4000-8000-000000000003', '57400000-0000-4000-8000-000000000010', 'review-member'),
  ('57400000-0000-4000-8000-000000000004', '57400000-0000-4000-8000-000000000010', 'review-member');

set local session_replication_role = origin;

select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claims', '{}', true);
select throws_ok(
  $$select api.qry_national_carbon_organization_contributions(10)$$,
  '28000',
  'AUTH_REQUIRED',
  'an anonymous database context cannot read contribution aggregates'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '57400000-0000-4000-8000-000000000006', true);
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"57400000-0000-4000-8000-000000000006"}',
  true
);
select throws_ok(
  $$select api.qry_national_carbon_organization_contributions(10)$$,
  '42501',
  'SYSTEM_MANAGER_REQUIRED',
  'an ordinary authenticated user cannot read contribution aggregates'
);
reset role;

create temporary table organization_contribution_result (
  snapshot jsonb not null
) on commit drop;
grant all on organization_contribution_result to authenticated;

set local role authenticated;
select set_config('request.jwt.claim.sub', '57400000-0000-4000-8000-000000000001', true);
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"57400000-0000-4000-8000-000000000001"}',
  true
);
select throws_ok(
  $$select api.qry_national_carbon_organization_contributions(0)$$,
  '22023',
  'INVALID_LIMIT',
  'the RPC rejects limits outside the bounded range'
);
insert into organization_contribution_result (snapshot)
select api.qry_national_carbon_organization_contributions(10);
reset role;

select is(snapshot ->> 'schemaVersion', 'national_carbon_organization_contribution_v4', 'process-only v4 contract') from organization_contribution_result;
select is(snapshot ->> 'datasetScope', 'process', 'scope is fixed to processes') from organization_contribution_result;
select ok(not snapshot ?| array['scopes', 'defaultScope'], 'no model or combined scopes') from organization_contribution_result;
select is(snapshot #>> '{summary,organizationCount}', '3', 'all registered normalized units count') from organization_contribution_result;
select is(snapshot #>> '{summary,publishedDatasetCount}', '3', 'latest open process IDs attributed to current units') from organization_contribution_result;
select is(snapshot #>> '{summary,pendingReviewDatasetCount}', '2', 'only latest state-20 processes count') from organization_contribution_result;
select is(snapshot #>> '{summary,reviewerCount}', '2', 'only distinct platform review-members, not admins or foreign-team memberships') from organization_contribution_result;
select ok(not (snapshot -> 'summary') ? 'publishedLast30DaysCount', 'retired 30-day KPI omitted') from organization_contribution_result;
select is(jsonb_array_length(snapshot -> 'rankings'), 1, 'chart excludes zero-published units') from organization_contribution_result;
select is(jsonb_array_length(snapshot -> 'organizations'), 3, 'table includes pending-only and zero-data units') from organization_contribution_result;
select is(snapshot #>> '{rankings,0,organizationKey}', 'acme labs', 'case and whitespace normalization retained') from organization_contribution_result;
select is(snapshot #>> '{rankings,0,publishedDatasetCount}', '3', 'model publication does not affect process rankings') from organization_contribution_result;
select is(snapshot #>> '{organizations,1,assignedReviewerDatasetCount}', '1', 'Beta assigned review counted by current version') from organization_contribution_result;
select is(snapshot #>> '{organizations,1,unassignedReviewerDatasetCount}', '1', 'Beta pending assignment counted independently') from organization_contribution_result;
select is(snapshot #>> '{organizations,2,publishedDatasetCount}', '0', 'unit with no facts is returned with zeroes') from organization_contribution_result;
select is(snapshot #>> '{regions,totalProcessCount}', '4', 'regions include every open process, including unknown organizations') from organization_contribution_result;
select is(snapshot #> '{regions,items}', '[{"locationCode":"CN","processCount":1},{"locationCode":"ZZ","processCount":1}]'::jsonb, 'latest published geography is normalized; unknown codes are retained; old DE version excluded') from organization_contribution_result;
select is(snapshot #>> '{regions,globalProcessCount}', '1', 'GLO is its own non-overlapping bucket') from organization_contribution_result;
select is(snapshot #>> '{regions,unassignedProcessCount}', '1', 'missing geography is counted separately') from organization_contribution_result;
select is((select sum((d->>'processCount')::bigint) from organization_contribution_result,
  lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') d), 9::numeric,
  'daily creation counts all process versions, regardless of state or unit; ignores models');
select ok(not exists(select 1 from organization_contribution_result,
  lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') d
  where d ?| array['modelCount','allCount']), 'daily series contains no model or combined counts');
select ok(snapshot #>> '{dailyCreation,metric}' = 'dataset_version_created_count'
  and snapshot #> '{dailyCreation,deduplicationKey}' = '["datasetType","datasetId","version"]'::jsonb
  and snapshot #>> '{dailyCreation,timezone}' = 'Asia/Shanghai', 'daily identity/timezone contract retained')
from organization_contribution_result;
select ok(snapshot #>> '{dailyCreation,startDate}' =
  (date_trunc('week', timezone('Asia/Shanghai', statement_timestamp()))::date - 364)::text
  and jsonb_array_length(snapshot #> '{dailyCreation,days}') between 365 and 371,
  'continuous 53-week daily window') from organization_contribution_result;

-- The limit applies only to chart output. More than ten units need no extra query.
set local session_replication_role = replica;
insert into auth.users (id, raw_user_meta_data)
select ('57400000-0000-4000-8000-' || lpad((100 + n)::text, 12, '0'))::uuid,
  jsonb_build_object('organization', 'Extra Unit ' || n) from generate_series(1,12) n;
insert into private.users (id, raw_user_meta_data)
select id, raw_user_meta_data from auth.users
where id::text like '57400000-%' and (raw_user_meta_data->>'organization') like 'Extra Unit %'
on conflict (id) do update set raw_user_meta_data = excluded.raw_user_meta_data;
set local session_replication_role = origin;
select is(jsonb_array_length(api.qry_national_carbon_organization_contributions(1)->'organizations'),
  15, 'all 15 units are returned even with p_limit=1');
select is(jsonb_array_length(api.qry_national_carbon_organization_contributions(1)->'rankings'),
  1, 'chart limit still enforced');
select is(api.qry_national_carbon_organization_contributions(1) #>> '{summary,organizationCount}',
  '15', 'all-unit total independent of chart');

-- Metadata changes dynamically reattribute existing facts; no owner snapshot is persisted.
update private.users set raw_user_meta_data = '{"organization":"Beta Institute"}'
where id = '57400000-0000-4000-8000-000000000002';
select is(api.qry_national_carbon_organization_contributions(10) #>> '{rankings,0,organizationKey}',
  'beta institute', 'current profile changes reattribute historical process contributions');

-- Commercial rows never enter the regional open-data count.
set local session_replication_role = replica;
update public.processes set state_code = 200
where id = '57410000-0000-4000-8000-000000000002';
select is(api.qry_national_carbon_organization_contributions(10) #>> '{regions,totalProcessCount}',
  '3', 'commercial state 200 excluded from regions');
delete from public.processes where id = '57410000-0000-4000-8000-000000000005' and version = '02.00.000';
set local session_replication_role = origin;
select is((select sum((d->>'processCount')::bigint) from jsonb_array_elements(
  api.qry_national_carbon_organization_contributions(10) #> '{dailyCreation,days}') d),
  8::numeric, 'deletion removes the version from daily history');
select * from finish();
rollback;
