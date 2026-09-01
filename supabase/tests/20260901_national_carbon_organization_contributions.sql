begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(27);

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

select is(
  snapshot ->> 'schemaVersion',
  'national_carbon_organization_contribution_v2'::text,
  'the response uses the frozen v2 schema'
)
from organization_contribution_result;

select ok(
  snapshot #>> '{dailyCreation,metric}' = 'dataset_version_created_count'
    and snapshot #> '{dailyCreation,deduplicationKey}' =
      '["datasetType", "datasetId", "version"]'::jsonb
    and snapshot #>> '{dailyCreation,timezone}' = 'Asia/Shanghai',
  'daily creation metadata freezes the version identity and reporting timezone'
)
from organization_contribution_result;

select ok(
  snapshot #>> '{dailyCreation,startDate}' = (
    date_trunc('week', timezone('Asia/Shanghai', statement_timestamp()))::date - 364
  )::text
    and snapshot #>> '{dailyCreation,endDate}' =
      timezone('Asia/Shanghai', statement_timestamp())::date::text
    and jsonb_array_length(snapshot #> '{dailyCreation,days}') =
      timezone('Asia/Shanghai', statement_timestamp())::date
        - (
          date_trunc('week', timezone('Asia/Shanghai', statement_timestamp()))::date - 364
        ) + 1,
  'daily creation covers 53 calendar-week columns through the current Shanghai date'
)
from organization_contribution_result;

select is(
  (
    select sum((day ->> 'processCount')::bigint)
    from organization_contribution_result
    cross join lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') as day
  ),
  9::numeric,
  'daily Process creation counts every retained Process id-version row regardless of state or organization'
);

select is(
  (
    select sum((day ->> 'modelCount')::bigint)
    from organization_contribution_result
    cross join lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') as day
  ),
  5::numeric,
  'daily Model creation counts every retained LifecycleModel id-version row'
);

select is(
  (
    select sum((day ->> 'allCount')::bigint)
    from organization_contribution_result
    cross join lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') as day
  ),
  14::numeric,
  'daily All creation keeps Process and Model identities separate before summing them'
);

select is(
  (
    select day - 'date'
    from organization_contribution_result
    cross join lateral jsonb_array_elements(snapshot #> '{dailyCreation,days}') as day
    where day ->> 'date' = (
      timezone('Asia/Shanghai', statement_timestamp())::date - 2
    )::text
  ),
  '{"processCount": 1, "modelCount": 1, "allCount": 2}'::jsonb,
  'the same local day reports separate Process and Model counts plus their total'
);

select is(
  snapshot #>> '{scopes,process,summary,publishedDatasetCount}',
  '3'::text,
  'Process published total counts latest-published IDs assigned to a current organization'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,process,summary,pendingReviewDatasetCount}',
  '2'::text,
  'Process pending review includes only IDs whose current latest version is state 20'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,model,summary,publishedDatasetCount}',
  '2'::text,
  'Model published total excludes a latest-published ID without a current organization'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,model,summary,pendingReviewDatasetCount}',
  '1'::text,
  'Model pending-review total excludes a current state-20 ID without an organization'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,summary,publishedDatasetCount}',
  '5'::text,
  'All keeps typed datasets separate while excluding rows without a current organization'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,summary,organizationCount}',
  '3'::text,
  'participating-unit count includes every current organization even without contribution facts'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,summary,publishedLast30DaysCount}',
  '5'::text,
  'last-30-day published total includes only rows assigned to a current organization'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,rankings,0,organizationKey}',
  'acme labs'::text,
  'organization normalization merges casing and repeated whitespace'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,rankings,0,publishedDatasetCount}',
  '4'::text,
  'the combined ranking aggregates published Process and Model facts'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,rankings,0,contributorCount}',
  '2'::text,
  'combined contributors are distinct across Process and Model'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,rankings,1,reviewingDatasetCount}',
  '2'::text,
  'reviewing rows use the contributor current organization independently of published rows'
)
from organization_contribution_result;

select cmp_ok(
  (snapshot #>> '{scopes,all,rankings,0,contributionShare}')::numeric,
  '=',
  round(4::numeric / 5::numeric, 6),
  'contribution share uses the organization-attributed published total'
)
from organization_contribution_result;

select is(
  jsonb_array_length(snapshot #> '{scopes,process,rankings}'),
  1,
  'pending-only and unassigned organizations do not enter the Process ranking'
)
from organization_contribution_result;

delete from public.processes
where id = '57410000-0000-4000-8000-000000000005'
  and version = '02.00.000';

set local role authenticated;
select set_config('request.jwt.claim.sub', '57400000-0000-4000-8000-000000000001', true);
select set_config(
  'request.jwt.claims',
  '{"role":"authenticated","sub":"57400000-0000-4000-8000-000000000001"}',
  true
);
select is(
  (
    select sum((day ->> 'allCount')::bigint)
    from jsonb_array_elements(
      api.qry_national_carbon_organization_contributions(10) #> '{dailyCreation,days}'
    ) as day
  ),
  13::numeric,
  'physical deletion removes the version from historical daily creation counts'
);
reset role;

select * from finish();

rollback;
