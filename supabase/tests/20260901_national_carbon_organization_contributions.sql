begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(19);

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
  );

insert into private.users (id, raw_user_meta_data)
values
  ('57400000-0000-4000-8000-000000000001', '{}'),
  ('57400000-0000-4000-8000-000000000002', '{"organization":"Acme Labs"}'),
  ('57400000-0000-4000-8000-000000000003', '{"organization":"acme   labs"}'),
  ('57400000-0000-4000-8000-000000000004', '{"organization":"Beta Institute"}'),
  ('57400000-0000-4000-8000-000000000005', '{}'),
  ('57400000-0000-4000-8000-000000000006', '{}')
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
  ('57410000-0000-4000-8000-000000000003', '01.00.000', '57400000-0000-4000-8000-000000000005', 100, now() - interval '40 days', now() - interval '40 days'),
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
  ('57420000-0000-4000-8000-000000000004', '01.00.000', '57400000-0000-4000-8000-000000000005', 100, now() - interval '35 days', now() - interval '35 days');

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
  'national_carbon_organization_contribution_v1'::text,
  'the response uses the frozen v1 schema'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,process,summary,publishedDatasetCount}',
  '4'::text,
  'Process counts each dataset ID once at its latest published version'
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
  '3'::text,
  'Model returns its independent latest-published count'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,model,summary,pendingReviewDatasetCount}',
  '1'::text,
  'Model returns its independent pending-review count'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,summary,publishedDatasetCount}',
  '7'::text,
  'All keeps equal UUIDs in Process and Model as separate typed datasets'
)
from organization_contribution_result;

select is(
  snapshot #>> '{scopes,all,summary,organizationCount}',
  '2'::text,
  'participating-unit count excludes unassigned and pending-only units'
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
  round(4::numeric / 7::numeric, 6),
  'contribution share uses all published typed datasets including unassigned rows'
)
from organization_contribution_result;

select is(
  jsonb_array_length(snapshot #> '{scopes,process,rankings}'),
  1,
  'pending-only and unassigned organizations do not enter the Process ranking'
)
from organization_contribution_result;

select * from finish();

rollback;
