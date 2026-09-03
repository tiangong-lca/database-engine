-- Rollback-only fixtures for an isolated local migration rebuild.
begin;
create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;
select no_plan();

create temporary table activity_bounds as
select timezone('Asia/Shanghai', statement_timestamp())::date - 30 as d,
  date_trunc('week', timezone('Asia/Shanghai', statement_timestamp()))::date - 364 as start_day,
  timezone('Asia/Shanghai', statement_timestamp())::date as end_day;

-- Isolate authored timestamps from ordinary writer triggers; no real data is touched.
set local session_replication_role = replica;
insert into auth.users (id, raw_user_meta_data)
values ('60600000-0000-4000-8000-000000000001', '{}');
insert into private.users (id, raw_user_meta_data)
values ('60600000-0000-4000-8000-000000000001', '{}');
insert into private.roles (user_id, team_id, role)
values ('60600000-0000-4000-8000-000000000001',
  '00000000-0000-0000-0000-000000000000', 'admin');

insert into public.processes (id, version, state_code, created_at, modified_at)
select ('60610000-0000-4000-8000-' || lpad(n::text, 12, '0'))::uuid,
  version, state_code, created_at, modified_at
from activity_bounds b cross join lateral (values
  -- Different UTC dates, same Shanghai date: one activity.
  (1, '01.00.000', 0, (b.d::timestamp - interval '7 hours') at time zone 'UTC',
    (b.d::timestamp + interval '10 hours') at time zone 'UTC'),
  (2, '01.00.000', 20, b.d::timestamp at time zone 'Asia/Shanghai',
    (b.d + 1)::timestamp at time zone 'Asia/Shanghai'),
  (2, '02.00.000', 0, (b.d + 1)::timestamp at time zone 'Asia/Shanghai',
    (b.d + 1)::timestamp at time zone 'Asia/Shanghai'),
  (3, '01.00.000', 100, (b.start_day - 1)::timestamp at time zone 'Asia/Shanghai',
    (b.d + 2)::timestamp at time zone 'Asia/Shanghai'),
  (4, '01.00.000', 200, null, (b.d + 3)::timestamp at time zone 'Asia/Shanghai'),
  (5, '01.00.000', 0, (b.d + 4)::timestamp at time zone 'Asia/Shanghai', null),
  (6, '01.00.000', 0, null, null),
  (7, '01.00.000', 0, b.start_day::timestamp at time zone 'Asia/Shanghai',
    b.start_day::timestamp at time zone 'Asia/Shanghai'),
  (8, '01.00.000', 0, (b.d + 5)::timestamp at time zone 'Asia/Shanghai',
    (b.end_day + 1)::timestamp at time zone 'Asia/Shanghai'),
  -- Same UTC date, different Shanghai dates: two activities.
  (9, '01.00.000', 0, ((b.d + 6)::timestamp + interval '15:59:59') at time zone 'UTC',
    ((b.d + 6)::timestamp + interval '16 hours') at time zone 'UTC'),
  (10, '01.00.000', 0, (b.end_day + 1)::timestamp at time zone 'Asia/Shanghai',
    (b.end_day + 1)::timestamp at time zone 'Asia/Shanghai'),
  (11, '01.00.000', 0, (b.end_day::timestamp + interval '23:59:59') at time zone 'Asia/Shanghai', null),
  (12, '01.00.000', 0, (b.start_day - 2)::timestamp at time zone 'Asia/Shanghai',
    (b.start_day - 1)::timestamp at time zone 'Asia/Shanghai')
) v(n, version, state_code, created_at, modified_at);

insert into public.lifecyclemodels (id, version, created_at, modified_at)
select '60610000-0000-4000-8000-000000000001', '01.00.000',
  d::timestamp at time zone 'Asia/Shanghai', (d + 8)::timestamp at time zone 'Asia/Shanghai'
from activity_bounds;
set local session_replication_role = origin;

create temporary table activity_result (snapshot jsonb);
grant all on activity_result to authenticated;
set local role authenticated;
select set_config('request.jwt.claim.sub', '60600000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claims', '{"role":"authenticated","sub":"60600000-0000-4000-8000-000000000001"}', true);
insert into activity_result select api.qry_national_carbon_organization_contributions(10);
reset role;

select is(snapshot->>'schemaVersion', 'national_carbon_organization_contribution_v5', 'explicit activity v5 contract') from activity_result;
select ok(snapshot ? 'dailyActivity' and not snapshot ? 'dailyCreation', 'creation-only field is not silently reinterpreted') from activity_result;
select is(snapshot #>> '{dailyActivity,metric}', 'dataset_version_activity_count', 'activity metric acknowledged') from activity_result;
select is(snapshot #> '{dailyActivity,deduplicationKey}', '["datasetType","datasetId","version","date"]'::jsonb, 'deduplication includes local day') from activity_result;
select is(snapshot #>> '{dailyActivity,timezone}', 'Asia/Shanghai', 'timezone is explicit') from activity_result;

create temporary view activity_days as
select (item->>'date')::date as day, (item->>'processCount')::bigint as count
from activity_result, lateral jsonb_array_elements(snapshot #> '{dailyActivity,days}') item;
create temporary table expected_activity as
select day, count from activity_bounds b cross join lateral (values
  (b.start_day, 1::bigint), (b.d, 2), (b.d + 1, 2), (b.d + 2, 1),
  (b.d + 3, 1), (b.d + 4, 1), (b.d + 5, 1), (b.d + 6, 1), (b.d + 7, 1), (b.end_day, 1)
) v(day, count);
select results_eq('select day, count from activity_days where count > 0 order by day',
  'select day, count from expected_activity order by day',
  'exact union covers versions, status/unit independence, timezone, NULL and both window boundaries');
select is((select sum(count) from activity_days), 12::numeric, 'total counts version-days, not operations or globally distinct versions');
select is((select count from activity_days, activity_bounds where day = d + 8), 0::bigint, 'model-only modification does not enter activity');
select is((select count from activity_days, activity_bounds where day = d + 9), 0::bigint, 'missing activity dates are zero-filled');
select is((select count(*) from activity_days), (select (end_day - start_day + 1)::bigint from activity_bounds), 'continuous date window unchanged');

-- Changing the session timezone must not move any local-day buckets.
set local time zone 'America/Los_Angeles';
select is(api.qry_national_carbon_organization_contributions(1)->'dailyActivity',
  (select snapshot->'dailyActivity' from activity_result), 'session timezone and chart limit do not alter activity');

set local session_replication_role = replica;
update public.processes set modified_at = (select (d + 8)::timestamp at time zone 'Asia/Shanghai' from activity_bounds)
where id = '60610000-0000-4000-8000-000000000002' and version = '01.00.000';
set local session_replication_role = origin;
update activity_result set snapshot = api.qry_national_carbon_organization_contributions(10);
select is((select count from activity_days, activity_bounds where day = d + 1), 1::bigint, 'previous modification removed; distinct version still counts');
select is((select count from activity_days, activity_bounds where day = d + 8), 1::bigint, 'latest modification replaces earlier modification');
select is((select count from activity_days, activity_bounds where day = d), 2::bigint, 'creation day preserved after later modification');

set local session_replication_role = replica;
delete from public.processes where id = '60610000-0000-4000-8000-000000000002' and version = '01.00.000';
set local session_replication_role = origin;
update activity_result set snapshot = api.qry_national_carbon_organization_contributions(10);
select is((select sum(count) from activity_days), 10::numeric, 'deletion removes both creation and latest-modification activity');
select is((select count from activity_days, activity_bounds where day = d + 8), 0::bigint, 'no deleted-version activity history retained');
select is((select count from activity_days, activity_bounds where day = d + 1), 1::bigint, 'deleting one version preserves the other');
select * from finish();
rollback;
