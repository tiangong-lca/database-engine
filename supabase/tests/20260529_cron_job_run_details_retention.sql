begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_empty_search_path(p_signature text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_proc p
    cross join lateral unnest(coalesce(p.proconfig, array[]::text[])) as config(setting)
    where p.oid = p_signature::regprocedure
      and config.setting in ('search_path=', 'search_path=""')
  );
$$;

select plan(17);

select ok(
  exists (select 1 from pg_extension where extname = 'pg_cron'),
  'pg_cron extension is available for retention assertions'
);

select ok(
  to_regprocedure('util.preview_cron_job_run_details_retention(interval)') is not null,
  'cron job run-detail retention preview function exists'
);

select ok(
  to_regprocedure('util.purge_cron_job_run_details(interval)') is not null,
  'cron job run-detail retention purge function exists'
);

select is(
  (
    select count(*)::integer
    from cron.job
    where jobname = 'purge-cron-job-run-details'
  ),
  1,
  'purge-cron-job-run-details is scheduled exactly once'
);

select is(
  (
    select schedule
    from cron.job
    where jobname = 'purge-cron-job-run-details'
  ),
  '17 3 * * *',
  'purge-cron-job-run-details uses the daily low-frequency cadence'
);

select is(
  (
    select active
    from cron.job
    where jobname = 'purge-cron-job-run-details'
  ),
  true,
  'purge-cron-job-run-details is active'
);

select ok(
  (
    select command like '%util.purge_cron_job_run_details%'
      and command like '%14 days%'
    from cron.job
    where jobname = 'purge-cron-job-run-details'
  ),
  'purge-cron-job-run-details uses the 14 day first-rollout retention window'
);

delete from cron.job_run_details
where jobid between 9141001 and 9141004
   or runid between -9141004 and -9141001;

insert into cron.job_run_details (
  jobid,
  runid,
  database,
  username,
  command,
  status,
  return_message,
  start_time,
  end_time
) values
  (
    9141001,
    -9141001,
    current_database(),
    current_user,
    'select 1 -- pgtap old completed retention candidate',
    'succeeded',
    'SELECT 1',
    pg_catalog.now() - interval '20 days',
    pg_catalog.now() - interval '20 days'
  ),
  (
    9141002,
    -9141002,
    current_database(),
    current_user,
    'select 1 -- pgtap recent completed retention protected',
    'succeeded',
    'SELECT 1',
    pg_catalog.now() - interval '1 day',
    pg_catalog.now() - interval '1 day'
  ),
  (
    9141003,
    -9141003,
    current_database(),
    current_user,
    'select 1 -- pgtap old open retention protected',
    'running',
    null,
    pg_catalog.now() - interval '20 days',
    null
  ),
  (
    9141004,
    -9141004,
    current_database(),
    current_user,
    'select 1 -- pgtap old running retention protected',
    'running',
    null,
    pg_catalog.now() - interval '20 days',
    pg_catalog.now() - interval '20 days'
  );

select ok(
  (
    select eligible_rows >= 1
    from util.preview_cron_job_run_details_retention(interval '14 days')
  ),
  'preview reports at least the inserted old completed run as eligible'
);

select ok(
  (
    select protected_open_or_running_rows >= 2
    from util.preview_cron_job_run_details_retention(interval '14 days')
  ),
  'preview reports open or running rows as protected'
);

create temporary table pg_temp.cron_retention_purge_result as
select util.purge_cron_job_run_details(interval '14 days') as deleted_rows;

select ok(
  (select deleted_rows >= 1 from pg_temp.cron_retention_purge_result),
  'purge deletes at least the inserted old completed run'
);

select is(
  (
    select count(*)::integer
    from cron.job_run_details
    where jobid = 9141001
  ),
  0,
  'purge deletes completed rows older than the retention window'
);

select is(
  (
    select count(*)::integer
    from cron.job_run_details
    where jobid = 9141002
  ),
  1,
  'purge keeps completed rows inside the retention window'
);

select is(
  (
    select count(*)::integer
    from cron.job_run_details
    where jobid = 9141003
  ),
  1,
  'purge keeps open rows with null end_time'
);

select is(
  (
    select count(*)::integer
    from cron.job_run_details
    where jobid = 9141004
  ),
  1,
  'purge keeps status=running rows even when end_time is populated'
);

create temporary table pg_temp.cron_retention_error_check (
  raised boolean not null
);

do $$
begin
  perform util.purge_cron_job_run_details(interval '12 hours');
  insert into pg_temp.cron_retention_error_check values (false);
exception when invalid_parameter_value then
  insert into pg_temp.cron_retention_error_check values (true);
end
$$;

select is(
  (select raised from pg_temp.cron_retention_error_check),
  true,
  'purge rejects retention windows shorter than one day'
);

select ok(
  pg_temp.has_empty_search_path('util.preview_cron_job_run_details_retention(interval)'),
  'cron retention preview function pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.purge_cron_job_run_details(interval)'),
  'cron retention purge function pins an empty search_path'
);

select * from finish();

rollback;
