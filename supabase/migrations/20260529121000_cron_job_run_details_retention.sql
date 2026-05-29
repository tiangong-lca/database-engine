create or replace function util.preview_cron_job_run_details_retention(
  p_retention_window interval default interval '14 days'
) returns table (
  retention_window interval,
  cutoff_time timestamp with time zone,
  eligible_rows bigint,
  protected_open_or_running_rows bigint,
  oldest_eligible_end_time timestamp with time zone,
  newest_eligible_end_time timestamp with time zone
)
language plpgsql
stable
set search_path to ''
as $$
begin
  if p_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'cron job run details retention window must be at least 1 day';
  end if;

  return query
  select
    p_retention_window as retention_window,
    pg_catalog.now() - p_retention_window as cutoff_time,
    count(*) filter (
      where details.end_time is not null
        and details.end_time < pg_catalog.now() - p_retention_window
        and coalesce(details.status, '') <> 'running'
    )::bigint as eligible_rows,
    count(*) filter (
      where details.end_time is null
        or coalesce(details.status, '') = 'running'
    )::bigint as protected_open_or_running_rows,
    min(details.end_time) filter (
      where details.end_time is not null
        and details.end_time < pg_catalog.now() - p_retention_window
        and coalesce(details.status, '') <> 'running'
    ) as oldest_eligible_end_time,
    max(details.end_time) filter (
      where details.end_time is not null
        and details.end_time < pg_catalog.now() - p_retention_window
        and coalesce(details.status, '') <> 'running'
    ) as newest_eligible_end_time
  from cron.job_run_details as details;
end;
$$;

alter function util.preview_cron_job_run_details_retention(interval) owner to postgres;
revoke all on function util.preview_cron_job_run_details_retention(interval) from public;

create or replace function util.purge_cron_job_run_details(
  p_retention_window interval default interval '14 days'
) returns bigint
language plpgsql
set search_path to ''
as $$
declare
  deleted_count bigint;
begin
  if p_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'cron job run details retention window must be at least 1 day';
  end if;

  if not pg_catalog.pg_try_advisory_xact_lock(
    pg_catalog.hashtext('util.purge_cron_job_run_details')
  ) then
    return 0;
  end if;

  delete from cron.job_run_details as details
   where details.end_time is not null
     and details.end_time < pg_catalog.now() - p_retention_window
     and coalesce(details.status, '') <> 'running';

  get diagnostics deleted_count = row_count;
  return deleted_count;
end;
$$;

alter function util.purge_cron_job_run_details(interval) owner to postgres;
revoke all on function util.purge_cron_job_run_details(interval) from public;

do $$
begin
  if exists (select 1 from pg_extension where extname = 'pg_cron') then
    begin
      perform cron.unschedule('purge-cron-job-run-details');
    exception when others then
      null;
    end;

    perform cron.schedule(
      'purge-cron-job-run-details',
      '17 3 * * *',
      $cron$
        select util.purge_cron_job_run_details(interval '14 days');
      $cron$
    );
  end if;
end
$$;

comment on function util.preview_cron_job_run_details_retention(interval) is
  'Operator dry-run helper for pg_cron run-detail retention; reports rows older than the retention window while protecting open or running records.';

comment on function util.purge_cron_job_run_details(interval) is
  'Deletes completed pg_cron run details older than the retention window; open rows and status=running rows are protected.';
