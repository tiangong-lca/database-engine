CREATE OR REPLACE FUNCTION "util"."preview_cron_job_run_details_retention"("p_retention_window" interval DEFAULT '14 days'::interval) RETURNS TABLE("retention_window" interval, "cutoff_time" timestamp with time zone, "eligible_rows" bigint, "protected_open_or_running_rows" bigint, "oldest_eligible_end_time" timestamp with time zone, "newest_eligible_end_time" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $$
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

ALTER FUNCTION "util"."preview_cron_job_run_details_retention"("p_retention_window" interval) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."preview_cron_job_run_details_retention"("p_retention_window" interval) FROM PUBLIC;
