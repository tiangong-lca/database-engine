CREATE OR REPLACE FUNCTION "util"."purge_cron_job_run_details"("p_retention_window" interval DEFAULT '14 days'::interval) RETURNS bigint
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
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

ALTER FUNCTION "util"."purge_cron_job_run_details"("p_retention_window" interval) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."purge_cron_job_run_details"("p_retention_window" interval) FROM PUBLIC;
