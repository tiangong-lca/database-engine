CREATE OR REPLACE FUNCTION "private"."lcia_result_package_touch_task_projection"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  update private.worker_jobs
  set updated_at = clock_timestamp()
  where id = new.build_worker_job_id;
  return new;
end;
$$;

ALTER FUNCTION "private"."lcia_result_package_touch_task_projection"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_result_package_touch_task_projection"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_result_package_touch_task_projection"() TO "api_internal_executor";
