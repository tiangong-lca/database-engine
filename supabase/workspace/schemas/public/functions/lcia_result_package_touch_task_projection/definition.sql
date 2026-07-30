CREATE OR REPLACE FUNCTION "public"."lcia_result_package_touch_task_projection"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  update public.worker_jobs
  set updated_at = clock_timestamp()
  where id = new.build_worker_job_id;
  return new;
end;
$$;

ALTER FUNCTION "public"."lcia_result_package_touch_task_projection"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_result_package_touch_task_projection"() FROM PUBLIC;
