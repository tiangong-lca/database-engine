CREATE OR REPLACE FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, p_include_internal)
  );
end;
$$;

ALTER FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."worker_read_job"("p_job_id" "uuid", "p_include_internal" boolean) TO "service_role";
