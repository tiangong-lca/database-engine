CREATE OR REPLACE FUNCTION "api"."svc_ai_tidas_suggestion_read"("p_requested_by" "uuid", "p_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read AI jobs'
    );
  end if;

  if p_requested_by is null or p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_JOB_NOT_FOUND',
      'status', 404,
      'message', 'AI job was not found'
    );
  end if;

  select * into v_job
  from private.worker_jobs
  where id = p_job_id
    and requested_by = p_requested_by
    and job_kind = 'ai.tidas_suggestion';

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_JOB_NOT_FOUND',
      'status', 404,
      'message', 'AI job was not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.worker_job_payload(v_job, false)
  );
end;
$$;

ALTER FUNCTION "api"."svc_ai_tidas_suggestion_read"("p_requested_by" "uuid", "p_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_ai_tidas_suggestion_read"("p_requested_by" "uuid", "p_job_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_ai_tidas_suggestion_read"("p_requested_by" "uuid", "p_job_id" "uuid") TO "service_role";
