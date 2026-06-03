CREATE OR REPLACE FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_max_attempts" integer DEFAULT NULL::integer, "p_reason" "text" DEFAULT NULL::"text") RETURNS "jsonb"
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
      'message', 'Service role is required to retry worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status not in ('failed', 'blocked', 'stale', 'waiting', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RETRYABLE',
      'status', 409,
      'message', 'Worker job is not in a retryable status'
    );
  end if;

  update public.worker_jobs
    set status = 'queued',
        run_after = coalesce(p_run_after, now()),
        attempt_count = 0,
        max_attempts = greatest(1, coalesce(p_max_attempts, max_attempts)),
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        error_code = null,
        error_message = null,
        error_details = null,
        blocker_codes = '{}'::text[],
        resolution_scope = null,
        retryable = null,
        cancelled_at = null,
        cancelled_by = null,
        finished_at = null,
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    message,
    details
  ) values (
    v_job.id,
    'retried',
    v_job.status,
    p_reason,
    jsonb_build_object('runAfter', v_job.run_after)
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

ALTER FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."worker_retry_job"("p_job_id" "uuid", "p_run_after" timestamp with time zone, "p_max_attempts" integer, "p_reason" "text") TO "service_role";
