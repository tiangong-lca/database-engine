CREATE OR REPLACE FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text" DEFAULT NULL::"text", "p_progress" numeric DEFAULT NULL::numeric, "p_diagnostics" "jsonb" DEFAULT NULL::"jsonb", "p_lease_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to heartbeat worker jobs'
    );
  end if;

  if p_progress is not null and (p_progress < 0 or p_progress > 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PROGRESS',
      'status', 400,
      'message', 'progress must be between 0 and 1'
    );
  end if;

  if p_diagnostics is not null and jsonb_typeof(p_diagnostics) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_DIAGNOSTICS',
      'status', 400,
      'message', 'diagnostics must be a JSON object'
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

  if v_job.status <> 'running' then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RUNNING',
      'status', 409,
      'message', 'Worker job is not running'
    );
  end if;

  if v_job.lease_token is distinct from p_lease_token then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_TOKEN_MISMATCH',
      'status', 409,
      'message', 'Worker job lease token does not match'
    );
  end if;

  if v_job.lease_expires_at < now() then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_EXPIRED',
      'status', 409,
      'message', 'Worker job lease has expired'
    );
  end if;

  update public.worker_jobs
    set phase = coalesce(nullif(trim(p_phase), ''), phase),
        progress = coalesce(p_progress, progress),
        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),
        heartbeat_at = now(),
        lease_expires_at = now() + make_interval(secs => v_lease_seconds),
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    worker_id,
    lease_token,
    details
  ) values (
    v_job.id,
    'heartbeat',
    v_job.status,
    v_job.phase,
    v_job.progress,
    v_job.leased_by,
    v_job.lease_token,
    jsonb_build_object(
      'leaseExpiresAt', v_job.lease_expires_at,
      'diagnostics', coalesce(p_diagnostics, '{}'::jsonb)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

ALTER FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) TO "service_role";
