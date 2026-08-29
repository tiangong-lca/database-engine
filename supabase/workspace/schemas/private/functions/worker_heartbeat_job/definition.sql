CREATE OR REPLACE FUNCTION "private"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text" DEFAULT NULL::"text", "p_progress" numeric DEFAULT NULL::numeric, "p_diagnostics" "jsonb" DEFAULT NULL::"jsonb", "p_lease_seconds" integer DEFAULT 300) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_phase text;
  v_progress numeric;
  v_diagnostics jsonb;
  v_business_changed boolean;
  v_emit_event boolean;
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
  from private.worker_jobs
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

  v_phase := coalesce(nullif(trim(p_phase), ''), v_job.phase);
  v_progress := coalesce(p_progress, v_job.progress);
  v_diagnostics := v_job.diagnostics || coalesce(p_diagnostics, '{}'::jsonb);
  v_business_changed := v_phase is distinct from v_job.phase
    or v_progress is distinct from v_job.progress
    or v_diagnostics is distinct from v_job.diagnostics;
  v_emit_event := v_phase is distinct from v_job.phase
    or (v_job.progress is null and p_progress is not null)
    or (
      v_job.progress is not null
      and p_progress is not null
      and floor(p_progress * 20) > floor(v_job.progress * 20)
    );

  update private.worker_jobs
    set phase = v_phase,
        progress = v_progress,
        diagnostics = v_diagnostics,
        heartbeat_at = now(),
        lease_expires_at = now() + make_interval(secs => v_lease_seconds),
        updated_at = case when v_business_changed then now() else updated_at end
  where id = v_job.id
  returning *
    into v_job;

  if v_emit_event then
    insert into private.worker_job_events (
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
      jsonb_build_object('leaseExpiresAt', v_job.lease_expires_at)
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'eventEmitted', v_emit_event,
    'data', private.worker_job_payload(v_job, true)
  );
end;
$$;

ALTER FUNCTION "private"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) TO "service_role";

GRANT ALL ON FUNCTION "private"."worker_heartbeat_job"("p_job_id" "uuid", "p_lease_token" "uuid", "p_phase" "text", "p_progress" numeric, "p_diagnostics" "jsonb", "p_lease_seconds" integer) TO "api_internal_executor";
