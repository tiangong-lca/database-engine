CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_result" "jsonb" DEFAULT NULL::"jsonb", "p_error_code" "text" DEFAULT NULL::"text", "p_error_message" "text" DEFAULT NULL::"text", "p_error_details" "jsonb" DEFAULT NULL::"jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_job public.dataset_review_submit_requests%rowtype;
  v_gate public.dataset_review_submit_gate_runs%rowtype;
  v_completed_at timestamptz;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record review-submit job results'
    );
  end if;

  if v_status not in ('waiting_gate', 'blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_STATUS',
      'status', 400,
      'message', 'status must be waiting_gate, blocked, stale, error, or cancelled'
    );
  end if;

  if p_result is not null and jsonb_typeof(p_result) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_gate
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id;

    if v_gate.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_gate.dataset_table <> v_job.dataset_table
      or v_gate.dataset_id <> v_job.dataset_id
      or v_gate.dataset_version <> v_job.dataset_version
      or v_gate.revision_checksum <> v_job.revision_checksum
      or v_gate.policy_profile <> v_job.policy_profile
      or v_gate.report_schema_version <> v_job.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run does not match this review-submit job'
      );
    end if;
  end if;

  if v_status in ('blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  update public.dataset_review_submit_requests
    set status = v_status,
        gate_run_id = coalesce(p_gate_run_id, gate_run_id),
        result = p_result,
        last_error_code =
          case when v_status = 'waiting_gate' then null else p_error_code end,
        last_error_message =
          case when v_status = 'waiting_gate' then null else p_error_message end,
        last_error_details =
          case when v_status = 'waiting_gate' then null else p_error_details end,
        modified_at = now(),
        completed_at = v_completed_at
  where id = v_job.id
  returning *
    into v_job;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_job_record_result',
    v_job.requested_by,
    v_job.dataset_table,
    v_job.dataset_id,
    v_job.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_run_id', v_job.gate_run_id,
      'status', v_status,
      'error_code', p_error_code
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_record_result"("p_job_id" "uuid", "p_status" "text", "p_gate_run_id" "uuid", "p_result" "jsonb", "p_error_code" "text", "p_error_message" "text", "p_error_details" "jsonb", "p_audit" "jsonb") TO "service_role";
