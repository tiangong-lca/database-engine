CREATE OR REPLACE FUNCTION "private"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_job private.dataset_review_submit_requests%rowtype;
  v_submit_result jsonb;
  v_error_code text;
  v_error_status integer;
  v_error_message text;
  v_prev_sub text;
  v_prev_role text;
  v_prev_claims text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to submit review from a job'
    );
  end if;

  select request.*
  into v_job
  from private.dataset_review_submit_requests as request
  where request.id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if v_job.status = 'submitted' then
    return jsonb_build_object(
      'ok', true,
      'data', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.status = 'cancelled' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_CANCELLED',
      'status', 409,
      'message', 'Review-submit job was cancelled',
      'details', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_REQUESTER_REQUIRED',
      'status', 409,
      'message', 'Review-submit job has no requester'
    );
  end if;

  v_prev_sub := current_setting('request.jwt.claim.sub', true);
  v_prev_role := current_setting('request.jwt.claim.role', true);
  v_prev_claims := current_setting('request.jwt.claims', true);

  perform set_config(
    'request.jwt.claim.sub',
    v_job.requested_by::text,
    true
  );
  perform set_config('request.jwt.claim.role', 'authenticated', true);
  perform set_config(
    'request.jwt.claims',
    jsonb_build_object(
      'sub', v_job.requested_by::text,
      'role', 'authenticated'
    )::text,
    true
  );

  v_submit_result := api.cmd_review_submit(
    v_job.dataset_table,
    v_job.dataset_id,
    v_job.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'source', 'cmd_review_submit_from_job',
      'compatibility_entrypoint', true,
      'review_submit_job_id', v_job.id
    )
  );

  perform set_config(
    'request.jwt.claim.sub',
    coalesce(v_prev_sub, ''),
    true
  );
  perform set_config(
    'request.jwt.claim.role',
    coalesce(v_prev_role, ''),
    true
  );
  perform set_config(
    'request.jwt.claims',
    coalesce(v_prev_claims, ''),
    true
  );

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update private.dataset_review_submit_requests
    set status = 'submitted',
        result = v_submit_result->'data',
        last_error_code = null,
        last_error_message = null,
        last_error_details = null,
        modified_at = now(),
        completed_at = now()
    where id = v_job.id
    returning * into v_job;

    insert into private.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    ) values (
      'cmd_review_submit_from_job',
      v_job.requested_by,
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_submit_job_id', v_job.id,
        'compatibility_entrypoint', true
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', private.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_error_code := coalesce(
    v_submit_result->>'code',
    'REVIEW_SUBMIT_JOB_ERROR'
  );
  v_error_status := coalesce(
    nullif(v_submit_result->>'status', '')::integer,
    500
  );
  v_error_message := coalesce(
    v_submit_result->>'message',
    'Review-submit job failed'
  );

  update private.dataset_review_submit_requests
  set status = 'error',
      last_error_code = v_error_code,
      last_error_message = v_error_message,
      last_error_details = jsonb_build_object(
        'submitResult', v_submit_result
      ),
      modified_at = now(),
      completed_at = now()
  where id = v_job.id
  returning * into v_job;

  return jsonb_build_object(
    'ok', false,
    'code', v_error_code,
    'status', v_error_status,
    'message', v_error_message,
    'details', private.cmd_dataset_review_submit_job_payload(v_job)
  );
exception
  when others then
    perform set_config(
      'request.jwt.claim.sub',
      coalesce(v_prev_sub, ''),
      true
    );
    perform set_config(
      'request.jwt.claim.role',
      coalesce(v_prev_role, ''),
      true
    );
    perform set_config(
      'request.jwt.claims',
      coalesce(v_prev_claims, ''),
      true
    );
    raise;
end;
$$;

ALTER FUNCTION "private"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") TO "api_internal_executor";
