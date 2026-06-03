CREATE OR REPLACE FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_job public.dataset_review_submit_requests%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_result_checksum text;
  v_dataset_found boolean;
  v_owner_id uuid;
  v_state_code integer;
  v_modified_at timestamptz;
  v_submit_result jsonb;
  v_error_code text;
  v_error_status integer;
  v_error_message text;
  v_job_status text;
  v_prev_sub text;
  v_prev_role text;
  v_prev_claims text;
  v_submit_audit jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to submit review from a job'
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

  if v_job.status = 'submitted' then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.status in ('blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', coalesce(v_job.last_error_code, 'REVIEW_SUBMIT_JOB_NOT_ACTIVE'),
      'status', 409,
      'message', coalesce(v_job.last_error_message, 'Review-submit job is not active'),
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is null and v_job.gate_run_id is null then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
          last_error_message = 'Review-submit job is missing a gate job',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
      'status', 409,
      'message', 'Review-submit job is missing a gate job',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_job.gate_worker_job_id
    for update;

    if v_worker_job.id is null then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_NOT_FOUND',
            last_error_message = 'Review-submit gate worker job not found',
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_job.dataset_table
      or v_worker_job.subject_id <> v_job.dataset_id
      or v_worker_job.subject_version <> v_job.dataset_version
      or v_worker_job.requested_by is distinct from v_job.requested_by
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_job.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_job.report_schema_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker job does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'jobKind', v_worker_job.job_kind,
              'subjectType', v_worker_job.subject_type,
              'subjectId', v_worker_job.subject_id,
              'subjectVersion', v_worker_job.subject_version,
              'requestedBy', v_worker_job.requested_by,
              'policyProfile', v_worker_job.payload_json #>> '{policy,profile}',
              'reportSchemaVersion', v_worker_job.payload_json #>> '{policy,reportSchemaVersion}'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      update public.dataset_review_submit_requests
        set status = 'waiting_gate',
            last_error_code = null,
            last_error_message = null,
            last_error_details = null,
            modified_at = now(),
            completed_at = null
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'blocked' then
      update public.dataset_review_submit_requests
        set status = 'blocked',
            last_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED',
            last_error_message = 'Review-submit gate blocked this dataset revision',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'cancelled' then
      update public.dataset_review_submit_requests
        set status = 'cancelled',
            last_error_code = 'REVIEW_SUBMIT_JOB_CANCELLED',
            last_error_message = 'Review-submit gate worker job was cancelled',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_JOB_CANCELLED',
        'status', 409,
        'message', 'Review-submit gate worker job was cancelled',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status <> 'completed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
            last_error_message = coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_ERROR',
            last_error_message = 'Review-submit gate worker job completed without a passed result',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.result_json #>> '{datasetRevision,table}' is distinct from v_job.dataset_table
      or v_worker_job.result_json #>> '{datasetRevision,id}' is distinct from v_job.dataset_id::text
      or v_worker_job.result_json #>> '{datasetRevision,version}' is distinct from v_job.dataset_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker result does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'resultDatasetRevision', v_worker_job.result_json->'datasetRevision'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker result does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    v_worker_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';

    if v_worker_result_checksum is distinct from v_job.revision_checksum then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_STALE',
            last_error_message = 'Review-submit gate worker job is stale for the submitted dataset revision',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'expectedRevisionChecksum', v_job.revision_checksum,
              'actualRevisionChecksum', v_worker_result_checksum
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker job is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;
  end if;

  execute format(
    'select true, user_id, state_code, modified_at from public.%I where id = $1 and version = $2',
    v_job.dataset_table
  )
    into v_dataset_found, v_owner_id, v_state_code, v_modified_at
    using v_job.dataset_id, v_job.dataset_version;

  if coalesce(v_dataset_found, false) is false then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_NOT_FOUND',
          last_error_message = 'Dataset not found',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_owner_id is distinct from v_job.requested_by then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_OWNER_REQUIRED',
          last_error_message = 'Only the job requester can submit this dataset for review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can submit this dataset for review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 100 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_ALREADY_PUBLISHED',
          last_error_message = 'Published datasets cannot be submitted for review again',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 20 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_UNDER_REVIEW',
          last_error_message = 'Dataset is already under review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_modified_at > v_job.created_at then
    update public.dataset_review_submit_requests
      set status = 'stale',
          last_error_code = 'REVIEW_SUBMIT_JOB_STALE',
          last_error_message = 'Dataset changed after this review-submit job was created',
          last_error_details = jsonb_build_object(
            'jobCreatedAt', to_jsonb(v_job.created_at),
            'datasetModifiedAt', to_jsonb(v_modified_at)
          ),
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_STALE',
      'status', 409,
      'message', 'Dataset changed after this review-submit job was created',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_prev_sub := current_setting('request.jwt.claim.sub', true);
  v_prev_role := current_setting('request.jwt.claim.role', true);
  v_prev_claims := current_setting('request.jwt.claims', true);

  perform set_config('request.jwt.claim.sub', v_job.requested_by::text, true);
  perform set_config('request.jwt.claim.role', 'authenticated', true);
  perform set_config(
    'request.jwt.claims',
    jsonb_build_object(
      'sub', v_job.requested_by::text,
      'role', 'authenticated'
    )::text,
    true
  );

  v_submit_audit := coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
    'source', 'cmd_review_submit_from_job',
    'review_submit_job_id', v_job.id,
    'review_submit_request_id', v_job.id,
    'review_submit_gate_worker_job_id', v_job.gate_worker_job_id
  );

  if v_job.gate_worker_job_id is not null then
    v_submit_result := public.cmd_review_submit_without_gate(
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      v_submit_audit || jsonb_build_object(
        'review_submit_revision_checksum', v_job.revision_checksum,
        'review_submit_policy_profile', v_job.policy_profile,
        'review_submit_report_schema_version', v_job.report_schema_version
      )
    );
  else
    v_submit_result := public.cmd_review_submit(
      p_table => v_job.dataset_table,
      p_id => v_job.dataset_id,
      p_version => v_job.dataset_version,
      p_audit => v_submit_audit,
      p_review_submit_gate_run_id => v_job.gate_run_id,
      p_review_submit_revision_checksum => v_job.revision_checksum,
      p_review_submit_policy_profile => v_job.policy_profile,
      p_review_submit_report_schema_version => v_job.report_schema_version
    );
  end if;

  perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
  perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
  perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update public.dataset_review_submit_requests
      set status = 'submitted',
          result = v_submit_result->'data',
          last_error_code = null,
          last_error_message = null,
          last_error_details = null,
          modified_at = now(),
          completed_at = now()
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
      'cmd_review_submit_from_job',
      v_job.requested_by,
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_submit_job_id', v_job.id,
        'review_submit_request_id', v_job.id,
        'gate_run_id', v_job.gate_run_id,
        'gate_worker_job_id', v_job.gate_worker_job_id
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_error_code := coalesce(v_submit_result->>'code', 'REVIEW_SUBMIT_JOB_ERROR');
  v_error_status := coalesce(nullif(v_submit_result->>'status', '')::integer, 500);
  v_error_message := coalesce(v_submit_result->>'message', 'Review-submit job failed');
  v_job_status := case
    when v_error_code = 'REVIEW_SUBMIT_GATE_NOT_READY' then 'waiting_gate'
    when v_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED' then 'blocked'
    when v_error_code in ('REVIEW_SUBMIT_GATE_STALE', 'REVIEW_SUBMIT_JOB_STALE') then 'stale'
    else 'error'
  end;

  update public.dataset_review_submit_requests
    set status = v_job_status,
        last_error_code = case when v_job_status = 'waiting_gate' then null else v_error_code end,
        last_error_message = case when v_job_status = 'waiting_gate' then null else v_error_message end,
        last_error_details = case
          when v_job_status = 'waiting_gate' then null
          else jsonb_build_object('submitResult', v_submit_result)
        end,
        modified_at = now(),
        completed_at = case
          when v_job_status = 'waiting_gate' then null
          else now()
        end
  where id = v_job.id
  returning *
    into v_job;

  return jsonb_build_object(
    'ok', false,
    'code', v_error_code,
    'status', v_error_status,
    'message', v_error_message,
    'details', public.cmd_dataset_review_submit_job_payload(v_job)
  );
exception
  when others then
    perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
    perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
    perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);
    raise;
end;
$_$;

ALTER FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_submit_from_job"("p_job_id" "uuid", "p_audit" "jsonb") TO "service_role";
