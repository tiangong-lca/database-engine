CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_kind public.worker_job_kinds%rowtype;
  v_worker_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
  v_worker_status text;
  v_job_status text := 'waiting_gate';
  v_last_error_code text;
  v_last_error_message text;
  v_last_error_details jsonb;
  v_completed_at timestamptz;
  v_existing public.dataset_review_submit_requests%rowtype;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;
  v_state_code := coalesce(nullif(v_dataset_row->>'state_code', '')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can enqueue review submission'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again'
    );
  end if;

  if v_state_code >= 20 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review'
    );
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_request',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version,
        v_actor::text
      ),
      0
    )
  );

  select *
    into v_existing
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and revision_checksum = p_revision_checksum
    and policy_profile = p_policy_profile
    and report_schema_version = p_report_schema_version
    and requested_by = v_actor
    and status in ('queued', 'waiting_gate', 'submitting', 'submitted')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_existing)
    );
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.gate';

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 500,
      'message', 'review_submit.gate worker job kind is not registered'
    );
  end if;

  v_worker_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version,
      'revisionChecksum', p_revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', p_policy_profile,
      'reportSchemaVersion', p_report_schema_version
    ),
    'requestedBy', v_actor
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_actor::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    v_actor::text
  );

  select *
    into v_worker_existing
  from public.worker_jobs
  where worker_runtime = 'calculator'
    and job_kind = 'review_submit.gate'
    and requested_by is not distinct from v_actor
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_worker_existing.id is not null then
    v_worker_job := v_worker_existing;
  else
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = 'review_submit_gate'
      and concurrency_key = v_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1
    for update;

    if v_worker_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting review-submit gate job is already active',
        'details', public.worker_job_payload(v_worker_existing, false)
      );
    end if;

    insert into public.worker_jobs (
      job_kind,
      worker_runtime,
      worker_queue,
      priority,
      subject_type,
      subject_id,
      subject_version,
      requester_type,
      requested_by,
      idempotency_key,
      concurrency_key,
      visibility,
      max_attempts,
      payload_schema_version,
      payload_json,
      result_schema_version
    ) values (
      v_kind.job_kind,
      v_kind.worker_runtime,
      v_kind.worker_queue,
      v_kind.default_priority,
      p_table,
      p_id,
      p_version,
      'user',
      v_actor,
      v_idempotency_key,
      v_concurrency_key,
      v_kind.default_visibility,
      v_kind.default_max_attempts,
      v_kind.payload_schema_version,
      v_worker_payload,
      v_kind.result_schema_version
    )
    returning *
      into v_worker_job;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_worker_job.id,
      'enqueued',
      v_worker_job.status,
      jsonb_build_object(
        'jobKind', v_worker_job.job_kind,
        'workerQueue', v_worker_job.worker_queue,
        'idempotencyKey', v_worker_job.idempotency_key,
        'concurrencyKey', v_worker_job.concurrency_key,
        'source', 'cmd_dataset_review_submit_job_enqueue'
      )
    );
  end if;

  v_worker_status := v_worker_job.status;
  v_job_status := case
    when v_worker_status = 'completed' then 'queued'
    when v_worker_status in ('queued', 'running', 'waiting', 'stale') then 'waiting_gate'
    when v_worker_status = 'blocked' then 'blocked'
    when v_worker_status = 'cancelled' then 'cancelled'
    else 'error'
  end;

  if v_job_status = 'blocked' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_BLOCKED';
    v_last_error_message := 'Review-submit gate blocked this dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'stale' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_STALE';
    v_last_error_message := 'Review-submit gate run is stale for the submitted dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'error' then
    v_last_error_code := coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR');
    v_last_error_message := coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission');
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'cancelled' then
    v_last_error_code := 'REVIEW_SUBMIT_JOB_CANCELLED';
    v_last_error_message := 'Review-submit job was cancelled';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  end if;

  if v_job_status in ('submitted', 'blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  insert into public.dataset_review_submit_requests (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    gate_worker_job_id,
    last_error_code,
    last_error_message,
    last_error_details,
    completed_at
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_job_status,
    v_actor,
    v_worker_job.id,
    v_last_error_code,
    v_last_error_message,
    v_last_error_details,
    v_completed_at
  )
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
    'cmd_dataset_review_submit_job_enqueue',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_worker_job_id', v_worker_job.id,
      'gate_worker_job_status', v_worker_status,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_enqueue"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_audit" "jsonb") TO "authenticated";
