CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_action" "text" DEFAULT 'ensure'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_kind public.worker_job_kinds%rowtype;
  v_worker_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required for review-submit gate worker jobs'
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

  v_worker_payload := jsonb_strip_nulls(
    jsonb_build_object(
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
      'requestedBy', p_requested_by,
      'gateRunId', p_gate_run_id
    )
  );

  v_idempotency_key := case
    when v_action = 'rerun' and p_gate_run_id is not null then concat_ws(
      ':',
      'review_submit.gate.rerun',
      p_gate_run_id::text
    )
    else concat_ws(
      ':',
      'review_submit.gate',
      p_table,
      p_id::text,
      p_version,
      p_revision_checksum,
      p_policy_profile,
      p_report_schema_version,
      p_requested_by::text
    )
  end;

  v_concurrency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    p_requested_by::text
  );

  if v_action = 'rerun' then
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1
    for update;
  else
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale', 'blocked', 'completed')
    order by created_at desc
    limit 1
    for update;
  end if;

  if v_worker_existing.id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', public.worker_job_payload(v_worker_existing, true),
      'reused', true
    );
  end if;

  select *
    into v_worker_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and worker_queue = v_kind.worker_queue
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
    p_requested_by,
    v_idempotency_key,
    v_concurrency_key,
    v_kind.default_visibility,
    greatest(1, coalesce(v_kind.default_max_attempts, 3)),
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
      'gateRunId', p_gate_run_id,
      'source', 'cmd_dataset_review_submit_gate'
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_worker_job, true),
    'reused', false
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_enqueue_worker_job"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_requested_by" "uuid", "p_gate_run_id" "uuid", "p_action" "text") TO "service_role";
