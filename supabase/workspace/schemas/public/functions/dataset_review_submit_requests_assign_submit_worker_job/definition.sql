CREATE OR REPLACE FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if new.submit_worker_job_id is not null then
    return new;
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.submit';

  if v_kind.job_kind is null then
    raise exception
      using
        errcode = 'P0001',
        message = 'review_submit.submit worker job kind is not registered';
  end if;

  v_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', new.dataset_table,
      'id', new.dataset_id,
      'version', new.dataset_version,
      'revisionChecksum', new.revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', new.policy_profile,
      'reportSchemaVersion', new.report_schema_version
    ),
    'requestedBy', new.requested_by,
    'reviewSubmitJobId', new.id
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.revision_checksum,
    new.policy_profile,
    new.report_schema_version,
    new.requested_by::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.requested_by::text
  );

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by is not distinct from new.requested_by
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    new.submit_worker_job_id := v_existing.id;
    return new;
  end if;

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and worker_queue = v_kind.worker_queue
    and concurrency_key = v_concurrency_key
    and status in ('queued', 'running', 'waiting', 'stale')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    raise exception
      using
        errcode = 'P0001',
        message = 'conflicting active review-submit root worker job exists',
        detail = jsonb_build_object(
          'existingWorkerJobId', v_existing.id,
          'concurrencyKey', v_concurrency_key
        )::text;
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
    request_hash,
    concurrency_key,
    status,
    phase,
    progress,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version,
    result_ref
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_kind.default_priority,
    new.dataset_table,
    new.dataset_id,
    new.dataset_version,
    'user',
    new.requested_by,
    v_idempotency_key,
    new.revision_checksum,
    v_concurrency_key,
    'waiting',
    new.status,
    0,
    v_kind.default_visibility,
    greatest(1, v_kind.default_max_attempts),
    v_kind.payload_schema_version,
    v_payload,
    v_kind.result_schema_version,
    jsonb_build_object(
      'domainSource', 'dataset_review_submit_requests',
      'reviewSubmitJobId', new.id
    )
  )
  returning *
    into v_worker_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    details
  ) values (
    v_worker_job.id,
    'enqueued',
    v_worker_job.status,
    v_worker_job.phase,
    v_worker_job.progress,
    jsonb_build_object(
      'jobKind', v_worker_job.job_kind,
      'workerQueue', v_worker_job.worker_queue,
      'idempotencyKey', v_worker_job.idempotency_key,
      'concurrencyKey', v_worker_job.concurrency_key,
      'source', 'dataset_review_submit_requests_assign_submit_worker_job'
    )
  );

  new.submit_worker_job_id := v_worker_job.id;
  return new;
end;
$$;

ALTER FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."dataset_review_submit_requests_assign_submit_worker_job"() TO "service_role";
