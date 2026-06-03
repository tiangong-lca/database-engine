create or replace function public.cmd_dataset_review_submit_gate_payload(
  p_run public.dataset_review_submit_gate_runs,
  p_status_override text default null
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  with worker as (
    select w.*
    from public.worker_jobs as w
    where w.id = (p_run).worker_job_id
    limit 1
  ),
  shaped as (
    select
      worker.id as worker_job_id,
      case
        when p_status_override is not null then p_status_override
        when worker.id is null then (p_run).status
        when worker.status in ('queued', 'waiting', 'stale') then 'queued'
        when worker.status = 'running' then 'running'
        when worker.status = 'blocked' then 'blocked'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'passed'
          then 'passed'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'blocked'
          then 'blocked'
        when worker.status in ('completed', 'failed', 'cancelled') then 'error'
        else (p_run).status
      end as effective_status,
      case
        when (p_run).calculator_report is not null then
          jsonb_build_object(
            'schemaVersion',
            coalesce((p_run).report_schema_version, worker.result_schema_version)
          ) || (p_run).calculator_report
        when worker.id is not null
          and jsonb_typeof(worker.result_json->'calculatorReport') = 'object'
          then jsonb_build_object(
            'schemaVersion',
            coalesce(
              worker.result_json #>> '{calculatorReport,schemaVersion}',
              (p_run).report_schema_version,
              worker.result_schema_version
            )
          ) || (worker.result_json->'calculatorReport')
        else null::jsonb
      end as effective_calculator_report,
      coalesce(
        case
          when jsonb_typeof(worker.result_json->'blockingReasons') = 'array'
            then worker.result_json->'blockingReasons'
          else null::jsonb
        end,
        case
          when cardinality(worker.blocker_codes) > 0 then (
            select jsonb_agg(jsonb_build_object('code', code) order by code)
            from unnest(worker.blocker_codes) as code
          )
          else null::jsonb
        end,
        (p_run).blocking_reasons,
        '[]'::jsonb
      ) as effective_blocking_reasons,
      greatest(
        (p_run).modified_at,
        coalesce(worker.updated_at, (p_run).modified_at)
      ) as effective_modified_at,
      coalesce(
        (p_run).completed_at,
        case
          when worker.status in ('completed', 'blocked', 'failed', 'cancelled')
            then worker.finished_at
          else null::timestamptz
        end
      ) as effective_completed_at
    from (select 1) as seed
    left join worker on true
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', shaped.effective_status,
      'gateRunId', (p_run).id,
      'workerJobId', shaped.worker_job_id,
      'workerJob',
        case
          when shaped.worker_job_id is null then null
          else (
            select public.worker_job_payload(worker, false)
            from worker
          )
        end,
      'datasetRevision', jsonb_build_object(
        'table', (p_run).dataset_table,
        'id', (p_run).dataset_id,
        'version', (p_run).dataset_version,
        'revisionChecksum', (p_run).revision_checksum
      ),
      'policy', jsonb_build_object(
        'profile', (p_run).policy_profile,
        'reportSchemaVersion', (p_run).report_schema_version
      ),
      'calculatorReport', shaped.effective_calculator_report,
      'blockingReasons', shaped.effective_blocking_reasons,
      'createdAt', to_jsonb((p_run).created_at),
      'modifiedAt', to_jsonb(shaped.effective_modified_at),
      'completedAt', to_jsonb(shaped.effective_completed_at)
    )
  )
  from shaped
$$;

create or replace function public.cmd_dataset_review_submit_gate_enqueue_worker_job(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text,
  p_policy_profile text,
  p_report_schema_version text,
  p_requested_by uuid,
  p_gate_run_id uuid default null,
  p_action text default 'ensure'
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
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

create or replace function public.cmd_dataset_review_submit_gate_link_worker_job(
  p_gate_run_id uuid,
  p_action text default 'ensure'
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_result jsonb;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_job_id uuid;
  v_status text;
  v_calculator_report jsonb;
  v_blocking_reasons jsonb;
begin
  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  if v_run.worker_job_id is null and v_run.status in ('queued', 'running') then
    v_worker_result := public.cmd_dataset_review_submit_gate_enqueue_worker_job(
      v_run.dataset_table,
      v_run.dataset_id,
      v_run.dataset_version,
      v_run.revision_checksum,
      v_run.policy_profile,
      v_run.report_schema_version,
      v_run.requested_by,
      v_run.id,
      p_action
    );

    if coalesce((v_worker_result->>'ok')::boolean, false) is false then
      return v_worker_result;
    end if;

    v_worker_job_id := nullif(v_worker_result->'data'->>'id', '')::uuid;

    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_worker_job_id;

    v_status := case
      when v_worker_job.status in ('queued', 'waiting', 'stale') then 'queued'
      when v_worker_job.status = 'running' then 'running'
      when v_worker_job.status = 'blocked' then 'blocked'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'passed'
        then 'passed'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'blocked'
        then 'blocked'
      when v_worker_job.status in ('completed', 'failed', 'cancelled') then 'error'
      else v_run.status
    end;

    v_calculator_report := case
      when jsonb_typeof(v_worker_job.result_json->'calculatorReport') = 'object'
        then v_worker_job.result_json->'calculatorReport'
      else v_run.calculator_report
    end;

    v_blocking_reasons := coalesce(
      case
        when jsonb_typeof(v_worker_job.result_json->'blockingReasons') = 'array'
          then v_worker_job.result_json->'blockingReasons'
        else null::jsonb
      end,
      v_run.blocking_reasons,
      '[]'::jsonb
    );

    update public.dataset_review_submit_gate_runs
      set worker_job_id = v_worker_job.id,
          status = v_status,
          calculator_report = v_calculator_report,
          blocking_reasons = v_blocking_reasons,
          modified_at = now(),
          completed_at = case
            when v_status in ('passed', 'blocked', 'error') then coalesce(v_worker_job.finished_at, now())
            else completed_at
          end
    where id = v_run.id
    returning *
      into v_run;
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_gate(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1',
  p_action text default 'ensure',
  p_gate_run_id uuid default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_existing public.dataset_review_submit_gate_runs%rowtype;
  v_supersedes uuid;
  v_link_result jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review-submit gate'
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

  if v_action not in ('ensure', 'read', 'rerun') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_ACTION',
      'status', 400,
      'message', 'action must be ensure, read, or rerun'
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

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can run or read the review-submit gate'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_existing
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id
    for update;

    if v_existing.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_existing.dataset_table <> p_table
      or v_existing.dataset_id <> p_id
      or v_existing.dataset_version <> p_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run belongs to a different dataset revision'
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(
          v_existing,
          case
            when v_existing.revision_checksum <> p_revision_checksum then 'stale'
            else null
          end
        )
      );
    end if;

    if v_action = 'ensure' and v_existing.revision_checksum = p_revision_checksum then
      return public.cmd_dataset_review_submit_gate_link_worker_job(v_existing.id, 'ensure');
    end if;

    v_supersedes := v_existing.id;
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_gate',
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

  if v_action in ('ensure', 'read') and p_gate_run_id is null then
    select *
      into v_run
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and revision_checksum = p_revision_checksum
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1
    for update;

    if v_run.id is not null then
      if v_action = 'ensure' then
        return public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, 'ensure');
      end if;

      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;
  end if;

  if v_action = 'rerun' and p_gate_run_id is null then
    select id
      into v_supersedes
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1;
  end if;

  insert into public.dataset_review_submit_gate_runs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    supersedes_gate_run_id
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    'queued',
    v_actor,
    v_supersedes
  )
  returning *
    into v_run;

  v_link_result := public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, v_action);

  if coalesce((v_link_result->>'ok')::boolean, false) is false then
    return v_link_result;
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = v_run.id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', v_action,
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version,
      'supersedes_gate_run_id', v_supersedes
    )
  );

  return v_link_result;
end;
$$;

create or replace function public.cmd_dataset_review_submit_gate_record_result(
  p_gate_run_id uuid,
  p_status text,
  p_calculator_report jsonb default null,
  p_blocking_reasons jsonb default '[]'::jsonb,
  p_report_schema_version text default 'review_submit_gate_report.v1',
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_status text := lower(trim(coalesce(p_status, '')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_status text;
  v_worker_result jsonb;
  v_blocker_codes text[];
begin
  if v_status not in ('passed', 'blocked', 'error') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT_STATUS',
      'status', 400,
      'message', 'result status must be passed, blocked, or error'
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

  if p_calculator_report is not null and jsonb_typeof(p_calculator_report) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_REPORT',
      'status', 400,
      'message', 'calculator report must be a JSON object'
    );
  end if;

  if jsonb_typeof(coalesce(p_blocking_reasons, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_BLOCKING_REASONS',
      'status', 400,
      'message', 'blockingReasons must be a JSON array'
    );
  end if;

  if v_status = 'passed' and jsonb_array_length(coalesce(p_blocking_reasons, '[]'::jsonb)) > 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT',
      'status', 400,
      'message', 'passed gate results cannot include blockingReasons'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  update public.dataset_review_submit_gate_runs
    set status = v_status,
        calculator_report = p_calculator_report,
        blocking_reasons = coalesce(p_blocking_reasons, '[]'::jsonb),
        report_schema_version = p_report_schema_version,
        modified_at = now(),
        completed_at = now()
  where id = p_gate_run_id
  returning *
    into v_run;

  if v_run.worker_job_id is not null then
    v_worker_status := case
      when v_status = 'passed' then 'completed'
      when v_status = 'blocked' then 'blocked'
      else 'failed'
    end;

    select coalesce(
      array_agg(distinct nullif(reason->>'code', '')) filter (where nullif(reason->>'code', '') is not null),
      '{}'::text[]
    )
      into v_blocker_codes
    from jsonb_array_elements(coalesce(p_blocking_reasons, '[]'::jsonb)) as reason;

    if v_worker_status = 'blocked' and cardinality(v_blocker_codes) = 0 then
      v_blocker_codes := array['review_submit_gate_blocked'];
    end if;

    v_worker_result := jsonb_strip_nulls(
      jsonb_build_object(
        'status', v_status,
        'datasetRevision', jsonb_build_object(
          'table', v_run.dataset_table,
          'id', v_run.dataset_id,
          'version', v_run.dataset_version,
          'revisionChecksum', v_run.revision_checksum
        ),
        'policy', jsonb_build_object(
          'profile', v_run.policy_profile,
          'reportSchemaVersion', p_report_schema_version
        ),
        'calculatorReport', p_calculator_report,
        'blockingReasons', coalesce(p_blocking_reasons, '[]'::jsonb),
        'gateRunId', v_run.id,
        'recordedBy', 'cmd_dataset_review_submit_gate_record_result'
      )
    );

    update public.worker_jobs
      set status = v_worker_status,
          result_json = case
            when v_worker_status in ('completed', 'blocked') then v_worker_result
            else result_json
          end,
          result_schema_version = coalesce(result_schema_version, 'review_submit.gate.result.v1'),
          error_code = case
            when v_worker_status = 'failed' then 'REVIEW_SUBMIT_GATE_ERROR'
            else null
          end,
          error_message = case
            when v_worker_status = 'failed' then 'Review-submit gate failed before review submission'
            else null
          end,
          blocker_codes = case
            when v_worker_status = 'blocked' then v_blocker_codes
            else '{}'::text[]
          end,
          resolution_scope = case
            when v_worker_status = 'blocked' then 'user'
            else null
          end,
          retryable = case
            when v_worker_status = 'failed' then true
            when v_worker_status in ('completed', 'blocked') then false
            else retryable
          end,
          updated_at = now(),
          finished_at = now()
    where id = v_run.worker_job_id;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_run.worker_job_id,
      'legacy_gate_result_recorded',
      v_worker_status,
      jsonb_build_object(
        'gateRunId', v_run.id,
        'gateStatus', v_status,
        'source', 'cmd_dataset_review_submit_gate_record_result'
      )
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate_record_result',
    coalesce(v_actor, v_run.requested_by),
    v_run.dataset_table,
    v_run.dataset_id,
    v_run.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'status', v_status,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

create or replace function public.cmd_dataset_assert_review_submit_gate_passed(
  p_table text,
  p_id uuid,
  p_version text,
  p_gate_run_id uuid default null,
  p_revision_checksum text default null,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1'
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_result_checksum text;
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
    return jsonb_build_object('ok', true);
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

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if p_gate_run_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_REQUIRED',
      'status', 400,
      'message', 'A passed review-submit gate run is required before process review submission'
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

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  v_payload := public.cmd_dataset_review_submit_gate_payload(v_run);

  if v_run.dataset_table <> p_table
    or v_run.dataset_id <> p_id
    or v_run.dataset_version <> p_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run belongs to a different dataset revision'
    );
  end if;

  if v_run.revision_checksum <> p_revision_checksum then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_STALE',
      'status', 409,
      'message', 'Review-submit gate run is stale for the submitted dataset revision',
      'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
    );
  end if;

  if v_run.policy_profile <> coalesce(p_policy_profile, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different policy profile',
      'details', jsonb_build_object(
        'expected', p_policy_profile,
        'actual', v_run.policy_profile
      )
    );
  end if;

  if v_run.report_schema_version <> coalesce(p_report_schema_version, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different report schema version',
      'details', jsonb_build_object(
        'expected', p_report_schema_version,
        'actual', v_run.report_schema_version
      )
    );
  end if;

  if v_run.worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_run.worker_job_id;

    if v_worker_job.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', v_payload
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_run.dataset_table
      or v_worker_job.subject_id <> v_run.dataset_id
      or v_worker_job.subject_version <> v_run.dataset_version
      or v_worker_job.requested_by is distinct from v_run.requested_by
      or v_worker_job.payload_json #>> '{datasetRevision,revisionChecksum}' is distinct from v_run.revision_checksum
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_run.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_run.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this gate run',
        'details', v_payload
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    end if;

    if v_worker_job.status = 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    end if;

    if v_worker_job.status <> 'completed' then
      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', v_payload
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', v_payload
      );
    end if;

    v_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';
    if v_result_checksum is not null and v_result_checksum <> p_revision_checksum then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker result is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'data', v_payload
    );
  end if;

  case v_run.status
    when 'passed' then
      return jsonb_build_object(
        'ok', true,
        'data', v_payload
      );
    when 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    when 'queued', 'running' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    when 'error' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate failed before review submission',
        'details', v_payload
      );
    else
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate run is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
  end case;
end;
$$;

revoke all on function public.cmd_dataset_review_submit_gate_enqueue_worker_job(
  text,
  uuid,
  text,
  text,
  text,
  text,
  uuid,
  uuid,
  text
) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_gate_link_worker_job(
  uuid,
  text
) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_gate_payload(
  public.dataset_review_submit_gate_runs,
  text
) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_gate_record_result(
  uuid,
  text,
  jsonb,
  jsonb,
  text,
  jsonb
) from public, anon, authenticated;
revoke all on function public.cmd_dataset_assert_review_submit_gate_passed(
  text,
  uuid,
  text,
  uuid,
  text,
  text,
  text
) from public, anon, authenticated;

grant execute on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_gate_record_result(
  uuid,
  text,
  jsonb,
  jsonb,
  text,
  jsonb
) to service_role;
grant execute on function public.cmd_dataset_assert_review_submit_gate_passed(
  text,
  uuid,
  text,
  uuid,
  text,
  text,
  text
) to authenticated, service_role;

comment on function public.cmd_dataset_review_submit_gate(
  text,
  uuid,
  text,
  text,
  text,
  text,
  text,
  uuid,
  jsonb
) is 'User-facing review-submit gate RPC. Standalone ensure/rerun now creates or reuses review_submit.gate worker_jobs and links them to retained gate history rows.';
