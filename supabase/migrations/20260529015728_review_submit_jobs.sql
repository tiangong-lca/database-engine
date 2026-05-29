create table if not exists public.dataset_review_submit_jobs (
  id uuid primary key default gen_random_uuid(),
  dataset_table text not null,
  dataset_id uuid not null,
  dataset_version text not null,
  revision_checksum text not null,
  policy_profile text not null default 'review_submit_fast.v1',
  report_schema_version text not null default 'review_submit_gate_report.v1',
  status text not null default 'queued',
  requested_by uuid not null,
  gate_run_id uuid references public.dataset_review_submit_gate_runs(id),
  attempt_count integer not null default 0,
  last_error_code text,
  last_error_message text,
  last_error_details jsonb,
  result jsonb,
  created_at timestamptz not null default now(),
  modified_at timestamptz not null default now(),
  completed_at timestamptz,
  constraint dataset_review_submit_jobs_table_check
    check (dataset_table in ('processes')),
  constraint dataset_review_submit_jobs_checksum_check
    check (revision_checksum ~ '^[a-f0-9]{64}$'),
  constraint dataset_review_submit_jobs_status_check
    check (status in (
      'queued',
      'waiting_gate',
      'submitting',
      'submitted',
      'blocked',
      'stale',
      'error',
      'cancelled'
    )),
  constraint dataset_review_submit_jobs_attempt_count_check
    check (attempt_count >= 0),
  constraint dataset_review_submit_jobs_last_error_details_check
    check (last_error_details is null or jsonb_typeof(last_error_details) = 'object'),
  constraint dataset_review_submit_jobs_result_check
    check (result is null or jsonb_typeof(result) = 'object')
);

alter table public.dataset_review_submit_jobs enable row level security;

create unique index if not exists dataset_review_submit_jobs_active_revision_uidx
  on public.dataset_review_submit_jobs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    requested_by
  )
  where status in ('queued', 'waiting_gate', 'submitting');

create index if not exists dataset_review_submit_jobs_requested_by_idx
  on public.dataset_review_submit_jobs (requested_by, created_at desc);

create index if not exists dataset_review_submit_jobs_status_idx
  on public.dataset_review_submit_jobs (status, modified_at asc, created_at asc);

create index if not exists dataset_review_submit_jobs_gate_run_idx
  on public.dataset_review_submit_jobs (gate_run_id)
  where gate_run_id is not null;

revoke all on public.dataset_review_submit_jobs from anon, authenticated;
grant all on public.dataset_review_submit_jobs to service_role;

create or replace function public.cmd_dataset_review_submit_job_payload(
  p_job public.dataset_review_submit_jobs
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', (p_job).status,
      'reviewSubmitJobId', (p_job).id,
      'gateRunId', (p_job).gate_run_id,
      'datasetRevision', jsonb_build_object(
        'table', (p_job).dataset_table,
        'id', (p_job).dataset_id,
        'version', (p_job).dataset_version,
        'revisionChecksum', (p_job).revision_checksum
      ),
      'policy', jsonb_build_object(
        'profile', (p_job).policy_profile,
        'reportSchemaVersion', (p_job).report_schema_version
      ),
      'requestedBy', (p_job).requested_by,
      'attemptCount', (p_job).attempt_count,
      'error',
        case
          when (p_job).last_error_code is null
            and (p_job).last_error_message is null
            and (p_job).last_error_details is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'code', (p_job).last_error_code,
              'message', (p_job).last_error_message,
              'details', (p_job).last_error_details
            )
          )
        end,
      'result', (p_job).result,
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = (p_job).gate_run_id
        ),
      'createdAt', to_jsonb((p_job).created_at),
      'modifiedAt', to_jsonb((p_job).modified_at),
      'completedAt', to_jsonb((p_job).completed_at)
    )
  )
$$;

create or replace function public.cmd_dataset_review_submit_job_enqueue(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1',
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
  v_state_code integer;
  v_gate_result jsonb;
  v_gate_run_id uuid;
  v_gate_status text;
  v_job_status text;
  v_last_error_code text;
  v_last_error_message text;
  v_last_error_details jsonb;
  v_completed_at timestamptz;
  v_existing public.dataset_review_submit_jobs%rowtype;
  v_job public.dataset_review_submit_jobs%rowtype;
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
        'review_submit_job',
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
  from public.dataset_review_submit_jobs
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

  v_gate_result := public.cmd_dataset_review_submit_gate(
    p_table => p_table,
    p_id => p_id,
    p_version => p_version,
    p_revision_checksum => p_revision_checksum,
    p_policy_profile => p_policy_profile,
    p_report_schema_version => p_report_schema_version,
    p_action => 'ensure',
    p_audit => coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'source', 'cmd_dataset_review_submit_job_enqueue'
    )
  );

  if coalesce((v_gate_result->>'ok')::boolean, false) is false then
    return v_gate_result;
  end if;

  v_gate_run_id := (v_gate_result->'data'->>'gateRunId')::uuid;
  v_gate_status := v_gate_result->'data'->>'status';
  v_job_status := case
    when v_gate_status in ('queued', 'running') then 'waiting_gate'
    when v_gate_status = 'passed' then 'queued'
    when v_gate_status = 'blocked' then 'blocked'
    when v_gate_status = 'stale' then 'stale'
    else 'error'
  end;

  if v_job_status = 'blocked' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_BLOCKED';
    v_last_error_message := 'Review-submit gate blocked this dataset revision';
    v_last_error_details := v_gate_result->'data';
  elsif v_job_status = 'stale' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_STALE';
    v_last_error_message := 'Review-submit gate run is stale for the submitted dataset revision';
    v_last_error_details := v_gate_result->'data';
  elsif v_job_status = 'error' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_ERROR';
    v_last_error_message := 'Review-submit gate failed before review submission';
    v_last_error_details := v_gate_result->'data';
  end if;

  if v_job_status in ('submitted', 'blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  insert into public.dataset_review_submit_jobs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    gate_run_id,
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
    v_gate_run_id,
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
      'gate_run_id', v_gate_run_id,
      'gate_status', v_gate_status,
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
$$;

create or replace function public.cmd_dataset_review_submit_job_read(
  p_job_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_is_service boolean := coalesce(util.is_service_request(), false);
  v_job public.dataset_review_submit_jobs%rowtype;
begin
  if p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_ID_REQUIRED',
      'status', 400,
      'message', 'reviewSubmitJobId is required'
    );
  end if;

  if v_actor is null and not v_is_service then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_jobs
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if not v_is_service and v_job.requested_by is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can read this review-submit job'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_read_latest(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_job public.dataset_review_submit_jobs%rowtype;
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

  if p_revision_checksum is not null
    and p_revision_checksum !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
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
      'message', 'Only the dataset owner can read review-submit jobs'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_jobs
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and requested_by = v_actor
    and (p_revision_checksum is null or revision_checksum = p_revision_checksum)
  order by created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_claim(
  p_qty integer default 10,
  p_stale_submitting_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_qty integer := greatest(1, least(coalesce(p_qty, 10), 50));
  v_stale_seconds integer := greatest(30, coalesce(p_stale_submitting_seconds, 300));
  v_jobs jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim review-submit jobs'
    );
  end if;

  with claimed as (
    select id
    from public.dataset_review_submit_jobs
    where status in ('queued', 'waiting_gate')
      or (
        status = 'submitting'
        and modified_at < now() - make_interval(secs => v_stale_seconds)
      )
    order by
      case status
        when 'queued' then 0
        when 'waiting_gate' then 1
        else 2
      end,
      created_at asc
    for update skip locked
    limit v_qty
  ),
  updated as (
    update public.dataset_review_submit_jobs as j
      set status = 'submitting',
          attempt_count = j.attempt_count + 1,
          modified_at = now()
    from claimed
    where j.id = claimed.id
    returning j.*
  )
  select coalesce(jsonb_agg(public.cmd_dataset_review_submit_job_payload(updated)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_record_result(
  p_job_id uuid,
  p_status text,
  p_gate_run_id uuid default null,
  p_result jsonb default null,
  p_error_code text default null,
  p_error_message text default null,
  p_error_details jsonb default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_job public.dataset_review_submit_jobs%rowtype;
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
  from public.dataset_review_submit_jobs
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

  update public.dataset_review_submit_jobs
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

create or replace function public.cmd_review_submit_from_job(
  p_job_id uuid,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.dataset_review_submit_jobs%rowtype;
  v_gate public.dataset_review_submit_gate_runs%rowtype;
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
  from public.dataset_review_submit_jobs
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

  if v_job.gate_run_id is null then
    update public.dataset_review_submit_jobs
      set status = 'error',
          last_error_code = 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
          last_error_message = 'Review-submit job is missing a gate run',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
      'status', 409,
      'message', 'Review-submit job is missing a gate run',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  select *
    into v_gate
  from public.dataset_review_submit_gate_runs
  where id = v_job.gate_run_id
  for update;

  if v_gate.id is null then
    update public.dataset_review_submit_jobs
      set status = 'error',
          last_error_code = 'REVIEW_SUBMIT_GATE_NOT_FOUND',
          last_error_message = 'Review-submit gate run not found',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_gate.dataset_table <> v_job.dataset_table
    or v_gate.dataset_id <> v_job.dataset_id
    or v_gate.dataset_version <> v_job.dataset_version
    or v_gate.revision_checksum <> v_job.revision_checksum
    or v_gate.policy_profile <> v_job.policy_profile
    or v_gate.report_schema_version <> v_job.report_schema_version then
    update public.dataset_review_submit_jobs
      set status = 'stale',
          last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
          last_error_message = 'Review-submit gate run does not match this review-submit job',
          last_error_details = jsonb_build_object(
            'gateRunId', v_gate.id,
            'expectedRevisionChecksum', v_job.revision_checksum,
            'actualRevisionChecksum', v_gate.revision_checksum
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
      'message', 'Review-submit gate run does not match this review-submit job',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_gate.status <> 'passed' then
    v_job_status := case
      when v_gate.status in ('queued', 'running') then 'waiting_gate'
      when v_gate.status = 'blocked' then 'blocked'
      when v_gate.status = 'stale' then 'stale'
      else 'error'
    end;

    v_error_code := case
      when v_gate.status in ('queued', 'running') then 'REVIEW_SUBMIT_GATE_NOT_READY'
      when v_gate.status = 'blocked' then 'REVIEW_SUBMIT_GATE_BLOCKED'
      when v_gate.status = 'stale' then 'REVIEW_SUBMIT_GATE_STALE'
      else 'REVIEW_SUBMIT_GATE_ERROR'
    end;

    v_error_status := case
      when v_gate.status = 'error' then 502
      else 409
    end;

    v_error_message := case
      when v_gate.status in ('queued', 'running') then 'Review-submit gate has not passed yet'
      when v_gate.status = 'blocked' then 'Review-submit gate blocked this dataset revision'
      when v_gate.status = 'stale' then 'Review-submit gate run is stale for the submitted dataset revision'
      else 'Review-submit gate failed before review submission'
    end;

    update public.dataset_review_submit_jobs
      set status = v_job_status,
          last_error_code = case when v_job_status = 'waiting_gate' then null else v_error_code end,
          last_error_message = case when v_job_status = 'waiting_gate' then null else v_error_message end,
          last_error_details = case
            when v_job_status = 'waiting_gate' then null
            else public.cmd_dataset_review_submit_gate_payload(v_gate)
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
  end if;

  execute format(
    'select true, user_id, state_code, modified_at from public.%I where id = $1 and version = $2',
    v_job.dataset_table
  )
    into v_dataset_found, v_owner_id, v_state_code, v_modified_at
    using v_job.dataset_id, v_job.dataset_version;

  if coalesce(v_dataset_found, false) is false then
    update public.dataset_review_submit_jobs
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
    update public.dataset_review_submit_jobs
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
    update public.dataset_review_submit_jobs
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
    update public.dataset_review_submit_jobs
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
    update public.dataset_review_submit_jobs
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

  v_submit_result := public.cmd_review_submit(
    p_table => v_job.dataset_table,
    p_id => v_job.dataset_id,
    p_version => v_job.dataset_version,
    p_audit => coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'source', 'cmd_review_submit_from_job',
      'review_submit_job_id', v_job.id
    ),
    p_review_submit_gate_run_id => v_job.gate_run_id,
    p_review_submit_revision_checksum => v_job.revision_checksum,
    p_review_submit_policy_profile => v_job.policy_profile,
    p_review_submit_report_schema_version => v_job.report_schema_version
  );

  perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
  perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
  perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update public.dataset_review_submit_jobs
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
        'gate_run_id', v_job.gate_run_id
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
    when v_error_code = 'REVIEW_SUBMIT_GATE_STALE' then 'stale'
    else 'error'
  end;

  update public.dataset_review_submit_jobs
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
end;
$$;

revoke all on function public.cmd_dataset_review_submit_job_payload(
  public.dataset_review_submit_jobs
) from public;
revoke all on function public.cmd_dataset_review_submit_job_enqueue(
  text,
  uuid,
  text,
  text,
  text,
  text,
  jsonb
) from public;
revoke all on function public.cmd_dataset_review_submit_job_read(uuid) from public;
revoke all on function public.cmd_dataset_review_submit_job_read_latest(
  text,
  uuid,
  text,
  text
) from public;
revoke all on function public.cmd_dataset_review_submit_job_claim(
  integer,
  integer
) from public;
revoke all on function public.cmd_dataset_review_submit_job_record_result(
  uuid,
  text,
  uuid,
  jsonb,
  text,
  text,
  jsonb,
  jsonb
) from public;
revoke all on function public.cmd_review_submit_from_job(uuid, jsonb) from public;

grant execute on function public.cmd_dataset_review_submit_job_enqueue(
  text,
  uuid,
  text,
  text,
  text,
  text,
  jsonb
) to authenticated;
grant execute on function public.cmd_dataset_review_submit_job_enqueue(
  text,
  uuid,
  text,
  text,
  text,
  text,
  jsonb
) to service_role;
grant execute on function public.cmd_dataset_review_submit_job_read(uuid)
  to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_job_read_latest(
  text,
  uuid,
  text,
  text
) to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_job_claim(
  integer,
  integer
) to service_role;
grant execute on function public.cmd_dataset_review_submit_job_record_result(
  uuid,
  text,
  uuid,
  jsonb,
  text,
  text,
  jsonb,
  jsonb
) to service_role;
grant execute on function public.cmd_review_submit_from_job(uuid, jsonb)
  to service_role;
