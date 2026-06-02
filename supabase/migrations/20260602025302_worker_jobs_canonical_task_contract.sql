-- Make worker_jobs the canonical task fact surface while retaining legacy
-- domain/history tables during the cross-repo cutover.

alter table public.worker_job_kinds
  drop constraint if exists worker_job_kinds_queue_check;

alter table public.worker_job_kinds
  add constraint worker_job_kinds_queue_check
  check (worker_queue in ('solver', 'review_submit', 'review_submit_gate', 'package', 'maintenance'));

alter table public.worker_jobs
  drop constraint if exists worker_jobs_queue_check;

alter table public.worker_jobs
  add constraint worker_jobs_queue_check
  check (worker_queue in ('solver', 'review_submit', 'review_submit_gate', 'package', 'maintenance'));

insert into public.worker_job_kinds (
  job_kind,
  worker_runtime,
  worker_queue,
  default_visibility,
  default_priority,
  default_max_attempts,
  default_lease_seconds,
  payload_schema_version,
  result_schema_version,
  user_visible,
  description
) values (
  'review_submit.submit',
  'calculator',
  'review_submit',
  'user',
  100,
  1,
  900,
  'review_submit.submit.request.v1',
  'review_submit.submit.result.v1',
  true,
  'Root review-submit coordinator job; child gate execution uses review_submit.gate'
)
on conflict (job_kind) do update
set worker_runtime = excluded.worker_runtime,
    worker_queue = excluded.worker_queue,
    default_visibility = excluded.default_visibility,
    default_priority = excluded.default_priority,
    default_max_attempts = excluded.default_max_attempts,
    default_lease_seconds = excluded.default_lease_seconds,
    payload_schema_version = excluded.payload_schema_version,
    result_schema_version = excluded.result_schema_version,
    user_visible = excluded.user_visible,
    description = excluded.description,
    updated_at = now();

alter table public.lca_jobs
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_results
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_result_cache
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_latest_all_unit_results
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_factorization_registry
  add column if not exists prepared_worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_package_jobs
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_package_artifacts
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_package_export_items
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.lca_package_request_cache
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.dataset_review_submit_jobs
  add column if not exists submit_worker_job_id uuid references public.worker_jobs(id) on delete set null;

alter table public.dataset_review_submit_gate_runs
  add column if not exists worker_job_id uuid references public.worker_jobs(id) on delete set null;

create index if not exists lca_jobs_worker_job_idx
  on public.lca_jobs (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_results_worker_job_idx
  on public.lca_results (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_result_cache_worker_job_idx
  on public.lca_result_cache (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_latest_all_unit_results_worker_job_idx
  on public.lca_latest_all_unit_results (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_factorization_registry_prepared_worker_job_idx
  on public.lca_factorization_registry (prepared_worker_job_id)
  where prepared_worker_job_id is not null;

create index if not exists lca_package_jobs_worker_job_idx
  on public.lca_package_jobs (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_package_artifacts_worker_job_idx
  on public.lca_package_artifacts (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_package_export_items_worker_job_idx
  on public.lca_package_export_items (worker_job_id)
  where worker_job_id is not null;

create index if not exists lca_package_request_cache_worker_job_idx
  on public.lca_package_request_cache (worker_job_id)
  where worker_job_id is not null;

create index if not exists dataset_review_submit_jobs_submit_worker_job_idx
  on public.dataset_review_submit_jobs (submit_worker_job_id)
  where submit_worker_job_id is not null;

create index if not exists dataset_review_submit_gate_runs_worker_job_idx
  on public.dataset_review_submit_gate_runs (worker_job_id)
  where worker_job_id is not null;

create index if not exists worker_jobs_subject_kind_updated_idx
  on public.worker_jobs (subject_type, subject_id, subject_version, job_kind, updated_at desc)
  where subject_type is not null and subject_id is not null;

create index if not exists worker_jobs_requested_kind_updated_idx
  on public.worker_jobs (requested_by, job_kind, updated_at desc)
  where requested_by is not null;

comment on table public.worker_jobs
  is 'Canonical task fact table for work executed or coordinated by tiangong-lca-worker. Legacy job tables may remain only as domain artifact/cache/history compatibility surfaces.';
comment on column public.worker_jobs.root_job_id
  is 'Optional root worker job for multi-step flows such as review_submit.submit -> review_submit.gate.';
comment on column public.worker_jobs.parent_job_id
  is 'Immediate parent worker job for child execution records.';
comment on column public.worker_jobs.subject_type
  is 'Domain subject table or logical entity name used for latest/read/list projections.';
comment on column public.worker_jobs.subject_id
  is 'Domain subject UUID used with subject_type and subject_version.';
comment on column public.worker_jobs.subject_version
  is 'Domain subject version used with subject_type and subject_id.';
comment on column public.worker_jobs.payload_ref
  is 'Internal reference to request payload artifacts or legacy compatibility rows. Exposed only through internal worker projections.';
comment on column public.worker_jobs.result_ref
  is 'Internal reference to result/artifact/cache rows. Exposed only through internal worker projections.';

comment on column public.lca_jobs.worker_job_id
  is 'Canonical worker_jobs task fact for this retained legacy LCA domain/history row.';
comment on column public.lca_results.worker_job_id
  is 'Canonical worker_jobs task that produced this LCA result artifact.';
comment on column public.lca_result_cache.worker_job_id
  is 'Canonical worker_jobs task currently backing this LCA result cache row.';
comment on column public.lca_latest_all_unit_results.worker_job_id
  is 'Canonical worker_jobs task that produced the latest all-unit result artifact.';
comment on column public.lca_factorization_registry.prepared_worker_job_id
  is 'Canonical worker_jobs task that prepared this factorization artifact.';
comment on column public.lca_package_jobs.worker_job_id
  is 'Canonical worker_jobs task fact for this retained legacy TIDAS package history row.';
comment on column public.lca_package_artifacts.worker_job_id
  is 'Canonical worker_jobs task that produced this TIDAS package artifact.';
comment on column public.lca_package_export_items.worker_job_id
  is 'Canonical worker_jobs task that discovered or exported this package item.';
comment on column public.lca_package_request_cache.worker_job_id
  is 'Canonical worker_jobs task currently backing this package request cache row.';
comment on column public.dataset_review_submit_jobs.submit_worker_job_id
  is 'Canonical root review_submit.submit worker_jobs task for this retained review-submit coordinator/history row.';
comment on column public.dataset_review_submit_gate_runs.worker_job_id
  is 'Canonical review_submit.gate worker_jobs execution that produced this retained gate report row.';

comment on table public.lca_jobs
  is 'Legacy LCA domain/history table retained for result/cache/artifact compatibility. New task lifecycle must use public.worker_jobs.';
comment on table public.lca_package_jobs
  is 'Legacy TIDAS package domain/history table retained for artifact/cache compatibility. New task lifecycle must use public.worker_jobs.';
comment on table public.dataset_review_submit_jobs
  is 'Retained review-submit coordinator/history compatibility table. New review-submit task lifecycle should use public.worker_jobs with review_submit.submit as the root job.';
comment on table public.dataset_review_submit_gate_runs
  is 'Review-submit gate report/history table retained for compatibility. New gate execution lifecycle is public.worker_jobs.';

create or replace function public.worker_claim_jobs(
  p_worker_queue text,
  p_worker_id text default null,
  p_limit integer default 10,
  p_lease_seconds integer default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_worker_queue text := lower(trim(coalesce(p_worker_queue, '')));
  v_worker_id text := nullif(trim(p_worker_id), '');
  v_limit integer := greatest(1, least(coalesce(p_limit, 10), 50));
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim worker jobs'
    );
  end if;

  if v_worker_queue not in ('solver', 'review_submit', 'review_submit_gate', 'package', 'maintenance') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'workerQueue must be solver, review_submit, review_submit_gate, package, or maintenance'
    );
  end if;

  with expired as (
    update public.worker_jobs as j
      set status = 'failed',
          error_code = coalesce(j.error_code, 'lease_expired_max_attempts'),
          error_message = coalesce(j.error_message, 'Worker job lease expired after the maximum attempt count'),
          error_details = coalesce(j.error_details, '{}'::jsonb) || jsonb_build_object(
            'leasedBy', j.leased_by,
            'leaseExpiresAt', j.lease_expires_at,
            'attemptCount', j.attempt_count,
            'maxAttempts', j.max_attempts
          ),
          leased_by = null,
          lease_token = null,
          lease_expires_at = null,
          heartbeat_at = coalesce(j.heartbeat_at, now()),
          updated_at = now(),
          finished_at = now()
    where j.worker_runtime = 'calculator'
      and j.worker_queue = v_worker_queue
      and j.status = 'running'
      and j.lease_expires_at < now()
      and j.attempt_count >= j.max_attempts
    returning j.*
  ),
  expired_events as (
    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      worker_id,
      message,
      details
    )
    select
      expired.id,
      'failed',
      expired.status,
      expired.leased_by,
      'Worker job lease expired after the maximum attempt count',
      jsonb_build_object(
        'errorCode', expired.error_code,
        'attemptCount', expired.attempt_count,
        'maxAttempts', expired.max_attempts
      )
    from expired
    returning id
  ),
  candidate as (
    select id
    from public.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and run_after <= now()
      and attempt_count < max_attempts
      and (
        status in ('queued', 'stale')
        or (status = 'running' and lease_expires_at < now())
      )
    order by priority desc, run_after asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  updated as (
    update public.worker_jobs as j
      set status = 'running',
          attempt_count = j.attempt_count + 1,
          leased_by = v_worker_id,
          lease_token = gen_random_uuid(),
          lease_expires_at = now() + make_interval(secs => v_lease_seconds),
          heartbeat_at = now(),
          started_at = coalesce(j.started_at, now()),
          updated_at = now(),
          error_code = null,
          error_message = null,
          error_details = null
    from candidate
    where j.id = candidate.id
    returning j.*
  ),
  claim_events as (
    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      phase,
      progress,
      worker_id,
      lease_token,
      details
    )
    select
      updated.id,
      'claimed',
      updated.status,
      updated.phase,
      updated.progress,
      updated.leased_by,
      updated.lease_token,
      jsonb_build_object(
        'attemptCount', updated.attempt_count,
        'leaseExpiresAt', updated.lease_expires_at
      )
    from updated
    returning id
  )
  select coalesce(jsonb_agg(public.worker_job_payload(updated, true)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

create or replace function public.worker_read_latest_job(
  p_requested_by uuid default null,
  p_subject_type text default null,
  p_subject_id uuid default null,
  p_subject_version text default null,
  p_job_kind text default null,
  p_statuses text[] default null,
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_subject_type text := nullif(trim(p_subject_type), '');
  v_subject_version text := nullif(trim(p_subject_version), '');
  v_job_kind text := nullif(lower(trim(p_job_kind)), '');
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read worker jobs'
    );
  end if;

  if p_statuses is not null
    and exists (
      select 1
      from unnest(p_statuses) as status_value
      where status_value not in (
        'queued',
        'running',
        'waiting',
        'completed',
        'blocked',
        'stale',
        'failed',
        'cancelled'
      )
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_STATUS',
      'status', 400,
      'message', 'statuses contains an unsupported worker job status'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where (p_requested_by is null or requested_by = p_requested_by)
    and (v_subject_type is null or subject_type = v_subject_type)
    and (p_subject_id is null or subject_id = p_subject_id)
    and (v_subject_version is null or subject_version = v_subject_version)
    and (v_job_kind is null or job_kind = v_job_kind)
    and (p_statuses is null or status = any(p_statuses))
  order by updated_at desc, created_at desc
  limit 1;

  return jsonb_build_object(
    'ok', true,
    'data', case
      when v_job.id is null then null
      else public.worker_job_payload(v_job, p_include_internal)
    end
  );
end;
$$;

create or replace function public.dataset_review_submit_jobs_assign_submit_worker_job()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if tg_op <> 'INSERT' or new.submit_worker_job_id is not null then
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
      'domainSource', 'dataset_review_submit_jobs',
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
      'source', 'dataset_review_submit_jobs_assign_submit_worker_job'
    )
  );

  new.submit_worker_job_id := v_worker_job.id;
  return new;
end;
$$;

create or replace function public.dataset_review_submit_jobs_sync_submit_worker_job()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_worker_status text;
  v_progress numeric;
  v_result_json jsonb;
  v_result_ref jsonb;
  v_error_code text;
  v_error_message text;
  v_error_details jsonb;
  v_blocker_codes text[];
  v_resolution_scope text;
begin
  if new.submit_worker_job_id is null then
    return new;
  end if;

  v_worker_status := case
    when new.status in ('queued', 'waiting_gate', 'submitting') then 'waiting'
    when new.status = 'submitted' then 'completed'
    when new.status = 'blocked' then 'blocked'
    when new.status = 'stale' then 'stale'
    when new.status = 'cancelled' then 'cancelled'
    else 'failed'
  end;

  v_progress := case
    when new.status = 'queued' then 0
    when new.status = 'waiting_gate' then 0.25
    when new.status = 'submitting' then 0.75
    when new.status = 'submitted' then 1
    else null
  end;

  v_result_ref := jsonb_build_object(
    'domainSource', 'dataset_review_submit_jobs',
    'reviewSubmitJobId', new.id
  );

  v_result_json := case
    when new.status = 'submitted' then jsonb_strip_nulls(
      jsonb_build_object(
        'status', 'submitted',
        'reviewSubmitJobId', new.id,
        'datasetRevision', jsonb_build_object(
          'table', new.dataset_table,
          'id', new.dataset_id,
          'version', new.dataset_version,
          'revisionChecksum', new.revision_checksum
        ),
        'result', new.result
      )
    )
    else null
  end;

  v_error_code := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_code, 'REVIEW_SUBMIT_JOB_' || upper(new.status))
    else null
  end;
  v_error_message := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_message, 'Review-submit job status is ' || new.status)
    else null
  end;
  v_error_details := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_details, '{}'::jsonb)
    else null
  end;
  v_blocker_codes := case
    when v_worker_status = 'blocked' then array[coalesce(new.last_error_code, 'REVIEW_SUBMIT_GATE_BLOCKED')]
    else '{}'::text[]
  end;
  v_resolution_scope := case
    when v_worker_status = 'blocked' then 'user'
    else null
  end;

  update public.worker_jobs
    set status = v_worker_status,
        phase = new.status,
        progress = v_progress,
        result_json = case
          when v_worker_status = 'completed' then v_result_json
          else result_json
        end,
        result_ref = coalesce(result_ref, '{}'::jsonb) || v_result_ref,
        error_code = v_error_code,
        error_message = v_error_message,
        error_details = v_error_details,
        blocker_codes = v_blocker_codes,
        resolution_scope = v_resolution_scope,
        retryable = case
          when v_worker_status in ('failed', 'stale') then true
          when v_worker_status in ('completed', 'blocked', 'cancelled') then false
          else null
        end,
        updated_at = now(),
        finished_at = case
          when v_worker_status in ('completed', 'blocked', 'stale', 'failed', 'cancelled') then coalesce(finished_at, now())
          else null
        end,
        cancelled_at = case
          when v_worker_status = 'cancelled' then coalesce(cancelled_at, now())
          else cancelled_at
        end
  where id = new.submit_worker_job_id;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    message,
    details
  )
  select
    new.submit_worker_job_id,
    'review_submit_status_synced',
    v_worker_status,
    new.status,
    v_progress,
    v_error_message,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewSubmitJobId', new.id,
        'errorCode', v_error_code,
        'blockerCodes', to_jsonb(v_blocker_codes),
        'resolutionScope', v_resolution_scope
      )
    );

  return new;
end;
$$;

drop trigger if exists dataset_review_submit_jobs_assign_submit_worker_job_trigger
  on public.dataset_review_submit_jobs;

create trigger dataset_review_submit_jobs_assign_submit_worker_job_trigger
  before insert on public.dataset_review_submit_jobs
  for each row
  execute function public.dataset_review_submit_jobs_assign_submit_worker_job();

drop trigger if exists dataset_review_submit_jobs_sync_submit_worker_job_trigger
  on public.dataset_review_submit_jobs;

create trigger dataset_review_submit_jobs_sync_submit_worker_job_trigger
  after insert or update of
    status,
    last_error_code,
    last_error_message,
    last_error_details,
    result,
    submit_worker_job_id
  on public.dataset_review_submit_jobs
  for each row
  execute function public.dataset_review_submit_jobs_sync_submit_worker_job();

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
      'submitWorkerJobId', (p_job).submit_worker_job_id,
      'gateRunId', (p_job).gate_run_id,
      'gateWorkerJobId', (p_job).gate_worker_job_id,
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
      'submitWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = (p_job).submit_worker_job_id
        ),
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = (p_job).gate_run_id
        ),
      'gateWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = (p_job).gate_worker_job_id
        ),
      'createdAt', to_jsonb((p_job).created_at),
      'modifiedAt', to_jsonb((p_job).modified_at),
      'completedAt', to_jsonb((p_job).completed_at)
    )
  )
$$;

drop view if exists public.worker_job_domain_refs;

create view public.worker_job_domain_refs
with (security_invoker = true)
as
select
  worker_job_id,
  'lca_jobs'::text as domain_source,
  id as domain_id,
  'legacy_lca_task'::text as domain_role,
  id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_jobs
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_results'::text as domain_source,
  id as domain_id,
  'lca_result_artifact'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  created_at as updated_at
from public.lca_results
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_result_cache'::text as domain_source,
  id as domain_id,
  'lca_result_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_result_cache
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_latest_all_unit_results'::text as domain_source,
  id as domain_id,
  'lca_latest_all_unit_result'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_latest_all_unit_results
where worker_job_id is not null
union all
select
  prepared_worker_job_id as worker_job_id,
  'lca_factorization_registry'::text as domain_source,
  id as domain_id,
  'lca_factorization_artifact'::text as domain_role,
  prepared_job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_factorization_registry
where prepared_worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_jobs'::text as domain_source,
  id as domain_id,
  'legacy_package_task'::text as domain_role,
  id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_jobs
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_artifacts'::text as domain_source,
  id as domain_id,
  'package_artifact'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_artifacts
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_export_items'::text as domain_source,
  id as domain_id,
  'package_export_item'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  updated_at
from public.lca_package_export_items
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_request_cache'::text as domain_source,
  id as domain_id,
  'package_request_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_request_cache
where worker_job_id is not null
union all
select
  submit_worker_job_id as worker_job_id,
  'dataset_review_submit_jobs'::text as domain_source,
  id as domain_id,
  'review_submit_coordinator'::text as domain_role,
  null::uuid as legacy_job_id,
  status,
  created_at,
  modified_at as updated_at
from public.dataset_review_submit_jobs
where submit_worker_job_id is not null
union all
select
  worker_job_id,
  'dataset_review_submit_gate_runs'::text as domain_source,
  id as domain_id,
  'review_submit_gate_report'::text as domain_role,
  null::uuid as legacy_job_id,
  status,
  created_at,
  modified_at as updated_at
from public.dataset_review_submit_gate_runs
where worker_job_id is not null;

revoke all on public.worker_job_domain_refs from public, anon, authenticated;
grant select on public.worker_job_domain_refs to service_role;

comment on view public.worker_job_domain_refs
  is 'Service-role projection from canonical worker_jobs to retained domain artifact/cache/history rows.';

drop view if exists public.worker_legacy_lifecycle_audit;

create view public.worker_legacy_lifecycle_audit
with (security_invoker = true)
as
select
  'lca_jobs'::text as legacy_source,
  job_type::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running', 'ready'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(updated_at) as latest_updated_at
from public.lca_jobs
group by job_type, status
union all
select
  'lca_package_jobs'::text as legacy_source,
  job_type::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running', 'ready'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(updated_at) as latest_updated_at
from public.lca_package_jobs
group by job_type, status
union all
select
  'dataset_review_submit_jobs'::text as legacy_source,
  'review_submit.submit'::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'waiting_gate', 'submitting'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(modified_at) as latest_updated_at
from public.dataset_review_submit_jobs
group by status
union all
select
  'dataset_review_submit_gate_runs'::text as legacy_source,
  'review_submit.gate'::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(modified_at) as latest_updated_at
from public.dataset_review_submit_gate_runs
group by status;

revoke all on public.worker_legacy_lifecycle_audit from public, anon, authenticated;
grant select on public.worker_legacy_lifecycle_audit to service_role;

comment on view public.worker_legacy_lifecycle_audit
  is 'Service-role audit view for retained legacy lifecycle/domain history after worker_jobs canonical task cutover.';

revoke all on function public.worker_claim_jobs(text, text, integer, integer) from public, anon, authenticated;
revoke all on function public.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_payload(public.dataset_review_submit_jobs) from public, anon, authenticated;
revoke all on function public.dataset_review_submit_jobs_assign_submit_worker_job() from public, anon, authenticated;
revoke all on function public.dataset_review_submit_jobs_sync_submit_worker_job() from public, anon, authenticated;

grant execute on function public.worker_claim_jobs(text, text, integer, integer) to service_role;
grant execute on function public.worker_read_latest_job(uuid, text, uuid, text, text, text[], boolean) to service_role;
grant execute on function public.cmd_dataset_review_submit_job_payload(public.dataset_review_submit_jobs) to service_role;
