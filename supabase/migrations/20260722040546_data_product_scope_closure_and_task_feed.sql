-- Data-product scope closure certificates and the safe TaskSummaryV2 feed.
-- All public entrypoints are RPCs: tables remain service-role only and RLS is
-- enabled as a defence-in-depth boundary for an exposed Supabase schema.

alter table public.worker_job_kinds
  add column if not exists task_center_category text,
  add column if not exists task_center_surface text,
  add column if not exists presenter_key text;

alter table public.worker_job_kinds
  drop constraint if exists worker_job_kinds_task_center_surface_check;

alter table public.worker_job_kinds
  add constraint worker_job_kinds_task_center_surface_check
  check (task_center_surface is null or task_center_surface in ('global', 'inline'));

update public.worker_job_kinds
set task_center_category = 'data_product',
    task_center_surface = 'global',
    presenter_key = 'data_product.lcia_result_package_build.v1',
    user_visible = false,
    updated_at = now()
where job_kind = 'lcia_result.package_build';

insert into public.worker_job_kinds (
  job_kind, worker_runtime, worker_queue, default_visibility, default_priority,
  default_max_attempts, default_lease_seconds, payload_schema_version,
  result_schema_version, user_visible, description, task_center_category,
  task_center_surface, presenter_key
) values (
  'lcia.scope_closure_check', 'calculator', 'solver', 'operator', 10,
  3, 3600, 'lcia.scope_closure_check.request.v1',
  'lcia.scope_closure_check.result.v1', false,
  'Builds an immutable LCIA scope-closure certificate and report.',
  'data_product', 'global', 'data_product.lcia_scope_closure_check.v1'
) on conflict (job_kind) do update
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
    task_center_category = excluded.task_center_category,
    task_center_surface = excluded.task_center_surface,
    presenter_key = excluded.presenter_key,
    updated_at = now();

create table if not exists public.lcia_scope_closure_checks (
  id uuid primary key default gen_random_uuid(),
  worker_job_id uuid not null unique references public.worker_jobs(id) on delete restrict,
  requested_by uuid not null,
  request_idempotency_token text not null,
  request_key text not null,
  request_fingerprint text not null,
  requested_scope_hash text not null,
  effective_scope_hash text,
  policy_fingerprint text not null,
  data_snapshot_token text not null,
  expected_validator_scanner_fingerprint text not null,
  status text not null default 'queued',
  scan_completeness text,
  certificate_status text not null default 'pending',
  certificate_hash text,
  report_artifact_id uuid references public.worker_job_artifacts(id) on delete restrict,
  result_summary jsonb not null default '{}'::jsonb,
  blocker_codes text[] not null default '{}'::text[],
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  finished_at timestamptz,
  constraint lcia_scope_closure_checks_status_check
    check (status in ('queued', 'running', 'passed', 'blocked', 'failed', 'cancelled')),
  constraint lcia_scope_closure_checks_completeness_check
    check (scan_completeness is null or scan_completeness in ('complete', 'incomplete')),
  constraint lcia_scope_closure_checks_certificate_check
    check (certificate_status in ('pending', 'valid', 'stale', 'revoked', 'unavailable')),
  constraint lcia_scope_closure_checks_result_summary_check
    check (jsonb_typeof(result_summary) = 'object'),
  constraint lcia_scope_closure_checks_request_token_check
    check (length(trim(request_idempotency_token)) between 1 and 200),
  constraint lcia_scope_closure_checks_hash_check
    check (length(trim(requested_scope_hash)) > 0 and length(trim(policy_fingerprint)) > 0)
);

create table if not exists public.lcia_scope_closure_config (
  singleton boolean primary key default true check (singleton),
  expected_validator_scanner_fingerprint text not null,
  require_certificate_for_builds boolean not null default false,
  updated_at timestamptz not null default now()
);

insert into public.lcia_scope_closure_config (singleton, expected_validator_scanner_fingerprint)
values (true, 'scope-closure-validator-scanner.v1')
on conflict (singleton) do nothing;

create table if not exists public.lcia_scope_closure_certificate_events (
  id uuid primary key default gen_random_uuid(),
  closure_check_id uuid not null references public.lcia_scope_closure_checks(id) on delete cascade,
  certificate_status text not null check (certificate_status in ('stale', 'revoked')),
  reason text not null,
  created_by uuid,
  created_at timestamptz not null default now()
);

create index if not exists lcia_scope_closure_certificate_events_check_created_idx
  on public.lcia_scope_closure_certificate_events (closure_check_id, created_at desc, id desc);

create unique index if not exists lcia_scope_closure_checks_request_key_uidx
  on public.lcia_scope_closure_checks (requested_by, request_key);
create index if not exists lcia_scope_closure_checks_requested_updated_idx
  on public.lcia_scope_closure_checks (requested_by, updated_at desc, id desc);

create table if not exists public.lcia_scope_closure_issues (
  id uuid primary key default gen_random_uuid(),
  closure_check_id uuid not null references public.lcia_scope_closure_checks(id) on delete cascade,
  issue_key text not null,
  severity text not null,
  blocking boolean not null default false,
  issue_code text not null,
  source_dataset_type text,
  source_dataset_id uuid,
  source_dataset_version text,
  json_path text,
  reference_role text,
  requested_target_type text,
  requested_target_id uuid,
  requested_target_version text,
  message text not null,
  suggested_action text,
  occurrence_count integer not null default 1,
  affected_root_count integer not null default 0,
  created_at timestamptz not null default now(),
  constraint lcia_scope_closure_issues_severity_check
    check (severity in ('blocker', 'warning', 'info')),
  constraint lcia_scope_closure_issues_counts_check
    check (occurrence_count > 0 and affected_root_count >= 0),
  constraint lcia_scope_closure_issues_key_uidx unique (closure_check_id, issue_key)
);

create index if not exists lcia_scope_closure_issues_check_id_idx
  on public.lcia_scope_closure_issues (closure_check_id, severity, issue_code, id);

alter table public.lcia_scope_closure_checks enable row level security;
alter table public.lcia_scope_closure_issues enable row level security;
alter table public.lcia_scope_closure_config enable row level security;
alter table public.lcia_scope_closure_certificate_events enable row level security;
revoke all on public.lcia_scope_closure_checks, public.lcia_scope_closure_issues, public.lcia_scope_closure_config, public.lcia_scope_closure_certificate_events from public, anon, authenticated;
grant all on public.lcia_scope_closure_checks, public.lcia_scope_closure_issues, public.lcia_scope_closure_config, public.lcia_scope_closure_certificate_events to service_role;

create or replace function public.lcia_scope_closure_is_manager()
returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select public.lcia_result_is_manager()
$$;

create or replace function public.lcia_scope_closure_error(p_code text, p_status integer, p_message text)
returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_build_object('ok', false, 'code', p_code, 'status', p_status, 'message', p_message)
$$;

create or replace function public.cmd_lcia_scope_closure_check_request(
  p_requested_scope_hash text,
  p_policy_fingerprint text,
  p_request_idempotency_token text,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_request_key text; v_current_manifest jsonb; v_data_snapshot_token text; v_expected_validator_scanner_fingerprint text;
  v_request_fingerprint text;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error('not_data_product_manager', 403, 'Data product manager role is required');
  end if;
  if coalesce(nullif(trim(p_requested_scope_hash), ''), '') = ''
    or coalesce(nullif(trim(p_policy_fingerprint), ''), '') = ''
    or coalesce(nullif(trim(p_request_idempotency_token), ''), '') = '' then
    return public.lcia_scope_closure_error('invalid_closure_request', 400, 'Closure scope, policy, and idempotency token are required');
  end if;
  select expected_validator_scanner_fingerprint into v_expected_validator_scanner_fingerprint
  from public.lcia_scope_closure_config where singleton;
  if v_expected_validator_scanner_fingerprint is null then
    return public.lcia_scope_closure_error('closure_evidence_unavailable', 503, 'Closure validator configuration is unavailable');
  end if;
  v_current_manifest := public.lcia_result_current_eligible_manifest();
  v_data_snapshot_token := v_current_manifest->>'inputManifestHash';

  v_request_fingerprint := encode(digest(
    trim(p_requested_scope_hash) || '|' || trim(p_policy_fingerprint) || '|' ||
    v_expected_validator_scanner_fingerprint || '|' || v_data_snapshot_token, 'sha256'), 'hex');
  v_request_key := encode(digest(v_actor::text || '|' || trim(p_request_idempotency_token) || '|' || v_request_fingerprint, 'sha256'), 'hex');

  select * into v_check
  from public.lcia_scope_closure_checks
  where requested_by = v_actor and request_key = v_request_key
  for update;
  if v_check.id is not null then
    select * into v_job from public.worker_jobs where id = v_check.worker_job_id;
    return jsonb_build_object('ok', true, 'data', jsonb_build_object(
      'closureCheckId', v_check.id, 'workerJob', public.worker_job_payload(v_job, false),
      'requestFingerprint', v_request_fingerprint, 'reused', true));
  end if;

  insert into public.worker_jobs (
    job_kind, worker_runtime, worker_queue, priority, subject_type, requester_type,
    requested_by, idempotency_key, request_hash, concurrency_key, visibility,
    max_attempts, payload_schema_version, payload_json, result_schema_version
  ) values (
    'lcia.scope_closure_check', 'calculator', 'solver', 10, 'lcia_scope_closure_check', 'user',
    v_actor, trim(p_request_idempotency_token), v_request_fingerprint, v_request_key, 'operator',
    3, 'lcia.scope_closure_check.request.v1', jsonb_build_object(
      'closure_check_id', null, 'requested_by', v_actor, 'requested_scope_hash', trim(p_requested_scope_hash),
      'policy_fingerprint', trim(p_policy_fingerprint), 'data_snapshot_token', v_data_snapshot_token,
      'expected_validator_scanner_fingerprint', v_expected_validator_scanner_fingerprint,
      'request_fingerprint', v_request_fingerprint, 'request_key', v_request_key),
    'lcia.scope_closure_check.result.v1'
  ) returning * into v_job;

  insert into public.lcia_scope_closure_checks (
    worker_job_id, requested_by, request_idempotency_token, request_key, request_fingerprint,
    requested_scope_hash, policy_fingerprint, data_snapshot_token, expected_validator_scanner_fingerprint
  ) values (
    v_job.id, v_actor, trim(p_request_idempotency_token), v_request_key, v_request_fingerprint,
    trim(p_requested_scope_hash), trim(p_policy_fingerprint), v_data_snapshot_token, v_expected_validator_scanner_fingerprint
  ) returning * into v_check;

  update public.worker_jobs
  set subject_id = v_check.id,
      payload_json = payload_json || jsonb_build_object('closure_check_id', v_check.id),
      updated_at = now()
  where id = v_job.id
  returning * into v_job;

  insert into public.worker_job_events (job_id, event_type, status, details)
  values (v_job.id, 'enqueued', 'queued', jsonb_build_object('closureCheckId', v_check.id, 'requestFingerprint', v_request_fingerprint));

  insert into public.command_audit_log (command, actor_user_id, target_table, target_id, payload)
  values ('cmd_lcia_scope_closure_check_request', v_actor, 'lcia_scope_closure_checks', v_check.id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('requestFingerprint', v_request_fingerprint));

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id, 'workerJob', public.worker_job_payload(v_job, false),
    'requestFingerprint', v_request_fingerprint, 'reused', false));
end;
$$;

create or replace function public.get_lcia_scope_closure_check(p_closure_check_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_actor uuid := auth.uid(); v_check public.lcia_scope_closure_checks%rowtype; v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id;
  if v_check.id is null or v_check.requested_by <> v_actor or not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  select * into v_job from public.worker_jobs where id = v_check.worker_job_id;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'id', v_check.id, 'status', v_check.status, 'scanCompleteness', v_check.scan_completeness,
    'certificateStatus', v_check.certificate_status, 'requestedScopeHash', v_check.requested_scope_hash,
    'effectiveScopeHash', v_check.effective_scope_hash, 'policyFingerprint', v_check.policy_fingerprint,
    'blockerCodes', v_check.blocker_codes, 'resultSummary', v_check.result_summary,
    'workerJob', public.worker_job_payload(v_job, false), 'createdAt', v_check.created_at, 'updatedAt', v_check.updated_at));
end;
$$;

create or replace function public.list_lcia_scope_closure_issues(
  p_closure_check_id uuid, p_after_issue_id uuid default null, p_limit integer default 100
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_actor uuid := auth.uid(); v_limit integer := greatest(1, least(coalesce(p_limit, 100), 200)); v_after uuid := p_after_issue_id;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  if not public.lcia_scope_closure_is_manager() or not exists (
    select 1 from public.lcia_scope_closure_checks c where c.id = p_closure_check_id and c.requested_by = v_actor
  ) then return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  return jsonb_build_object('ok', true, 'data', coalesce((
    select jsonb_agg(to_jsonb(x) order by x.id)
    from (select * from public.lcia_scope_closure_issues i where i.closure_check_id = p_closure_check_id
      and (v_after is null or i.id > v_after) order by i.id limit v_limit) x
  ), '[]'::jsonb));
end;
$$;

create or replace function public.get_lcia_scope_closure_report_download(p_closure_check_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_actor uuid := auth.uid(); v_artifact public.worker_job_artifacts%rowtype;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  if not public.lcia_scope_closure_is_manager() then return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  select a.* into v_artifact from public.lcia_scope_closure_checks c join public.worker_job_artifacts a on a.id = c.report_artifact_id
    where c.id = p_closure_check_id and c.requested_by = v_actor and c.status in ('passed', 'blocked');
  if v_artifact.id is null then return public.lcia_scope_closure_error('closure_report_unavailable', 404, 'Closure report is not available'); end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id, 'bucket', v_artifact.storage_bucket, 'objectPath', v_artifact.storage_path,
    'mediaType', v_artifact.content_type, 'size', v_artifact.byte_size, 'checksumSha256', v_artifact.checksum_sha256));
end;
$$;

create or replace function public.svc_lcia_scope_closure_check_record_result(
  p_closure_check_id uuid,
  p_status text,
  p_scan_completeness text,
  p_effective_scope_hash text,
  p_certificate_hash text default null,
  p_result_summary jsonb default '{}'::jsonb,
  p_blocker_codes text[] default '{}'::text[],
  p_report_artifact_id uuid default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare v_check public.lcia_scope_closure_checks%rowtype; v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if p_status not in ('passed', 'blocked', 'failed', 'cancelled')
    or p_scan_completeness not in ('complete', 'incomplete')
    or jsonb_typeof(coalesce(p_result_summary, '{}'::jsonb)) <> 'object' then
    return public.lcia_scope_closure_error('invalid_closure_result', 400, 'Invalid closure result payload');
  end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id for update;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  if p_report_artifact_id is not null and not exists (
    select 1 from public.worker_job_artifacts a where a.id = p_report_artifact_id and a.job_id = v_check.worker_job_id
  ) then return public.lcia_scope_closure_error('closure_report_unavailable', 409, 'Report artifact does not belong to the closure job'); end if;
  if p_status = 'blocked' and p_report_artifact_id is null then
    return public.lcia_scope_closure_error('closure_report_unavailable', 409, 'Blocked closure checks require a report artifact');
  end if;
  if p_status = 'passed' and p_scan_completeness <> 'complete' then
    return public.lcia_scope_closure_error('closure_check_incomplete', 409, 'A passed closure check must be complete');
  end if;
  if p_status = 'passed' and (coalesce(nullif(trim(p_effective_scope_hash), ''), '') = '' or coalesce(nullif(trim(p_certificate_hash), ''), '') = '') then
    return public.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Passed closure checks require effective scope and certificate evidence');
  end if;
  if p_status = 'passed' and coalesce(p_result_summary->>'evidenceHash', '') <> coalesce(nullif(trim(p_certificate_hash), ''), '') then
    return public.lcia_scope_closure_error('closure_evidence_hash_mismatch', 409, 'Certificate hash must match the worker evidence hash');
  end if;
  update public.lcia_scope_closure_checks
  set status = p_status, scan_completeness = p_scan_completeness,
      effective_scope_hash = nullif(trim(p_effective_scope_hash), ''), certificate_status = case when p_status = 'passed' then 'valid' else 'unavailable' end,
      certificate_hash = nullif(trim(p_certificate_hash), ''), result_summary = coalesce(p_result_summary, '{}'::jsonb),
      blocker_codes = coalesce(p_blocker_codes, '{}'::text[]), report_artifact_id = p_report_artifact_id,
      updated_at = now(), finished_at = now()
  where id = v_check.id returning * into v_check;
  update public.worker_jobs set status = case when p_status = 'passed' then 'completed' else p_status end,
      result_json = jsonb_build_object('closureCheckId', v_check.id, 'status', p_status,
        'scanCompleteness', p_scan_completeness, 'certificateStatus', case when p_status = 'passed' then 'valid' else 'unavailable' end,
        'effectiveScopeHash', v_check.effective_scope_hash, 'certificateHash', v_check.certificate_hash),
      blocker_codes = case when p_status = 'blocked' then coalesce(p_blocker_codes, '{}'::text[]) else blocker_codes end,
      resolution_scope = case when p_status = 'blocked' then 'operator' else resolution_scope end,
      finished_at = now(), updated_at = now()
  where id = v_check.worker_job_id returning * into v_job;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object('closureCheckId', v_check.id, 'workerJob', public.worker_job_payload(v_job, true)));
end;
$$;

create or replace function public.svc_lcia_scope_closure_certificate_event(
  p_closure_check_id uuid, p_certificate_status text, p_reason text
) returns jsonb
language plpgsql security definer set search_path = public, pg_temp as $$
declare v_check public.lcia_scope_closure_checks%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required'); end if;
  if p_certificate_status not in ('stale', 'revoked') or coalesce(nullif(trim(p_reason), ''), '') = '' then
    return public.lcia_scope_closure_error('invalid_certificate_event', 400, 'Certificate event status and reason are required');
  end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id for update;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  if v_check.certificate_status <> 'valid' then return public.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Only valid certificates can transition by event'); end if;
  insert into public.lcia_scope_closure_certificate_events (closure_check_id, certificate_status, reason)
  values (v_check.id, p_certificate_status, trim(p_reason));
  update public.lcia_scope_closure_checks set certificate_status = p_certificate_status, updated_at = now() where id = v_check.id;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object('closureCheckId', v_check.id, 'certificateStatus', p_certificate_status));
end;
$$;

-- Preserve the historical implementation as an internal callable. The public
-- positional v1 surface is recreated below as a fail-closed compatibility API.
alter function public.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb)
  rename to cmd_lcia_result_build_request_legacy;

create or replace function public.cmd_lcia_result_build_request_v2(
  p_name text, p_processes jsonb, p_coverage_mode text, p_default_impact_category text,
  p_lcia_method_set jsonb, p_idempotency_key text, p_closure_check_id uuid,
  p_requested_scope_hash text, p_policy_fingerprint text, p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid := auth.uid(); v_check public.lcia_scope_closure_checks%rowtype; v_result jsonb;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id and requested_by = v_actor;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found'); end if;
  if v_check.status <> 'passed' then return public.lcia_scope_closure_error('closure_check_not_passed', 409, 'Closure check has not passed'); end if;
  if v_check.scan_completeness <> 'complete' then return public.lcia_scope_closure_error('closure_check_incomplete', 409, 'Closure scan is incomplete'); end if;
  if v_check.certificate_status = 'stale' then return public.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate is stale'); end if;
  if v_check.certificate_status = 'revoked' then return public.lcia_scope_closure_error('closure_check_revoked', 409, 'Closure certificate is revoked'); end if;
  if v_check.certificate_status <> 'valid' then return public.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Closure certificate is unavailable'); end if;
  if v_check.requested_scope_hash <> trim(coalesce(p_requested_scope_hash, '')) then return public.lcia_scope_closure_error('closure_check_scope_mismatch', 409, 'Requested scope does not match the closure certificate'); end if;
  if v_check.policy_fingerprint <> trim(coalesce(p_policy_fingerprint, '')) then return public.lcia_scope_closure_error('closure_check_policy_mismatch', 409, 'Policy does not match the closure certificate'); end if;
  -- v1 is intentionally not callable by API clients; migration history retains
  -- its original worker-payload behavior only for this fully-authorized wrapper.
  v_result := public.cmd_lcia_result_build_request_legacy(p_name, p_processes, p_coverage_mode, p_default_impact_category, p_lcia_method_set, p_idempotency_key,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object('closureCheckId', v_check.id, 'requestedScopeHash', v_check.requested_scope_hash, 'policyFingerprint', v_check.policy_fingerprint));
  return v_result || jsonb_build_object('closureCheckId', v_check.id, 'effectiveScopeHash', v_check.effective_scope_hash, 'certificateHash', v_check.certificate_hash);
end;
$$;

-- Keep the v1 contract available while the server-owned feature flag is off.
-- Once enabled, only new manager build commands are certificate-bound.
create function public.cmd_lcia_result_build_request(
  p_name text, p_processes jsonb default null, p_coverage_mode text default 'global_eligible',
  p_default_impact_category text default null, p_lcia_method_set jsonb default '[]'::jsonb,
  p_idempotency_key text default null, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if coalesce((select require_certificate_for_builds from public.lcia_scope_closure_config where singleton), false) then
    return public.lcia_scope_closure_error('closure_check_required', 409, 'A valid scope-closure certificate is required');
  end if;
  return public.cmd_lcia_result_build_request_legacy(p_name, p_processes, p_coverage_mode, p_default_impact_category, p_lcia_method_set, p_idempotency_key, p_audit);
end;
$$;

create or replace function public.get_task_summary_v2_feed(
  p_category text default null, p_job_kinds text[] default null, p_statuses text[] default null,
  p_updated_since timestamptz default null, p_cursor_updated_at timestamptz default null,
  p_cursor_job_id uuid default null, p_limit integer default 50, p_root_only boolean default false
) returns jsonb
language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid := auth.uid(); v_limit integer := greatest(1, least(coalesce(p_limit, 50), 200)); v_is_manager boolean;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  v_is_manager := public.lcia_scope_closure_is_manager();
  return jsonb_build_object('ok', true, 'serverTime', now(), 'data', coalesce((
    with projected as (
      select j.*, k.task_center_category, k.presenter_key,
        greatest(j.updated_at, coalesce(c.updated_at, '-infinity'::timestamptz), coalesce(p.updated_at, '-infinity'::timestamptz)) as projection_updated_at,
        c.id as closure_check_id, c.status as closure_status, c.certificate_status,
        p.id as package_id
      from public.worker_jobs j
      join public.worker_job_kinds k on k.job_kind = j.job_kind
      left join public.lcia_scope_closure_checks c on c.worker_job_id = j.id
      left join public.lcia_result_packages p on p.build_worker_job_id = j.id
      where j.requested_by = v_actor
        and (j.visibility = 'user' or (v_is_manager and j.visibility = 'operator' and k.task_center_category = 'data_product'))
        and (p_category is null or k.task_center_category = p_category)
        and (p_job_kinds is null or j.job_kind = any(p_job_kinds))
        and (p_statuses is null or j.status = any(p_statuses))
        and (p_updated_since is null or greatest(j.updated_at, coalesce(c.updated_at, '-infinity'::timestamptz), coalesce(p.updated_at, '-infinity'::timestamptz)) >= p_updated_since)
        and (not p_root_only or j.root_job_id is null or j.root_job_id = j.id)
    ), page as (
      select * from projected
      where p_cursor_updated_at is null or (projection_updated_at, id) < (p_cursor_updated_at, p_cursor_job_id)
      order by projection_updated_at desc, id desc limit v_limit + 1
    ), shown as (select * from page order by projection_updated_at desc, id desc limit v_limit)
    select jsonb_build_object(
      'items', coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'jobId', id, 'jobKind', job_kind, 'category', task_center_category, 'requestedBy', requested_by,
        'workerStatus', status, 'phase', phase, 'progressFraction', progress, 'progressCounters', diagnostics->'progressCounters',
        'domainStatus', coalesce(closure_status, result_json->>'status'), 'domainValidity', certificate_status,
        'projectionUpdatedAt', projection_updated_at, 'title', coalesce(payload_json->>'name', job_kind),
        'blockerCodes', blocker_codes, 'errorSummary', error_message,
        'capabilities', jsonb_build_object('canCancel', status in ('queued','running','waiting'),
          'canDownloadReport', closure_check_id is not null and closure_status in ('passed','blocked'),
          'canOpenWorkbench', task_center_category = 'data_product', 'canPreviewResult', package_id is not null),
        'deepLink', case when closure_check_id is not null then jsonb_build_object('routeKey','data_product.closure_check','params',jsonb_build_object('closureCheckId', closure_check_id))
          when package_id is not null then jsonb_build_object('routeKey','data_product.package','params',jsonb_build_object('packageId',package_id)) end,
        'closureCheckId', closure_check_id, 'resultPackageId', package_id
      )) order by projection_updated_at desc, id desc), '[]'::jsonb),
      'nextCursor', case when exists(select 1 from page offset v_limit) then (
        select jsonb_build_object('updatedAt', projection_updated_at, 'jobId', id)
        from shown order by projection_updated_at asc, id asc limit 1
      ) else null end
    ) from shown
  ), jsonb_build_object('items','[]'::jsonb, 'nextCursor', null)));
end;
$$;

revoke all on function public.cmd_lcia_scope_closure_check_request(text, text, text, jsonb) from public, anon, authenticated;
revoke all on function public.get_lcia_scope_closure_check(uuid) from public, anon, authenticated;
revoke all on function public.list_lcia_scope_closure_issues(uuid, uuid, integer) from public, anon, authenticated;
revoke all on function public.get_lcia_scope_closure_report_download(uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_check_record_result(uuid, text, text, text, text, jsonb, text[], uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_certificate_event(uuid, text, text) from public, anon, authenticated;
revoke all on function public.get_task_summary_v2_feed(text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean) from public, anon, authenticated;
revoke all on function public.cmd_lcia_result_build_request_legacy(text, jsonb, text, text, jsonb, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb) from public, anon, authenticated;
grant execute on function public.cmd_lcia_scope_closure_check_request(text, text, text, jsonb) to authenticated;
grant execute on function public.get_lcia_scope_closure_check(uuid) to authenticated;
grant execute on function public.list_lcia_scope_closure_issues(uuid, uuid, integer) to authenticated;
grant execute on function public.get_lcia_scope_closure_report_download(uuid) to authenticated;
grant execute on function public.svc_lcia_scope_closure_check_record_result(uuid, text, text, text, text, jsonb, text[], uuid) to service_role;
grant execute on function public.svc_lcia_scope_closure_certificate_event(uuid, text, text) to service_role;
grant execute on function public.get_task_summary_v2_feed(text, text[], text[], timestamptz, timestamptz, uuid, integer, boolean) to authenticated;
grant execute on function public.cmd_lcia_result_build_request_v2(text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb) to authenticated;
grant execute on function public.cmd_lcia_result_build_request(text, jsonb, text, text, jsonb, text, jsonb) to authenticated;
revoke all on function public.lcia_scope_closure_is_manager() from public, anon, authenticated;
grant execute on function public.lcia_scope_closure_is_manager() to authenticated, service_role;
