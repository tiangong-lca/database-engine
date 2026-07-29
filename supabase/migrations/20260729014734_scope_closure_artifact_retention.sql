-- Expiring scope-closure evidence lifecycle.
--
-- Storage deletion remains a Worker responsibility.  The database owns the
-- seven-day deadline, actor-safe download projection, certificate admission
-- fence, idempotent GC lease, and the compact audit residue left after detail
-- cleanup.

alter table public.worker_job_artifacts
  add column if not exists artifact_role text,
  add column if not exists lifecycle_state text,
  add column if not exists gc_claim_token uuid,
  add column if not exists gc_claimed_at timestamptz,
  add column if not exists gc_claim_expires_at timestamptz,
  add column if not exists gc_failure_count integer not null default 0,
  add column if not exists gc_last_error text,
  add column if not exists deleted_at timestamptz;

alter table public.worker_job_artifacts
  drop constraint if exists worker_job_artifacts_lifecycle_state_check,
  add constraint worker_job_artifacts_lifecycle_state_check
    check (
      lifecycle_state is null
      or lifecycle_state in ('ready', 'expired', 'deleted')
    ),
  drop constraint if exists worker_job_artifacts_gc_failure_count_check,
  add constraint worker_job_artifacts_gc_failure_count_check
    check (gc_failure_count >= 0);

create or replace function public.lcia_scope_closure_artifact_role(
  p_artifact_type text
) returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  select case p_artifact_type
    when 'closure_report_xlsx' then 'closure_report'
    when 'closure_complete_machine_result' then 'complete_machine_result'
    when 'closure_bundle' then 'closure_bundle'
    else null
  end
$$;

create or replace function public.lcia_scope_closure_artifact_lifecycle_guard()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_old_expected_role text := case
    when tg_op = 'UPDATE'
    then public.lcia_scope_closure_artifact_role(old.artifact_type)
  end;
  v_new_expected_role text :=
    public.lcia_scope_closure_artifact_role(new.artifact_type);
begin
  if tg_op = 'INSERT'
     and v_new_expected_role is null
     and new.artifact_role is null then
    return new;
  end if;
  if tg_op = 'UPDATE'
     and v_old_expected_role is null
     and v_new_expected_role is null
     and old.artifact_role is null
     and new.artifact_role is null then
    return new;
  end if;

  if tg_op = 'INSERT' then
    new.artifact_role := coalesce(new.artifact_role, v_new_expected_role);
    new.expires_at := coalesce(
      new.expires_at,
      coalesce(new.created_at, now()) + interval '7 days'
    );
    new.lifecycle_state := coalesce(
      new.lifecycle_state,
      case when new.expires_at <= now() then 'expired' else 'ready' end
    );
  else
    if new.artifact_type is distinct from old.artifact_type
       or new.artifact_role is distinct from old.artifact_role
       or new.created_at is distinct from old.created_at
       or new.expires_at is distinct from old.expires_at then
      raise exception 'scope_closure_artifact_identity_or_expiry_is_immutable'
        using errcode = '23514';
    end if;

    if old.lifecycle_state = 'deleted'
       and new.lifecycle_state is distinct from 'deleted' then
      raise exception 'scope_closure_artifact_deleted_is_terminal'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'expired'
       and new.lifecycle_state not in ('expired', 'deleted') then
      raise exception 'scope_closure_artifact_cannot_return_to_ready'
        using errcode = '23514';
    end if;
    if old.lifecycle_state = 'ready'
       and new.lifecycle_state not in ('ready', 'expired') then
      raise exception 'scope_closure_artifact_invalid_transition'
        using errcode = '23514';
    end if;
  end if;

  if v_new_expected_role is null
     or new.artifact_role is distinct from v_new_expected_role
     or new.expires_at is null
     or new.expires_at > coalesce(new.created_at, now()) + interval '7 days'
     or new.lifecycle_state is null then
    raise exception 'invalid_scope_closure_artifact_lifecycle'
      using errcode = '23514';
  end if;

  if new.lifecycle_state = 'ready'
     and (
       new.storage_bucket is null
       or nullif(trim(new.storage_bucket), '') is null
       or new.storage_path is null
       or nullif(trim(new.storage_path), '') is null
       or new.content_type is null
       or new.byte_size is null
       or new.checksum_sha256 is null
     ) then
    raise exception 'scope_closure_artifact_ready_metadata_incomplete'
      using errcode = '23514';
  end if;

  if new.lifecycle_state = 'deleted' then
    new.deleted_at := coalesce(new.deleted_at, now());
    new.storage_bucket := null;
    new.storage_path := null;
  end if;

  return new;
end;
$$;

update public.worker_job_artifacts
set artifact_role = public.lcia_scope_closure_artifact_role(artifact_type),
    lifecycle_state = case
      when least(
        coalesce(expires_at, created_at + interval '7 days'),
        created_at + interval '7 days'
      ) <= now() then 'expired'
      else 'ready'
    end,
    expires_at = least(
      coalesce(expires_at, created_at + interval '7 days'),
      created_at + interval '7 days'
    )
where public.lcia_scope_closure_artifact_role(artifact_type) is not null;

alter table public.worker_job_artifacts
  drop constraint if exists worker_job_artifacts_artifact_role_check,
  add constraint worker_job_artifacts_artifact_role_check
    check (
      artifact_role is not distinct from
        public.lcia_scope_closure_artifact_role(artifact_type)
    );

drop trigger if exists worker_job_artifacts_scope_closure_lifecycle
  on public.worker_job_artifacts;
create trigger worker_job_artifacts_scope_closure_lifecycle
before insert or update on public.worker_job_artifacts
for each row execute function public.lcia_scope_closure_artifact_lifecycle_guard();

create index if not exists worker_job_artifacts_scope_closure_gc_idx
  on public.worker_job_artifacts (
    expires_at,
    gc_claim_expires_at,
    created_at,
    id
  )
  where artifact_role is not null
    and lifecycle_state in ('ready', 'expired');

alter table public.lcia_scope_closure_checks
  add column if not exists complete_machine_result_artifact_id uuid,
  add column if not exists valid_until timestamptz;

create or replace function public.lcia_scope_closure_certificate_validity_guard()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_report public.worker_job_artifacts%rowtype;
  v_machine_result public.worker_job_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
begin
  if new.certificate_status <> 'valid' then
    return new;
  end if;

  select * into v_report
  from public.worker_job_artifacts
  where id = new.report_artifact_id;
  select * into v_bundle
  from public.worker_job_artifacts
  where id = new.closure_bundle_artifact_id;
  select * into v_machine_result
  from public.worker_job_artifacts
  where id = coalesce(
    new.complete_machine_result_artifact_id,
    case
      when coalesce(
        v_bundle.metadata->>'completeMachineResultArtifactId', ''
      ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      then (v_bundle.metadata->>'completeMachineResultArtifactId')::uuid
    end
  );

  if v_report.id is null
     or v_report.job_id <> new.worker_job_id
     or v_report.artifact_role <> 'closure_report'
     or v_report.lifecycle_state <> 'ready'
     or v_machine_result.id is null
     or (
       v_machine_result.job_id <> new.worker_job_id
       and not exists (
         select 1
         from public.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_machine_result.job_id
       )
     )
     or v_machine_result.artifact_role <> 'complete_machine_result'
     or v_machine_result.lifecycle_state <> 'ready'
     or v_bundle.id is null
     or (
       v_bundle.job_id <> new.worker_job_id
       and not exists (
         select 1
         from public.lcia_scope_closure_checks source
         where source.id = new.reused_from_check_id
           and source.worker_job_id = v_bundle.job_id
       )
     )
     or v_bundle.artifact_role <> 'closure_bundle'
     or v_bundle.lifecycle_state <> 'ready'
     or v_report.expires_at is null
     or v_machine_result.expires_at is null
     or v_bundle.expires_at is null then
    raise exception 'closure_certificate_evidence_lifecycle_invalid'
      using errcode = '23514';
  end if;

  new.complete_machine_result_artifact_id := v_machine_result.id;
  new.valid_until := least(
    v_report.expires_at,
    v_machine_result.expires_at,
    v_bundle.expires_at
  );
  if new.valid_until <= coalesce(new.finished_at, now())
     or new.valid_until <= now() then
    raise exception 'closure_certificate_evidence_already_expired'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

with resolved as (
  select
    closure_check.id,
    machine_result.id as machine_result_artifact_id,
    least(
      report.expires_at,
      machine_result.expires_at,
      bundle.expires_at
    ) as evidence_valid_until,
    (
      report.id is not null
      and report.job_id = closure_check.worker_job_id
      and report.artifact_role = 'closure_report'
      and report.lifecycle_state = 'ready'
      and machine_result.id is not null
      and (
        machine_result.job_id = closure_check.worker_job_id
        or (
          source_check.id is not null
          and source_check.worker_job_id = machine_result.job_id
        )
      )
      and machine_result.artifact_role = 'complete_machine_result'
      and machine_result.lifecycle_state = 'ready'
      and bundle.id is not null
      and (
        bundle.job_id = closure_check.worker_job_id
        or (
          source_check.id is not null
          and source_check.worker_job_id = bundle.job_id
        )
      )
      and bundle.artifact_role = 'closure_bundle'
      and bundle.lifecycle_state = 'ready'
      and report.expires_at > now()
      and machine_result.expires_at > now()
      and bundle.expires_at > now()
      and least(
        report.expires_at,
        machine_result.expires_at,
        bundle.expires_at
      ) > coalesce(closure_check.finished_at, now())
      and least(
        report.expires_at,
        machine_result.expires_at,
        bundle.expires_at
      ) > now()
    ) as remains_valid
  from public.lcia_scope_closure_checks closure_check
  left join public.worker_job_artifacts report
    on report.id = closure_check.report_artifact_id
  left join public.worker_job_artifacts bundle
    on bundle.id = closure_check.closure_bundle_artifact_id
  left join public.lcia_scope_closure_checks source_check
    on source_check.id = closure_check.reused_from_check_id
  left join public.worker_job_artifacts machine_result
    on machine_result.id = coalesce(
      closure_check.complete_machine_result_artifact_id,
      case
        when coalesce(
          bundle.metadata->>'completeMachineResultArtifactId', ''
        ) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        then (bundle.metadata->>'completeMachineResultArtifactId')::uuid
      end
    )
  where closure_check.certificate_status = 'valid'
), classified as (
  update public.lcia_scope_closure_checks closure_check
  set complete_machine_result_artifact_id =
        resolved.machine_result_artifact_id,
      valid_until = resolved.evidence_valid_until,
      certificate_status = case
        when resolved.remains_valid then 'valid'
        else 'stale'
      end,
      updated_at = case
        when resolved.remains_valid then closure_check.updated_at
        else now()
      end
  from resolved
  where closure_check.id = resolved.id
  returning closure_check.id, closure_check.certificate_status
)
insert into public.lcia_scope_closure_certificate_events (
  closure_check_id,
  certificate_status,
  reason,
  created_by
)
select
  id,
  'stale',
  'artifact_retention_migration_evidence_expired_or_incomplete',
  null
from classified
where certificate_status = 'stale';

drop trigger if exists lcia_scope_closure_checks_certificate_validity
  on public.lcia_scope_closure_checks;
create trigger lcia_scope_closure_checks_certificate_validity
before insert or update
on public.lcia_scope_closure_checks
for each row execute function public.lcia_scope_closure_certificate_validity_guard();

alter table public.lcia_scope_closure_checks
  drop constraint if exists lcia_scope_closure_checks_valid_until_check,
  add constraint lcia_scope_closure_checks_valid_until_check
    check (
      certificate_status <> 'valid'
      or (
        complete_machine_result_artifact_id is not null
        and valid_until is not null
        and valid_until > finished_at
      )
    ) not valid;

create index if not exists
  lcia_scope_closure_checks_complete_machine_result_artifact_idx
  on public.lcia_scope_closure_checks (complete_machine_result_artifact_id)
  where complete_machine_result_artifact_id is not null;

create or replace function public.lcia_scope_closure_evidence_usable(
  p_check public.lcia_scope_closure_checks
) returns boolean
language sql
stable
security definer
set search_path = public, pg_temp
as $$
  select
    (p_check).status = 'passed'
    and (p_check).scan_completeness = 'complete'
    and (p_check).certificate_status = 'valid'
    and (p_check).valid_until > now()
    and exists (
      select 1
      from public.worker_job_artifacts report
      cross join public.worker_job_artifacts machine_result
      cross join public.worker_job_artifacts bundle
      where report.id = (p_check).report_artifact_id
        and report.job_id = (p_check).worker_job_id
        and report.artifact_role = 'closure_report'
        and report.lifecycle_state = 'ready'
        and report.expires_at > now()
        and report.storage_bucket is not null
        and report.storage_path is not null
        and report.checksum_sha256 is not null
        and machine_result.id =
          (p_check).complete_machine_result_artifact_id
        and (
          machine_result.job_id = (p_check).worker_job_id
          or exists (
            select 1
            from public.lcia_scope_closure_checks source
            where source.id = (p_check).reused_from_check_id
              and source.worker_job_id = machine_result.job_id
          )
        )
        and machine_result.artifact_role = 'complete_machine_result'
        and machine_result.lifecycle_state = 'ready'
        and machine_result.expires_at > now()
        and machine_result.storage_bucket is not null
        and machine_result.storage_path is not null
        and machine_result.checksum_sha256 is not null
        and bundle.id = (p_check).closure_bundle_artifact_id
        and (
          bundle.job_id = (p_check).worker_job_id
          or exists (
            select 1
            from public.lcia_scope_closure_checks source
            where source.id = (p_check).reused_from_check_id
              and source.worker_job_id = bundle.job_id
          )
        )
        and bundle.artifact_role = 'closure_bundle'
        and bundle.lifecycle_state = 'ready'
        and bundle.expires_at > now()
        and bundle.storage_bucket is not null
        and bundle.storage_path is not null
        and bundle.checksum_sha256 is not null
    )
$$;

create or replace function public.lcia_scope_closure_build_admission_guard()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
  v_check_id uuid;
begin
  if new.job_kind <> 'lcia_result.package_build'
     or new.payload_schema_version <> 'lcia_result.package_build.request.v2' then
    return new;
  end if;

  begin
    v_check_id := nullif(new.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    raise exception 'closure_certificate_expired_or_unavailable'
      using errcode = '23514';
  end;

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_check_id
    and requested_by = new.requested_by;
  if v_check.id is null
     or not public.lcia_scope_closure_evidence_usable(v_check) then
    raise exception 'closure_certificate_expired_or_unavailable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

drop trigger if exists worker_jobs_scope_closure_build_admission
  on public.worker_jobs;
create trigger worker_jobs_scope_closure_build_admission
before insert or update of status, payload_json on public.worker_jobs
for each row execute function public.lcia_scope_closure_build_admission_guard();

alter function public.cmd_lcia_result_build_request_v2(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) rename to cmd_lcia_result_build_request_v2_without_expiry;

create or replace function public.cmd_lcia_result_build_request_v2(
  p_name text,
  p_processes jsonb,
  p_coverage_mode text,
  p_default_impact_category text,
  p_lcia_method_set jsonb,
  p_idempotency_key text,
  p_closure_check_id uuid,
  p_requested_scope_hash text,
  p_policy_fingerprint text,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;
  if v_check.valid_until <= now()
     or exists (
       select 1
       from public.worker_job_artifacts artifact
       where artifact.id in (
         v_check.report_artifact_id,
         v_check.complete_machine_result_artifact_id,
         v_check.closure_bundle_artifact_id
       )
         and (
           artifact.lifecycle_state in ('expired', 'deleted')
           or artifact.expires_at <= now()
         )
     ) then
    return public.lcia_scope_closure_error(
      'closure_certificate_expired', 410, 'Closure certificate evidence has expired'
    );
  end if;
  if not public.lcia_scope_closure_evidence_usable(v_check) then
    return public.lcia_scope_closure_error(
      'closure_check_not_usable', 409, 'Closure certificate evidence is not usable'
    );
  end if;

  return public.cmd_lcia_result_build_request_v2_without_expiry(
    p_name,
    p_processes,
    p_coverage_mode,
    p_default_impact_category,
    p_lcia_method_set,
    p_idempotency_key,
    p_closure_check_id,
    p_requested_scope_hash,
    p_policy_fingerprint,
    p_audit
  );
end;
$$;

alter function public.svc_lcia_scope_closure_build_binding(uuid)
  rename to svc_lcia_scope_closure_build_binding_without_expiry;

create or replace function public.svc_lcia_scope_closure_build_binding(
  build_worker_job_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_check_id uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select * into v_job
  from public.worker_jobs
  where id = build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build' then
    return public.lcia_scope_closure_error(
      'build_binding_not_found', 404, 'Build job not found'
    );
  end if;
  begin
    v_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    return public.lcia_scope_closure_error(
      'build_binding_invalid', 409, 'Build payload contains an invalid closure check identity'
    );
  end;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_check_id
    and requested_by = v_job.requested_by;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_usable', 409, 'Closure certificate is not valid for this build owner'
    );
  end if;
  if v_check.valid_until <= now()
     or not public.lcia_scope_closure_evidence_usable(v_check) then
    return public.lcia_scope_closure_error(
      'closure_certificate_expired', 410, 'Closure certificate evidence has expired'
    );
  end if;
  return public.svc_lcia_scope_closure_build_binding_without_expiry(
    build_worker_job_id
  );
end;
$$;

revoke all on function public.cmd_lcia_result_build_request_v2_without_expiry(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_lcia_result_build_request_v2(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) to authenticated;
revoke all on function public.svc_lcia_scope_closure_build_binding_without_expiry(
  uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_build_binding(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_build_binding(uuid)
  to service_role;

create or replace function public.get_lcia_scope_closure_report_download(
  p_closure_check_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_artifact public.worker_job_artifacts%rowtype;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not public.lcia_scope_closure_is_manager() then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id
    and requested_by = v_actor;
  if v_check.id is null then
    return public.lcia_scope_closure_error(
      'closure_check_not_found', 404, 'Closure check not found'
    );
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = v_check.report_artifact_id
    and job_id = v_check.worker_job_id
    and artifact_role = 'closure_report';

  if v_artifact.id is not null
     and (
       v_artifact.lifecycle_state in ('expired', 'deleted')
       or v_artifact.expires_at <= now()
       or v_check.valid_until <= now()
     ) then
    return public.lcia_scope_closure_error(
      'closure_report_expired', 410, 'Closure report has expired'
    );
  end if;

  if v_artifact.id is null
     or v_check.status not in ('passed', 'blocked')
     or v_artifact.lifecycle_state <> 'ready'
     or v_artifact.expires_at is null
     or v_artifact.storage_bucket is null
     or v_artifact.storage_path is null
     or v_artifact.content_type is null
     or v_artifact.byte_size is null
     or v_artifact.checksum_sha256 is null then
    return public.lcia_scope_closure_error(
      'closure_report_unavailable', 404, 'Closure report is not available'
    );
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'artifactId', v_artifact.id,
    'artifactRole', v_artifact.artifact_role,
    'filename', 'scope-closure-' || v_check.id::text || '.xlsx',
    'bucket', v_artifact.storage_bucket,
    'objectPath', v_artifact.storage_path,
    'mediaType', v_artifact.content_type,
    'size', v_artifact.byte_size,
    'checksumSha256', v_artifact.checksum_sha256,
    'artifactExpiresAt', v_artifact.expires_at
  ));
end;
$$;

revoke all on function public.get_lcia_scope_closure_report_download(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.get_lcia_scope_closure_report_download(uuid)
  to authenticated;

create table if not exists public.lcia_scope_closure_retention_summaries (
  closure_check_id uuid primary key
    references public.lcia_scope_closure_checks(id) on delete cascade,
  issue_count bigint not null,
  occurrence_count bigint not null,
  affected_root_count bigint not null,
  issue_content_hash text not null,
  compact_result_summary jsonb not null,
  retained_at timestamptz not null default now(),
  constraint lcia_scope_closure_retention_counts_check
    check (
      issue_count >= 0
      and occurrence_count >= 0
      and affected_root_count >= 0
    ),
  constraint lcia_scope_closure_retention_hash_check
    check (issue_content_hash ~ '^[a-f0-9]{64}$'),
  constraint lcia_scope_closure_retention_summary_check
    check (jsonb_typeof(compact_result_summary) = 'object')
);

alter table public.lcia_scope_closure_retention_summaries
  enable row level security;
revoke all on public.lcia_scope_closure_retention_summaries
  from public, anon, authenticated, service_role;
grant all on public.lcia_scope_closure_retention_summaries
  to service_role;

create or replace function public.svc_lcia_scope_closure_artifact_gc_claim(
  p_limit integer default 100,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_claim_token uuid := gen_random_uuid();
  v_items jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_limit not between 1 and 500
     or p_lease_seconds not between 30 and 3600 then
    return public.lcia_scope_closure_error(
      'invalid_gc_claim', 400, 'Invalid GC claim bounds'
    );
  end if;

  with candidates as (
    select id
    from public.worker_job_artifacts
    where artifact_role is not null
      and lifecycle_state in ('ready', 'expired')
      and expires_at <= now()
      and (
        gc_claim_token is null
        or gc_claim_expires_at is null
        or gc_claim_expires_at <= now()
      )
    order by expires_at, created_at, id
    for update skip locked
    limit p_limit
  ), claimed as (
    update public.worker_job_artifacts artifact
    set lifecycle_state = 'expired',
        gc_claim_token = v_claim_token,
        gc_claimed_at = now(),
        gc_claim_expires_at = now() + make_interval(secs => p_lease_seconds),
        gc_last_error = null
    from candidates
    where artifact.id = candidates.id
    returning artifact.*
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'artifactId', id,
    'artifactRole', artifact_role,
    'bucket', storage_bucket,
    'objectPath', storage_path,
    'checksumSha256', checksum_sha256,
    'artifactExpiresAt', expires_at
  ) order by expires_at, created_at, id), '[]'::jsonb)
  into v_items
  from claimed;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'claimToken', v_claim_token,
    'leaseExpiresAt', now() + make_interval(secs => p_lease_seconds),
    'items', v_items
  ));
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_gc_complete(
  p_artifact_id uuid,
  p_claim_token uuid,
  p_object_missing boolean default false,
  p_detail_limit integer default 10000
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_artifact public.worker_job_artifacts%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_deleted_occurrences integer := 0;
  v_deleted_roots integer := 0;
  v_deleted_issues integer := 0;
  v_remaining bigint := 0;
  v_reused boolean := false;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if p_detail_limit not between 1 and 50000 then
    return public.lcia_scope_closure_error(
      'invalid_gc_completion', 400, 'Invalid detail cleanup bound'
    );
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = p_artifact_id
  for update;
  if v_artifact.id is null then
    return public.lcia_scope_closure_error(
      'artifact_not_found', 404, 'Artifact not found'
    );
  end if;
  v_reused := v_artifact.lifecycle_state = 'deleted'
    and v_artifact.gc_claim_token = p_claim_token;
  if not v_reused
     and (
       v_artifact.lifecycle_state <> 'expired'
       or v_artifact.gc_claim_token is distinct from p_claim_token
       or v_artifact.gc_claim_expires_at < now()
     ) then
    return public.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;

  select * into v_check
  from public.lcia_scope_closure_checks
  where report_artifact_id = v_artifact.id
     or complete_machine_result_artifact_id = v_artifact.id
     or closure_bundle_artifact_id = v_artifact.id
  order by finished_at nulls last, id
  limit 1
  for update;

  if v_check.id is not null then
    insert into public.lcia_scope_closure_retention_summaries (
      closure_check_id,
      issue_count,
      occurrence_count,
      affected_root_count,
      issue_content_hash,
      compact_result_summary
    )
    select
      v_check.id,
      (
        select count(*)
        from public.lcia_scope_closure_issues issue
        where issue.closure_check_id = v_check.id
      ),
      (
        select count(*)
        from public.lcia_scope_closure_issue_occurrences occurrence
        join public.lcia_scope_closure_issues issue
          on issue.id = occurrence.closure_issue_id
        where issue.closure_check_id = v_check.id
      ),
      (
        select count(*)
        from public.lcia_scope_closure_issue_roots root
        join public.lcia_scope_closure_issues issue
          on issue.id = root.closure_issue_id
        where issue.closure_check_id = v_check.id
      ),
      public.lcia_scope_closure_sha256(coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'issueKey', issue.issue_key,
            'issueCode', issue.issue_code,
            'severity', issue.severity,
            'blocking', issue.blocking,
            'occurrenceCount', issue.occurrence_count,
            'affectedRootCount', issue.affected_root_count
          )
          order by issue.issue_key, issue.id
        )
        from public.lcia_scope_closure_issues issue
        where issue.closure_check_id = v_check.id
      ), '[]'::jsonb)),
      v_check.result_summary
    on conflict (closure_check_id) do nothing;

    with doomed as (
      select occurrence.ctid
      from public.lcia_scope_closure_issue_occurrences occurrence
      join public.lcia_scope_closure_issues issue
        on issue.id = occurrence.closure_issue_id
      where issue.closure_check_id = v_check.id
      limit p_detail_limit
    )
    delete from public.lcia_scope_closure_issue_occurrences occurrence
    using doomed
    where occurrence.ctid = doomed.ctid;
    get diagnostics v_deleted_occurrences = row_count;

    with doomed as (
      select root.ctid
      from public.lcia_scope_closure_issue_roots root
      join public.lcia_scope_closure_issues issue
        on issue.id = root.closure_issue_id
      where issue.closure_check_id = v_check.id
      limit p_detail_limit
    )
    delete from public.lcia_scope_closure_issue_roots root
    using doomed
    where root.ctid = doomed.ctid;
    get diagnostics v_deleted_roots = row_count;

    if not exists (
      select 1
      from public.lcia_scope_closure_issue_occurrences occurrence
      join public.lcia_scope_closure_issues issue
        on issue.id = occurrence.closure_issue_id
      where issue.closure_check_id = v_check.id
    ) and not exists (
      select 1
      from public.lcia_scope_closure_issue_roots root
      join public.lcia_scope_closure_issues issue
        on issue.id = root.closure_issue_id
      where issue.closure_check_id = v_check.id
    ) then
      with doomed as (
        select ctid
        from public.lcia_scope_closure_issues
        where closure_check_id = v_check.id
        limit p_detail_limit
      )
      delete from public.lcia_scope_closure_issues issue
      using doomed
      where issue.ctid = doomed.ctid;
      get diagnostics v_deleted_issues = row_count;
    end if;

    select
      (select count(*) from public.lcia_scope_closure_issues
       where closure_check_id = v_check.id)
      + (select count(*)
         from public.lcia_scope_closure_issue_occurrences occurrence
         join public.lcia_scope_closure_issues issue
           on issue.id = occurrence.closure_issue_id
         where issue.closure_check_id = v_check.id)
      + (select count(*)
         from public.lcia_scope_closure_issue_roots root
         join public.lcia_scope_closure_issues issue
           on issue.id = root.closure_issue_id
         where issue.closure_check_id = v_check.id)
    into v_remaining;
  end if;

  update public.worker_job_artifacts
  set lifecycle_state = 'deleted',
      deleted_at = now(),
      gc_claim_expires_at = null,
      gc_last_error = null
  where id = v_artifact.id
  returning * into v_artifact;

  return jsonb_build_object('ok', true, 'reused', v_reused, 'data',
    jsonb_build_object(
      'artifactId', v_artifact.id,
      'state', v_artifact.lifecycle_state,
      'objectMissing', p_object_missing,
      'deletedOccurrences', v_deleted_occurrences,
      'deletedAffectedRoots', v_deleted_roots,
      'deletedIssues', v_deleted_issues,
      'detailsRemaining', v_remaining
    )
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_artifact_gc_fail(
  p_artifact_id uuid,
  p_claim_token uuid,
  p_error text
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_artifact public.worker_job_artifacts%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return public.lcia_scope_closure_error(
      'invalid_gc_failure', 400, 'GC failure reason is required'
    );
  end if;

  select * into v_artifact
  from public.worker_job_artifacts
  where id = p_artifact_id
  for update;
  if v_artifact.id is null then
    return public.lcia_scope_closure_error(
      'artifact_not_found', 404, 'Artifact not found'
    );
  end if;
  if v_artifact.lifecycle_state = 'deleted'
     and v_artifact.gc_claim_token = p_claim_token then
    return jsonb_build_object('ok', true, 'reused', true);
  end if;
  if v_artifact.lifecycle_state <> 'expired'
     or v_artifact.gc_claim_token is distinct from p_claim_token then
    return public.lcia_scope_closure_error(
      'gc_claim_invalid', 409, 'GC claim is not current'
    );
  end if;

  update public.worker_job_artifacts
  set gc_claim_token = null,
      gc_claimed_at = null,
      gc_claim_expires_at = null,
      gc_failure_count = gc_failure_count + 1,
      gc_last_error = left(trim(p_error), 1000)
  where id = v_artifact.id
  returning * into v_artifact;

  return jsonb_build_object('ok', true, 'reused', false, 'data',
    jsonb_build_object(
      'artifactId', v_artifact.id,
      'state', v_artifact.lifecycle_state,
      'failureCount', v_artifact.gc_failure_count
    )
  );
end;
$$;

revoke all on function public.lcia_scope_closure_artifact_role(text)
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_artifact_lifecycle_guard()
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_certificate_validity_guard()
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_evidence_usable(
  public.lcia_scope_closure_checks
) from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_build_admission_guard()
  from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_claim(
  integer, integer
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_complete(
  uuid, uuid, boolean, integer
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_artifact_gc_fail(
  uuid, uuid, text
) from public, anon, authenticated, service_role;

grant execute on function public.svc_lcia_scope_closure_artifact_gc_claim(
  integer, integer
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_complete(
  uuid, uuid, boolean, integer
) to service_role;
grant execute on function public.svc_lcia_scope_closure_artifact_gc_fail(
  uuid, uuid, text
) to service_role;

comment on column public.worker_job_artifacts.artifact_role is
  'Authoritative role for seven-day scope-closure evidence artifacts.';
comment on column public.worker_job_artifacts.expires_at is
  'Trusted retention deadline. Scope-closure evidence expires no later than seven days after creation.';
comment on column public.lcia_scope_closure_checks.valid_until is
  'Certificate admission deadline, bounded by every required closure evidence artifact.';
comment on column public.lcia_scope_closure_checks.complete_machine_result_artifact_id is
  'Soft immutable link to the complete machine-readable closure result whose expiry bounds certificate admission.';
comment on table public.lcia_scope_closure_retention_summaries is
  'Compact counts, result summary, and content hash retained after high-cardinality closure detail GC.';
