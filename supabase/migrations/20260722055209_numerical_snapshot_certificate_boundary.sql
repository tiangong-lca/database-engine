-- Bind closure certificates to the numerical snapshot that the solver will
-- actually consume.  The previous certificate contract accepted snapshot
-- identities from worker JSON evidence; this migration makes those identities
-- database-owned and foreign-key protected.

alter table public.lca_snapshot_artifacts
  add column if not exists snapshot_index_sha256 text,
  add column if not exists snapshot_build_contract_hash text,
  add column if not exists effective_scope_hash text,
  add column if not exists data_snapshot_token text,
  add column if not exists closure_bundle_hash text;

alter table public.lca_snapshot_artifacts
  add constraint lca_snapshot_artifacts_certificate_hashes_chk
  check (
    (snapshot_index_sha256 is null or snapshot_index_sha256 ~ '^[0-9a-f]{64}$')
    and (snapshot_build_contract_hash is null or snapshot_build_contract_hash ~ '^[0-9a-f]{64}$')
    and (effective_scope_hash is null or effective_scope_hash ~ '^[0-9a-f]{64}$')
    and (closure_bundle_hash is null or closure_bundle_hash ~ '^[0-9a-f]{64}$')
  ) not valid;

alter table public.lca_snapshot_artifacts
  validate constraint lca_snapshot_artifacts_certificate_hashes_chk;

create index if not exists lca_snapshot_artifacts_certificate_lookup_idx
  on public.lca_snapshot_artifacts (snapshot_id, status, snapshot_index_sha256);

alter table public.lcia_scope_closure_scan_executions
  add column if not exists numerical_snapshot_id uuid;

update public.lcia_scope_closure_scan_executions
set numerical_snapshot_id = gen_random_uuid()
where numerical_snapshot_id is null;

insert into public.lca_network_snapshots (
  id, scope, process_filter, provider_matching_rule, status
)
select
  e.numerical_snapshot_id,
  'data_product',
  jsonb_build_object(
    'schemaVersion', 'lcia.numerical-snapshot-preallocation.v1',
    'scanExecutionId', e.id,
    'requestedScopeHash', e.requested_scope_hash,
    'dataSnapshotToken', e.data_snapshot_token
  ),
  'split_by_evidence_hybrid',
  'draft'
from public.lcia_scope_closure_scan_executions e
left join public.lca_network_snapshots s on s.id = e.numerical_snapshot_id
where s.id is null;

alter table public.lcia_scope_closure_scan_executions
  alter column numerical_snapshot_id set not null,
  add constraint lcia_scope_closure_scan_executions_numerical_snapshot_uidx
    unique (numerical_snapshot_id);

create or replace function public.lcia_scope_closure_preallocate_numerical_snapshot()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if tg_op = 'UPDATE' then
    if new.numerical_snapshot_id is distinct from old.numerical_snapshot_id then
      raise exception 'numerical_snapshot_id_is_immutable' using errcode = '23514';
    end if;
    return new;
  end if;

  new.numerical_snapshot_id := coalesce(new.numerical_snapshot_id, gen_random_uuid());
  insert into public.lca_network_snapshots (
    id, scope, process_filter, provider_matching_rule, status
  ) values (
    new.numerical_snapshot_id,
    'data_product',
    jsonb_build_object(
      'schemaVersion', 'lcia.numerical-snapshot-preallocation.v1',
      'scanExecutionId', new.id,
      'requestedScopeHash', new.requested_scope_hash,
      'dataSnapshotToken', new.data_snapshot_token
    ),
    'split_by_evidence_hybrid',
    'draft'
  ) on conflict (id) do nothing;
  return new;
end;
$$;

drop trigger if exists lcia_scope_closure_scan_executions_preallocate_snapshot
  on public.lcia_scope_closure_scan_executions;
create trigger lcia_scope_closure_scan_executions_preallocate_snapshot
before insert or update of numerical_snapshot_id
on public.lcia_scope_closure_scan_executions
for each row execute function public.lcia_scope_closure_preallocate_numerical_snapshot();

alter table public.lcia_scope_closure_checks
  add column if not exists closure_bundle_artifact_id uuid,
  add column if not exists snapshot_artifact_id uuid,
  add column if not exists snapshot_index_sha256 text,
  add column if not exists snapshot_build_contract_hash text;

alter table public.lcia_scope_closure_checks
  add constraint lcia_scope_closure_checks_closure_bundle_artifact_fkey
    foreign key (closure_bundle_artifact_id)
    references public.worker_job_artifacts(id) on delete restrict not valid,
  add constraint lcia_scope_closure_checks_certificate_snapshot_chk
  check (
    status <> 'passed'
    or certificate_status not in ('valid', 'stale', 'revoked')
    or (
      scan_completeness = 'complete'
      and effective_scope_manifest is not null
      and effective_scope_hash ~ '^[0-9a-f]{64}$'
      and source_fingerprint is not null
      and resolution_map_hash ~ '^[0-9a-f]{64}$'
      and closure_bundle_hash ~ '^[0-9a-f]{64}$'
      and closure_bundle_artifact_id is not null
      and snapshot_id is not null
      and snapshot_hash ~ '^[0-9a-f]{64}$'
      and snapshot_artifact_id is not null
      and snapshot_index_sha256 ~ '^[0-9a-f]{64}$'
      and snapshot_build_contract_hash ~ '^[0-9a-f]{64}$'
      and evidence_hash ~ '^[0-9a-f]{64}$'
      and certificate_hash ~ '^[0-9a-f]{64}$'
    )
  ) not valid;

-- Snapshot identities remain immutable audit values after a certificate is
-- stale or revoked, but they intentionally are not unconditional foreign keys:
-- retention may remove expired bytes once no usable certificate or result
-- package needs them.  The guards below provide state-aware deletion fences.
comment on column public.lcia_scope_closure_scan_executions.numerical_snapshot_id is
  'Immutable database-preallocated snapshot UUID. It remains as a soft audit reference after retention deletes a stale or revoked snapshot.';
comment on column public.lcia_scope_closure_checks.snapshot_id is
  'Immutable certificate snapshot UUID. It is a soft audit reference after the certificate is stale or revoked and retention deletes the snapshot.';
comment on column public.lcia_scope_closure_checks.snapshot_artifact_id is
  'Immutable certificate snapshot artifact UUID. It is a soft audit reference after stale or revoked evidence bytes are retained out.';

create or replace function public.lcia_scope_closure_guard_snapshot_delete()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if exists (
    select 1
    from public.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_id = old.id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  ) then
    raise exception 'lca_snapshot_has_valid_closure_certificate'
      using errcode = '23503';
  end if;
  if exists (
    select 1 from public.lcia_result_packages package
    where package.snapshot_id = old.id
  ) or exists (
    select 1 from public.lca_results result
    where result.snapshot_id = old.id
  ) then
    raise exception 'lca_snapshot_has_result_reference'
      using errcode = '23503';
  end if;
  return old;
end;
$$;

create or replace function public.lcia_scope_closure_snapshot_refs_immutable()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if (old.snapshot_id is not null and new.snapshot_id is distinct from old.snapshot_id)
     or (old.snapshot_artifact_id is not null and new.snapshot_artifact_id is distinct from old.snapshot_artifact_id) then
    raise exception 'lcia_scope_closure_snapshot_reference_is_immutable'
      using errcode = '23514';
  end if;
  return new;
end;
$$;

create or replace function public.lcia_scope_closure_guard_snapshot_artifact_delete()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if exists (
    select 1
    from public.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_artifact_id = old.id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  ) then
    raise exception 'lca_snapshot_artifact_has_valid_closure_certificate'
      using errcode = '23503';
  end if;
  if exists (
    select 1 from public.lcia_result_packages package
    where package.snapshot_id = old.snapshot_id
  ) or exists (
    select 1 from public.lca_results result
    where result.snapshot_id = old.snapshot_id
  ) then
    raise exception 'lca_snapshot_artifact_has_result_reference'
      using errcode = '23503';
  end if;
  return old;
end;
$$;

drop trigger if exists lca_network_snapshots_closure_delete_guard
  on public.lca_network_snapshots;
create trigger lca_network_snapshots_closure_delete_guard
before delete on public.lca_network_snapshots
for each row execute function public.lcia_scope_closure_guard_snapshot_delete();

drop trigger if exists lca_snapshot_artifacts_closure_delete_guard
  on public.lca_snapshot_artifacts;
create trigger lca_snapshot_artifacts_closure_delete_guard
before delete on public.lca_snapshot_artifacts
for each row execute function public.lcia_scope_closure_guard_snapshot_artifact_delete();

drop trigger if exists lcia_scope_closure_checks_snapshot_refs_immutable
  on public.lcia_scope_closure_checks;
create trigger lcia_scope_closure_checks_snapshot_refs_immutable
before update on public.lcia_scope_closure_checks
for each row execute function public.lcia_scope_closure_snapshot_refs_immutable();

revoke all on function public.lcia_scope_closure_guard_snapshot_delete()
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_guard_snapshot_artifact_delete()
  from public, anon, authenticated, service_role;
revoke all on function public.lcia_scope_closure_snapshot_refs_immutable()
  from public, anon, authenticated, service_role;

create index if not exists lcia_scope_closure_checks_snapshot_idx
  on public.lcia_scope_closure_checks (snapshot_id)
  where snapshot_id is not null;
create index if not exists lcia_scope_closure_checks_snapshot_artifact_idx
  on public.lcia_scope_closure_checks (snapshot_artifact_id)
  where snapshot_artifact_id is not null;
create index if not exists lcia_scope_closure_checks_closure_bundle_artifact_idx
  on public.lcia_scope_closure_checks (closure_bundle_artifact_id)
  where closure_bundle_artifact_id is not null;

-- Certificate validity changes are an append-only event stream.  The service
-- role may append through the event RPC, but even a direct table grant must not
-- permit history to be rewritten or removed.
create or replace function public.lcia_scope_closure_certificate_event_immutable()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  raise exception 'lcia_scope_closure_certificate_event_immutable'
    using errcode = '23514';
end;
$$;

drop trigger if exists lcia_scope_closure_certificate_events_immutable
  on public.lcia_scope_closure_certificate_events;
create trigger lcia_scope_closure_certificate_events_immutable
before update or delete
on public.lcia_scope_closure_certificate_events
for each row execute function public.lcia_scope_closure_certificate_event_immutable();

-- These rows are mutated only by the SECURITY DEFINER request/lease/result and
-- validity-event RPCs.  Direct service DML would bypass those state machines.
revoke insert, update, delete on table public.lcia_scope_closure_checks,
  public.lcia_scope_closure_scan_executions,
  public.lcia_scope_closure_certificate_events
  from service_role;
revoke all on function public.lcia_scope_closure_certificate_event_immutable()
  from public, anon, authenticated, service_role;

comment on column public.lcia_scope_closure_scan_executions.numerical_snapshot_id is
  'Stable database-preallocated snapshot UUID reused by retries and closure-check runs; retained as a soft audit reference after stale or revoked evidence is garbage-collected.';
comment on column public.lcia_scope_closure_checks.snapshot_artifact_id is
  'Ready numerical snapshot artifact UUID verified by record_result_v3 before signing and retained as a soft audit reference after stale or revoked evidence is garbage-collected.';

revoke all on function public.lcia_scope_closure_preallocate_numerical_snapshot()
  from public, anon, authenticated, service_role;

create or replace function public.lcia_scope_closure_sha256_text(p_value text)
returns text
language sql
immutable
set search_path = public, pg_temp
as $$
  select encode(extensions.digest(coalesce(p_value, ''), 'sha256'), 'hex')
$$;
revoke all on function public.lcia_scope_closure_sha256_text(text)
  from public, anon, authenticated, service_role;

create or replace function public.svc_lcia_scope_closure_check_get_worker_input(
  p_closure_check_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_check public.lcia_scope_closure_checks%rowtype;
  v_data_snapshot public.lcia_scope_closure_data_snapshots%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id;
  select * into v_data_snapshot
  from public.lcia_scope_closure_data_snapshots
  where data_snapshot_token = v_check.data_snapshot_token;
  select * into v_execution
  from public.lcia_scope_closure_scan_executions
  where id = v_check.scan_execution_id;
  if v_check.id is null
     or v_check.requested_scope_manifest is null
     or v_data_snapshot.data_snapshot_token is null
     or v_execution.id is null
     or v_execution.numerical_snapshot_id is null then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check or preallocated numerical snapshot not found');
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id,
    'scanExecutionId', v_check.scan_execution_id,
    'numericalSnapshotId', v_execution.numerical_snapshot_id,
    'requestedScope', v_check.requested_scope_manifest,
    'requestedScopeHash', v_check.requested_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'dataSnapshotToken', v_check.data_snapshot_token,
    'dataSnapshotManifest', v_data_snapshot.root_manifest,
    'dataSnapshotManifestHash', v_data_snapshot.root_manifest_hash,
    'publicationEpoch', v_data_snapshot.publication_epoch,
    'expectedValidatorScannerFingerprint', v_check.expected_validator_scanner_fingerprint,
    'requestFingerprint', v_check.request_fingerprint
  ));
end;
$$;

alter function public.svc_lcia_scope_closure_check_record_result_v2(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid
) rename to svc_lcia_scope_closure_check_record_result_v2_legacy;

create or replace function public.svc_lcia_scope_closure_check_record_result_v2(
  p_closure_check_id uuid,
  p_job_id uuid,
  p_lease_token uuid,
  p_status text,
  p_scan_completeness text,
  p_effective_scope_manifest jsonb,
  p_evidence jsonb,
  p_result_summary jsonb default '{}'::jsonb,
  p_issues jsonb default '[]'::jsonb,
  p_blocker_codes text[] default '{}'::text[],
  p_report_artifact_id uuid default null
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  if lower(trim(coalesce(p_status, ''))) = 'passed' then
    return public.lcia_scope_closure_error(
      'closure_snapshot_evidence_v3_required', 409,
      'Passed closure checks require database-verified numerical snapshot evidence through record_result_v3'
    );
  end if;
  return public.svc_lcia_scope_closure_check_record_result_v2_legacy(
    p_closure_check_id,
    p_job_id,
    p_lease_token,
    p_status,
    p_scan_completeness,
    p_effective_scope_manifest,
    coalesce(p_evidence, '{}'::jsonb)
      - array['snapshotId', 'snapshotHash', 'snapshotArtifactId',
              'snapshotIndexSha256', 'snapshotBuildContractHash'],
    p_result_summary,
    p_issues,
    p_blocker_codes,
    p_report_artifact_id
  );
end;
$$;

create or replace function public.svc_lcia_scope_closure_check_record_result_v3(
  check_id uuid,
  worker_job_id uuid,
  lease_token uuid,
  status text,
  scan_completeness text,
  effective_scope jsonb,
  evidence jsonb,
  result_summary jsonb,
  issues jsonb,
  blocker_codes text[],
  report_artifact_id uuid,
  closure_bundle_artifact_id uuid,
  snapshot_artifact_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_status text := lower(trim(coalesce(status, '')));
  v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_snapshot_artifact public.lca_snapshot_artifacts%rowtype;
  v_report public.worker_job_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
  v_effective_scope_hash text;
  v_report_manifest_hash text;
  v_evidence_hash text;
  v_build_contract_hash text;
  v_certificate_hash text;
  v_certificate_bindings jsonb;
  v_issue jsonb;
  v_occurrence jsonb;
  v_root jsonb;
  v_closure_issue public.lcia_scope_closure_issues%rowtype;
  v_worker_result jsonb;
  v_scan_key text;
  v_existing_execution uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if v_status not in ('passed', 'blocked', 'failed')
     or scan_completeness not in ('complete', 'incomplete', 'unknown')
     or jsonb_typeof(coalesce(effective_scope, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(evidence, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(result_summary, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(issues, 'null'::jsonb)) <> 'array' then
    return public.lcia_scope_closure_error('invalid_closure_result', 400, 'Invalid closure result payload');
  end if;

  -- Blocked and failed checks intentionally do not acquire or persist a
  -- numerical snapshot binding.  V2 remains the compatibility path for those
  -- non-certificate terminal states.
  if v_status <> 'passed' then
    return public.svc_lcia_scope_closure_check_record_result_v2_legacy(
      check_id, worker_job_id, lease_token, v_status, scan_completeness,
      effective_scope,
      coalesce(evidence, '{}'::jsonb)
        - array['snapshotId', 'snapshotHash', 'snapshotArtifactId',
                'snapshotIndexSha256', 'snapshotBuildContractHash'],
      result_summary, issues, blocker_codes, report_artifact_id
    );
  end if;

  if scan_completeness <> 'complete'
     or exists (
       select 1 from jsonb_array_elements(issues) issue(value)
       where coalesce((issue.value->>'blocking')::boolean, false)
     ) then
    return public.lcia_scope_closure_error('closure_check_incomplete', 409, 'Passed closure checks must be complete and free of blocking issues');
  end if;
  if evidence->>'schemaVersion' is distinct from 'lcia.scope-closure-evidence.v2'
     or result_summary->>'schemaVersion' is distinct from 'lcia.scope-closure-summary.v1'
     or not (evidence ?& array[
    'sourceFingerprint', 'resolutionMapHash', 'closureBundleHash',
    'closureBundleArtifactId', 'snapshotId', 'snapshotHash', 'snapshotArtifactId',
    'snapshotIndexSha256', 'snapshotBuildContractHash', 'evidenceHash'
  ]) or not (result_summary ? 'evidenceHash') then
    return public.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Passed closure checks require complete numerical snapshot evidence');
  end if;

  select * into v_check
  from public.lcia_scope_closure_checks
  where id = check_id
  for update;
  if v_check.id is null or v_check.worker_job_id <> worker_job_id then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_check.status not in ('queued', 'running') then
    return public.lcia_scope_closure_error('closure_check_already_terminal', 409, 'Closure check is already terminal');
  end if;

  select * into v_job
  from public.worker_jobs
  where id = worker_job_id
  for update;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from lease_token
     or v_job.lease_expires_at < now() then
    return public.lcia_scope_closure_error('worker_job_lease_invalid', 409, 'Worker job lease is no longer valid');
  end if;

  select * into v_execution
  from public.lcia_scope_closure_scan_executions
  where id = v_check.scan_execution_id
  for update;
  if v_execution.id is null
     or v_execution.status <> 'running'
     or v_execution.leased_by_job_id <> worker_job_id
     or v_execution.lease_token is distinct from lease_token then
    return public.lcia_scope_closure_error('scan_execution_lease_invalid', 409, 'Scan execution is not held by this worker job');
  end if;

  select * into v_report
  from public.worker_job_artifacts
  where id = report_artifact_id
    and job_id = v_job.id
    and artifact_type = 'closure_report_xlsx';
  if v_report.id is null then
    return public.lcia_scope_closure_error('closure_report_unavailable', 409, 'Report artifact does not belong to the closure job or has the wrong type');
  end if;

  select * into v_bundle
  from public.worker_job_artifacts
  where id = closure_bundle_artifact_id
    and job_id = v_job.id
    and artifact_type = 'closure_bundle';
  if v_bundle.id is null
     or v_bundle.checksum_sha256 is null
     or v_bundle.id::text <> evidence->>'closureBundleArtifactId'
     or v_bundle.checksum_sha256 <> evidence->>'closureBundleHash'
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text then
    return public.lcia_scope_closure_error('closure_bundle_binding_invalid', 409, 'Closure bundle does not belong to this check or its hash does not match');
  end if;

  select * into v_snapshot
  from public.lca_network_snapshots
  where id = v_execution.numerical_snapshot_id
  for update;
  select * into v_snapshot_artifact
  from public.lca_snapshot_artifacts
  where id = snapshot_artifact_id;

  v_effective_scope_hash := public.lcia_scope_closure_sha256(effective_scope);
  v_build_contract_hash := public.lcia_scope_closure_sha256_text(
    'lcia.numerical-snapshot-build-contract.v1' || chr(10)
    || v_effective_scope_hash || chr(10)
    || v_check.data_snapshot_token || chr(10)
    || v_bundle.checksum_sha256 || chr(10)
    || v_execution.numerical_snapshot_id::text || chr(10)
    || coalesce(v_snapshot_artifact.artifact_format, '')
  );

  if v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_snapshot.scope <> 'data_product'
     or v_snapshot.provider_matching_rule <> 'split_by_evidence_hybrid'
     or v_snapshot.process_filter->>'schemaVersion' <> 'lcia.numerical-snapshot-preallocation.v1'
     or v_snapshot.process_filter->>'scanExecutionId' <> v_execution.id::text
     or v_snapshot.process_filter->>'requestedScopeHash' <> v_execution.requested_scope_hash
     or v_snapshot.process_filter->>'dataSnapshotToken' <> v_execution.data_snapshot_token
     or v_snapshot.source_hash is distinct from evidence->>'sourceFingerprint'
     or v_snapshot.id::text <> evidence->>'snapshotId'
     or v_snapshot_artifact.id is null
     or v_snapshot_artifact.id::text <> evidence->>'snapshotArtifactId'
     or v_snapshot_artifact.snapshot_id <> v_snapshot.id
     or v_snapshot_artifact.status <> 'ready'
     or v_snapshot_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_snapshot_artifact.artifact_sha256 <> evidence->>'snapshotHash'
     or v_snapshot_artifact.snapshot_index_sha256 <> evidence->>'snapshotIndexSha256'
     or v_snapshot_artifact.snapshot_build_contract_hash <> evidence->>'snapshotBuildContractHash'
     or v_snapshot_artifact.snapshot_build_contract_hash <> v_build_contract_hash
     or v_snapshot_artifact.effective_scope_hash <> v_effective_scope_hash
     or v_snapshot_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_snapshot_artifact.closure_bundle_hash <> v_bundle.checksum_sha256 then
    return public.lcia_scope_closure_error('numerical_snapshot_binding_invalid', 409, 'Numerical snapshot or artifact does not satisfy the closure certificate contract');
  end if;

  v_report_manifest_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if coalesce(evidence->>'reportArtifactManifestHash', '') <> v_report_manifest_hash then
    return public.lcia_scope_closure_error('closure_report_hash_mismatch', 409, 'Report artifact manifest hash does not match persisted artifact metadata');
  end if;

  v_evidence_hash := public.lcia_scope_closure_sha256_text(
    'lcia.scope-closure-evidence.v2' || chr(10)
    || (evidence->>'sourceFingerprint') || chr(10)
    || (evidence->>'resolutionMapHash') || chr(10)
    || v_bundle.checksum_sha256 || chr(10)
    || v_snapshot.id::text || chr(10)
    || v_snapshot_artifact.artifact_sha256 || chr(10)
    || v_snapshot_artifact.id::text || chr(10)
    || v_snapshot_artifact.snapshot_index_sha256 || chr(10)
    || v_snapshot_artifact.snapshot_build_contract_hash
  );
  if evidence->>'evidenceHash' <> v_evidence_hash
     or result_summary->>'evidenceHash' <> v_evidence_hash then
    return public.lcia_scope_closure_error('closure_evidence_hash_mismatch', 409, 'Closure evidence hash does not match database-owned snapshot evidence');
  end if;

  v_certificate_bindings := jsonb_build_object(
    'certificateSchemaVersion', 'lcia.scope-closure-certificate.v2',
    'closureCheckId', v_check.id,
    'requestedScopeHash', v_check.requested_scope_hash,
    'effectiveScopeHash', v_effective_scope_hash,
    'effectiveScope', effective_scope,
    'policyFingerprint', v_check.policy_fingerprint,
    'dataSnapshotToken', v_check.data_snapshot_token,
    'validatorScannerFingerprint', v_check.expected_validator_scanner_fingerprint,
    'sourceFingerprint', evidence->>'sourceFingerprint',
    'resolutionMapHash', evidence->>'resolutionMapHash',
    'closureBundleArtifactId', v_bundle.id,
    'closureBundleHash', v_bundle.checksum_sha256,
    'snapshotId', v_snapshot.id,
    'snapshotHash', v_snapshot_artifact.artifact_sha256,
    'snapshotArtifactId', v_snapshot_artifact.id,
    'snapshotIndexSha256', v_snapshot_artifact.snapshot_index_sha256,
    'snapshotBuildContractHash', v_snapshot_artifact.snapshot_build_contract_hash,
    'reportArtifactManifestHash', v_report_manifest_hash,
    'evidenceHash', v_evidence_hash
  );
  v_certificate_hash := public.lcia_scope_closure_sha256(v_certificate_bindings);

  delete from public.lcia_scope_closure_issues where closure_check_id = v_check.id;
  for v_issue in select value from jsonb_array_elements(issues) loop
    insert into public.lcia_scope_closure_issues(
      closure_check_id, issue_key, severity, blocking, issue_code,
      source_dataset_type, source_dataset_id, source_dataset_version,
      json_path, reference_role, requested_target_type, requested_target_id,
      requested_target_version, message, suggested_action,
      occurrence_count, affected_root_count, details
    ) values (
      v_check.id,
      coalesce(nullif(trim(v_issue->>'issueKey'), ''), public.lcia_scope_closure_sha256(v_issue)),
      coalesce(v_issue->>'severity', 'blocker'),
      coalesce((v_issue->>'blocking')::boolean, false),
      coalesce(nullif(trim(v_issue->>'issueCode'), ''), 'closure_issue'),
      nullif(v_issue->>'sourceDatasetType', ''),
      nullif(v_issue->>'sourceDatasetId', '')::uuid,
      nullif(v_issue->>'sourceDatasetVersion', ''),
      nullif(v_issue->>'jsonPath', ''),
      nullif(v_issue->>'referenceRole', ''),
      nullif(v_issue->>'requestedTargetType', ''),
      nullif(v_issue->>'requestedTargetId', '')::uuid,
      nullif(v_issue->>'requestedTargetVersion', ''),
      coalesce(nullif(v_issue->>'message', ''), 'Closure validation issue'),
      nullif(v_issue->>'suggestedAction', ''),
      greatest(1, coalesce((v_issue->>'occurrenceCount')::integer, 1)),
      greatest(0, coalesce((v_issue->>'affectedRootCount')::integer, 0)),
      coalesce(v_issue->'details', '{}'::jsonb)
    ) returning * into v_closure_issue;
    for v_occurrence in
      select value from jsonb_array_elements(coalesce(v_issue->'occurrences', '[]'::jsonb))
    loop
      insert into public.lcia_scope_closure_issue_occurrences(
        closure_issue_id, occurrence_key, source_dataset_type,
        source_dataset_id, source_dataset_version, json_path, reference_role, details
      ) values (
        v_closure_issue.id,
        coalesce(nullif(v_occurrence->>'occurrenceKey', ''), public.lcia_scope_closure_sha256(v_occurrence)),
        nullif(v_occurrence->>'sourceDatasetType', ''),
        nullif(v_occurrence->>'sourceDatasetId', '')::uuid,
        nullif(v_occurrence->>'sourceDatasetVersion', ''),
        nullif(v_occurrence->>'jsonPath', ''),
        nullif(v_occurrence->>'referenceRole', ''),
        coalesce(v_occurrence->'details', '{}'::jsonb)
      );
    end loop;
    for v_root in
      select value from jsonb_array_elements(coalesce(v_issue->'affectedRoots', '[]'::jsonb))
    loop
      insert into public.lcia_scope_closure_issue_roots(
        closure_issue_id, root_dataset_type, root_dataset_id,
        root_dataset_version, impact_role, witness_path
      ) values (
        v_closure_issue.id,
        coalesce(nullif(v_root->>'datasetType', ''), 'process'),
        (v_root->>'id')::uuid,
        coalesce(nullif(v_root->>'version', ''), '00.00.000'),
        coalesce(nullif(v_root->>'impactRole', ''), 'root'),
        coalesce(v_root->'witnessPath', '[]'::jsonb)
      );
    end loop;
  end loop;

  update public.lcia_scope_closure_checks set
    status = 'passed',
    scan_completeness = 'complete',
    effective_scope_manifest = effective_scope,
    effective_scope_hash = v_effective_scope_hash,
    certificate_schema_version = 'lcia.scope-closure-certificate.v2',
    certificate_status = 'valid',
    certificate_hash = v_certificate_hash,
    source_fingerprint = nullif(evidence->>'sourceFingerprint', ''),
    resolution_map_hash = nullif(evidence->>'resolutionMapHash', ''),
    closure_bundle_hash = v_bundle.checksum_sha256,
    closure_bundle_artifact_id = v_bundle.id,
    snapshot_id = v_snapshot.id,
    snapshot_hash = v_snapshot_artifact.artifact_sha256,
    snapshot_artifact_id = v_snapshot_artifact.id,
    snapshot_index_sha256 = v_snapshot_artifact.snapshot_index_sha256,
    snapshot_build_contract_hash = v_snapshot_artifact.snapshot_build_contract_hash,
    report_artifact_manifest_hash = v_report_manifest_hash,
    evidence_hash = v_evidence_hash,
    result_summary = svc_lcia_scope_closure_check_record_result_v3.result_summary,
    blocker_codes = coalesce(svc_lcia_scope_closure_check_record_result_v3.blocker_codes, '{}'::text[]),
    report_artifact_id = v_report.id,
    updated_at = now(),
    finished_at = now()
  where id = v_check.id
  returning * into v_check;

  v_scan_key := public.lcia_scope_closure_sha256(jsonb_build_object(
    'effectiveScopeHash', v_check.effective_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'validatorScannerFingerprint', v_check.expected_validator_scanner_fingerprint,
    'dataSnapshotToken', v_check.data_snapshot_token
  ));
  select id into v_existing_execution
  from public.lcia_scope_closure_scan_executions
  where scan_key = v_scan_key and id <> v_execution.id
  limit 1;
  if v_existing_execution is not null then v_scan_key := null; end if;
  update public.lcia_scope_closure_scan_executions set
    scan_key = v_scan_key,
    status = 'completed',
    lease_token = null,
    leased_by_job_id = null,
    lease_expires_at = null,
    completed_check_id = v_check.id,
    source_fingerprint = v_check.source_fingerprint,
    evidence_hash = v_check.evidence_hash,
    updated_at = now(),
    completed_at = now()
  where id = v_execution.id;

  select public.worker_record_job_result(
    v_job.id,
    lease_token,
    'completed',
    jsonb_build_object(
      'closureCheckId', v_check.id,
      'status', v_check.status,
      'scanCompleteness', v_check.scan_completeness,
      'certificateStatus', v_check.certificate_status,
      'certificateHash', v_check.certificate_hash,
      'effectiveScopeHash', v_check.effective_scope_hash,
      'snapshotId', v_check.snapshot_id,
      'snapshotHash', v_check.snapshot_hash,
      'snapshotArtifactId', v_check.snapshot_artifact_id,
      'snapshotIndexSha256', v_check.snapshot_index_sha256,
      'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
      'evidenceHash', v_check.evidence_hash
    ),
    'lcia.scope_closure_check.result.v2',
    jsonb_build_object(
      'reportArtifactId', v_report.id,
      'closureBundleArtifactId', v_bundle.id,
      'snapshotArtifactId', v_snapshot_artifact.id
    ),
    jsonb_build_object('progressCounters', coalesce(result_summary->'progressCounters', '{}'::jsonb)),
    null, null, null, null, null, false
  ) into v_worker_result;
  if coalesce((v_worker_result->>'ok')::boolean, false) is not true then
    raise exception using errcode = 'P0001', message = 'worker_job_result_rejected';
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id,
    'status', v_check.status,
    'scanCompleteness', v_check.scan_completeness,
    'certificateStatus', v_check.certificate_status,
    'certificateHash', v_check.certificate_hash,
    'effectiveScopeHash', v_check.effective_scope_hash,
    'snapshotId', v_check.snapshot_id,
    'snapshotHash', v_check.snapshot_hash,
    'snapshotArtifactId', v_check.snapshot_artifact_id,
    'snapshotIndexSha256', v_check.snapshot_index_sha256,
    'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
    'evidenceHash', v_check.evidence_hash
  ));
exception
  when invalid_text_representation then
    return public.lcia_scope_closure_error('invalid_closure_result', 400, 'Closure result contains invalid identity values');
end;
$$;

revoke all on function public.svc_lcia_scope_closure_check_get_worker_input(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_check_get_worker_input(uuid)
  to service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v2_legacy(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid
) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v2(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid
) from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_check_record_result_v2(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid
) to service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v3(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid, uuid, uuid
) from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_check_record_result_v3(
  uuid, uuid, uuid, text, text, jsonb, jsonb, jsonb, jsonb, text[], uuid, uuid, uuid
) to service_role;

update public.worker_job_kinds
set payload_schema_version = 'lcia_result.package_build.request.v2',
    updated_at = now()
where job_kind = 'lcia_result.package_build';

create or replace function public.svc_lcia_scope_closure_build_binding(
  build_worker_job_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
  v_report public.worker_job_artifacts%rowtype;
  v_closure_check_id uuid;
  v_input_manifest jsonb;
  v_input_manifest_hash text;
  v_report_manifest_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_job
  from public.worker_jobs
  where id = build_worker_job_id
  for update;
  if v_job.id is null or v_job.job_kind <> 'lcia_result.package_build' then
    return public.lcia_scope_closure_error('build_binding_not_found', 404, 'Build job not found');
  end if;
  if v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
     or v_job.status <> 'running'
     or v_job.lease_token is null
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return public.lcia_scope_closure_error('build_worker_job_lease_invalid', 409, 'Build job does not hold a current running lease');
  end if;
  begin
    v_closure_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    return public.lcia_scope_closure_error('build_binding_invalid', 409, 'Build payload contains an invalid closure check identity');
  end;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_closure_check_id
  for share;
  if v_check.id is null
     or v_check.requested_by <> v_job.requested_by
     or v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.certificate_hash is null then
    return public.lcia_scope_closure_error('closure_check_not_usable', 409, 'Closure certificate is not valid for this build owner');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not public.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return public.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;

  select * into v_snapshot
  from public.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from public.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from public.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from public.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_input_manifest_hash := public.lcia_scope_closure_sha256(v_input_manifest);
  v_report_manifest_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_snapshot.source_hash is distinct from v_check.source_fingerprint
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or v_bundle.id is null
     or v_bundle.job_id <> v_check.worker_job_id
     or v_bundle.artifact_type <> 'closure_bundle'
     or v_bundle.checksum_sha256 <> v_check.closure_bundle_hash
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash then
    return public.lcia_scope_closure_error('closure_snapshot_binding_invalid', 409, 'Persisted closure snapshot evidence is no longer usable');
  end if;

  if coalesce(v_job.payload_json->>'closure_check_id', '') <> v_check.id::text
     or coalesce(v_job.payload_json->>'closure_certificate_hash', '') <> v_check.certificate_hash
     or coalesce(v_job.payload_json->>'requested_scope_hash', '') <> v_check.requested_scope_hash
     or coalesce(v_job.payload_json->>'policy_fingerprint', '') <> v_check.policy_fingerprint
     or coalesce(v_job.payload_json->>'effective_scope_hash', '') <> v_check.effective_scope_hash
     or coalesce(v_job.payload_json->>'data_snapshot_token', '') <> v_check.data_snapshot_token
     or coalesce(v_job.payload_json->>'snapshot_id', '') <> v_check.snapshot_id::text
     or coalesce(v_job.payload_json->>'snapshot_hash', '') <> v_check.snapshot_hash
     or coalesce(v_job.payload_json->>'closure_bundle_artifact_id', '') <> v_check.closure_bundle_artifact_id::text
     or coalesce(v_job.payload_json->>'closure_bundle_hash', '') <> v_check.closure_bundle_hash
     or coalesce(v_job.payload_json->>'report_artifact_manifest_hash', '') <> v_check.report_artifact_manifest_hash
     or coalesce(v_job.payload_json->>'snapshot_artifact_id', '') <> v_check.snapshot_artifact_id::text
     or coalesce(v_job.payload_json->>'snapshot_index_sha256', '') <> v_check.snapshot_index_sha256
     or coalesce(v_job.payload_json->>'snapshot_build_contract_hash', '') <> v_check.snapshot_build_contract_hash
     or coalesce(v_job.payload_json->'effective_scope', 'null'::jsonb) <> v_check.effective_scope_manifest
     or coalesce(v_job.payload_json->>'coverage_mode', '') <> v_check.effective_scope_manifest->>'coverageMode'
     or coalesce(v_job.payload_json->'lcia_method_set', 'null'::jsonb)
          <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or coalesce(v_job.payload_json->'input_manifest', 'null'::jsonb) <> v_input_manifest
     or coalesce(v_job.payload_json->>'input_manifest_hash', '') <> v_input_manifest_hash then
    return public.lcia_scope_closure_error('build_binding_mismatch', 409, 'Build payload does not exactly match the database-owned certificate');
  end if;

  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_check.id,
    'certificateHash', v_check.certificate_hash,
    'requestedScopeHash', v_check.requested_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'effectiveScopeHash', v_check.effective_scope_hash,
    'dataSnapshotToken', v_check.data_snapshot_token,
    'snapshotId', v_check.snapshot_id,
    'snapshotHash', v_check.snapshot_hash,
    'closureBundleArtifactId', v_check.closure_bundle_artifact_id,
    'closureBundleHash', v_check.closure_bundle_hash,
    'snapshotArtifactId', v_check.snapshot_artifact_id,
    'snapshotIndexSha256', v_check.snapshot_index_sha256,
    'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
    'coverageMode', v_check.effective_scope_manifest->>'coverageMode',
    'lciaMethodSet', v_check.effective_scope_manifest->'lciaMethods',
    'inputManifest', v_input_manifest,
    'inputManifestHash', v_input_manifest_hash,
    'effectiveScope', v_check.effective_scope_manifest
  ));
end;
$$;

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
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_check public.lcia_scope_closure_checks%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
  v_report public.worker_job_artifacts%rowtype;
  v_result jsonb;
  v_kind public.worker_job_kinds%rowtype;
  v_job public.worker_jobs%rowtype;
  v_build_id uuid;
  v_payload jsonb;
  v_input_manifest jsonb;
  v_report_manifest_hash text;
  v_idempotency_key text;
begin
  if v_actor is null then
    return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = p_closure_check_id and requested_by = v_actor
  for share;
  if v_check.id is null then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_check.certificate_status = 'stale' then
    return public.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate is stale');
  end if;
  if v_check.certificate_status = 'revoked' then
    return public.lcia_scope_closure_error('closure_check_revoked', 409, 'Closure certificate is revoked');
  end if;
  if v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.certificate_hash is null then
    return public.lcia_scope_closure_error('closure_check_not_usable', 409, 'A valid complete numerical snapshot certificate is required');
  end if;
  if v_check.requested_scope_hash <> trim(coalesce(p_requested_scope_hash, '')) then
    return public.lcia_scope_closure_error('closure_check_scope_mismatch', 409, 'Requested scope does not match the closure certificate');
  end if;
  if v_check.policy_fingerprint <> trim(coalesce(p_policy_fingerprint, '')) then
    return public.lcia_scope_closure_error('closure_check_policy_mismatch', 409, 'Policy does not match the closure certificate');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not public.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return public.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;

  select * into v_snapshot
  from public.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from public.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from public.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from public.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_report_manifest_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_snapshot.source_hash is distinct from v_check.source_fingerprint
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or v_bundle.id is null
     or v_bundle.job_id <> v_check.worker_job_id
     or v_bundle.artifact_type <> 'closure_bundle'
     or v_bundle.checksum_sha256 <> v_check.closure_bundle_hash
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash then
    return public.lcia_scope_closure_error('closure_snapshot_binding_invalid', 409, 'Numerical snapshot certificate binding is not ready');
  end if;

  v_result := public.cmd_lcia_result_build_request_v2_envelope(
    p_name, p_processes, p_coverage_mode, p_default_impact_category,
    p_lcia_method_set, p_idempotency_key, p_closure_check_id,
    p_requested_scope_hash, p_policy_fingerprint, p_audit
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then return v_result; end if;
  v_build_id := nullif(v_result->'data'->>'buildId', '')::uuid;
  select * into v_kind
  from public.worker_job_kinds
  where job_kind = 'lcia_result.package_build';
  if v_build_id is null or v_kind.job_kind is null then
    return public.lcia_scope_closure_error('build_enqueue_unavailable', 503, 'Build queue configuration is unavailable');
  end if;

  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_payload := coalesce(v_result->'data'->'workerJob'->'payload', '{}'::jsonb)
    || jsonb_build_object(
      'coverage_mode', v_check.effective_scope_manifest->>'coverageMode',
      'input_manifest', v_input_manifest,
      'input_manifest_hash', public.lcia_scope_closure_sha256(v_input_manifest),
      'lcia_method_set', v_check.effective_scope_manifest->'lciaMethods',
      'closure_check_id', v_check.id,
      'closure_certificate_hash', v_check.certificate_hash,
      'requested_scope_hash', v_check.requested_scope_hash,
      'policy_fingerprint', v_check.policy_fingerprint,
      'effective_scope_hash', v_check.effective_scope_hash,
      'data_snapshot_token', v_check.data_snapshot_token,
      'snapshot_id', v_check.snapshot_id,
      'snapshot_hash', v_check.snapshot_hash,
      'closure_bundle_artifact_id', v_check.closure_bundle_artifact_id,
      'closure_bundle_hash', v_check.closure_bundle_hash,
      'report_artifact_manifest_hash', v_check.report_artifact_manifest_hash,
      'snapshot_artifact_id', v_check.snapshot_artifact_id,
      'snapshot_index_sha256', v_check.snapshot_index_sha256,
      'snapshot_build_contract_hash', v_check.snapshot_build_contract_hash,
      'effective_scope', v_check.effective_scope_manifest
    );
  v_idempotency_key := nullif(v_result->'data'->'workerJob'->>'idempotencyKey', '');
  select * into v_job
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by = v_actor
    and idempotency_key is not distinct from v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc limit 1
  for update;
  if v_job.id is null then
    insert into public.worker_jobs(
      job_kind, worker_runtime, worker_queue, priority, queue_key,
      subject_type, subject_id, requester_type, requested_by,
      idempotency_key, request_hash, concurrency_key, visibility,
      max_attempts, payload_schema_version, payload_json, payload_ref,
      result_schema_version
    ) values (
      v_kind.job_kind, v_kind.worker_runtime, v_kind.worker_queue,
      v_kind.default_priority, nullif(v_result->'data'->'workerJob'->>'queueKey', ''),
      'lcia_result_build', v_build_id, 'operator', v_actor,
      v_idempotency_key, nullif(v_result->'data'->'workerJob'->>'requestHash', ''),
      nullif(v_result->'data'->'workerJob'->>'queueKey', ''), 'operator',
      v_kind.default_max_attempts, 'lcia_result.package_build.request.v2',
      v_payload,
      jsonb_build_object('closureCertificate', jsonb_build_object(
        'closureCheckId', v_check.id,
        'certificateHash', v_check.certificate_hash,
        'requestedScopeHash', v_check.requested_scope_hash,
        'policyFingerprint', v_check.policy_fingerprint,
        'effectiveScopeHash', v_check.effective_scope_hash,
        'dataSnapshotToken', v_check.data_snapshot_token,
        'snapshotId', v_check.snapshot_id,
        'snapshotHash', v_check.snapshot_hash,
        'closureBundleArtifactId', v_check.closure_bundle_artifact_id,
        'closureBundleHash', v_check.closure_bundle_hash,
        'reportArtifactManifestHash', v_check.report_artifact_manifest_hash,
        'snapshotArtifactId', v_check.snapshot_artifact_id,
        'snapshotIndexSha256', v_check.snapshot_index_sha256,
        'snapshotBuildContractHash', v_check.snapshot_build_contract_hash,
        'effectiveScope', v_check.effective_scope_manifest
      )),
      v_kind.result_schema_version
    ) returning * into v_job;
    insert into public.worker_job_events(job_id, event_type, status, details)
    values(v_job.id, 'enqueued', v_job.status, jsonb_build_object(
      'jobKind', v_job.job_kind,
      'closureCheckId', v_check.id,
      'certificateHash', v_check.certificate_hash,
      'snapshotId', v_check.snapshot_id
    ));
  elsif v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
        or v_job.payload_json <> v_payload then
    return public.lcia_scope_closure_error('build_enqueue_conflict', 409, 'Existing active build does not match the certificate-bound V2 payload');
  end if;
  return jsonb_set(
    jsonb_set(v_result, '{data,workerJob}', public.worker_job_payload(v_job, false), true),
    '{data,workerJobId}', to_jsonb(v_job.id), true
  );
exception
  when unique_violation then
    return public.lcia_scope_closure_error('build_enqueue_conflict', 409, 'A conflicting certificate-bound build is already active');
end;
$$;

revoke all on function public.svc_lcia_scope_closure_build_binding(uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_build_binding(uuid)
  to service_role;
-- The predecessor accepted caller-supplied check identities and rewrote the
-- build payload without enforcing lease, owner, freshness, or exact evidence.
revoke all on function public.svc_lcia_result_build_bind_closure(uuid, uuid)
  from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_lcia_result_build_request_v2(
  text, jsonb, text, text, jsonb, text, uuid, text, text, jsonb
) to authenticated;

create or replace function public.lcia_result_package_bind_closure_certificate()
returns trigger
language plpgsql
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
  v_report public.worker_job_artifacts%rowtype;
  v_closure_check_id uuid;
  v_input_manifest jsonb;
  v_input_manifest_hash text;
  v_report_manifest_hash text;
begin
  select * into v_job
  from public.worker_jobs
  where id = new.build_worker_job_id
  for share;
  if v_job.payload_schema_version <> 'lcia_result.package_build.request.v2' then
    return new;
  end if;
  v_closure_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_closure_check_id
  for share;
  select * into v_snapshot
  from public.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from public.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from public.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from public.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_input_manifest := jsonb_build_object(
    'predicateVersion', v_check.effective_scope_manifest->>'eligibilityPredicateVersion',
    'selectionMode', 'closure_certificate',
    'processes', v_check.effective_scope_manifest->'processes'
  );
  v_input_manifest_hash := public.lcia_scope_closure_sha256(v_input_manifest);
  v_report_manifest_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_check.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.status <> 'running'
     or v_job.lease_token is null
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now()
     or v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.requested_by <> v_job.requested_by
     or v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_snapshot.source_hash is distinct from v_check.source_fingerprint
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or v_bundle.id is null
     or v_bundle.job_id <> v_check.worker_job_id
     or v_bundle.artifact_type <> 'closure_bundle'
     or v_bundle.checksum_sha256 <> v_check.closure_bundle_hash
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash
     or new.build_id <> v_job.subject_id
     or new.created_by <> v_job.requested_by
     or new.snapshot_id <> v_check.snapshot_id
     or new.coverage_mode <> v_check.effective_scope_manifest->>'coverageMode'
     or new.lcia_method_set <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or new.input_manifest <> v_input_manifest
     or new.input_manifest_hash <> v_input_manifest_hash
     or coalesce(v_job.payload_json->>'closure_check_id', '') <> v_check.id::text
     or coalesce(v_job.payload_json->>'closure_certificate_hash', '') <> v_check.certificate_hash
     or coalesce(v_job.payload_json->>'requested_scope_hash', '') <> v_check.requested_scope_hash
     or coalesce(v_job.payload_json->>'policy_fingerprint', '') <> v_check.policy_fingerprint
     or coalesce(v_job.payload_json->>'effective_scope_hash', '') <> v_check.effective_scope_hash
     or coalesce(v_job.payload_json->>'data_snapshot_token', '') <> v_check.data_snapshot_token
     or coalesce(v_job.payload_json->>'snapshot_id', '') <> v_check.snapshot_id::text
     or coalesce(v_job.payload_json->>'snapshot_hash', '') <> v_check.snapshot_hash
     or coalesce(v_job.payload_json->>'closure_bundle_artifact_id', '') <> v_check.closure_bundle_artifact_id::text
     or coalesce(v_job.payload_json->>'closure_bundle_hash', '') <> v_check.closure_bundle_hash
     or coalesce(v_job.payload_json->>'report_artifact_manifest_hash', '') <> v_check.report_artifact_manifest_hash
     or coalesce(v_job.payload_json->>'snapshot_artifact_id', '') <> v_check.snapshot_artifact_id::text
     or coalesce(v_job.payload_json->>'snapshot_index_sha256', '') <> v_check.snapshot_index_sha256
     or coalesce(v_job.payload_json->>'snapshot_build_contract_hash', '') <> v_check.snapshot_build_contract_hash
     or coalesce(v_job.payload_json->'effective_scope', 'null'::jsonb) <> v_check.effective_scope_manifest
     or coalesce(v_job.payload_json->>'coverage_mode', '') <> v_check.effective_scope_manifest->>'coverageMode'
     or coalesce(v_job.payload_json->'lcia_method_set', 'null'::jsonb)
          <> coalesce(v_check.effective_scope_manifest->'lciaMethods', 'null'::jsonb)
     or coalesce(v_job.payload_json->'input_manifest', 'null'::jsonb) <> v_input_manifest
     or coalesce(v_job.payload_json->>'input_manifest_hash', '') <> v_input_manifest_hash then
    raise exception 'closure_certificate_binding_mismatch' using errcode = '23514';
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not public.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    raise exception 'closure_certificate_stale' using errcode = '23514';
  end if;
  new.closure_check_id := v_check.id;
  new.closure_certificate_hash := v_check.certificate_hash;
  new.closure_snapshot_hash := v_check.snapshot_hash;
  return new;
end;
$$;

alter function public.cmd_lcia_result_package_mark_ready(
  uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb
) rename to cmd_lcia_result_package_mark_ready_without_closure_recheck;

create or replace function public.cmd_lcia_result_package_mark_ready(
  p_build_worker_job_id uuid,
  p_package_version text,
  p_snapshot_id uuid,
  p_result_id uuid,
  p_latest_all_unit_result_id uuid default null::uuid,
  p_result_artifact_ref jsonb default '{}'::jsonb,
  p_query_artifact_ref jsonb default '{}'::jsonb,
  p_artifact_manifest jsonb default '{}'::jsonb,
  p_available_impact_categories jsonb default '[]'::jsonb,
  p_default_impact_category text default null::text,
  p_package_result_hash text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_check public.lcia_scope_closure_checks%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_closure_check_id uuid;
  v_binding jsonb;
begin
  if not public.lcia_result_is_service_request() then
    return public.lcia_result_error('service_role_required', 403, 'Service role is required to mark LCIA result packages ready');
  end if;
  select * into v_job
  from public.worker_jobs
  where id = p_build_worker_job_id
  for update;
  if v_job.id is null or v_job.job_kind <> 'lcia_result.package_build' then
    return public.lcia_result_error('build_worker_job_not_found', 404, 'LCIA result package build worker job not found');
  end if;
  if v_job.payload_schema_version <> 'lcia_result.package_build.request.v2' then
    return public.cmd_lcia_result_package_mark_ready_without_closure_recheck(
      p_build_worker_job_id, p_package_version, p_snapshot_id, p_result_id,
      p_latest_all_unit_result_id, p_result_artifact_ref, p_query_artifact_ref,
      p_artifact_manifest, p_available_impact_categories,
      p_default_impact_category, p_package_result_hash, p_audit
    );
  end if;
  if v_job.status <> 'running'
     or v_job.lease_token is null
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at < now() then
    return public.lcia_result_error('build_worker_job_lease_invalid', 409, 'Build job does not hold a current running lease');
  end if;
  begin
    v_closure_check_id := nullif(v_job.payload_json->>'closure_check_id', '')::uuid;
  exception when invalid_text_representation then
    return public.lcia_result_error('closure_certificate_binding_mismatch', 409, 'Build payload contains an invalid closure check identity');
  end;
  select * into v_check
  from public.lcia_scope_closure_checks
  where id = v_closure_check_id
  for share;
  if v_check.id is null then
    return public.lcia_result_error('closure_check_not_usable', 409, 'Closure certificate is not available');
  end if;
  if v_check.certificate_status = 'revoked' then
    return public.lcia_result_error('closure_check_revoked', 409, 'Closure certificate was revoked before package readiness');
  end if;
  if v_check.certificate_status = 'stale' then
    return public.lcia_result_error('closure_check_stale', 409, 'Closure certificate became stale before package readiness');
  end if;
  if v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.requested_by <> v_job.requested_by then
    return public.lcia_result_error('closure_check_not_usable', 409, 'Closure certificate is no longer usable');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not public.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return public.lcia_result_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;
  v_binding := public.svc_lcia_scope_closure_build_binding(p_build_worker_job_id);
  if coalesce((v_binding->>'ok')::boolean, false) is not true then
    return public.lcia_result_error('closure_certificate_binding_mismatch', 409, 'Build payload or persisted evidence no longer matches the numerical snapshot certificate');
  end if;
  select * into v_snapshot
  from public.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from public.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  if p_snapshot_id <> v_check.snapshot_id
     or v_snapshot.id is null
     or v_snapshot.status <> 'ready'
     or v_artifact.id is null
     or v_artifact.snapshot_id <> v_snapshot.id
     or v_artifact.status <> 'ready'
     or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
     or v_artifact.artifact_sha256 <> v_check.snapshot_hash
     or v_artifact.snapshot_index_sha256 <> v_check.snapshot_index_sha256
     or v_artifact.snapshot_build_contract_hash <> v_check.snapshot_build_contract_hash
     or v_artifact.effective_scope_hash <> v_check.effective_scope_hash
     or v_artifact.data_snapshot_token <> v_check.data_snapshot_token
     or v_artifact.closure_bundle_hash <> v_check.closure_bundle_hash
     or coalesce(v_job.payload_json->>'closure_certificate_hash', '') <> v_check.certificate_hash
     or coalesce(v_job.payload_json->>'snapshot_id', '') <> v_check.snapshot_id::text
     or coalesce(v_job.payload_json->>'snapshot_hash', '') <> v_check.snapshot_hash
     or coalesce(v_job.payload_json->>'snapshot_artifact_id', '') <> v_check.snapshot_artifact_id::text
     or coalesce(v_job.payload_json->>'snapshot_index_sha256', '') <> v_check.snapshot_index_sha256
     or coalesce(v_job.payload_json->>'snapshot_build_contract_hash', '') <> v_check.snapshot_build_contract_hash
     or coalesce(v_job.payload_json->'effective_scope', 'null'::jsonb) <> v_check.effective_scope_manifest
     or coalesce(v_job.payload_json->'input_manifest'->'processes', 'null'::jsonb)
          <> coalesce(v_check.effective_scope_manifest->'processes', 'null'::jsonb) then
    return public.lcia_result_error('closure_certificate_binding_mismatch', 409, 'Build result does not match the current numerical snapshot certificate');
  end if;
  return public.cmd_lcia_result_package_mark_ready_without_closure_recheck(
    p_build_worker_job_id, p_package_version, p_snapshot_id, p_result_id,
    p_latest_all_unit_result_id, p_result_artifact_ref, p_query_artifact_ref,
    p_artifact_manifest, p_available_impact_categories,
    p_default_impact_category, p_package_result_hash, p_audit
  );
end;
$$;

revoke all on function public.cmd_lcia_result_package_mark_ready_without_closure_recheck(
  uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb
) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_package_mark_ready(
  uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb
) from public, anon, authenticated, service_role;
grant execute on function public.cmd_lcia_result_package_mark_ready(
  uuid, text, uuid, uuid, uuid, jsonb, jsonb, jsonb, jsonb, text, text, jsonb
) to service_role;

create or replace function public.svc_lcia_scope_closure_reuse_completed_scan(
  p_closure_check_id uuid,
  p_worker_job_id uuid,
  p_lease_token uuid,
  p_completed_check_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_target public.lcia_scope_closure_checks%rowtype;
  v_source public.lcia_scope_closure_checks%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
  v_job public.worker_jobs%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_target from public.lcia_scope_closure_checks where id = p_closure_check_id for update;
  select * into v_source
  from public.lcia_scope_closure_checks
  where id = p_completed_check_id
  for share;
  select * into v_job from public.worker_jobs where id = p_worker_job_id for update;
  if v_target.id is null or v_source.id is null
     or v_target.worker_job_id <> p_worker_job_id
     or v_target.status not in ('queued', 'running') then
    return public.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_job.status <> 'running'
     or v_job.lease_token is distinct from p_lease_token
     or v_job.lease_expires_at < now() then
    return public.lcia_scope_closure_error('worker_job_lease_invalid', 409, 'Worker job lease is no longer valid');
  end if;
  select * into v_execution
  from public.lcia_scope_closure_scan_executions
  where id = v_target.scan_execution_id
  for update;
  if v_execution.id is null
     or v_execution.status <> 'completed'
     or v_execution.completed_check_id <> v_source.id
     or v_execution.numerical_snapshot_id is distinct from v_source.snapshot_id
     or v_source.status not in ('passed', 'blocked')
     or v_source.scan_completeness <> 'complete'
     or v_source.requested_scope_hash <> v_target.requested_scope_hash
     or v_source.policy_fingerprint <> v_target.policy_fingerprint
     or v_source.data_snapshot_token <> v_target.data_snapshot_token then
    return public.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Completed scan evidence does not match this closure run');
  end if;
  if v_source.status = 'passed' then
    select * into v_snapshot from public.lca_network_snapshots where id = v_source.snapshot_id;
    select * into v_artifact from public.lca_snapshot_artifacts where id = v_source.snapshot_artifact_id;
    select * into v_bundle from public.worker_job_artifacts where id = v_source.closure_bundle_artifact_id;
    if v_source.certificate_status <> 'valid'
       or v_source.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
       or (
         v_source.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
         and not public.lcia_scope_closure_current_release_matches(v_source.data_snapshot_token)
       )
       or v_snapshot.id is null
       or v_snapshot.status <> 'ready'
       or v_snapshot.source_hash is distinct from v_source.source_fingerprint
       or v_artifact.id is null
       or v_artifact.snapshot_id <> v_snapshot.id
       or v_artifact.status <> 'ready'
       or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
       or v_artifact.artifact_sha256 <> v_source.snapshot_hash
       or v_artifact.snapshot_index_sha256 <> v_source.snapshot_index_sha256
       or v_artifact.snapshot_build_contract_hash <> v_source.snapshot_build_contract_hash
       or v_artifact.effective_scope_hash <> v_source.effective_scope_hash
       or v_artifact.data_snapshot_token <> v_source.data_snapshot_token
       or v_artifact.closure_bundle_hash <> v_source.closure_bundle_hash
       or v_bundle.id is null
       or v_bundle.job_id <> v_source.worker_job_id
       or v_bundle.artifact_type <> 'closure_bundle'
       or v_bundle.checksum_sha256 <> v_source.closure_bundle_hash
       or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_source.id::text then
      return public.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Source numerical snapshot certificate is not reusable');
    end if;
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'reuseAvailable', true,
    'closureCheckId', v_target.id,
    'workerJobId', v_job.id,
    'completedCheckId', v_source.id,
    'status', v_source.status,
    'scanCompleteness', v_source.scan_completeness,
    'evidence', jsonb_strip_nulls(jsonb_build_object(
      'schemaVersion', 'lcia.scope-closure-evidence.v2',
      'sourceFingerprint', v_source.source_fingerprint,
      'resolutionMapHash', v_source.resolution_map_hash,
      'closureBundleHash', v_source.closure_bundle_hash,
      'closureBundleArtifactId', v_source.closure_bundle_artifact_id,
      'snapshotId', v_source.snapshot_id,
      'snapshotHash', v_source.snapshot_hash,
      'snapshotArtifactId', v_source.snapshot_artifact_id,
      'snapshotIndexSha256', v_source.snapshot_index_sha256,
      'snapshotBuildContractHash', v_source.snapshot_build_contract_hash,
      'evidenceHash', v_source.evidence_hash
    )),
    'blockerCodes', to_jsonb(v_source.blocker_codes)
  ));
end;
$$;

create or replace function public.svc_lcia_scope_closure_finalize_reused_scan(
  p_closure_check_id uuid,
  p_worker_job_id uuid,
  p_lease_token uuid,
  p_completed_check_id uuid,
  p_report_artifact_id uuid,
  p_result_summary jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_target public.lcia_scope_closure_checks%rowtype;
  v_source public.lcia_scope_closure_checks%rowtype;
  v_execution public.lcia_scope_closure_scan_executions%rowtype;
  v_job public.worker_jobs%rowtype;
  v_report public.worker_job_artifacts%rowtype;
  v_snapshot public.lca_network_snapshots%rowtype;
  v_artifact public.lca_snapshot_artifacts%rowtype;
  v_bundle public.worker_job_artifacts%rowtype;
  v_old_issue public.lcia_scope_closure_issues%rowtype;
  v_new_issue public.lcia_scope_closure_issues%rowtype;
  v_report_hash text;
  v_certificate_hash text;
  v_worker_status text;
  v_worker_record jsonb;
  v_summary jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if jsonb_typeof(coalesce(p_result_summary, 'null'::jsonb)) <> 'object'
     or p_result_summary->>'schemaVersion' is distinct from 'lcia.scope-closure-summary.v1' then
    return public.lcia_scope_closure_error('invalid_closure_result', 400, 'Reused closure result summary must be an object');
  end if;
  select * into v_target from public.lcia_scope_closure_checks where id = p_closure_check_id for update;
  select * into v_source
  from public.lcia_scope_closure_checks
  where id = p_completed_check_id
  for share;
  select * into v_job from public.worker_jobs where id = p_worker_job_id for update;
  select * into v_execution from public.lcia_scope_closure_scan_executions where id = v_target.scan_execution_id for update;
  select * into v_report from public.worker_job_artifacts
  where id = p_report_artifact_id
    and job_id = p_worker_job_id
    and artifact_type = 'closure_report_xlsx';
  if v_target.id is null or v_source.id is null
     or v_target.worker_job_id <> v_job.id
     or v_target.status not in ('queued', 'running')
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_lease_token
     or v_job.lease_expires_at < now()
     or v_execution.id is null
     or v_execution.status <> 'completed'
     or v_execution.completed_check_id <> v_source.id
     or v_report.id is null
     or v_source.status not in ('passed', 'blocked')
     or v_source.scan_completeness <> 'complete'
     or v_source.requested_scope_hash <> v_target.requested_scope_hash
     or v_source.policy_fingerprint <> v_target.policy_fingerprint
     or v_source.data_snapshot_token <> v_target.data_snapshot_token then
    return public.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Reusable scan evidence or new report artifact is invalid');
  end if;
  if coalesce(v_report.metadata->>'closureCheckId', v_target.id::text) <> v_target.id::text then
    return public.lcia_scope_closure_error('closure_report_unavailable', 409, 'New report does not belong to this closure run');
  end if;
  if v_source.status = 'passed' then
    select * into v_snapshot from public.lca_network_snapshots where id = v_source.snapshot_id;
    select * into v_artifact from public.lca_snapshot_artifacts where id = v_source.snapshot_artifact_id;
    select * into v_bundle from public.worker_job_artifacts where id = v_source.closure_bundle_artifact_id;
    if v_source.certificate_status <> 'valid'
       or v_source.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
       or (
         v_source.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
         and not public.lcia_scope_closure_current_release_matches(v_source.data_snapshot_token)
       )
       or v_execution.numerical_snapshot_id is distinct from v_source.snapshot_id
       or v_snapshot.id is null or v_snapshot.status <> 'ready'
       or v_snapshot.source_hash is distinct from v_source.source_fingerprint
       or v_artifact.id is null or v_artifact.snapshot_id <> v_snapshot.id
       or v_artifact.status <> 'ready'
       or v_artifact.artifact_format <> 'snapshot-hdf5:v1'
       or v_artifact.artifact_sha256 <> v_source.snapshot_hash
       or v_artifact.snapshot_index_sha256 <> v_source.snapshot_index_sha256
       or v_artifact.snapshot_build_contract_hash <> v_source.snapshot_build_contract_hash
       or v_artifact.effective_scope_hash <> v_source.effective_scope_hash
       or v_artifact.data_snapshot_token <> v_source.data_snapshot_token
       or v_artifact.closure_bundle_hash <> v_source.closure_bundle_hash
       or v_bundle.id is null
       or v_bundle.job_id <> v_source.worker_job_id
       or v_bundle.artifact_type <> 'closure_bundle'
       or v_bundle.checksum_sha256 <> v_source.closure_bundle_hash
       or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_source.id::text then
      return public.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Source numerical snapshot certificate is not reusable');
    end if;
  end if;

  v_report_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_source.status = 'passed' then
    v_certificate_hash := public.lcia_scope_closure_sha256(jsonb_build_object(
      'certificateSchemaVersion', 'lcia.scope-closure-certificate.v2',
      'closureCheckId', v_target.id,
      'requestedScopeHash', v_target.requested_scope_hash,
      'effectiveScopeHash', v_source.effective_scope_hash,
      'effectiveScope', v_source.effective_scope_manifest,
      'policyFingerprint', v_target.policy_fingerprint,
      'dataSnapshotToken', v_target.data_snapshot_token,
      'validatorScannerFingerprint', v_target.expected_validator_scanner_fingerprint,
      'sourceFingerprint', v_source.source_fingerprint,
      'resolutionMapHash', v_source.resolution_map_hash,
      'closureBundleArtifactId', v_source.closure_bundle_artifact_id,
      'closureBundleHash', v_source.closure_bundle_hash,
      'snapshotId', v_source.snapshot_id,
      'snapshotHash', v_source.snapshot_hash,
      'snapshotArtifactId', v_source.snapshot_artifact_id,
      'snapshotIndexSha256', v_source.snapshot_index_sha256,
      'snapshotBuildContractHash', v_source.snapshot_build_contract_hash,
      'reportArtifactManifestHash', v_report_hash,
      'evidenceHash', v_source.evidence_hash
    ));
  end if;

  for v_old_issue in
    select * from public.lcia_scope_closure_issues
    where closure_check_id = v_source.id order by id
  loop
    insert into public.lcia_scope_closure_issues(
      closure_check_id, issue_key, severity, blocking, issue_code,
      source_dataset_type, source_dataset_id, source_dataset_version,
      json_path, reference_role, requested_target_type, requested_target_id,
      requested_target_version, message, suggested_action,
      occurrence_count, affected_root_count, details
    ) values (
      v_target.id, v_old_issue.issue_key, v_old_issue.severity,
      v_old_issue.blocking, v_old_issue.issue_code,
      v_old_issue.source_dataset_type, v_old_issue.source_dataset_id,
      v_old_issue.source_dataset_version, v_old_issue.json_path,
      v_old_issue.reference_role, v_old_issue.requested_target_type,
      v_old_issue.requested_target_id, v_old_issue.requested_target_version,
      v_old_issue.message, v_old_issue.suggested_action,
      v_old_issue.occurrence_count, v_old_issue.affected_root_count,
      v_old_issue.details
    ) returning * into v_new_issue;
    insert into public.lcia_scope_closure_issue_occurrences(
      closure_issue_id, occurrence_key, source_dataset_type,
      source_dataset_id, source_dataset_version, json_path, reference_role, details
    ) select
      v_new_issue.id, occurrence_key, source_dataset_type,
      source_dataset_id, source_dataset_version, json_path, reference_role, details
    from public.lcia_scope_closure_issue_occurrences
    where closure_issue_id = v_old_issue.id;
    insert into public.lcia_scope_closure_issue_roots(
      closure_issue_id, root_dataset_type, root_dataset_id,
      root_dataset_version, impact_role, witness_path
    ) select
      v_new_issue.id, root_dataset_type, root_dataset_id,
      root_dataset_version, impact_role, witness_path
    from public.lcia_scope_closure_issue_roots
    where closure_issue_id = v_old_issue.id;
  end loop;

  v_summary := jsonb_strip_nulls(p_result_summary || jsonb_build_object(
    'reusedFromCheckId', v_source.id,
    'reportArtifactId', v_report.id,
    'reportArtifactManifestHash', v_report_hash,
    'evidenceHash', v_source.evidence_hash
  ));
  update public.lcia_scope_closure_checks set
    status = v_source.status,
    scan_completeness = v_source.scan_completeness,
    effective_scope_manifest = v_source.effective_scope_manifest,
    effective_scope_hash = v_source.effective_scope_hash,
    certificate_schema_version = case when v_source.status = 'passed' then 'lcia.scope-closure-certificate.v2' else null end,
    certificate_status = case when v_source.status = 'passed' then 'valid' else 'unavailable' end,
    certificate_hash = v_certificate_hash,
    source_fingerprint = v_source.source_fingerprint,
    resolution_map_hash = v_source.resolution_map_hash,
    closure_bundle_hash = case when v_source.status = 'passed' then v_source.closure_bundle_hash else null end,
    closure_bundle_artifact_id = case when v_source.status = 'passed' then v_source.closure_bundle_artifact_id else null end,
    snapshot_id = case when v_source.status = 'passed' then v_source.snapshot_id else null end,
    snapshot_hash = case when v_source.status = 'passed' then v_source.snapshot_hash else null end,
    snapshot_artifact_id = case when v_source.status = 'passed' then v_source.snapshot_artifact_id else null end,
    snapshot_index_sha256 = case when v_source.status = 'passed' then v_source.snapshot_index_sha256 else null end,
    snapshot_build_contract_hash = case when v_source.status = 'passed' then v_source.snapshot_build_contract_hash else null end,
    report_artifact_manifest_hash = v_report_hash,
    evidence_hash = case when v_source.status = 'passed' then v_source.evidence_hash else null end,
    result_summary = v_summary,
    blocker_codes = v_source.blocker_codes,
    report_artifact_id = v_report.id,
    reused_from_check_id = v_source.id,
    updated_at = now(),
    finished_at = now()
  where id = v_target.id
  returning * into v_target;

  v_worker_status := case when v_target.status = 'passed' then 'completed' else 'blocked' end;
  select public.worker_record_job_result(
    v_job.id, p_lease_token, v_worker_status,
    jsonb_build_object(
      'closureCheckId', v_target.id,
      'status', v_target.status,
      'scanCompleteness', v_target.scan_completeness,
      'certificateStatus', v_target.certificate_status,
      'certificateHash', v_target.certificate_hash,
      'snapshotId', v_target.snapshot_id,
      'snapshotHash', v_target.snapshot_hash,
      'snapshotArtifactId', v_target.snapshot_artifact_id,
      'snapshotIndexSha256', v_target.snapshot_index_sha256,
      'snapshotBuildContractHash', v_target.snapshot_build_contract_hash,
      'evidenceHash', v_target.evidence_hash,
      'reusedFromCheckId', v_source.id
    ),
    'lcia.scope_closure_check.result.v2',
    jsonb_build_object('reportArtifactId', v_report.id, 'reportArtifactManifestHash', v_report_hash),
    jsonb_build_object('progressCounters', coalesce(v_target.result_summary->'progressCounters', '{}'::jsonb)),
    null, null, null,
    case when v_worker_status = 'blocked' then v_target.blocker_codes else null end,
    case when v_worker_status = 'blocked' then 'operator' else null end,
    false
  ) into v_worker_record;
  if coalesce((v_worker_record->>'ok')::boolean, false) is not true then
    raise exception using errcode = 'P0001', message = 'worker_job_result_rejected';
  end if;
  return jsonb_build_object('ok', true, 'data', jsonb_build_object(
    'closureCheckId', v_target.id,
    'workerJobId', v_job.id,
    'status', v_target.status,
    'scanCompleteness', v_target.scan_completeness,
    'certificateStatus', v_target.certificate_status,
    'certificateHash', v_target.certificate_hash,
    'effectiveScopeHash', v_target.effective_scope_hash,
    'snapshotId', v_target.snapshot_id,
    'snapshotHash', v_target.snapshot_hash,
    'snapshotArtifactId', v_target.snapshot_artifact_id,
    'snapshotIndexSha256', v_target.snapshot_index_sha256,
    'snapshotBuildContractHash', v_target.snapshot_build_contract_hash,
    'evidenceHash', v_target.evidence_hash,
    'reportArtifactId', v_report.id,
    'reusedFromCheckId', v_source.id
  ));
end;
$$;

revoke all on function public.svc_lcia_scope_closure_reuse_completed_scan(uuid, uuid, uuid, uuid)
  from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_reuse_completed_scan(uuid, uuid, uuid, uuid)
  to service_role;
revoke all on function public.svc_lcia_scope_closure_finalize_reused_scan(uuid, uuid, uuid, uuid, uuid, jsonb)
  from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_scope_closure_finalize_reused_scan(uuid, uuid, uuid, uuid, uuid, jsonb)
  to service_role;

-- Retention must not nominate a numerical snapshot while it is the immutable
-- evidence of a currently usable certificate or a persisted result/package.  The guards
-- on snapshot and artifact deletion remain the final fence, but candidate generation
-- should not turn protected references into noisy failed GC runs.
alter function util.list_lca_snapshot_gc_candidates(
  interval, interval, timestamptz, integer, integer, bigint
) rename to list_lca_snapshot_gc_candidates_without_closure_protection;

create or replace function util.list_lca_snapshot_gc_candidates(
  p_preview_retention interval default interval '30 days',
  p_draft_retention interval default interval '7 days',
  p_as_of timestamptz default now(),
  p_limit integer default 500,
  p_keep_latest_per_scope integer default 1,
  p_max_total_bytes bigint default null
)
returns table(
  candidate_type text,
  snapshot_id uuid,
  snapshot_directory text,
  bucket_id text,
  object_name text,
  storage_bytes bigint,
  reason text,
  delete_db_snapshot boolean,
  snapshot_status text,
  snapshot_created_at timestamptz,
  snapshot_updated_at timestamptz,
  effective_expires_at timestamptz,
  object_count bigint,
  snapshot_storage_bytes bigint,
  downstream_active_count bigint,
  downstream_job_count bigint,
  downstream_result_count bigint,
  downstream_cache_count bigint,
  downstream_latest_count bigint,
  downstream_factorization_count bigint,
  downstream_artifact_count bigint
)
language sql
security definer
set search_path = ''
as $$
  select candidate.*
  from util.list_lca_snapshot_gc_candidates_without_closure_protection(
    p_preview_retention,
    p_draft_retention,
    p_as_of,
    p_limit,
    p_keep_latest_per_scope,
    p_max_total_bytes
  ) candidate
  where not exists (
    select 1
    from public.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_id = candidate.snapshot_id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  )
  and not exists (
    select 1
    from public.lcia_result_packages package
    where package.snapshot_id = candidate.snapshot_id
  )
  and not exists (
    select 1
    from public.lca_results result
    where result.snapshot_id = candidate.snapshot_id
  )
$$;

revoke all on function util.list_lca_snapshot_gc_candidates_without_closure_protection(
  interval, interval, timestamptz, integer, integer, bigint
) from public, anon, authenticated, service_role;
revoke all on function util.list_lca_snapshot_gc_candidates(
  interval, interval, timestamptz, integer, integer, bigint
) from public, anon, authenticated, service_role;
grant execute on function util.list_lca_snapshot_gc_candidates(
  interval, interval, timestamptz, integer, integer, bigint
) to service_role;
