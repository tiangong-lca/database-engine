-- Scope runs are explicit user actions, while scan execution and document
-- validation evidence are reusable immutable work products.  Keeping those
-- identities separate prevents a retry from masquerading as the old run.

create table if not exists public.lcia_scope_closure_data_snapshots (
  data_snapshot_token text primary key,
  root_manifest jsonb not null,
  root_manifest_hash text not null,
  publication_epoch bigint not null default 0,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(root_manifest) = 'object'),
  check (length(trim(data_snapshot_token)) > 0),
  check (length(trim(root_manifest_hash)) > 0)
);
create sequence if not exists public.lcia_scope_closure_publication_epoch_seq;
alter table public.lcia_scope_closure_data_snapshots
  alter column publication_epoch set default nextval('public.lcia_scope_closure_publication_epoch_seq');

create table if not exists public.lcia_scope_closure_scan_executions (
  id uuid primary key default gen_random_uuid(),
  request_fingerprint text not null unique,
  requested_scope_hash text not null,
  policy_fingerprint text not null,
  data_snapshot_token text not null references public.lcia_scope_closure_data_snapshots(data_snapshot_token) on delete restrict,
  validator_scanner_fingerprint text not null,
  scan_key text unique,
  status text not null default 'queued' check (status in ('queued', 'running', 'completed', 'failed')),
  lease_token uuid,
  leased_by_job_id uuid references public.worker_jobs(id) on delete set null,
  lease_expires_at timestamptz,
  completed_check_id uuid references public.lcia_scope_closure_checks(id) on delete set null,
  source_fingerprint text,
  evidence_hash text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  completed_at timestamptz,
  check ((status = 'running') = (lease_token is not null and leased_by_job_id is not null and lease_expires_at is not null))
);

alter table public.lcia_scope_closure_checks
  add column if not exists scan_execution_id uuid references public.lcia_scope_closure_scan_executions(id) on delete restrict,
  add column if not exists reused_from_check_id uuid references public.lcia_scope_closure_checks(id) on delete restrict;

alter table public.lcia_result_packages
  add column if not exists closure_check_id uuid references public.lcia_scope_closure_checks(id) on delete restrict,
  add column if not exists closure_certificate_hash text,
  add column if not exists closure_snapshot_hash text;
create index if not exists lcia_result_packages_closure_check_idx
  on public.lcia_result_packages (closure_check_id, created_at desc);

create or replace function public.lcia_result_package_bind_closure_certificate()
returns trigger language plpgsql set search_path = public, pg_temp as $$
declare v_job public.worker_jobs%rowtype; v_check public.lcia_scope_closure_checks%rowtype; v_closure_check_id uuid;
begin
  select * into v_job from public.worker_jobs where id=new.build_worker_job_id;
  v_closure_check_id:=nullif(v_job.payload_json->>'closure_check_id','')::uuid;
  if v_closure_check_id is null then return new; end if;
  select * into v_check from public.lcia_scope_closure_checks where id=v_closure_check_id;
  if v_check.id is null or v_check.status<>'passed' or v_check.scan_completeness<>'complete' or v_check.certificate_status<>'valid' then raise exception 'closure_check_not_usable' using errcode='23514'; end if;
  if new.snapshot_id is distinct from v_check.snapshot_id or coalesce(v_job.payload_json->>'snapshot_hash','')<>coalesce(v_check.snapshot_hash,'') or coalesce(v_job.payload_json->>'closure_certificate_hash','')<>coalesce(v_check.certificate_hash,'') then raise exception 'closure_certificate_binding_mismatch' using errcode='23514'; end if;
  new.closure_check_id:=v_check.id;
  new.closure_certificate_hash:=v_check.certificate_hash;
  new.closure_snapshot_hash:=v_check.snapshot_hash;
  return new;
end;
$$;
drop trigger if exists lcia_result_packages_bind_closure_certificate on public.lcia_result_packages;
create trigger lcia_result_packages_bind_closure_certificate before insert on public.lcia_result_packages for each row execute function public.lcia_result_package_bind_closure_certificate();

create index if not exists lcia_scope_closure_checks_execution_idx
  on public.lcia_scope_closure_checks (scan_execution_id, created_at desc, id desc);
create index if not exists lcia_scope_closure_scan_executions_status_idx
  on public.lcia_scope_closure_scan_executions (status, lease_expires_at, created_at);

create table if not exists public.lcia_document_validation_evidence (
  id uuid primary key default gen_random_uuid(),
  dataset_type text not null,
  dataset_id uuid not null,
  dataset_version text not null,
  canonical_content_hash text not null,
  document_validator_version text not null,
  document_validation_profile text not null,
  validation_report_schema_version text not null,
  validator_engine_fingerprint text not null,
  tidas_schema_lock_sha256 text not null,
  status text not null check (status in ('passed', 'failed')),
  summary jsonb not null default '{}'::jsonb,
  issue_artifact_ref jsonb not null default '{}'::jsonb,
  issue_artifact_hash text,
  source_worker_job_id uuid references public.worker_jobs(id) on delete set null,
  created_at timestamptz not null default now(),
  check (jsonb_typeof(summary) = 'object'),
  check (jsonb_typeof(issue_artifact_ref) = 'object'),
  unique (dataset_type, dataset_id, dataset_version, canonical_content_hash,
    document_validator_version, document_validation_profile,
    validation_report_schema_version, validator_engine_fingerprint,
    tidas_schema_lock_sha256)
);
create index if not exists lcia_document_validation_evidence_lookup_idx
  on public.lcia_document_validation_evidence (dataset_type, dataset_id, dataset_version, canonical_content_hash);

alter table public.lcia_scope_closure_data_snapshots enable row level security;
alter table public.lcia_scope_closure_scan_executions enable row level security;
alter table public.lcia_document_validation_evidence enable row level security;
revoke all on public.lcia_scope_closure_data_snapshots, public.lcia_scope_closure_scan_executions, public.lcia_document_validation_evidence from public, anon, authenticated;
grant all on public.lcia_scope_closure_data_snapshots, public.lcia_scope_closure_scan_executions, public.lcia_document_validation_evidence to service_role;

create or replace function public.svc_lcia_document_validation_evidence_lookup(
  p_cache_keys jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_cache_keys,'null'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache keys must be an array'); end if;
  return jsonb_build_object('ok',true,'data',coalesce((
    with requested as (
      select (value->>'datasetType') dataset_type, nullif(value->>'datasetId','')::uuid dataset_id,
        value->>'datasetVersion' dataset_version, value->>'canonicalContentHash' canonical_content_hash,
        value->>'documentValidatorVersion' document_validator_version,
        value->>'documentValidationProfile' document_validation_profile,
        value->>'validationReportSchemaVersion' validation_report_schema_version,
        value->>'validatorEngineFingerprint' validator_engine_fingerprint,
        value->>'tidasSchemaLockSha256' tidas_schema_lock_sha256
      from jsonb_array_elements(p_cache_keys)
    )
    select jsonb_agg(jsonb_build_object('datasetType',e.dataset_type,'datasetId',e.dataset_id,'datasetVersion',e.dataset_version,'canonicalContentHash',e.canonical_content_hash,'documentValidatorVersion',e.document_validator_version,'documentValidationProfile',e.document_validation_profile,'validationReportSchemaVersion',e.validation_report_schema_version,'validatorEngineFingerprint',e.validator_engine_fingerprint,'tidasSchemaLockSha256',e.tidas_schema_lock_sha256,'status',e.status,'summary',e.summary,'issueArtifactRef',e.issue_artifact_ref,'issueArtifactHash',e.issue_artifact_hash) order by e.dataset_type,e.dataset_id,e.dataset_version)
    from requested r join public.lcia_document_validation_evidence e on (e.dataset_type,e.dataset_id,e.dataset_version,e.canonical_content_hash,e.document_validator_version,e.document_validation_profile,e.validation_report_schema_version,e.validator_engine_fingerprint,e.tidas_schema_lock_sha256)=(r.dataset_type,r.dataset_id,r.dataset_version,r.canonical_content_hash,r.document_validator_version,r.document_validation_profile,r.validation_report_schema_version,r.validator_engine_fingerprint,r.tidas_schema_lock_sha256)
  ),'[]'::jsonb));
exception when invalid_text_representation then return public.lcia_scope_closure_error('invalid_document_evidence_keys',400,'Cache key contains invalid identity values');
end;
$$;

create or replace function public.svc_lcia_document_validation_evidence_record(
  p_records jsonb, p_source_worker_job_id uuid default null
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_record jsonb; v_inserted integer:=0;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_records,'null'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence records must be an array'); end if;
  for v_record in select value from jsonb_array_elements(p_records) loop
    insert into public.lcia_document_validation_evidence(dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256,status,summary,issue_artifact_ref,issue_artifact_hash,source_worker_job_id)
    values(nullif(v_record->>'datasetType',''),nullif(v_record->>'datasetId','')::uuid,nullif(v_record->>'datasetVersion',''),nullif(v_record->>'canonicalContentHash',''),nullif(v_record->>'documentValidatorVersion',''),nullif(v_record->>'documentValidationProfile',''),nullif(v_record->>'validationReportSchemaVersion',''),nullif(v_record->>'validatorEngineFingerprint',''),nullif(v_record->>'tidasSchemaLockSha256',''),nullif(v_record->>'status',''),coalesce(v_record->'summary','{}'::jsonb),coalesce(v_record->'issueArtifactRef','{}'::jsonb),nullif(v_record->>'issueArtifactHash',''),p_source_worker_job_id)
    on conflict (dataset_type,dataset_id,dataset_version,canonical_content_hash,document_validator_version,document_validation_profile,validation_report_schema_version,validator_engine_fingerprint,tidas_schema_lock_sha256) do nothing;
    if found then v_inserted:=v_inserted+1; end if;
  end loop;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('insertedCount',v_inserted));
exception when not_null_violation or invalid_text_representation or check_violation then return public.lcia_scope_closure_error('invalid_document_evidence_records',400,'Evidence record violates the cache contract');
end;
$$;

create or replace function public.svc_lcia_scope_closure_claim_scan_execution(
  p_scan_execution_id uuid, p_worker_job_id uuid, p_lease_token uuid
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_execution public.lcia_scope_closure_scan_executions%rowtype; v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_job from public.worker_jobs where id=p_worker_job_id;
  if v_job.id is null or v_job.status<>'running' or v_job.lease_token is distinct from p_lease_token or v_job.lease_expires_at<now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into v_execution from public.lcia_scope_closure_scan_executions where id=p_scan_execution_id for update;
  if v_execution.id is null or not exists (select 1 from public.lcia_scope_closure_checks c where c.scan_execution_id=v_execution.id and c.worker_job_id=v_job.id and c.request_fingerprint=v_execution.request_fingerprint) then return public.lcia_scope_closure_error('scan_execution_not_found',404,'Scan execution not found'); end if;
  if v_execution.status='completed' then return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',false,'completed',true,'completedCheckId',v_execution.completed_check_id)); end if;
  if v_execution.status='running' and v_execution.lease_expires_at>=now() and v_execution.leased_by_job_id<>v_job.id then return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',false,'completed',false)); end if;
  update public.lcia_scope_closure_scan_executions set status='running',lease_token=p_lease_token,leased_by_job_id=v_job.id,lease_expires_at=v_job.lease_expires_at,updated_at=now() where id=v_execution.id;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('acquired',true,'completed',false));
end;
$$;

create or replace function public.svc_lcia_result_build_bind_closure(
  p_worker_job_id uuid, p_closure_check_id uuid
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_job public.worker_jobs%rowtype; v_check public.lcia_scope_closure_checks%rowtype; v_binding jsonb;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_job from public.worker_jobs where id=p_worker_job_id for update;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_job.id is null or v_job.job_kind<>'lcia_result.package_build' or v_check.id is null then return public.lcia_scope_closure_error('build_binding_not_found',404,'Build job or closure certificate not found'); end if;
  if v_check.status<>'passed' or v_check.scan_completeness<>'complete' or v_check.certificate_status<>'valid' then return public.lcia_scope_closure_error('closure_check_not_usable',409,'Closure certificate is not usable'); end if;
  v_binding:=jsonb_strip_nulls(jsonb_build_object('closure_check_id',v_check.id,'closure_certificate_hash',v_check.certificate_hash,'effective_scope_hash',v_check.effective_scope_hash,'data_snapshot_token',v_check.data_snapshot_token,'snapshot_id',v_check.snapshot_id,'snapshot_hash',v_check.snapshot_hash,'closure_bundle_hash',v_check.closure_bundle_hash,'report_artifact_manifest_hash',v_check.report_artifact_manifest_hash));
  update public.worker_jobs set payload_json=payload_json||v_binding,payload_ref=coalesce(payload_ref,'{}'::jsonb)||jsonb_build_object('closureCertificate',v_binding),updated_at=now() where id=v_job.id returning * into v_job;
  insert into public.worker_job_events(job_id,event_type,status,details) values(v_job.id,'closure_certificate_bound',v_job.status,jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash));
  return jsonb_build_object('ok',true,'data',jsonb_build_object('workerJobId',v_job.id,'closureCheckId',v_check.id));
end;
$$;

alter function public.cmd_lcia_scope_closure_check_request_v2(jsonb,text,jsonb)
  rename to cmd_lcia_scope_closure_check_request_v2_untracked;

create or replace function public.cmd_lcia_scope_closure_check_request_v2(
  p_requested_scope jsonb, p_request_idempotency_token text, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_result jsonb; v_check public.lcia_scope_closure_checks%rowtype; v_execution public.lcia_scope_closure_scan_executions%rowtype; v_payload jsonb;
begin
  v_result:=public.cmd_lcia_scope_closure_check_request_v2_untracked(p_requested_scope,p_request_idempotency_token,p_audit);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  select * into v_check from public.lcia_scope_closure_checks where id=nullif(v_result->'data'->>'closureCheckId','')::uuid for update;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  insert into public.lcia_scope_closure_data_snapshots(data_snapshot_token,root_manifest,root_manifest_hash)
  values(v_check.data_snapshot_token,v_check.requested_scope_manifest,public.lcia_scope_closure_sha256(v_check.requested_scope_manifest))
  on conflict (data_snapshot_token) do nothing;
  select * into v_execution from public.lcia_scope_closure_scan_executions where request_fingerprint=v_check.request_fingerprint for update;
  if v_execution.id is null then
    insert into public.lcia_scope_closure_scan_executions(request_fingerprint,requested_scope_hash,policy_fingerprint,data_snapshot_token,validator_scanner_fingerprint)
    values(v_check.request_fingerprint,v_check.requested_scope_hash,v_check.policy_fingerprint,v_check.data_snapshot_token,v_check.expected_validator_scanner_fingerprint) returning * into v_execution;
  end if;
  update public.lcia_scope_closure_checks set scan_execution_id=v_execution.id,updated_at=now() where id=v_check.id returning * into v_check;
  v_payload:=jsonb_build_object('scan_execution_id',v_execution.id,'data_snapshot_token',v_check.data_snapshot_token);
  update public.worker_jobs set payload_json=payload_json||v_payload,updated_at=now() where id=v_check.worker_job_id;
  return jsonb_set(jsonb_set(v_result,'{data,workerJob,payload}',coalesce(v_result->'data'->'workerJob'->'payload','{}'::jsonb)||v_payload,true),'{data,scanExecutionId}',to_jsonb(v_execution.id),true);
end;
$$;

create or replace function public.svc_lcia_scope_closure_check_get_worker_input(p_closure_check_id uuid)
returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_check public.lcia_scope_closure_checks%rowtype; v_snapshot public.lcia_scope_closure_data_snapshots%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  select * into v_snapshot from public.lcia_scope_closure_data_snapshots where data_snapshot_token=v_check.data_snapshot_token;
  if v_check.id is null or v_check.requested_scope_manifest is null or v_snapshot.data_snapshot_token is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'scanExecutionId',v_check.scan_execution_id,'requestedScope',v_check.requested_scope_manifest,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token,'dataSnapshotManifest',v_snapshot.root_manifest,'dataSnapshotManifestHash',v_snapshot.root_manifest_hash,'publicationEpoch',v_snapshot.publication_epoch,'expectedValidatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'requestFingerprint',v_check.request_fingerprint));
end;
$$;

alter function public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid)
  rename to svc_lcia_scope_closure_check_record_result_v2_untracked;

create or replace function public.svc_lcia_scope_closure_check_record_result_v2(
  p_closure_check_id uuid, p_job_id uuid, p_lease_token uuid, p_status text, p_scan_completeness text,
  p_effective_scope_manifest jsonb, p_evidence jsonb, p_result_summary jsonb default '{}'::jsonb,
  p_issues jsonb default '[]'::jsonb, p_blocker_codes text[] default '{}'::text[], p_report_artifact_id uuid default null
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_result jsonb; v_check public.lcia_scope_closure_checks%rowtype; v_execution public.lcia_scope_closure_scan_executions%rowtype; v_scan_key text; v_existing_execution uuid;
begin
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.scan_execution_id is not null then
    select * into v_execution from public.lcia_scope_closure_scan_executions where id=v_check.scan_execution_id for update;
    if v_execution.status<>'running' or v_execution.leased_by_job_id<>p_job_id or v_execution.lease_token is distinct from p_lease_token then return public.lcia_scope_closure_error('scan_execution_lease_invalid',409,'Scan execution is not held by this worker job'); end if;
  end if;
  v_result:=public.svc_lcia_scope_closure_check_record_result_v2_untracked(p_closure_check_id,p_job_id,p_lease_token,p_status,p_scan_completeness,p_effective_scope_manifest,p_evidence,p_result_summary,p_issues,p_blocker_codes,p_report_artifact_id);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  if v_execution.id is not null then
    select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
    v_scan_key:=public.lcia_scope_closure_sha256(jsonb_build_object('effectiveScopeHash',v_check.effective_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'validatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token));
    select id into v_existing_execution from public.lcia_scope_closure_scan_executions where scan_key=v_scan_key and id<>v_execution.id limit 1;
    if v_existing_execution is not null then v_scan_key:=null; end if;
    update public.lcia_scope_closure_scan_executions set scan_key=v_scan_key,status=case when lower(trim(p_status))='passed' or (lower(trim(p_status))='blocked' and p_scan_completeness='complete') then 'completed' else 'failed' end,lease_token=null,leased_by_job_id=null,lease_expires_at=null,completed_check_id=case when lower(trim(p_status))='passed' or (lower(trim(p_status))='blocked' and p_scan_completeness='complete') then p_closure_check_id else null end,source_fingerprint=nullif(p_evidence->>'sourceFingerprint',''),evidence_hash=nullif(p_evidence->>'evidenceHash',''),updated_at=now(),completed_at=now() where id=v_execution.id;
  end if;
  return v_result;
end;
$$;

-- Build V2 owns the enqueue transaction.  The older Edge path returned an
-- unsigned envelope and inserted worker_jobs later with service credentials,
-- which left a claimable gap between Certificate validation and persistence.
alter function public.cmd_lcia_result_build_request_v2(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb)
  rename to cmd_lcia_result_build_request_v2_envelope;

create or replace function public.cmd_lcia_result_build_request_v2(
  p_name text, p_processes jsonb, p_coverage_mode text, p_default_impact_category text,
  p_lcia_method_set jsonb, p_idempotency_key text, p_closure_check_id uuid,
  p_requested_scope_hash text, p_policy_fingerprint text, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid:=auth.uid(); v_result jsonb; v_check public.lcia_scope_closure_checks%rowtype; v_kind public.worker_job_kinds%rowtype; v_job public.worker_jobs%rowtype; v_build_id uuid; v_payload jsonb; v_idempotency_key text;
begin
  v_result:=public.cmd_lcia_result_build_request_v2_envelope(p_name,p_processes,p_coverage_mode,p_default_impact_category,p_lcia_method_set,p_idempotency_key,p_closure_check_id,p_requested_scope_hash,p_policy_fingerprint,p_audit);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  v_build_id:=nullif(v_result->'data'->>'buildId','')::uuid;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  select * into v_kind from public.worker_job_kinds where job_kind='lcia_result.package_build';
  if v_actor is null or v_build_id is null or v_check.id is null or v_kind.job_kind is null then return public.lcia_scope_closure_error('build_enqueue_unavailable',503,'Build queue configuration is unavailable'); end if;
  v_payload:=coalesce(v_result->'data'->'workerJob'->'payload','{}'::jsonb);
  v_idempotency_key:=nullif(v_result->'data'->'workerJob'->>'idempotencyKey','');
  select * into v_job from public.worker_jobs where worker_runtime=v_kind.worker_runtime and job_kind=v_kind.job_kind and requested_by=v_actor and idempotency_key is not distinct from v_idempotency_key and status in ('queued','running','waiting','stale','blocked') order by created_at desc limit 1 for update;
  if v_job.id is null then
    insert into public.worker_jobs(job_kind,worker_runtime,worker_queue,priority,queue_key,subject_type,subject_id,requester_type,requested_by,idempotency_key,request_hash,concurrency_key,visibility,max_attempts,payload_schema_version,payload_json,payload_ref,result_schema_version)
    values(v_kind.job_kind,v_kind.worker_runtime,v_kind.worker_queue,v_kind.default_priority,nullif(v_result->'data'->'workerJob'->>'queueKey',''),'lcia_result_build',v_build_id,'operator',v_actor,v_idempotency_key,nullif(v_result->'data'->'workerJob'->>'requestHash',''),nullif(v_result->'data'->'workerJob'->>'queueKey',''),'operator',v_kind.default_max_attempts,coalesce(v_result->'data'->'workerJob'->>'payloadSchemaVersion',v_kind.payload_schema_version),v_payload,jsonb_build_object('closureCertificate',jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash,'snapshotId',v_check.snapshot_id,'snapshotHash',v_check.snapshot_hash,'closureBundleHash',v_check.closure_bundle_hash)),v_kind.result_schema_version) returning * into v_job;
    insert into public.worker_job_events(job_id,event_type,status,details) values(v_job.id,'enqueued',v_job.status,jsonb_build_object('jobKind',v_job.job_kind,'closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash));
  end if;
  return jsonb_set(jsonb_set(v_result,'{data,workerJob}',public.worker_job_payload(v_job,false),true),'{data,workerJobId}',to_jsonb(v_job.id),true);
exception when unique_violation then return public.lcia_scope_closure_error('build_enqueue_conflict',409,'A conflicting certificate-bound build is already active');
end;
$$;

-- Scope identity resolution uses the same immutable public-release manifest as
-- the snapshot.  A live draft that happens to be state_code 100..199 is not a
-- certificate-grade eligible input.
create or replace function public.lcia_scope_closure_normalize_request(p_requested_scope jsonb)
returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_mode text:=lower(trim(coalesce(p_requested_scope->>'coverageMode',''))); v_processes jsonb; v_methods jsonb; v_policy jsonb; v_freshness text; v_release_id uuid; v_count integer; v_requested integer; v_duplicate integer;
begin
  if jsonb_typeof(coalesce(p_requested_scope,'null'::jsonb))<>'object' or v_mode not in ('subset','global_eligible') then raise exception using errcode='22023',message='invalid_closure_scope'; end if;
  if jsonb_typeof(coalesce(p_requested_scope->'processes','[]'::jsonb))<>'array' or jsonb_typeof(coalesce(p_requested_scope->'lciaMethods','[]'::jsonb))<>'array' then raise exception using errcode='22023',message='invalid_closure_scope_identity_list'; end if;
  select release_run_id into v_release_id from public.lca_release_publications where is_current=true and status='current' order by published_at desc limit 1;
  if v_release_id is null then raise exception using errcode='22023',message='current_release_required'; end if;
  if v_mode='global_eligible' then
    if jsonb_array_length(coalesce(p_requested_scope->'processes','[]'::jsonb))<>0 then raise exception using errcode='22023',message='global_eligible_scope_must_not_supply_processes'; end if;
    select coalesce(jsonb_agg(jsonb_build_object('id',dataset_uuid,'version',dataset_version) order by dataset_uuid,dataset_version),'[]'::jsonb) into v_processes from public.lca_release_dataset_versions where release_run_id=v_release_id and dataset_type='process' and dataset_role='unit_process';
    if jsonb_array_length(v_processes)=0 then raise exception using errcode='22023',message='current_release_has_no_eligible_processes'; end if;
  else
    with requested as (select (x.value->>'id')::uuid id,(x.value->>'version')::text version from jsonb_array_elements(p_requested_scope->'processes') x(value)), resolved as (select r.id,r.version from requested r join public.lca_release_dataset_versions d on d.release_run_id=v_release_id and d.dataset_type='process' and d.dataset_role='unit_process' and d.dataset_uuid=r.id and d.dataset_version=r.version)
    select count(*), (select count(*) from requested), (select count(*)-count(distinct(id,version)) from requested), coalesce(jsonb_agg(jsonb_build_object('id',id,'version',version) order by id,version),'[]'::jsonb) into v_count,v_requested,v_duplicate,v_processes from resolved;
    if coalesce(v_requested,0)=0 or v_count<>v_requested or v_duplicate<>0 then raise exception using errcode='22023',message='process_not_in_current_public_release'; end if;
  end if;
  with requested as (select (x.value->>'id')::uuid id,(x.value->>'version')::text version from jsonb_array_elements(p_requested_scope->'lciaMethods') x(value)), resolved as (select r.id,r.version from requested r join public.lca_release_dataset_versions d on d.release_run_id=v_release_id and d.dataset_type='lciamethod' and d.dataset_uuid=r.id and d.dataset_version=r.version)
  select count(*),(select count(*) from requested),(select count(*)-count(distinct(id,version)) from requested),coalesce(jsonb_agg(jsonb_build_object('id',id,'version',version) order by id,version),'[]'::jsonb) into v_count,v_requested,v_duplicate,v_methods from resolved;
  if coalesce(v_requested,0)=0 or v_count<>v_requested or v_duplicate<>0 then raise exception using errcode='22023',message='lcia_method_not_in_current_public_release'; end if;
  v_freshness:=coalesce(nullif(trim(p_requested_scope->>'certificateFreshnessPolicy'),''),'frozen-artifact-reusable-v1');
  if v_freshness not in ('frozen-artifact-reusable-v1','current-membership-required-v1') then raise exception using errcode='22023',message='invalid_certificate_freshness_policy'; end if;
  v_policy:=coalesce(p_requested_scope->'linkPolicy','{}'::jsonb);
  if jsonb_typeof(v_policy)<>'object' or coalesce(v_policy->>'linkSemanticsVersion','signed-flow-balance-v1')<>'signed-flow-balance-v1' or coalesce(v_policy->>'flowIdentityPolicy','exact-flow-version-reference-unit-v2')<>'exact-flow-version-reference-unit-v2' or coalesce(v_policy->>'allocationSemanticsVersion','tidas-reference-allocation-v3')<>'tidas-reference-allocation-v3' or coalesce(v_policy->>'technosphereBoundaryPolicy','closed') not in ('closed','open','cutoff') or coalesce(v_policy->>'providerUniversePolicy','scope_only') not in ('scope_only','eligible_transitive_expansion-v1') then raise exception using errcode='22023',message='invalid_closure_link_policy'; end if;
  return jsonb_build_object('schemaVersion','lcia.scope-manifest.v1','coverageMode',v_mode,'eligibilityPredicateVersion','current-public-release-manifest:v2','processes',v_processes,'lciaMethods',v_methods,'versionResolutionPolicy','reference-version-resolution-v1','legacyOmittedVersionPolicy','reject','certificateFreshnessPolicy',v_freshness,'linkPolicy',jsonb_build_object('linkSemanticsVersion','signed-flow-balance-v1','flowIdentityPolicy','exact-flow-version-reference-unit-v2','allocationSemanticsVersion','tidas-reference-allocation-v3','technosphereBoundaryPolicy',coalesce(v_policy->>'technosphereBoundaryPolicy','closed'),'providerUniversePolicy',coalesce(v_policy->>'providerUniversePolicy','scope_only')),'processManifestHash',public.lcia_scope_closure_sha256(jsonb_build_object('processes',v_processes)));
exception when invalid_text_representation then raise exception using errcode='22023',message='invalid_scope_identity';
end;
$$;

create or replace function public.svc_lcia_scope_closure_reuse_completed_scan(
  p_closure_check_id uuid, p_worker_job_id uuid, p_lease_token uuid, p_completed_check_id uuid
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_target public.lcia_scope_closure_checks%rowtype; v_source public.lcia_scope_closure_checks%rowtype; v_execution public.lcia_scope_closure_scan_executions%rowtype; v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_target from public.lcia_scope_closure_checks where id=p_closure_check_id for update;
  select * into v_source from public.lcia_scope_closure_checks where id=p_completed_check_id;
  select * into v_job from public.worker_jobs where id=p_worker_job_id for update;
  if v_target.id is null or v_source.id is null or v_target.worker_job_id<>p_worker_job_id or v_target.status not in ('queued','running') then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_job.status<>'running' or v_job.lease_token is distinct from p_lease_token or v_job.lease_expires_at<now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into v_execution from public.lcia_scope_closure_scan_executions where id=v_target.scan_execution_id for update;
  if v_execution.id is null or v_execution.status<>'completed' or v_execution.completed_check_id<>v_source.id then return public.lcia_scope_closure_error('scan_execution_not_reusable',409,'Completed scan execution does not match this closure run'); end if;
  if v_source.status not in ('passed','blocked') or v_source.scan_completeness<>'complete' then return public.lcia_scope_closure_error('scan_execution_not_reusable',409,'Source closure evidence is not complete'); end if;
  if v_source.requested_scope_hash<>v_target.requested_scope_hash or v_source.policy_fingerprint<>v_target.policy_fingerprint or v_source.data_snapshot_token<>v_target.data_snapshot_token then return public.lcia_scope_closure_error('scan_execution_not_reusable',409,'Source closure bindings do not match'); end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('reuseAvailable',true,'closureCheckId',v_target.id,'workerJobId',v_job.id,'completedCheckId',v_source.id,'status',v_source.status,'scanCompleteness',v_source.scan_completeness,'evidence',jsonb_build_object('schemaVersion',coalesce(v_source.certificate_schema_version,'lcia.scope-closure-evidence.v1'),'sourceFingerprint',v_source.source_fingerprint,'resolutionMapHash',v_source.resolution_map_hash,'closureBundleHash',v_source.closure_bundle_hash,'snapshotId',v_source.snapshot_id,'snapshotHash',v_source.snapshot_hash,'evidenceHash',v_source.evidence_hash),'blockerCodes',to_jsonb(v_source.blocker_codes)));
end;
$$;

create or replace function public.svc_lcia_scope_closure_finalize_reused_scan(
  p_closure_check_id uuid, p_worker_job_id uuid, p_lease_token uuid, p_completed_check_id uuid, p_report_artifact_id uuid, p_result_summary jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare t public.lcia_scope_closure_checks%rowtype; s public.lcia_scope_closure_checks%rowtype; e public.lcia_scope_closure_scan_executions%rowtype; j public.worker_jobs%rowtype; a public.worker_job_artifacts%rowtype; oi public.lcia_scope_closure_issues%rowtype; ni public.lcia_scope_closure_issues%rowtype; rh text; ch text; ws text; wr jsonb; rs jsonb;
begin
  if not coalesce(util.is_service_request(),false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if jsonb_typeof(coalesce(p_result_summary,'null'::jsonb))<>'object' then return public.lcia_scope_closure_error('invalid_closure_result',400,'Reused closure result summary must be an object'); end if;
  select * into t from public.lcia_scope_closure_checks where id=p_closure_check_id for update; select * into s from public.lcia_scope_closure_checks where id=p_completed_check_id; select * into j from public.worker_jobs where id=p_worker_job_id for update;
  if t.id is null or s.id is null or t.worker_job_id<>j.id or t.status not in ('queued','running') or j.status<>'running' or j.lease_token is distinct from p_lease_token or j.lease_expires_at<now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into e from public.lcia_scope_closure_scan_executions where id=t.scan_execution_id for update;
  select * into a from public.worker_job_artifacts where id=p_report_artifact_id and job_id=j.id;
  if e.id is null or e.status<>'completed' or e.completed_check_id<>s.id or a.id is null or s.status not in ('passed','blocked') or s.scan_completeness<>'complete' or s.requested_scope_hash<>t.requested_scope_hash or s.policy_fingerprint<>t.policy_fingerprint or s.data_snapshot_token<>t.data_snapshot_token then return public.lcia_scope_closure_error('scan_execution_not_reusable',409,'Reusable scan evidence or new report artifact is invalid'); end if;
  rh:=public.lcia_scope_closure_sha256(jsonb_build_object('artifactId',a.id,'bucket',a.storage_bucket,'objectPath',a.storage_path,'mediaType',a.content_type,'byteSize',a.byte_size,'checksumSha256',a.checksum_sha256));
  ch:=case when s.status='passed' then public.lcia_scope_closure_sha256(jsonb_build_object('certificateSchemaVersion','lcia.scope-closure-certificate.v1','closureCheckId',t.id,'requestedScopeHash',t.requested_scope_hash,'scopeHash',s.effective_scope_hash,'policyFingerprint',t.policy_fingerprint,'dataSnapshotToken',t.data_snapshot_token,'sourceFingerprint',s.source_fingerprint,'resolutionMapHash',s.resolution_map_hash,'closureBundleHash',s.closure_bundle_hash,'snapshotId',s.snapshot_id,'snapshotHash',s.snapshot_hash,'reportArtifactManifestHash',rh,'evidenceHash',s.evidence_hash)) else null end;
  for oi in select * from public.lcia_scope_closure_issues where closure_check_id=s.id order by id loop insert into public.lcia_scope_closure_issues(closure_check_id,issue_key,severity,blocking,issue_code,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,requested_target_type,requested_target_id,requested_target_version,message,suggested_action,occurrence_count,affected_root_count,details) values(t.id,oi.issue_key,oi.severity,oi.blocking,oi.issue_code,oi.source_dataset_type,oi.source_dataset_id,oi.source_dataset_version,oi.json_path,oi.reference_role,oi.requested_target_type,oi.requested_target_id,oi.requested_target_version,oi.message,oi.suggested_action,oi.occurrence_count,oi.affected_root_count,oi.details) returning * into ni; insert into public.lcia_scope_closure_issue_occurrences(closure_issue_id,occurrence_key,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,details) select ni.id,occurrence_key,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,details from public.lcia_scope_closure_issue_occurrences where closure_issue_id=oi.id; insert into public.lcia_scope_closure_issue_roots(closure_issue_id,root_dataset_type,root_dataset_id,root_dataset_version,impact_role,witness_path) select ni.id,root_dataset_type,root_dataset_id,root_dataset_version,impact_role,witness_path from public.lcia_scope_closure_issue_roots where closure_issue_id=oi.id; end loop;
  rs:=jsonb_strip_nulls(p_result_summary||jsonb_build_object('reusedFromCheckId',s.id,'reportArtifactId',a.id,'reportArtifactManifestHash',rh));
  update public.lcia_scope_closure_checks set status=s.status,scan_completeness=s.scan_completeness,effective_scope_manifest=s.effective_scope_manifest,effective_scope_hash=s.effective_scope_hash,certificate_schema_version=case when s.status='passed' then 'lcia.scope-closure-certificate.v1' else null end,certificate_status=case when s.status='passed' then 'valid' else 'unavailable' end,certificate_hash=ch,source_fingerprint=s.source_fingerprint,resolution_map_hash=s.resolution_map_hash,closure_bundle_hash=s.closure_bundle_hash,snapshot_id=s.snapshot_id,snapshot_hash=s.snapshot_hash,report_artifact_manifest_hash=rh,evidence_hash=s.evidence_hash,result_summary=rs,blocker_codes=s.blocker_codes,report_artifact_id=a.id,reused_from_check_id=s.id,updated_at=now(),finished_at=now() where id=t.id returning * into t;
  ws:=case when t.status='passed' then 'completed' else 'blocked' end; select public.worker_record_job_result(j.id,p_lease_token,ws,jsonb_build_object('closureCheckId',t.id,'status',t.status,'scanCompleteness',t.scan_completeness,'certificateStatus',t.certificate_status,'certificateHash',t.certificate_hash,'reusedFromCheckId',s.id),'lcia.scope_closure_check.result.v1',jsonb_build_object('reportArtifactId',a.id,'reportArtifactManifestHash',rh),jsonb_build_object('progressCounters',coalesce(t.result_summary->'progressCounters','{}'::jsonb)),null,null,null,case when ws='blocked' then t.blocker_codes else null end,case when ws='blocked' then 'operator' else null end,false) into wr;
  if coalesce((wr->>'ok')::boolean,false) is not true then raise exception using errcode='P0001',message='worker_job_result_rejected'; end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',t.id,'workerJobId',j.id,'status',t.status,'scanCompleteness',t.scan_completeness,'certificateHash',t.certificate_hash,'reportArtifactId',a.id,'reusedFromCheckId',s.id));
end;
$$;

create or replace function public.svc_lcia_scope_closure_fail_before_scan(
  p_closure_check_id uuid, p_worker_job_id uuid, p_lease_token uuid, p_error_code text
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare c public.lcia_scope_closure_checks%rowtype; j public.worker_jobs%rowtype; e public.lcia_scope_closure_scan_executions%rowtype; wr jsonb;
begin
  if not coalesce(util.is_service_request(),false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into c from public.lcia_scope_closure_checks where id=p_closure_check_id for update; select * into j from public.worker_jobs where id=p_worker_job_id for update;
  if c.id is null or c.worker_job_id<>j.id or c.status not in ('queued','running') or j.status<>'running' or j.lease_token is distinct from p_lease_token or j.lease_expires_at<now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  select * into e from public.lcia_scope_closure_scan_executions where id=c.scan_execution_id for update;
  update public.lcia_scope_closure_checks set status='failed',scan_completeness='unknown',certificate_status='unavailable',result_summary=jsonb_build_object('errorCode',coalesce(nullif(trim(p_error_code),''),'closure_check_failed')),updated_at=now(),finished_at=now() where id=c.id;
  if e.id is not null and e.status='running' and e.leased_by_job_id=j.id and e.lease_token is not distinct from p_lease_token then update public.lcia_scope_closure_scan_executions set status='failed',lease_token=null,leased_by_job_id=null,lease_expires_at=null,updated_at=now(),completed_at=now() where id=e.id; end if;
  select public.worker_record_job_result(j.id,p_lease_token,'failed',jsonb_build_object('closureCheckId',c.id,'status','failed'),'lcia.scope_closure_check.result.v1',null,null,coalesce(nullif(trim(p_error_code),''),'closure_check_failed'),'Scope closure check failed',null,null,null,true) into wr;
  if coalesce((wr->>'ok')::boolean,false) is not true then raise exception using errcode='P0001',message='worker_job_result_rejected'; end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',c.id,'workerJobId',j.id,'status','failed'));
end;
$$;

create or replace function public.get_task_summary_v2_feed(
  p_category text default null, p_job_kinds text[] default null, p_statuses text[] default null,
  p_updated_since timestamptz default null, p_cursor_updated_at timestamptz default null,
  p_cursor_job_id uuid default null, p_limit integer default 50, p_root_only boolean default false
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare a uuid:=auth.uid(); lim integer:=greatest(1,least(coalesce(p_limit,50),200)); mgr boolean;
begin
  if a is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  if (p_cursor_updated_at is null)<>(p_cursor_job_id is null) then return public.lcia_scope_closure_error('invalid_task_cursor',400,'Task cursor fields must be supplied together'); end if;
  mgr:=public.lcia_scope_closure_is_manager();
  return jsonb_build_object('ok',true,'serverTime',now(),'data',coalesce((with x as (
    select j.*,k.task_center_category,p.id package_id,coalesce(cd.id,cp.id,ck.id) closure_id,coalesce(cd.status,cp.status,ck.status) closure_status,coalesce(cd.certificate_status,cp.certificate_status,ck.certificate_status) certificate_status,greatest(j.updated_at,coalesce(cd.updated_at,'-infinity'::timestamptz),coalesce(cp.updated_at,'-infinity'::timestamptz),coalesce(ck.updated_at,'-infinity'::timestamptz),coalesce(p.updated_at,'-infinity'::timestamptz)) pu
    from public.worker_jobs j join public.worker_job_kinds k on k.job_kind=j.job_kind
      left join public.lcia_scope_closure_checks cd on cd.worker_job_id=j.id
      left join public.lcia_result_packages p on p.build_worker_job_id=j.id
      left join public.lcia_scope_closure_checks cp on cp.id=case when (j.payload_json->>'closure_check_id')~'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$' then (j.payload_json->>'closure_check_id')::uuid else null end
      left join public.lcia_scope_closure_checks ck on ck.id=p.closure_check_id
    where j.requested_by=a and (j.visibility='user' or (mgr and j.visibility='operator' and j.job_kind=any(array['lcia.scope_closure_check','lcia_result.package_build']))) and (p_category is null or k.task_center_category=p_category) and (p_job_kinds is null or j.job_kind=any(p_job_kinds)) and (p_statuses is null or j.status=any(p_statuses)) and (p_updated_since is null or greatest(j.updated_at,coalesce(cd.updated_at,'-infinity'::timestamptz),coalesce(cp.updated_at,'-infinity'::timestamptz),coalesce(ck.updated_at,'-infinity'::timestamptz),coalesce(p.updated_at,'-infinity'::timestamptz))>=p_updated_since) and (not p_root_only or j.root_job_id is null or j.root_job_id=j.id)
  ), pg as (select * from x where p_cursor_updated_at is null or (pu,id)<(p_cursor_updated_at,p_cursor_job_id) order by pu desc,id desc limit lim+1), sh as (select * from pg order by pu desc,id desc limit lim)
  select jsonb_build_object('items',coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object('jobId',id,'jobKind',job_kind,'category',task_center_category,'requestedBy',requested_by,'workerStatus',status,'phase',phase,'progressFraction',case when progress is null then null else greatest(0::numeric,least(progress,1::numeric)) end,'progressCounters',diagnostics->'progressCounters','domainStatus',coalesce(closure_status,result_json->>'status'),'domainValidity',certificate_status,'projectionUpdatedAt',pu,'title',coalesce(payload_json->>'name',job_kind),'blockerCodes',blocker_codes,'errorSummary',error_code,'capabilities',jsonb_build_object('canCancel',status in ('queued','running','waiting'),'canDownloadReport',closure_id is not null and closure_status in ('passed','blocked'),'canOpenWorkbench',task_center_category='data_product','canPreviewResult',package_id is not null),'deepLink',case when package_id is not null then jsonb_build_object('routeKey','data_product.package','params',jsonb_strip_nulls(jsonb_build_object('packageId',package_id,'closureCheckId',closure_id))) when closure_id is not null then jsonb_build_object('routeKey','data_product.closure_check','params',jsonb_build_object('closureCheckId',closure_id)) end,'closureCheckId',closure_id,'resultPackageId',package_id)) order by pu desc,id desc),'[]'::jsonb),'nextCursor',case when exists(select 1 from pg offset lim) then (select jsonb_build_object('updatedAt',pu,'jobId',id) from sh order by pu asc,id asc limit 1) else null end) from sh),jsonb_build_object('items','[]'::jsonb,'nextCursor',null)));
end;
$$;

-- A build is created by the Edge worker-job enqueue command, so this binding
-- RPC is deliberately called immediately after enqueue.  It makes the DB row
-- authoritative even if a caller forged the returned envelope.

revoke all on function public.svc_lcia_document_validation_evidence_lookup(jsonb) from public, anon, authenticated;
revoke all on function public.svc_lcia_document_validation_evidence_record(jsonb,uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_claim_scan_execution(uuid,uuid,uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_result_build_bind_closure(uuid,uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_reuse_completed_scan(uuid,uuid,uuid,uuid) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_finalize_reused_scan(uuid,uuid,uuid,uuid,uuid,jsonb) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_fail_before_scan(uuid,uuid,uuid,text) from public, anon, authenticated;
revoke all on function public.cmd_lcia_scope_closure_check_request_v2(jsonb,text,jsonb) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid) from public, anon, authenticated;
revoke all on function public.cmd_lcia_scope_closure_check_request_v2_untracked(jsonb,text,jsonb) from public, anon, authenticated, service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v2_untracked(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2_envelope(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb) from public, anon, authenticated, service_role;
revoke all on function public.cmd_lcia_result_build_request_v2(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb) from public, anon, authenticated, service_role;
grant execute on function public.svc_lcia_document_validation_evidence_lookup(jsonb) to service_role;
grant execute on function public.svc_lcia_document_validation_evidence_record(jsonb,uuid) to service_role;
grant execute on function public.svc_lcia_scope_closure_claim_scan_execution(uuid,uuid,uuid) to service_role;
grant execute on function public.svc_lcia_result_build_bind_closure(uuid,uuid) to service_role;
grant execute on function public.cmd_lcia_scope_closure_check_request_v2(jsonb,text,jsonb) to authenticated;
grant execute on function public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid) to service_role;
grant execute on function public.svc_lcia_scope_closure_reuse_completed_scan(uuid,uuid,uuid,uuid) to service_role;
grant execute on function public.svc_lcia_scope_closure_finalize_reused_scan(uuid,uuid,uuid,uuid,uuid,jsonb) to service_role;
grant execute on function public.svc_lcia_scope_closure_fail_before_scan(uuid,uuid,uuid,text) to service_role;
grant execute on function public.cmd_lcia_result_build_request_v2(text,jsonb,text,text,jsonb,text,uuid,text,text,jsonb) to authenticated;

-- The read APIs are product contracts, not table projections.  Keep the
-- database-owned names stable so Edge and Next do not each invent aliases for
-- lifecycle and certificate state.
create or replace function public.get_lcia_scope_closure_check(p_closure_check_id uuid)
returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid:=auth.uid(); v_check public.lcia_scope_closure_checks%rowtype; v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_check.id is null or (v_check.requested_by<>v_actor and not public.lcia_scope_closure_is_manager()) then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  select * into v_job from public.worker_jobs where id=v_check.worker_job_id;
  return jsonb_build_object('ok',true,'data',jsonb_strip_nulls(jsonb_build_object(
    'schemaVersion','lcia.scope-closure-check.v1','closureCheckId',v_check.id,
    'runStatus',v_check.status,'scanCompleteness',v_check.scan_completeness,
    'certificateValidity',v_check.certificate_status,'requestedScopeHash',v_check.requested_scope_hash,
    'effectiveScopeHash',v_check.effective_scope_hash,'policyFingerprint',v_check.policy_fingerprint,
    'dataSnapshotToken',v_check.data_snapshot_token,'blockerCodes',to_jsonb(v_check.blocker_codes),
    'summary',v_check.result_summary,'scanExecutionId',v_check.scan_execution_id,
    'reusedFromCheckId',v_check.reused_from_check_id,'createdAt',v_check.created_at,
    'updatedAt',v_check.updated_at,'finishedAt',v_check.finished_at,
    'workerJob',case when v_job.id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'jobId',v_job.id,'status',v_job.status,'phase',v_job.phase,'progressFraction',v_job.progress,
      'errorCode',v_job.error_code,'blockerCodes',to_jsonb(v_job.blocker_codes),'createdAt',v_job.created_at,
      'updatedAt',v_job.updated_at,'finishedAt',v_job.finished_at)) end)));
end;
$$;

create or replace function public.list_lcia_scope_closure_issues(
  p_closure_check_id uuid, p_after_issue_id uuid default null, p_limit integer default 100
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid:=auth.uid(); v_limit integer:=greatest(1,least(coalesce(p_limit,100),200));
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  if not exists (select 1 from public.lcia_scope_closure_checks c where c.id=p_closure_check_id and (c.requested_by=v_actor or public.lcia_scope_closure_is_manager())) then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  return jsonb_build_object('ok',true,'data',(
    with page as (
      select i.* from public.lcia_scope_closure_issues i where i.closure_check_id=p_closure_check_id
        and (p_after_issue_id is null or i.id>p_after_issue_id)
      order by i.id limit v_limit+1
    ), shown as (select * from page order by id limit v_limit)
    select jsonb_build_object('schemaVersion','lcia.scope-closure-issues-page.v1','closureCheckId',p_closure_check_id,
      'issues',coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
        'issueId',id,'severity',severity,'blocking',blocking,'code',issue_code,'title',issue_code,
        'summary',message,'suggestedAction',suggested_action,'occurrenceCount',occurrence_count,
        'affectedRootCount',affected_root_count)) order by id),'[]'::jsonb),
      'nextCursor',case when exists(select 1 from page offset v_limit) then (select id from shown order by id desc limit 1) else null end)
    from shown));
end;
$$;

-- A certificate-grade scan is bound to the current public release, whose
-- immutable manifest already contains exact hashes for every released support
-- document.  Do not substitute the live state-code tables here: roots cannot
-- prove that their transitive Flow/UnitGroup/Source/Contact dependencies are
-- unchanged.  A deployment without a current public release therefore fails
-- closed rather than minting an unverifiable closure certificate.
create or replace function public.cmd_lcia_scope_closure_check_request_v2(
  p_requested_scope jsonb, p_request_idempotency_token text, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare
  v_actor uuid:=auth.uid(); v_scope jsonb; v_policy jsonb; v_requested_scope_hash text;
  v_policy_fingerprint text; v_expected_validator text; v_snapshot_manifest jsonb; v_snapshot_token text;
  v_request_fingerprint text; v_request_key text; v_result jsonb; v_check public.lcia_scope_closure_checks%rowtype;
  v_job public.worker_jobs%rowtype; v_execution public.lcia_scope_closure_scan_executions%rowtype;
  v_publication public.lca_release_publications%rowtype; v_run public.lca_release_runs%rowtype; v_dataset_manifest jsonb;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  if not public.lcia_scope_closure_is_manager() then return public.lcia_scope_closure_error('not_data_product_manager',403,'Data product manager role is required'); end if;
  if coalesce(nullif(trim(p_request_idempotency_token),''),'')='' then return public.lcia_scope_closure_error('invalid_closure_request',400,'Idempotency token is required'); end if;
  v_scope:=public.lcia_scope_closure_normalize_request(p_requested_scope);
  select * into v_publication from public.lca_release_publications where is_current=true and status='current' order by published_at desc limit 1;
  if v_publication.id is null then return public.lcia_scope_closure_error('current_release_required',409,'A current public release is required for certificate-grade closure'); end if;
  select * into v_run from public.lca_release_runs where id=v_publication.release_run_id;
  if v_run.id is null or v_run.release_manifest_hash is null then return public.lcia_scope_closure_error('closure_evidence_unavailable',503,'Current public release manifest is unavailable'); end if;
  select coalesce(jsonb_agg(jsonb_build_object('datasetType',d.dataset_type,'datasetId',d.dataset_uuid,'datasetVersion',d.dataset_version,'role',d.dataset_role,'sourceProcessId',d.source_process_uuid,'sourceProcessVersion',d.source_process_version,'versionSignificantHash',d.version_significant_hash,'semanticHash',d.semantic_hash,'canonicalContentHash',d.canonical_content_hash) order by d.dataset_type,d.dataset_uuid,d.dataset_version,d.dataset_role),'[]'::jsonb) into v_dataset_manifest from public.lca_release_dataset_versions d where d.release_run_id=v_run.id;
  if jsonb_array_length(v_dataset_manifest)=0 then return public.lcia_scope_closure_error('closure_evidence_unavailable',503,'Current public release dataset manifest is empty'); end if;
  v_snapshot_manifest:=jsonb_build_object('schemaVersion','lcia.scope-closure-data-snapshot.v2','requestedScope',v_scope,'currentPublicRelease',jsonb_build_object('publicationId',v_publication.id,'releaseRunId',v_run.id,'releaseVersion',v_run.release_version,'publishedAt',v_publication.published_at,'releaseManifestHash',v_run.release_manifest_hash),'datasets',v_dataset_manifest);
  v_snapshot_token:=public.lcia_scope_closure_sha256(v_snapshot_manifest);
  v_requested_scope_hash:=public.lcia_scope_closure_sha256(v_scope);
  v_policy:=jsonb_build_object('scopePolicy',v_scope-'processes'-'lciaMethods'-'processManifestHash','visibilityScope','data_product_manager.v1');
  v_policy_fingerprint:=public.lcia_scope_closure_sha256(v_policy);
  select expected_validator_scanner_fingerprint into v_expected_validator from public.lcia_scope_closure_config where singleton;
  if v_expected_validator is null then return public.lcia_scope_closure_error('closure_evidence_unavailable',503,'Closure validator configuration is unavailable'); end if;
  v_request_fingerprint:=encode(extensions.digest(v_requested_scope_hash||'|'||v_policy_fingerprint||'|'||v_expected_validator||'|'||v_snapshot_token,'sha256'),'hex');
  v_request_key:=encode(extensions.digest(v_actor::text||'|'||trim(p_request_idempotency_token)||'|'||v_request_fingerprint,'sha256'),'hex');
  select * into v_check from public.lcia_scope_closure_checks where requested_by=v_actor and request_key=v_request_key for update;
  if v_check.id is not null then select * into v_job from public.worker_jobs where id=v_check.worker_job_id; return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token,'scanExecutionId',v_check.scan_execution_id,'workerJob',public.worker_job_payload(v_job,false),'reused',true)); end if;
  v_result:=public.cmd_lcia_scope_closure_check_request_v2_untracked(p_requested_scope,p_request_idempotency_token,p_audit);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  select * into v_check from public.lcia_scope_closure_checks where id=nullif(v_result->'data'->>'closureCheckId','')::uuid for update;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if coalesce((v_result->'data'->>'reused')::boolean,false) and v_check.request_key<>v_request_key and v_check.data_snapshot_token<>v_snapshot_token then return public.lcia_scope_closure_error('idempotency_token_bound_to_different_snapshot',409,'Idempotency token is already bound to a different release snapshot'); end if;
  insert into public.lcia_scope_closure_data_snapshots(data_snapshot_token,root_manifest,root_manifest_hash,publication_epoch)
  values(v_snapshot_token,v_snapshot_manifest,public.lcia_scope_closure_sha256(v_snapshot_manifest),extract(epoch from v_publication.published_at)::bigint)
  on conflict (data_snapshot_token) do nothing;
  update public.lcia_scope_closure_checks set data_snapshot_token=v_snapshot_token,request_fingerprint=v_request_fingerprint,request_key=v_request_key,updated_at=now() where id=v_check.id returning * into v_check;
  select * into v_execution from public.lcia_scope_closure_scan_executions where request_fingerprint=v_request_fingerprint for update;
  if v_execution.id is null then insert into public.lcia_scope_closure_scan_executions(request_fingerprint,requested_scope_hash,policy_fingerprint,data_snapshot_token,validator_scanner_fingerprint) values(v_request_fingerprint,v_requested_scope_hash,v_policy_fingerprint,v_snapshot_token,v_expected_validator) returning * into v_execution; end if;
  update public.lcia_scope_closure_checks set scan_execution_id=v_execution.id,updated_at=now() where id=v_check.id returning * into v_check;
  update public.worker_jobs set request_hash=v_request_fingerprint,concurrency_key=v_request_key,payload_json=payload_json||jsonb_build_object('request_fingerprint',v_request_fingerprint,'scan_execution_id',v_execution.id,'data_snapshot_token',v_snapshot_token),updated_at=now() where id=v_check.worker_job_id returning * into v_job;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_snapshot_token,'scanExecutionId',v_execution.id,'workerJob',public.worker_job_payload(v_job,false),'reused',false));
exception when sqlstate '22023' then return public.lcia_scope_closure_error('invalid_closure_scope',400,sqlerrm);
end;
$$;

-- `current-membership-required-v1` means a Build cannot consume a closure
-- certificate from an earlier release; frozen certificates remain immutable
-- evidence and are never silently rewritten or auto-staled by publication.
create or replace function public.lcia_scope_closure_current_release_matches(p_snapshot_token text)
returns boolean language sql security definer set search_path = public, pg_temp as $$
  select exists (
    select 1 from public.lcia_scope_closure_data_snapshots s
    join public.lca_release_publications p on p.is_current=true and p.status='current'
    join public.lca_release_runs r on r.id=p.release_run_id
    where s.data_snapshot_token=p_snapshot_token
      and s.root_manifest->'currentPublicRelease'->>'releaseRunId'=r.id::text
      and s.root_manifest->'currentPublicRelease'->>'releaseManifestHash'=r.release_manifest_hash
  );
$$;
revoke all on function public.lcia_scope_closure_current_release_matches(text) from public, anon, authenticated, service_role;

create or replace function public.cmd_lcia_result_build_request_v2(
  p_name text, p_processes jsonb, p_coverage_mode text, p_default_impact_category text,
  p_lcia_method_set jsonb, p_idempotency_key text, p_closure_check_id uuid,
  p_requested_scope_hash text, p_policy_fingerprint text, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_check public.lcia_scope_closure_checks%rowtype; v_policy text; v_actor uuid:=auth.uid(); v_result jsonb; v_kind public.worker_job_kinds%rowtype; v_job public.worker_jobs%rowtype; v_build_id uuid; v_payload jsonb; v_idempotency_key text;
begin
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id;
  if v_check.id is not null then
    v_policy:=v_check.requested_scope_manifest->>'certificateFreshnessPolicy';
    if v_policy='current-membership-required-v1' and not public.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
      return public.lcia_scope_closure_error('closure_certificate_stale',409,'Closure certificate was created against an earlier public release');
    end if;
  end if;
  v_result:=public.cmd_lcia_result_build_request_v2_envelope(p_name,p_processes,p_coverage_mode,p_default_impact_category,p_lcia_method_set,p_idempotency_key,p_closure_check_id,p_requested_scope_hash,p_policy_fingerprint,p_audit);
  if coalesce((v_result->>'ok')::boolean,false) is not true then return v_result; end if;
  v_build_id:=nullif(v_result->'data'->>'buildId','')::uuid;
  select * into v_kind from public.worker_job_kinds where job_kind='lcia_result.package_build';
  if v_actor is null or v_build_id is null or v_check.id is null or v_kind.job_kind is null then return public.lcia_scope_closure_error('build_enqueue_unavailable',503,'Build queue configuration is unavailable'); end if;
  v_payload:=coalesce(v_result->'data'->'workerJob'->'payload','{}'::jsonb);
  v_idempotency_key:=nullif(v_result->'data'->'workerJob'->>'idempotencyKey','');
  select * into v_job from public.worker_jobs where worker_runtime=v_kind.worker_runtime and job_kind=v_kind.job_kind and requested_by=v_actor and idempotency_key is not distinct from v_idempotency_key and status in ('queued','running','waiting','stale','blocked') order by created_at desc limit 1 for update;
  if v_job.id is null then
    insert into public.worker_jobs(job_kind,worker_runtime,worker_queue,priority,queue_key,subject_type,subject_id,requester_type,requested_by,idempotency_key,request_hash,concurrency_key,visibility,max_attempts,payload_schema_version,payload_json,payload_ref,result_schema_version)
    values(v_kind.job_kind,v_kind.worker_runtime,v_kind.worker_queue,v_kind.default_priority,nullif(v_result->'data'->'workerJob'->>'queueKey',''),'lcia_result_build',v_build_id,'operator',v_actor,v_idempotency_key,nullif(v_result->'data'->'workerJob'->>'requestHash',''),nullif(v_result->'data'->'workerJob'->>'queueKey',''),'operator',v_kind.default_max_attempts,coalesce(v_result->'data'->'workerJob'->>'payloadSchemaVersion',v_kind.payload_schema_version),v_payload,jsonb_build_object('closureCertificate',jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash,'snapshotId',v_check.snapshot_id,'snapshotHash',v_check.snapshot_hash,'closureBundleHash',v_check.closure_bundle_hash)),v_kind.result_schema_version) returning * into v_job;
    insert into public.worker_job_events(job_id,event_type,status,details) values(v_job.id,'enqueued',v_job.status,jsonb_build_object('jobKind',v_job.job_kind,'closureCheckId',v_check.id,'certificateHash',v_check.certificate_hash));
  end if;
  return jsonb_set(jsonb_set(v_result,'{data,workerJob}',public.worker_job_payload(v_job,false),true),'{data,workerJobId}',to_jsonb(v_job.id),true);
exception when unique_violation then return public.lcia_scope_closure_error('build_enqueue_conflict',409,'A conflicting certificate-bound build is already active');
end;
$$;
