-- The first scope-closure migration deliberately kept the public request small
-- while the product contracts were being wired.  A client supplied hash alone
-- is not a scope: it cannot be re-read by the worker or used to prove what was
-- actually checked.  This migration replaces that transitional path with a
-- server-normalized, immutable manifest and a lease-fenced completion RPC.

alter table public.lcia_scope_closure_checks
  add column if not exists requested_scope_manifest jsonb,
  add column if not exists effective_scope_manifest jsonb,
  add column if not exists certificate_schema_version text,
  add column if not exists source_fingerprint text,
  add column if not exists resolution_map_hash text,
  add column if not exists closure_bundle_hash text,
  add column if not exists snapshot_id uuid,
  add column if not exists snapshot_hash text,
  add column if not exists report_artifact_manifest_hash text,
  add column if not exists evidence_hash text;

alter table public.lcia_scope_closure_checks
  drop constraint if exists lcia_scope_closure_checks_completeness_check;
alter table public.lcia_scope_closure_checks
  add constraint lcia_scope_closure_checks_completeness_check
  check (scan_completeness is null or scan_completeness in ('complete', 'incomplete', 'unknown'));
alter table public.lcia_scope_closure_checks
  drop constraint if exists lcia_scope_closure_checks_requested_scope_manifest_check;
alter table public.lcia_scope_closure_checks
  add constraint lcia_scope_closure_checks_requested_scope_manifest_check
  check (requested_scope_manifest is null or jsonb_typeof(requested_scope_manifest) = 'object');
alter table public.lcia_scope_closure_checks
  drop constraint if exists lcia_scope_closure_checks_effective_scope_manifest_check;
alter table public.lcia_scope_closure_checks
  add constraint lcia_scope_closure_checks_effective_scope_manifest_check
  check (effective_scope_manifest is null or jsonb_typeof(effective_scope_manifest) = 'object');

create or replace function public.lcia_scope_closure_sha256(p_document jsonb)
returns text language sql immutable set search_path = public, pg_temp as $$
  select encode(extensions.digest(coalesce(p_document, '{}'::jsonb)::text, 'sha256'), 'hex')
$$;

create or replace function public.lcia_scope_closure_normalize_request(p_requested_scope jsonb)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_coverage_mode text := lower(trim(coalesce(p_requested_scope->>'coverageMode', '')));
  v_processes jsonb;
  v_methods jsonb;
  v_link_policy jsonb;
  v_freshness text;
  v_process_count integer;
  v_method_count integer;
  v_invalid_count integer;
  v_duplicate_count integer;
  v_manifest jsonb;
begin
  if jsonb_typeof(coalesce(p_requested_scope, 'null'::jsonb)) <> 'object'
     or v_coverage_mode not in ('subset', 'global_eligible') then
    raise exception using errcode = '22023', message = 'invalid_closure_scope';
  end if;

  if jsonb_typeof(coalesce(p_requested_scope->'processes', '[]'::jsonb)) <> 'array'
     or jsonb_typeof(coalesce(p_requested_scope->'lciaMethods', '[]'::jsonb)) <> 'array' then
    raise exception using errcode = '22023', message = 'invalid_closure_scope_identity_list';
  end if;

  if v_coverage_mode = 'global_eligible' then
    if jsonb_array_length(coalesce(p_requested_scope->'processes', '[]'::jsonb)) <> 0 then
      raise exception using errcode = '22023', message = 'global_eligible_scope_must_not_supply_processes';
    end if;
    v_processes := public.lcia_result_current_eligible_manifest()->'inputManifest'->'processes';
  else
    with requested as (
      select (entry.value->>'id')::uuid as id,
             (entry.value->>'version')::character(9) as version
      from jsonb_array_elements(p_requested_scope->'processes') entry(value)
    ), resolved as (
      select r.id, r.version, p.state_code
      from requested r left join public.processes p on p.id = r.id and p.version = r.version
    )
    select count(*)::integer,
           count(*) filter (where state_code is null or state_code not between 100 and 199)::integer,
           count(*) - count(distinct (id, version))::integer,
           coalesce(jsonb_agg(jsonb_build_object('id', id, 'version', version) order by id, version), '[]'::jsonb)
      into v_process_count, v_invalid_count, v_duplicate_count, v_processes
      from resolved;
    if coalesce(v_process_count, 0) = 0 or v_invalid_count <> 0 or v_duplicate_count <> 0 then
      raise exception using errcode = '22023', message = 'invalid_or_ineligible_process_selection';
    end if;
  end if;

  with requested as (
    select (entry.value->>'id')::uuid as id,
           (entry.value->>'version')::character(9) as version
    from jsonb_array_elements(p_requested_scope->'lciaMethods') entry(value)
  ), resolved as (
    select r.id, r.version, (m.id is not null and m.state_code between 100 and 199) as exists_exact
    from requested r left join public.lciamethods m on m.id = r.id and m.version = r.version
  )
  select count(*)::integer,
         count(*) filter (where not exists_exact)::integer,
         count(*) - count(distinct (id, version))::integer,
         coalesce(jsonb_agg(jsonb_build_object('id', id, 'version', version) order by id, version), '[]'::jsonb)
    into v_method_count, v_invalid_count, v_duplicate_count, v_methods
    from resolved;
  if coalesce(v_method_count, 0) = 0 or v_invalid_count <> 0 or v_duplicate_count <> 0 then
    raise exception using errcode = '22023', message = 'invalid_lcia_method_selection';
  end if;

  v_freshness := coalesce(nullif(trim(p_requested_scope->>'certificateFreshnessPolicy'), ''), 'frozen-artifact-reusable-v1');
  if v_freshness not in ('frozen-artifact-reusable-v1', 'current-membership-required-v1') then
    raise exception using errcode = '22023', message = 'invalid_certificate_freshness_policy';
  end if;
  v_link_policy := coalesce(p_requested_scope->'linkPolicy', '{}'::jsonb);
  if jsonb_typeof(v_link_policy) <> 'object'
     or coalesce(v_link_policy->>'linkSemanticsVersion', 'signed-flow-balance-v1') <> 'signed-flow-balance-v1'
     or coalesce(v_link_policy->>'flowIdentityPolicy', 'exact-flow-version-reference-unit-v2') <> 'exact-flow-version-reference-unit-v2'
     or coalesce(v_link_policy->>'allocationSemanticsVersion', 'tidas-reference-allocation-v3') <> 'tidas-reference-allocation-v3'
     or coalesce(v_link_policy->>'technosphereBoundaryPolicy', 'closed') not in ('closed', 'open', 'cutoff')
     or coalesce(v_link_policy->>'providerUniversePolicy', 'scope_only') not in ('scope_only', 'eligible_transitive_expansion-v1') then
    raise exception using errcode = '22023', message = 'invalid_closure_link_policy';
  end if;

  v_manifest := jsonb_build_object(
    'schemaVersion', 'lcia.scope-manifest.v1',
    'coverageMode', v_coverage_mode,
    'eligibilityPredicateVersion', 'published-state-code-100-199:v1',
    'processes', v_processes,
    'lciaMethods', v_methods,
    'versionResolutionPolicy', 'reference-version-resolution-v1',
    'legacyOmittedVersionPolicy', 'reject',
    'certificateFreshnessPolicy', v_freshness,
    'linkPolicy', jsonb_build_object(
      'linkSemanticsVersion', 'signed-flow-balance-v1',
      'flowIdentityPolicy', 'exact-flow-version-reference-unit-v2',
      'allocationSemanticsVersion', 'tidas-reference-allocation-v3',
      'technosphereBoundaryPolicy', coalesce(v_link_policy->>'technosphereBoundaryPolicy', 'closed'),
      'providerUniversePolicy', coalesce(v_link_policy->>'providerUniversePolicy', 'scope_only')
    )
  );
  return v_manifest || jsonb_build_object('processManifestHash', public.lcia_scope_closure_sha256(jsonb_build_object('processes', v_processes)));
exception when invalid_text_representation then
  raise exception using errcode = '22023', message = 'invalid_scope_identity';
end;
$$;

create or replace function public.cmd_lcia_scope_closure_check_request_v2(
  p_requested_scope jsonb,
  p_request_idempotency_token text,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql security definer set search_path = public, pg_temp as $$
declare
  v_actor uuid := auth.uid(); v_scope jsonb; v_policy jsonb; v_requested_scope_hash text;
  v_policy_fingerprint text; v_data_snapshot_token text; v_expected_validator_scanner_fingerprint text;
  v_request_fingerprint text; v_request_key text; v_check public.lcia_scope_closure_checks%rowtype; v_job public.worker_jobs%rowtype;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required', 401, 'Authentication required'); end if;
  if not public.lcia_scope_closure_is_manager() then return public.lcia_scope_closure_error('not_data_product_manager', 403, 'Data product manager role is required'); end if;
  if coalesce(nullif(trim(p_request_idempotency_token), ''), '') = '' then return public.lcia_scope_closure_error('invalid_closure_request', 400, 'Idempotency token is required'); end if;
  v_scope := public.lcia_scope_closure_normalize_request(p_requested_scope);
  v_requested_scope_hash := public.lcia_scope_closure_sha256(v_scope);
  v_policy := jsonb_build_object('scopePolicy', v_scope - 'processes' - 'lciaMethods' - 'processManifestHash', 'visibilityScope', 'data_product_manager.v1');
  v_policy_fingerprint := public.lcia_scope_closure_sha256(v_policy);
  v_data_snapshot_token := public.lcia_scope_closure_sha256(jsonb_build_object('processes', v_scope->'processes', 'lciaMethods', v_scope->'lciaMethods'));
  select expected_validator_scanner_fingerprint into v_expected_validator_scanner_fingerprint from public.lcia_scope_closure_config where singleton;
  if v_expected_validator_scanner_fingerprint is null then return public.lcia_scope_closure_error('closure_evidence_unavailable', 503, 'Closure validator configuration is unavailable'); end if;
  v_request_fingerprint := encode(extensions.digest(v_requested_scope_hash || '|' || v_policy_fingerprint || '|' || v_expected_validator_scanner_fingerprint || '|' || v_data_snapshot_token, 'sha256'), 'hex');
  v_request_key := encode(extensions.digest(v_actor::text || '|' || trim(p_request_idempotency_token) || '|' || v_request_fingerprint, 'sha256'), 'hex');
  select * into v_check from public.lcia_scope_closure_checks where requested_by = v_actor and request_key = v_request_key for update;
  if v_check.id is not null then
    select * into v_job from public.worker_jobs where id = v_check.worker_job_id;
    return jsonb_build_object('ok', true, 'data', jsonb_build_object('closureCheckId', v_check.id, 'requestedScopeHash', v_check.requested_scope_hash, 'policyFingerprint', v_check.policy_fingerprint, 'workerJob', public.worker_job_payload(v_job, false), 'reused', true));
  end if;
  insert into public.worker_jobs (job_kind, worker_runtime, worker_queue, priority, subject_type, requester_type, requested_by, idempotency_key, request_hash, concurrency_key, visibility, max_attempts, payload_schema_version, payload_json, result_schema_version)
  values ('lcia.scope_closure_check','calculator','solver',10,'lcia_scope_closure_check','user',v_actor,trim(p_request_idempotency_token),v_request_fingerprint,v_request_key,'operator',3,'lcia.scope_closure_check.request.v1',jsonb_build_object('closure_check_id', null, 'request_fingerprint', v_request_fingerprint),'lcia.scope_closure_check.result.v1') returning * into v_job;
  insert into public.lcia_scope_closure_checks (worker_job_id,requested_by,request_idempotency_token,request_key,request_fingerprint,requested_scope_hash,requested_scope_manifest,policy_fingerprint,data_snapshot_token,expected_validator_scanner_fingerprint)
  values (v_job.id,v_actor,trim(p_request_idempotency_token),v_request_key,v_request_fingerprint,v_requested_scope_hash,v_scope,v_policy_fingerprint,v_data_snapshot_token,v_expected_validator_scanner_fingerprint) returning * into v_check;
  update public.worker_jobs set subject_id = v_check.id, payload_json = payload_json || jsonb_build_object('closure_check_id', v_check.id), updated_at = now() where id = v_job.id returning * into v_job;
  insert into public.worker_job_events(job_id,event_type,status,details) values(v_job.id,'enqueued','queued',jsonb_build_object('closureCheckId',v_check.id,'requestFingerprint',v_request_fingerprint));
  insert into public.command_audit_log(command,actor_user_id,target_table,target_id,payload) values ('cmd_lcia_scope_closure_check_request_v2',v_actor,'lcia_scope_closure_checks',v_check.id,coalesce(p_audit,'{}'::jsonb)||jsonb_build_object('requestFingerprint',v_request_fingerprint,'requestedScopeHash',v_requested_scope_hash,'policyFingerprint',v_policy_fingerprint));
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_requested_scope_hash,'policyFingerprint',v_policy_fingerprint,'workerJob',public.worker_job_payload(v_job,false),'reused',false));
exception when sqlstate '22023' then
  return public.lcia_scope_closure_error('invalid_closure_scope',400,sqlerrm);
end;
$$;

create or replace function public.svc_lcia_scope_closure_check_get_worker_input(p_closure_check_id uuid)
returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_check public.lcia_scope_closure_checks%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id;
  if v_check.id is null or v_check.requested_scope_manifest is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'requestedScope',v_check.requested_scope_manifest,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token,'expectedValidatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'requestFingerprint',v_check.request_fingerprint));
end;
$$;

create or replace function public.svc_lcia_scope_closure_check_record_result_v2(
  p_closure_check_id uuid, p_job_id uuid, p_lease_token uuid, p_status text, p_scan_completeness text,
  p_effective_scope_manifest jsonb, p_evidence jsonb, p_result_summary jsonb default '{}'::jsonb,
  p_issues jsonb default '[]'::jsonb, p_blocker_codes text[] default '{}'::text[], p_report_artifact_id uuid default null
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare
  v_check public.lcia_scope_closure_checks%rowtype; v_job public.worker_jobs%rowtype; v_status text := lower(trim(p_status));
  v_effective_hash text; v_certificate_bindings jsonb; v_certificate_hash text; v_worker_status text; v_issue jsonb; v_worker_result jsonb;
begin
  if not coalesce(util.is_service_request(), false) then return public.lcia_scope_closure_error('service_role_required',403,'Service role is required'); end if;
  if v_status not in ('passed','blocked','failed') or p_scan_completeness not in ('complete','incomplete','unknown')
     or jsonb_typeof(coalesce(p_effective_scope_manifest,'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_evidence,'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_result_summary,'{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_issues,'[]'::jsonb)) <> 'array' then return public.lcia_scope_closure_error('invalid_closure_result',400,'Invalid closure result payload'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id = p_closure_check_id for update;
  if v_check.id is null or v_check.worker_job_id <> p_job_id then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.status not in ('queued','running') then return public.lcia_scope_closure_error('closure_check_already_terminal',409,'Closure check is already terminal'); end if;
  select * into v_job from public.worker_jobs where id = p_job_id for update;
  if v_job.status <> 'running' or v_job.lease_token is distinct from p_lease_token or v_job.lease_expires_at < now() then return public.lcia_scope_closure_error('worker_job_lease_invalid',409,'Worker job lease is no longer valid'); end if;
  if p_report_artifact_id is not null and not exists (select 1 from public.worker_job_artifacts a where a.id = p_report_artifact_id and a.job_id = v_job.id) then return public.lcia_scope_closure_error('closure_report_unavailable',409,'Report artifact does not belong to the closure job'); end if;
  if v_status in ('passed','blocked') and p_report_artifact_id is null then return public.lcia_scope_closure_error('closure_report_unavailable',409,'Completed closure checks require a report artifact'); end if;
  if v_status = 'passed' and (p_scan_completeness <> 'complete' or exists (
    select 1 from jsonb_array_elements(p_issues) issue(value)
    where coalesce((issue.value->>'blocking')::boolean, false)
  )) then return public.lcia_scope_closure_error('closure_check_incomplete',409,'Passed closure checks must be complete and free of blocking issues'); end if;
  if v_status = 'blocked' and (cardinality(coalesce(p_blocker_codes,'{}'::text[])) = 0 or jsonb_array_length(p_issues) = 0) then return public.lcia_scope_closure_error('closure_blocker_details_required',409,'Blocked closure checks require issues and blocker codes'); end if;
  if v_status = 'passed' and not (p_evidence ?& array['schemaVersion','sourceFingerprint','resolutionMapHash','closureBundleHash','snapshotId','snapshotHash','reportArtifactManifestHash','evidenceHash']) then return public.lcia_scope_closure_error('closure_evidence_unavailable',409,'Passed closure checks require complete evidence'); end if;
  v_effective_hash := public.lcia_scope_closure_sha256(p_effective_scope_manifest);
  if v_status = 'passed' then
    v_certificate_bindings := jsonb_build_object('certificateSchemaVersion','lcia.scope-closure-certificate.v1','requestedScopeHash',v_check.requested_scope_hash,'scopeHash',v_effective_hash,'policyFingerprint',v_check.policy_fingerprint,'dataSnapshotToken',v_check.data_snapshot_token,'validatorScannerFingerprint',v_check.expected_validator_scanner_fingerprint,'sourceFingerprint',p_evidence->>'sourceFingerprint','resolutionMapHash',p_evidence->>'resolutionMapHash','closureBundleHash',p_evidence->>'closureBundleHash','snapshotId',p_evidence->>'snapshotId','snapshotHash',p_evidence->>'snapshotHash','reportArtifactManifestHash',p_evidence->>'reportArtifactManifestHash','evidenceHash',p_evidence->>'evidenceHash');
    v_certificate_hash := public.lcia_scope_closure_sha256(v_certificate_bindings);
  end if;
  delete from public.lcia_scope_closure_issues where closure_check_id = v_check.id;
  for v_issue in select value from jsonb_array_elements(p_issues) loop
    insert into public.lcia_scope_closure_issues(closure_check_id,issue_key,severity,blocking,issue_code,source_dataset_type,source_dataset_id,source_dataset_version,json_path,reference_role,requested_target_type,requested_target_id,requested_target_version,message,suggested_action,occurrence_count,affected_root_count)
    values(v_check.id,coalesce(nullif(trim(v_issue->>'issueKey'),''),public.lcia_scope_closure_sha256(v_issue)),coalesce(v_issue->>'severity','blocker'),coalesce((v_issue->>'blocking')::boolean,false),coalesce(nullif(trim(v_issue->>'issueCode'),''),'closure_issue'),nullif(v_issue->>'sourceDatasetType',''),nullif(v_issue->>'sourceDatasetId','')::uuid,nullif(v_issue->>'sourceDatasetVersion',''),nullif(v_issue->>'jsonPath',''),nullif(v_issue->>'referenceRole',''),nullif(v_issue->>'requestedTargetType',''),nullif(v_issue->>'requestedTargetId','')::uuid,nullif(v_issue->>'requestedTargetVersion',''),coalesce(nullif(v_issue->>'message',''),'Closure validation issue'),nullif(v_issue->>'suggestedAction',''),greatest(1,coalesce((v_issue->>'occurrenceCount')::integer,1)),greatest(0,coalesce((v_issue->>'affectedRootCount')::integer,0)));
  end loop;
  update public.lcia_scope_closure_checks set status=v_status,scan_completeness=p_scan_completeness,effective_scope_manifest=p_effective_scope_manifest,effective_scope_hash=v_effective_hash,certificate_schema_version=case when v_status='passed' then 'lcia.scope-closure-certificate.v1' else null end,certificate_status=case when v_status='passed' then 'valid' else 'unavailable' end,certificate_hash=v_certificate_hash,source_fingerprint=nullif(p_evidence->>'sourceFingerprint',''),resolution_map_hash=nullif(p_evidence->>'resolutionMapHash',''),closure_bundle_hash=nullif(p_evidence->>'closureBundleHash',''),snapshot_id=nullif(p_evidence->>'snapshotId','')::uuid,snapshot_hash=nullif(p_evidence->>'snapshotHash',''),report_artifact_manifest_hash=nullif(p_evidence->>'reportArtifactManifestHash',''),evidence_hash=nullif(p_evidence->>'evidenceHash',''),result_summary=p_result_summary,blocker_codes=coalesce(p_blocker_codes,'{}'::text[]),report_artifact_id=p_report_artifact_id,updated_at=now(),finished_at=now() where id=v_check.id returning * into v_check;
  v_worker_status := case v_status when 'passed' then 'completed' else v_status end;
  v_worker_result := jsonb_strip_nulls(jsonb_build_object('closureCheckId',v_check.id,'status',v_status,'scanCompleteness',p_scan_completeness,'certificateStatus',v_check.certificate_status,'effectiveScopeHash',v_check.effective_scope_hash,'certificateHash',v_check.certificate_hash));
  perform public.worker_record_job_result(v_job.id,p_lease_token,v_worker_status,v_worker_result,'lcia.scope_closure_check.result.v1',null,jsonb_build_object('progressCounters',coalesce(p_result_summary->'progressCounters','{}'::jsonb)),case when v_status='failed' then coalesce(p_result_summary->>'errorCode','closure_check_failed') else null end,case when v_status='failed' then 'Scope closure check failed' else null end,null,case when v_status='blocked' then coalesce(p_blocker_codes,'{}'::text[]) else null end,case when v_status='blocked' then 'operator' else null end,case when v_status='failed' then true else false end);
  return jsonb_build_object('ok',true,'data',jsonb_build_object('closureCheckId',v_check.id,'certificateHash',v_certificate_hash,'workerJobId',v_job.id));
exception when invalid_text_representation then return public.lcia_scope_closure_error('invalid_closure_result',400,'Closure result contains invalid identity values');
end;
$$;

create or replace function public.cmd_lcia_result_build_request_v2(
  p_name text, p_processes jsonb, p_coverage_mode text, p_default_impact_category text,
  p_lcia_method_set jsonb, p_idempotency_key text, p_closure_check_id uuid,
  p_requested_scope_hash text, p_policy_fingerprint text, p_audit jsonb default '{}'::jsonb
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid := auth.uid(); v_check public.lcia_scope_closure_checks%rowtype; v_result jsonb; v_effective jsonb; v_payload jsonb;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  select * into v_check from public.lcia_scope_closure_checks where id=p_closure_check_id and requested_by=v_actor;
  if v_check.id is null then return public.lcia_scope_closure_error('closure_check_not_found',404,'Closure check not found'); end if;
  if v_check.certificate_status='stale' then return public.lcia_scope_closure_error('closure_check_stale',409,'Closure certificate is stale'); end if;
  if v_check.certificate_status='revoked' then return public.lcia_scope_closure_error('closure_check_revoked',409,'Closure certificate is revoked'); end if;
  if v_check.status<>'passed' or v_check.scan_completeness<>'complete' or v_check.certificate_status<>'valid' or v_check.certificate_hash is null then return public.lcia_scope_closure_error('closure_check_not_usable',409,'A valid complete closure certificate is required'); end if;
  if v_check.requested_scope_hash<>trim(coalesce(p_requested_scope_hash,'')) then return public.lcia_scope_closure_error('closure_check_scope_mismatch',409,'Requested scope does not match the closure certificate'); end if;
  if v_check.policy_fingerprint<>trim(coalesce(p_policy_fingerprint,'')) then return public.lcia_scope_closure_error('closure_check_policy_mismatch',409,'Policy does not match the closure certificate'); end if;
  v_effective:=coalesce(v_check.effective_scope_manifest,v_check.requested_scope_manifest);
  v_result:=public.cmd_lcia_result_build_request_legacy(p_name,v_effective->'processes','subset',p_default_impact_category,v_effective->'lciaMethods',p_idempotency_key,coalesce(p_audit,'{}'::jsonb)||jsonb_build_object('closureCheckId',v_check.id,'requestedScopeHash',v_check.requested_scope_hash,'policyFingerprint',v_check.policy_fingerprint,'certificateHash',v_check.certificate_hash));
  v_payload:=(v_result->'data'->'workerJob'->'payload') || jsonb_build_object('coverage_mode',v_effective->>'coverageMode','input_manifest',jsonb_build_object('predicateVersion',v_effective->>'eligibilityPredicateVersion','selectionMode','closure_certificate','processes',v_effective->'processes'),'input_manifest_hash',public.lcia_scope_closure_sha256(jsonb_build_object('processes',v_effective->'processes')),'lcia_method_set',v_effective->'lciaMethods','closure_check_id',v_check.id,'closure_certificate_hash',v_check.certificate_hash,'effective_scope_hash',v_check.effective_scope_hash,'data_snapshot_token',v_check.data_snapshot_token);
  return jsonb_set(v_result,'{data,workerJob,payload}',v_payload,true) || jsonb_build_object('closureCheckId',v_check.id,'effectiveScopeHash',v_check.effective_scope_hash,'certificateHash',v_check.certificate_hash);
end;
$$;

create or replace function public.get_task_summary_v2_feed(
  p_category text default null, p_job_kinds text[] default null, p_statuses text[] default null,
  p_updated_since timestamptz default null, p_cursor_updated_at timestamptz default null,
  p_cursor_job_id uuid default null, p_limit integer default 50, p_root_only boolean default false
) returns jsonb language plpgsql security definer set search_path = public, pg_temp as $$
declare v_actor uuid:=auth.uid(); v_limit integer:=greatest(1,least(coalesce(p_limit,50),200)); v_is_manager boolean;
begin
  if v_actor is null then return public.lcia_scope_closure_error('auth_required',401,'Authentication required'); end if;
  if (p_cursor_updated_at is null) <> (p_cursor_job_id is null) then return public.lcia_scope_closure_error('invalid_task_cursor',400,'Task cursor fields must be supplied together'); end if;
  v_is_manager:=public.lcia_scope_closure_is_manager();
  return jsonb_build_object('ok',true,'serverTime',now(),'data',coalesce((with projected as (
    select j.*,k.task_center_category,greatest(j.updated_at,coalesce(c.updated_at,'-infinity'::timestamptz),coalesce(p.updated_at,'-infinity'::timestamptz)) projection_updated_at,c.id closure_check_id,c.status closure_status,c.certificate_status,p.id package_id
    from public.worker_jobs j join public.worker_job_kinds k on k.job_kind=j.job_kind left join public.lcia_scope_closure_checks c on c.worker_job_id=j.id left join public.lcia_result_packages p on p.build_worker_job_id=j.id
    where j.requested_by=v_actor and (j.visibility='user' or (v_is_manager and j.visibility='operator' and j.job_kind=any(array['lcia.scope_closure_check','lcia_result.package_build'])))
      and (p_category is null or k.task_center_category=p_category) and (p_job_kinds is null or j.job_kind=any(p_job_kinds)) and (p_statuses is null or j.status=any(p_statuses)) and (p_updated_since is null or greatest(j.updated_at,coalesce(c.updated_at,'-infinity'::timestamptz),coalesce(p.updated_at,'-infinity'::timestamptz))>=p_updated_since) and (not p_root_only or j.root_job_id is null or j.root_job_id=j.id)
  ), page as (select * from projected where p_cursor_updated_at is null or (projection_updated_at,id)<(p_cursor_updated_at,p_cursor_job_id) order by projection_updated_at desc,id desc limit v_limit+1), shown as (select * from page order by projection_updated_at desc,id desc limit v_limit)
  select jsonb_build_object('items',coalesce(jsonb_agg(jsonb_strip_nulls(jsonb_build_object('jobId',id,'jobKind',job_kind,'category',task_center_category,'requestedBy',requested_by,'workerStatus',status,'phase',phase,'progressFraction',case when progress is null then null else greatest(0::numeric,least(progress,1::numeric)) end,'progressCounters',diagnostics->'progressCounters','domainStatus',coalesce(closure_status,result_json->>'status'),'domainValidity',certificate_status,'projectionUpdatedAt',projection_updated_at,'title',coalesce(payload_json->>'name',job_kind),'blockerCodes',blocker_codes,'errorSummary',error_code,'capabilities',jsonb_build_object('canCancel',status in ('queued','running','waiting'),'canDownloadReport',closure_check_id is not null and closure_status in ('passed','blocked'),'canOpenWorkbench',task_center_category='data_product','canPreviewResult',package_id is not null),'deepLink',case when closure_check_id is not null then jsonb_build_object('routeKey','data_product.closure_check','params',jsonb_build_object('closureCheckId',closure_check_id)) when package_id is not null then jsonb_build_object('routeKey','data_product.package','params',jsonb_build_object('packageId',package_id)) end,'closureCheckId',closure_check_id,'resultPackageId',package_id)) order by projection_updated_at desc,id desc),'[]'::jsonb),'nextCursor',case when exists(select 1 from page offset v_limit) then (select jsonb_build_object('updatedAt',projection_updated_at,'jobId',id) from shown order by projection_updated_at asc,id asc limit 1) else null end) from shown),jsonb_build_object('items','[]'::jsonb,'nextCursor',null)));
end;
$$;

-- Old hash-only request cannot prove an immutable Scope Manifest.  Keep the
-- function for migration compatibility, but make it unavailable to clients.
revoke all on function public.cmd_lcia_scope_closure_check_request(text,text,text,jsonb) from public, anon, authenticated;
revoke all on function public.svc_lcia_scope_closure_check_record_result(uuid,text,text,text,text,jsonb,text[],uuid) from public, anon, authenticated, service_role;
grant execute on function public.cmd_lcia_scope_closure_check_request_v2(jsonb,text,jsonb) to authenticated;
revoke all on function public.svc_lcia_scope_closure_check_get_worker_input(uuid) from public, anon, authenticated;
grant execute on function public.svc_lcia_scope_closure_check_get_worker_input(uuid) to service_role;
revoke all on function public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid) from public, anon, authenticated;
grant execute on function public.svc_lcia_scope_closure_check_record_result_v2(uuid,uuid,uuid,text,text,jsonb,jsonb,jsonb,jsonb,text[],uuid) to service_role;
revoke all on function public.lcia_scope_closure_normalize_request(jsonb), public.lcia_scope_closure_sha256(jsonb) from public, anon, authenticated;
