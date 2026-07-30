CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_finalize_reused_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid", "p_report_artifact_id" "uuid", "p_result_summary" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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

ALTER FUNCTION "public"."svc_lcia_scope_closure_finalize_reused_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid", "p_report_artifact_id" "uuid", "p_result_summary" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_finalize_reused_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid", "p_report_artifact_id" "uuid", "p_result_summary" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_finalize_reused_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid", "p_report_artifact_id" "uuid", "p_result_summary" "jsonb") TO "service_role";
