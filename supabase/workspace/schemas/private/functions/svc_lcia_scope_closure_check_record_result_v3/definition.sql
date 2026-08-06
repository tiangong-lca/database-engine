CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v3"("check_id" "uuid", "worker_job_id" "uuid", "lease_token" "uuid", "status" "text", "scan_completeness" "text", "effective_scope" "jsonb", "evidence" "jsonb", "result_summary" "jsonb", "issues" "jsonb", "blocker_codes" "text"[], "report_artifact_id" "uuid", "closure_bundle_artifact_id" "uuid", "snapshot_artifact_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_status text := lower(trim(coalesce(status, '')));
  v_check private.lcia_scope_closure_checks%rowtype;
  v_job private.worker_jobs%rowtype;
  v_execution private.lcia_scope_closure_scan_executions%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_snapshot_artifact private.lca_snapshot_artifacts%rowtype;
  v_report private.worker_job_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
  v_effective_scope_hash text;
  v_report_manifest_hash text;
  v_evidence_hash text;
  v_build_contract_hash text;
  v_certificate_hash text;
  v_certificate_bindings jsonb;
  v_issue jsonb;
  v_occurrence jsonb;
  v_root jsonb;
  v_closure_issue private.lcia_scope_closure_issues%rowtype;
  v_worker_result jsonb;
  v_scan_key text;
  v_existing_execution uuid;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  if v_status not in ('passed', 'blocked', 'failed')
     or scan_completeness not in ('complete', 'incomplete', 'unknown')
     or jsonb_typeof(coalesce(effective_scope, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(evidence, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(result_summary, 'null'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(issues, 'null'::jsonb)) <> 'array' then
    return api.lcia_scope_closure_error('invalid_closure_result', 400, 'Invalid closure result payload');
  end if;

  -- Blocked and failed checks intentionally do not acquire or persist a
  -- numerical snapshot binding.  V2 remains the compatibility path for those
  -- non-certificate terminal states.
  if v_status <> 'passed' then
    return private.svc_lcia_scope_closure_check_record_result_v2_legacy(
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
    return api.lcia_scope_closure_error('closure_check_incomplete', 409, 'Passed closure checks must be complete and free of blocking issues');
  end if;
  if evidence->>'schemaVersion' is distinct from 'lcia.scope-closure-evidence.v2'
     or result_summary->>'schemaVersion' is distinct from 'lcia.scope-closure-summary.v1'
     or not (evidence ?& array[
    'sourceFingerprint', 'resolutionMapHash', 'closureBundleHash',
    'closureBundleArtifactId', 'snapshotId', 'snapshotHash', 'snapshotArtifactId',
    'snapshotIndexSha256', 'snapshotBuildContractHash', 'evidenceHash'
  ]) or not (result_summary ? 'evidenceHash') then
    return api.lcia_scope_closure_error('closure_evidence_unavailable', 409, 'Passed closure checks require complete numerical snapshot evidence');
  end if;

  select * into v_check
  from private.lcia_scope_closure_checks
  where id = check_id
  for update;
  if v_check.id is null or v_check.worker_job_id <> worker_job_id then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_check.status not in ('queued', 'running') then
    return api.lcia_scope_closure_error('closure_check_already_terminal', 409, 'Closure check is already terminal');
  end if;

  select * into v_job
  from private.worker_jobs
  where id = worker_job_id
  for update;
  if v_job.id is null
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from lease_token
     or v_job.lease_expires_at < now() then
    return api.lcia_scope_closure_error('worker_job_lease_invalid', 409, 'Worker job lease is no longer valid');
  end if;

  select * into v_execution
  from private.lcia_scope_closure_scan_executions
  where id = v_check.scan_execution_id
  for update;
  if v_execution.id is null
     or v_execution.status <> 'running'
     or v_execution.leased_by_job_id <> worker_job_id
     or v_execution.lease_token is distinct from lease_token then
    return api.lcia_scope_closure_error('scan_execution_lease_invalid', 409, 'Scan execution is not held by this worker job');
  end if;

  select * into v_report
  from private.worker_job_artifacts
  where id = report_artifact_id
    and job_id = v_job.id
    and artifact_type = 'closure_report_xlsx';
  if v_report.id is null then
    return api.lcia_scope_closure_error('closure_report_unavailable', 409, 'Report artifact does not belong to the closure job or has the wrong type');
  end if;

  select * into v_bundle
  from private.worker_job_artifacts
  where id = closure_bundle_artifact_id
    and job_id = v_job.id
    and artifact_type = 'closure_bundle';
  if v_bundle.id is null
     or v_bundle.checksum_sha256 is null
     or v_bundle.id::text <> evidence->>'closureBundleArtifactId'
     or v_bundle.checksum_sha256 <> evidence->>'closureBundleHash'
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text then
    return api.lcia_scope_closure_error('closure_bundle_binding_invalid', 409, 'Closure bundle does not belong to this check or its hash does not match');
  end if;

  select * into v_snapshot
  from private.lca_network_snapshots
  where id = v_execution.numerical_snapshot_id
  for update;
  select * into v_snapshot_artifact
  from private.lca_snapshot_artifacts
  where id = snapshot_artifact_id;

  v_effective_scope_hash := private.lcia_scope_closure_sha256(effective_scope);
  v_build_contract_hash := private.lcia_scope_closure_sha256_text(
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
     or v_snapshot.provider_matching_rule <> 'split_by_process_volume'
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
    return api.lcia_scope_closure_error('numerical_snapshot_binding_invalid', 409, 'Numerical snapshot or artifact does not satisfy the closure certificate contract');
  end if;

  v_report_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if coalesce(evidence->>'reportArtifactManifestHash', '') <> v_report_manifest_hash then
    return api.lcia_scope_closure_error('closure_report_hash_mismatch', 409, 'Report artifact manifest hash does not match persisted artifact metadata');
  end if;

  v_evidence_hash := private.lcia_scope_closure_sha256_text(
    'lcia.scope-closure-evidence.v2' || chr(10)
    || (evidence->>'sourceFingerprint') || chr(10)
    || (evidence->>'resolutionMapHash') || chr(10)
    || v_bundle.checksum_sha256 || chr(10)
    || v_bundle.id::text || chr(10)
    || v_snapshot.id::text || chr(10)
    || v_snapshot_artifact.artifact_sha256 || chr(10)
    || v_snapshot_artifact.id::text || chr(10)
    || v_snapshot_artifact.snapshot_index_sha256 || chr(10)
    || v_snapshot_artifact.snapshot_build_contract_hash
  );
  if evidence->>'evidenceHash' <> v_evidence_hash
     or result_summary->>'evidenceHash' <> v_evidence_hash then
    return api.lcia_scope_closure_error('closure_evidence_hash_mismatch', 409, 'Closure evidence hash does not match database-owned snapshot evidence');
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
  v_certificate_hash := private.lcia_scope_closure_sha256(v_certificate_bindings);

  delete from private.lcia_scope_closure_issues where closure_check_id = v_check.id;
  for v_issue in select value from jsonb_array_elements(issues) loop
    insert into private.lcia_scope_closure_issues(
      closure_check_id, issue_key, severity, blocking, issue_code,
      source_dataset_type, source_dataset_id, source_dataset_version,
      json_path, reference_role, requested_target_type, requested_target_id,
      requested_target_version, message, suggested_action,
      occurrence_count, affected_root_count, details
    ) values (
      v_check.id,
      coalesce(nullif(trim(v_issue->>'issueKey'), ''), private.lcia_scope_closure_sha256(v_issue)),
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
      insert into private.lcia_scope_closure_issue_occurrences(
        closure_issue_id, occurrence_key, source_dataset_type,
        source_dataset_id, source_dataset_version, json_path, reference_role, details
      ) values (
        v_closure_issue.id,
        coalesce(nullif(v_occurrence->>'occurrenceKey', ''), private.lcia_scope_closure_sha256(v_occurrence)),
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
      insert into private.lcia_scope_closure_issue_roots(
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

  update private.lcia_scope_closure_checks set
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

  v_scan_key := private.lcia_scope_closure_sha256(jsonb_build_object(
    'effectiveScopeHash', v_check.effective_scope_hash,
    'policyFingerprint', v_check.policy_fingerprint,
    'validatorScannerFingerprint', v_check.expected_validator_scanner_fingerprint,
    'dataSnapshotToken', v_check.data_snapshot_token
  ));
  select id into v_existing_execution
  from private.lcia_scope_closure_scan_executions
  where scan_key = v_scan_key and id <> v_execution.id
  limit 1;
  if v_existing_execution is not null then v_scan_key := null; end if;
  update private.lcia_scope_closure_scan_executions set
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

  select private.worker_record_job_result(
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
    return api.lcia_scope_closure_error('invalid_closure_result', 400, 'Closure result contains invalid identity values');
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v3"("check_id" "uuid", "worker_job_id" "uuid", "lease_token" "uuid", "status" "text", "scan_completeness" "text", "effective_scope" "jsonb", "evidence" "jsonb", "result_summary" "jsonb", "issues" "jsonb", "blocker_codes" "text"[], "report_artifact_id" "uuid", "closure_bundle_artifact_id" "uuid", "snapshot_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v3"("check_id" "uuid", "worker_job_id" "uuid", "lease_token" "uuid", "status" "text", "scan_completeness" "text", "effective_scope" "jsonb", "evidence" "jsonb", "result_summary" "jsonb", "issues" "jsonb", "blocker_codes" "text"[], "report_artifact_id" "uuid", "closure_bundle_artifact_id" "uuid", "snapshot_artifact_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v3"("check_id" "uuid", "worker_job_id" "uuid", "lease_token" "uuid", "status" "text", "scan_completeness" "text", "effective_scope" "jsonb", "evidence" "jsonb", "result_summary" "jsonb", "issues" "jsonb", "blocker_codes" "text"[], "report_artifact_id" "uuid", "closure_bundle_artifact_id" "uuid", "snapshot_artifact_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_check_record_result_v3"("check_id" "uuid", "worker_job_id" "uuid", "lease_token" "uuid", "status" "text", "scan_completeness" "text", "effective_scope" "jsonb", "evidence" "jsonb", "result_summary" "jsonb", "issues" "jsonb", "blocker_codes" "text"[], "report_artifact_id" "uuid", "closure_bundle_artifact_id" "uuid", "snapshot_artifact_id" "uuid") TO "api_internal_executor";
