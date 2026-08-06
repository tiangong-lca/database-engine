CREATE OR REPLACE FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_check private.lcia_scope_closure_checks%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_artifact private.lca_snapshot_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
  v_report private.worker_job_artifacts%rowtype;
  v_result jsonb;
  v_kind private.worker_job_kinds%rowtype;
  v_job private.worker_jobs%rowtype;
  v_build_id uuid;
  v_payload jsonb;
  v_input_manifest jsonb;
  v_report_manifest_hash text;
  v_idempotency_key text;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error('auth_required', 401, 'Authentication required');
  end if;
  select * into v_check
  from private.lcia_scope_closure_checks
  where id = p_closure_check_id and requested_by = v_actor
  for share;
  if v_check.id is null then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_check.certificate_status = 'stale' then
    return api.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate is stale');
  end if;
  if v_check.certificate_status = 'revoked' then
    return api.lcia_scope_closure_error('closure_check_revoked', 409, 'Closure certificate is revoked');
  end if;
  if v_check.status <> 'passed'
     or v_check.scan_completeness <> 'complete'
     or v_check.certificate_status <> 'valid'
     or v_check.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
     or v_check.certificate_hash is null then
    return api.lcia_scope_closure_error('closure_check_not_usable', 409, 'A valid complete numerical snapshot certificate is required');
  end if;
  if v_check.requested_scope_hash <> trim(coalesce(p_requested_scope_hash, '')) then
    return api.lcia_scope_closure_error('closure_check_scope_mismatch', 409, 'Requested scope does not match the closure certificate');
  end if;
  if v_check.policy_fingerprint <> trim(coalesce(p_policy_fingerprint, '')) then
    return api.lcia_scope_closure_error('closure_check_policy_mismatch', 409, 'Policy does not match the closure certificate');
  end if;
  if v_check.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(v_check.data_snapshot_token) then
    return api.lcia_scope_closure_error('closure_check_stale', 409, 'Closure certificate was created against an earlier public release');
  end if;

  select * into v_snapshot
  from private.lca_network_snapshots
  where id = v_check.snapshot_id;
  select * into v_artifact
  from private.lca_snapshot_artifacts
  where id = v_check.snapshot_artifact_id;
  select * into v_bundle
  from private.worker_job_artifacts
  where id = v_check.closure_bundle_artifact_id;
  select * into v_report
  from private.worker_job_artifacts
  where id = v_check.report_artifact_id;
  v_report_manifest_hash := private.lcia_scope_closure_sha256(jsonb_build_object(
    'artifactId', v_report.id,
    'bucket', v_report.storage_bucket,
    'objectPath', v_report.storage_path,
    'mediaType', v_report.content_type,
    'byteSize', v_report.byte_size,
    'checksumSha256', v_report.checksum_sha256
  ));
  if v_snapshot.id is null
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
     or v_bundle.id is null
     or v_bundle.job_id <> v_check.worker_job_id
     or v_bundle.artifact_type <> 'closure_bundle'
     or v_bundle.checksum_sha256 <> v_check.closure_bundle_hash
     or coalesce(v_bundle.metadata->>'closureCheckId', '') <> v_check.id::text
     or v_report.id is null
     or v_report.job_id <> v_check.worker_job_id
     or v_report.artifact_type <> 'closure_report_xlsx'
     or v_report_manifest_hash <> v_check.report_artifact_manifest_hash then
    return api.lcia_scope_closure_error('closure_snapshot_binding_invalid', 409, 'Numerical snapshot certificate binding is not ready');
  end if;

  v_result := private.cmd_lcia_result_build_request_v2_envelope(
    p_name, p_processes, p_coverage_mode, p_default_impact_category,
    p_lcia_method_set, p_idempotency_key, p_closure_check_id,
    p_requested_scope_hash, p_policy_fingerprint, p_audit
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then return v_result; end if;
  v_build_id := nullif(v_result->'data'->>'buildId', '')::uuid;
  select * into v_kind
  from private.worker_job_kinds
  where job_kind = 'lcia_result.package_build';
  if v_build_id is null or v_kind.job_kind is null then
    return api.lcia_scope_closure_error('build_enqueue_unavailable', 503, 'Build queue configuration is unavailable');
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
      'input_manifest_hash', private.lcia_scope_closure_sha256(v_input_manifest),
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
  from private.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by = v_actor
    and idempotency_key is not distinct from v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc limit 1
  for update;
  if v_job.id is null then
    insert into private.worker_jobs(
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
    insert into private.worker_job_events(job_id, event_type, status, details)
    values(v_job.id, 'enqueued', v_job.status, jsonb_build_object(
      'jobKind', v_job.job_kind,
      'closureCheckId', v_check.id,
      'certificateHash', v_check.certificate_hash,
      'snapshotId', v_check.snapshot_id
    ));
  elsif v_job.payload_schema_version <> 'lcia_result.package_build.request.v2'
        or v_job.payload_json <> v_payload then
    return api.lcia_scope_closure_error('build_enqueue_conflict', 409, 'Existing active build does not match the certificate-bound V2 payload');
  end if;
  return jsonb_set(
    jsonb_set(v_result, '{data,workerJob}', private.worker_job_payload(v_job, false), true),
    '{data,workerJobId}', to_jsonb(v_job.id), true
  );
exception
  when unique_violation then
    return api.lcia_scope_closure_error('build_enqueue_conflict', 409, 'A conflicting certificate-bound build is already active');
end;
$$;

ALTER FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."cmd_lcia_result_build_request_v2_without_expiry"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_closure_check_id" "uuid", "p_requested_scope_hash" "text", "p_policy_fingerprint" "text", "p_audit" "jsonb") TO "api_internal_executor";
