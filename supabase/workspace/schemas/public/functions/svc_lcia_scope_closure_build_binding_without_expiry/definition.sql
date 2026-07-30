CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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

ALTER FUNCTION "public"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_build_binding_without_expiry"("build_worker_job_id" "uuid") FROM PUBLIC;
