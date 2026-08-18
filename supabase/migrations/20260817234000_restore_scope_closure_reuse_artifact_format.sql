-- Restore the complete numerical evidence projection consumed by Worker when
-- a passed Scope Closure scan is reused. The RPC already validates the stored
-- artifact format before this response is built; return that validated fact.

set lock_timeout = '5s';
set statement_timeout = '120s';

CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_reuse_completed_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_target private.lcia_scope_closure_checks%rowtype;
  v_source private.lcia_scope_closure_checks%rowtype;
  v_execution private.lcia_scope_closure_scan_executions%rowtype;
  v_job private.worker_jobs%rowtype;
  v_snapshot private.lca_network_snapshots%rowtype;
  v_artifact private.lca_snapshot_artifacts%rowtype;
  v_bundle private.worker_job_artifacts%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error('service_role_required', 403, 'Service role is required');
  end if;
  select * into v_target from private.lcia_scope_closure_checks where id = p_closure_check_id for update;
  select * into v_source
  from private.lcia_scope_closure_checks
  where id = p_completed_check_id
  for share;
  select * into v_job from private.worker_jobs where id = p_worker_job_id for update;
  if v_target.id is null or v_source.id is null
     or v_target.worker_job_id <> p_worker_job_id
     or v_target.status not in ('queued', 'running') then
    return api.lcia_scope_closure_error('closure_check_not_found', 404, 'Closure check not found');
  end if;
  if v_job.status <> 'running'
     or v_job.lease_token is distinct from p_lease_token
     or v_job.lease_expires_at < now() then
    return api.lcia_scope_closure_error('worker_job_lease_invalid', 409, 'Worker job lease is no longer valid');
  end if;
  select * into v_execution
  from private.lcia_scope_closure_scan_executions
  where id = v_target.scan_execution_id
  for update;
  if v_execution.id is null
     or v_execution.status <> 'completed'
     or v_execution.completed_check_id <> v_source.id
     or (
       v_source.status = 'passed'
       and v_execution.numerical_snapshot_id is distinct from v_source.snapshot_id
     )
     or v_source.status not in ('passed', 'blocked')
     or v_source.scan_completeness <> 'complete'
     or v_source.requested_scope_hash <> v_target.requested_scope_hash
     or v_source.policy_fingerprint <> v_target.policy_fingerprint
     or v_source.data_snapshot_token <> v_target.data_snapshot_token then
    return api.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Completed scan evidence does not match this closure run');
  end if;
  if v_source.status = 'passed' then
    select * into v_snapshot from private.lca_network_snapshots where id = v_source.snapshot_id;
    select * into v_artifact from private.lca_snapshot_artifacts where id = v_source.snapshot_artifact_id;
    select * into v_bundle from private.worker_job_artifacts where id = v_source.closure_bundle_artifact_id;
    if v_source.certificate_status <> 'valid'
       or v_source.certificate_schema_version <> 'lcia.scope-closure-certificate.v2'
       or (
         v_source.requested_scope_manifest->>'certificateFreshnessPolicy' = 'current-membership-required-v1'
         and not private.lcia_scope_closure_current_release_matches(v_source.data_snapshot_token)
       )
       or v_snapshot.id is null
       or v_snapshot.status <> 'ready'
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
      return api.lcia_scope_closure_error('scan_execution_not_reusable', 409, 'Source numerical snapshot certificate is not reusable');
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
      'artifactFormat', v_artifact.artifact_format,
      'evidenceHash', v_source.evidence_hash
    )),
    'blockerCodes', to_jsonb(v_source.blocker_codes)
  ));
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_reuse_completed_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_reuse_completed_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_reuse_completed_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_reuse_completed_scan"("p_closure_check_id" "uuid", "p_worker_job_id" "uuid", "p_lease_token" "uuid", "p_completed_check_id" "uuid") TO "api_internal_executor";
