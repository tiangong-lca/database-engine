CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_v3_job_binding_valid_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_job private.worker_jobs%rowtype;
  v_check private.lcia_scope_closure_checks%rowtype;
  v_closure_check_id uuid;
begin
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return false;
  end if;
  begin
    v_closure_check_id := nullif(
      v_job.payload_json ->> 'closure_check_id', ''
    )::uuid;
  exception when invalid_text_representation then
    return false;
  end;
  select closure_check.* into v_check
  from private.lcia_scope_closure_checks as closure_check
  where closure_check.id = v_closure_check_id;
  if v_check.id is null
     or v_check.requested_by <> v_job.requested_by
     or not private.lcia_scope_closure_evidence_usable(v_check)
     or v_job.payload_json ->> 'closure_certificate_hash'
          is distinct from v_check.certificate_hash
     or v_job.payload_json ->> 'requested_scope_hash'
          is distinct from v_check.requested_scope_hash
     or v_job.payload_json ->> 'policy_fingerprint'
          is distinct from v_check.policy_fingerprint
     or v_job.payload_json ->> 'effective_scope_hash'
          is distinct from v_check.effective_scope_hash
     or v_job.payload_json ->> 'data_snapshot_token'
          is distinct from v_check.data_snapshot_token
     or v_job.payload_json ->> 'snapshot_id'
          is distinct from v_check.snapshot_id::text
     or v_job.payload_json ->> 'snapshot_hash'
          is distinct from v_check.snapshot_hash
     or v_job.payload_json ->> 'closure_bundle_artifact_id'
          is distinct from v_check.closure_bundle_artifact_id::text
     or v_job.payload_json ->> 'closure_bundle_hash'
          is distinct from v_check.closure_bundle_hash
     or v_job.payload_json ->> 'report_artifact_manifest_hash'
          is distinct from v_check.report_artifact_manifest_hash
     or v_job.payload_json ->> 'snapshot_artifact_id'
          is distinct from v_check.snapshot_artifact_id::text
     or v_job.payload_json ->> 'snapshot_index_sha256'
          is distinct from v_check.snapshot_index_sha256
     or v_job.payload_json ->> 'snapshot_build_contract_hash'
          is distinct from v_check.snapshot_build_contract_hash then
    return false;
  end if;
  if v_check.requested_scope_manifest ->> 'certificateFreshnessPolicy'
       = 'current-membership-required-v1'
     and not private.lcia_scope_closure_current_release_matches(
       v_check.data_snapshot_token
     ) then
    return false;
  end if;
  return true;
end
$$;

ALTER FUNCTION "private"."portal_lcia_projection_v3_job_binding_valid_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_v3_job_binding_valid_v1"("p_build_worker_job_id" "uuid", "p_lease_token" "uuid") FROM PUBLIC;
