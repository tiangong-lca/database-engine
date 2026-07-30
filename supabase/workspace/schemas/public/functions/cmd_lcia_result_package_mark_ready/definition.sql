CREATE OR REPLACE FUNCTION "public"."cmd_lcia_result_package_mark_ready"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid" DEFAULT NULL::"uuid", "p_result_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_query_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_artifact_manifest" "jsonb" DEFAULT '{}'::"jsonb", "p_available_impact_categories" "jsonb" DEFAULT '[]'::"jsonb", "p_default_impact_category" "text" DEFAULT NULL::"text", "p_package_result_hash" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
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

ALTER FUNCTION "public"."cmd_lcia_result_package_mark_ready"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_lcia_result_package_mark_ready"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_lcia_result_package_mark_ready"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") TO "service_role";
