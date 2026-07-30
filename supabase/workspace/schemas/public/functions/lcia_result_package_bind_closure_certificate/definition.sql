CREATE OR REPLACE FUNCTION "public"."lcia_result_package_bind_closure_certificate"() RETURNS "trigger"
    LANGUAGE "plpgsql"
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

ALTER FUNCTION "public"."lcia_result_package_bind_closure_certificate"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."lcia_result_package_bind_closure_certificate"() TO "anon";

GRANT ALL ON FUNCTION "public"."lcia_result_package_bind_closure_certificate"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."lcia_result_package_bind_closure_certificate"() TO "service_role";
