CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_stage_begin_v1"("p_build_worker_job_id" "uuid", "p_stage_lease_token" "uuid", "p_process_count" integer, "p_impact_count" integer, "p_source" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_job private.worker_jobs%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_expected_process_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;

  if p_build_worker_job_id is null
     or p_stage_lease_token is null
     or p_process_count is null
     or p_impact_count is null
     or p_process_count not between 1 and 1000000
     or p_impact_count not between 1 and 10000
     or p_process_count::bigint * p_impact_count::bigint > 100000000
     or private.portal_lcia_json_object_has_keys_v1(
       p_source,
       array[
         'schemaVersion',
         'bundleContentHash',
         'bundleManifestSha256',
         'lciaChunkSetSha256',
         'resultArtifactSha256',
         'queryArtifactSha256'
       ]
     ) is not true
     or jsonb_typeof(p_source -> 'schemaVersion') <> 'string'
     or jsonb_typeof(p_source -> 'bundleContentHash') <> 'string'
     or jsonb_typeof(p_source -> 'bundleManifestSha256') <> 'string'
     or jsonb_typeof(p_source -> 'lciaChunkSetSha256') <> 'string'
     or jsonb_typeof(p_source -> 'resultArtifactSha256') <> 'string'
     or jsonb_typeof(p_source -> 'queryArtifactSha256') <> 'string'
     or p_source ->> 'schemaVersion' <> 'portal.lcia-projection.source.v1'
     or coalesce(p_source ->> 'bundleContentHash', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'bundleManifestSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'lciaChunkSetSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'resultArtifactSha256', '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_source ->> 'queryArtifactSha256', '') !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;

  select job.*
  into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_not_found', 'status', 404
    );
  end if;
  if v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or jsonb_typeof(v_job.payload_json -> 'input_manifest' -> 'processes')
          <> 'array'
     or jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or coalesce(v_job.payload_json ->> 'input_manifest_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'closure_certificate_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'closure_bundle_hash', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_index_sha256', '')
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_job.payload_json ->> 'snapshot_build_contract_hash', '')
          !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_job_contract_invalid', 'status', 409
    );
  end if;

  if v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  v_expected_process_count := jsonb_array_length(
    v_job.payload_json -> 'input_manifest' -> 'processes'
  );
  if v_expected_process_count <> p_process_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_process_count_mismatch', 'status', 409
    );
  end if;
  if jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
       <> p_impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_impact_count_mismatch', 'status', 409
    );
  end if;

  select projection.*
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = p_build_worker_job_id
    and projection.stage_lease_token = p_stage_lease_token
  for update;

  if v_projection.id is not null then
    if v_projection.process_count <> p_process_count
       or v_projection.impact_count <> p_impact_count
       or v_projection.bundle_content_hash
            <> p_source ->> 'bundleContentHash'
       or v_projection.bundle_manifest_sha256
            <> p_source ->> 'bundleManifestSha256'
       or v_projection.lcia_chunk_set_sha256
            <> p_source ->> 'lciaChunkSetSha256'
       or v_projection.result_artifact_sha256
            <> p_source ->> 'resultArtifactSha256'
       or v_projection.query_artifact_sha256
            <> p_source ->> 'queryArtifactSha256' then
      return jsonb_build_object(
        'ok', false, 'code', 'projection_conflict', 'status', 409
      );
    end if;
    return jsonb_build_object(
      'ok', true,
      'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id,
        'buildWorkerJobId', v_projection.build_worker_job_id,
        'status', v_projection.status,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'expectedValueCount', v_projection.expected_value_count,
        'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    );
  end if;

  insert into private.portal_lcia_projection_headers (
    build_worker_job_id,
    stage_lease_token,
    projection_contract_version,
    process_count,
    impact_count,
    input_manifest_hash,
    closure_certificate_hash,
    snapshot_hash,
    closure_bundle_hash,
    snapshot_index_sha256,
    snapshot_build_contract_hash,
    bundle_content_hash,
    bundle_manifest_sha256,
    lcia_chunk_set_sha256,
    result_artifact_sha256,
    query_artifact_sha256
  ) values (
    v_job.id,
    p_stage_lease_token,
    'portal.lcia-projection.v1',
    p_process_count,
    p_impact_count,
    v_job.payload_json ->> 'input_manifest_hash',
    v_job.payload_json ->> 'closure_certificate_hash',
    v_job.payload_json ->> 'snapshot_hash',
    v_job.payload_json ->> 'closure_bundle_hash',
    v_job.payload_json ->> 'snapshot_index_sha256',
    v_job.payload_json ->> 'snapshot_build_contract_hash',
    p_source ->> 'bundleContentHash',
    p_source ->> 'bundleManifestSha256',
    p_source ->> 'lciaChunkSetSha256',
    p_source ->> 'resultArtifactSha256',
    p_source ->> 'queryArtifactSha256'
  )
  returning * into v_projection;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'status', v_projection.status,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'expectedValueCount', v_projection.expected_value_count,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_conflict', 'status', 409
    );
end
$_$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_stage_begin_v1"("p_build_worker_job_id" "uuid", "p_stage_lease_token" "uuid", "p_process_count" integer, "p_impact_count" integer, "p_source" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_begin_v1"("p_build_worker_job_id" "uuid", "p_stage_lease_token" "uuid", "p_process_count" integer, "p_impact_count" integer, "p_source" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_begin_v1"("p_build_worker_job_id" "uuid", "p_stage_lease_token" "uuid", "p_process_count" integer, "p_impact_count" integer, "p_source" "jsonb") TO "service_role";
