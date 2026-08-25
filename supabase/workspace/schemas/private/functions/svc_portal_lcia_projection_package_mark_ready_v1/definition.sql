CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_package_mark_ready_v1"("p_projection_id" "uuid", "p_build_worker_job_id" "uuid", "p_lease_token" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid" DEFAULT NULL::"uuid", "p_result_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_query_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_artifact_manifest" "jsonb" DEFAULT '{}'::"jsonb", "p_available_impact_categories" "jsonb" DEFAULT '[]'::"jsonb", "p_default_impact_category" "text" DEFAULT NULL::"text", "p_package_result_hash" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_result jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_package_id uuid;
  v_projection_impacts jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if private.portal_lcia_safe_audit_v1(p_audit) is not true
     or private.portal_lcia_public_text_valid_v1(p_package_version, 256)
          is not true
     or jsonb_typeof(p_result_artifact_ref) <> 'object'
     or jsonb_typeof(p_query_artifact_ref) <> 'object'
     or jsonb_typeof(p_artifact_manifest) <> 'object'
     or p_result_artifact_ref ->> 'artifactSha256'
          !~ '^[0-9a-f]{64}$'
     or p_query_artifact_ref ->> 'artifactSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'bundleContentHash'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'bundleManifestSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'lciaChunkSetSha256'
          !~ '^[0-9a-f]{64}$'
     or p_artifact_manifest ->> 'portalProjectionId'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or p_artifact_manifest ->> 'portalProjectionContentHash'
          !~ '^[0-9a-f]{64}$'
     or jsonb_typeof(p_available_impact_categories) <> 'array'
     or (
       p_default_impact_category is not null
       and private.portal_lcia_public_text_valid_v1(
         p_default_impact_category, 512
       ) is not true
     )
     or coalesce(p_package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_request', 'status', 400
    );
  end if;

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
    and projection.build_worker_job_id = p_build_worker_job_id
  for share;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  if v_projection.status <> 'prepared' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_prepared', 'status', 409
    );
  end if;
  if v_projection.stage_lease_token is distinct from p_lease_token
     or private.portal_lcia_projection_v3_job_binding_valid_v1(
       p_build_worker_job_id, p_lease_token
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.result_artifact_sha256
       <> p_result_artifact_ref ->> 'artifactSha256'
     or v_projection.query_artifact_sha256
       <> p_query_artifact_ref ->> 'artifactSha256'
     or v_projection.bundle_content_hash
       <> p_artifact_manifest ->> 'bundleContentHash'
     or v_projection.bundle_manifest_sha256
       <> p_artifact_manifest ->> 'bundleManifestSha256'
     or v_projection.lcia_chunk_set_sha256
       <> p_artifact_manifest ->> 'lciaChunkSetSha256'
     or v_projection.id::text
       <> p_artifact_manifest ->> 'portalProjectionId'
     or v_projection.content_hash
       <> p_artifact_manifest ->> 'portalProjectionContentHash' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_impacts
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if jsonb_typeof(p_available_impact_categories) <> 'array'
     or p_available_impact_categories is distinct from v_projection_impacts then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id
  for update;
  if v_job.payload_json ->> 'snapshot_id' is distinct from p_snapshot_id::text then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  -- The established insert trigger applies its exact certificate binding only
  -- to request.v2.  A row lock and transaction-local compatibility value let
  -- this V3-only wrapper reuse that unchanged trigger and legacy insert helper;
  -- no observer can see the temporary value and V1/V2 definitions/ACLs remain
  -- byte-stable.
  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v2'
  where id = v_job.id;
  begin
    v_result := private.cmd_lcia_result_package_mark_ready_without_closure_recheck(
      p_build_worker_job_id,
      p_package_version,
      p_snapshot_id,
      p_result_id,
      p_latest_all_unit_result_id,
      p_result_artifact_ref,
      p_query_artifact_ref,
      p_artifact_manifest,
      p_available_impact_categories,
      p_default_impact_category,
      p_package_result_hash,
      p_audit
    );
  exception when others then
    update private.worker_jobs
    set payload_schema_version = 'lcia_result.package_build.request.v3'
    where id = v_job.id;
    raise;
  end;
  update private.worker_jobs
  set payload_schema_version = 'lcia_result.package_build.request.v3'
  where id = v_job.id;

  if coalesce((v_result ->> 'ok')::boolean, false) is not true then
    return v_result;
  end if;
  begin
    v_package_id := nullif(v_result -> 'data' ->> 'packageId', '')::uuid;
  exception when invalid_text_representation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = v_package_id;
  if v_package.id is null
     or v_package.build_worker_job_id <> v_job.id
     or v_package.package_version <> p_package_version
     or v_package.package_result_hash <> p_package_result_hash
     or v_package.closure_certificate_hash
          <> v_projection.closure_certificate_hash
     or v_package.closure_snapshot_hash <> v_projection.snapshot_hash
     or v_package.artifact_manifest ->> 'portalProjectionId'
          <> v_projection.id::text
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          <> v_projection.content_hash then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end if;

  return jsonb_set(
    v_result,
    '{data,projection}',
    jsonb_build_object(
      'projectionId', v_projection.id,
      'contentHash', v_projection.content_hash,
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1'
    ),
    true
  );
end
$_$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_package_mark_ready_v1"("p_projection_id" "uuid", "p_build_worker_job_id" "uuid", "p_lease_token" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_package_mark_ready_v1"("p_projection_id" "uuid", "p_build_worker_job_id" "uuid", "p_lease_token" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_package_mark_ready_v1"("p_projection_id" "uuid", "p_build_worker_job_id" "uuid", "p_lease_token" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") TO "service_role";
