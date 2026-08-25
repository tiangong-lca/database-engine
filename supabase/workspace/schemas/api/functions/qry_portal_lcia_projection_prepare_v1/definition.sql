CREATE OR REPLACE FUNCTION "api"."qry_portal_lcia_projection_prepare_v1"("p_package_id" "uuid", "p_lcia_result_publication_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_job private.worker_jobs%rowtype;
  v_package private.lcia_result_packages%rowtype;
  v_publication private.lcia_result_publications%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_match_count integer;
  v_projection_methods jsonb;
begin
  if v_actor is null then
    return api.lcia_result_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_result_is_manager() then
    return api.lcia_result_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if p_package_id is null or p_lcia_result_publication_id is null then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Package and LCIA result publication identities are required'
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  select publication.* into v_publication
  from private.lcia_result_publications as publication
  where publication.id = p_lcia_result_publication_id;
  if v_package.id is null
     or v_publication.id is null
     or v_publication.package_id <> v_package.id then
    return api.lcia_result_error(
      'projection_package_not_found', 404,
      'Matching LCIA package and publication were not found'
    );
  end if;
  if not v_publication.is_current
     or v_publication.status <> 'current'
     or v_publication.publication_series_key <> 'global'
     or v_publication.publication_channel <> 'public'
     or v_publication.visibility_scope <> 'public'
     or v_publication.published_at is null then
    return api.lcia_result_error(
      'publication_not_current', 409,
      'Only the exact current public LCIA result publication can prepare'
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_package.build_worker_job_id;
  if v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_package.status <> 'preview_ready'
     or coalesce(v_package.package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Package is not a ready Portal LCIA V3 package'
    );
  end if;

  select count(*)
  into v_match_count
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = v_job.id
    and projection.status = 'prepared'
    and projection.input_manifest_hash = v_package.input_manifest_hash
    and projection.closure_certificate_hash = v_package.closure_certificate_hash
    and projection.snapshot_hash = v_package.closure_snapshot_hash
    and projection.result_artifact_sha256
          = v_package.result_artifact_ref ->> 'artifactSha256'
    and projection.query_artifact_sha256
          = v_package.query_artifact_ref ->> 'artifactSha256'
    and projection.bundle_content_hash
          = v_package.artifact_manifest ->> 'bundleContentHash'
    and projection.bundle_manifest_sha256
          = v_package.artifact_manifest ->> 'bundleManifestSha256'
    and projection.lcia_chunk_set_sha256
          = v_package.artifact_manifest ->> 'lciaChunkSetSha256'
    and projection.id::text
          = v_package.artifact_manifest ->> 'portalProjectionId'
    and projection.content_hash
          = v_package.artifact_manifest ->> 'portalProjectionContentHash'
    and projection.process_count = v_package.included_input_count
    and projection.impact_count = jsonb_array_length(
      v_package.available_impact_categories
    );

  if v_match_count = 0 then
    return api.lcia_result_error(
      'projection_not_prepared', 409,
      'No prepared projection exactly matches the package evidence'
    );
  end if;
  if v_match_count > 1 then
    return api.lcia_result_error(
      'projection_conflict', 409,
      'More than one prepared projection matches the package evidence'
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.build_worker_job_id = v_job.id
    and projection.status = 'prepared'
    and projection.input_manifest_hash = v_package.input_manifest_hash
    and projection.closure_certificate_hash = v_package.closure_certificate_hash
    and projection.snapshot_hash = v_package.closure_snapshot_hash
    and projection.result_artifact_sha256
          = v_package.result_artifact_ref ->> 'artifactSha256'
    and projection.query_artifact_sha256
          = v_package.query_artifact_ref ->> 'artifactSha256'
    and projection.bundle_content_hash
          = v_package.artifact_manifest ->> 'bundleContentHash'
    and projection.bundle_manifest_sha256
          = v_package.artifact_manifest ->> 'bundleManifestSha256'
    and projection.lcia_chunk_set_sha256
          = v_package.artifact_manifest ->> 'lciaChunkSetSha256'
    and projection.id::text
          = v_package.artifact_manifest ->> 'portalProjectionId'
    and projection.content_hash
          = v_package.artifact_manifest ->> 'portalProjectionContentHash'
    and projection.process_count = v_package.included_input_count
    and projection.impact_count = jsonb_array_length(
      v_package.available_impact_categories
    );
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_methods
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if v_package.available_impact_categories is distinct from v_projection_methods
     or exists (
       select 1
       from private.portal_lcia_projection_process_axis as process_row
       where process_row.projection_id = v_projection.id
         and (
           v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'id' is distinct from process_row.process_id::text
           or v_package.input_manifest -> 'processes' -> process_row.process_index
             ->> 'version' is distinct from process_row.process_version
         )
     ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection axes do not match the exact package manifest identities'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'buildWorkerJobId', v_projection.build_worker_job_id,
      'packageId', v_package.id,
      'lciaResultPublicationId', v_publication.id,
      'packageVersion', v_package.package_version,
      'packageResultHash', v_package.package_result_hash,
      'status', v_projection.status,
      'projectionContractVersion', v_projection.projection_contract_version,
      'hashContractVersion',
        'portal.lcia-projection.int32be-frame-sha256.v1',
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'publishedAt', private.portal_timestamp_v1(v_publication.published_at)
    )
  );
end
$_$;

ALTER FUNCTION "api"."qry_portal_lcia_projection_prepare_v1"("p_package_id" "uuid", "p_lcia_result_publication_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_portal_lcia_projection_prepare_v1"("p_package_id" "uuid", "p_lcia_result_publication_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_portal_lcia_projection_prepare_v1"("p_package_id" "uuid", "p_lcia_result_publication_id" "uuid") TO "authenticated";
