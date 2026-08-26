CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_finalize_unchecked_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_publication private.lcia_result_publications%rowtype;
  v_package private.lcia_result_packages%rowtype;
  v_job private.worker_jobs%rowtype;
  v_existing private.portal_lcia_projection_publications%rowtype;
  v_binding private.portal_lcia_projection_publications%rowtype;
  v_now timestamptz := clock_timestamp();
  v_evidence_hash text;
  v_recomputed jsonb;
  v_projection_impacts jsonb;
  v_idempotency_key text := btrim(coalesce(p_idempotency_key, ''));
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
  if p_projection_id is null
     or p_lcia_result_publication_id is null
     or coalesce(p_package_result_hash, '') !~ '^[0-9a-f]{64}$'
     or coalesce(p_projection_content_hash, '') !~ '^[0-9a-f]{64}$'
     or private.portal_lcia_public_text_valid_v1(p_package_version, 256)
          is not true
     or length(v_idempotency_key) not between 1 and 256
     or private.portal_lcia_safe_audit_v1(p_audit) is not true then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Invalid Portal LCIA projection finalization request'
    );
  end if;

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for share;
  if v_projection.id is null then
    return api.lcia_result_error(
      'projection_not_found', 404, 'Projection was not found'
    );
  end if;
  if v_projection.status <> 'prepared'
     or v_projection.content_hash <> p_projection_content_hash then
    return api.lcia_result_error(
      'projection_not_prepared', 409,
      'Projection is not prepared with the requested content hash'
    );
  end if;

  select publication.* into v_publication
  from private.lcia_result_publications as publication
  where publication.id = p_lcia_result_publication_id
  for update;
  if v_publication.id is null then
    return api.lcia_result_error(
      'publication_not_found', 404, 'LCIA result publication was not found'
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
      'Only the exact current public LCIA result publication can finalize'
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = v_publication.package_id
  for share;
  if v_package.id is null
     or v_package.status <> 'preview_ready'
     or v_package.package_version <> p_package_version
     or v_package.package_result_hash <> p_package_result_hash
     or v_package.build_worker_job_id <> v_projection.build_worker_job_id
     or v_package.input_manifest_hash <> v_projection.input_manifest_hash
     or v_package.closure_certificate_hash
          <> v_projection.closure_certificate_hash
     or v_package.closure_snapshot_hash <> v_projection.snapshot_hash
     or v_package.result_artifact_ref ->> 'artifactSha256'
          <> v_projection.result_artifact_sha256
     or v_package.query_artifact_ref ->> 'artifactSha256'
          <> v_projection.query_artifact_sha256
     or v_package.artifact_manifest ->> 'bundleContentHash'
          <> v_projection.bundle_content_hash
     or v_package.artifact_manifest ->> 'bundleManifestSha256'
          <> v_projection.bundle_manifest_sha256
     or v_package.artifact_manifest ->> 'lciaChunkSetSha256'
          <> v_projection.lcia_chunk_set_sha256
     or v_package.artifact_manifest ->> 'portalProjectionId'
          <> v_projection.id::text
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          <> v_projection.content_hash
     or v_package.included_input_count <> v_projection.process_count
     or jsonb_array_length(v_package.available_impact_categories)
          <> v_projection.impact_count then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection and package evidence do not exactly match'
    );
  end if;
  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_impacts
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  if v_package.available_impact_categories is distinct from v_projection_impacts
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

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.payload_json ->> 'input_manifest_hash'
          <> v_projection.input_manifest_hash
     or v_job.payload_json ->> 'closure_certificate_hash'
          <> v_projection.closure_certificate_hash
     or v_job.payload_json ->> 'snapshot_hash'
          <> v_projection.snapshot_hash
     or v_job.payload_json ->> 'closure_bundle_hash'
          <> v_projection.closure_bundle_hash
     or v_job.payload_json ->> 'snapshot_index_sha256'
          <> v_projection.snapshot_index_sha256
     or v_job.payload_json ->> 'snapshot_build_contract_hash'
          <> v_projection.snapshot_build_contract_hash then
    return api.lcia_result_error(
      'projection_job_contract_invalid', 409,
      'Projection source job is not the exact Portal LCIA V3 contract'
    );
  end if;
  if jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
          <> v_projection.impact_count
     or exists (
       select 1
       from private.portal_lcia_projection_impact_axis as impact_row
       where impact_row.projection_id = v_projection.id
         and (
           v_job.payload_json -> 'lcia_method_set' -> impact_row.impact_index
             ->> 'id' is distinct from impact_row.method_id::text
           or v_job.payload_json -> 'lcia_method_set' -> impact_row.impact_index
             ->> 'version' is distinct from impact_row.method_version
         )
     ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection Method axis does not match the certified V3 request'
    );
  end if;

  if (select count(*) from private.portal_lcia_projection_process_axis
      where projection_id = v_projection.id) <> v_projection.process_count
     or (select count(*) from private.portal_lcia_projection_impact_axis
         where projection_id = v_projection.id) <> v_projection.impact_count
     or (select count(*) from private.portal_lcia_projection_values
         where projection_id = v_projection.id)
          <> v_projection.expected_value_count then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection record counts changed after preparation'
    );
  end if;
  v_recomputed := private.portal_lcia_projection_recompute_evidence_v1(
    v_projection.id
  );
  if coalesce((v_recomputed ->> 'ok')::boolean, false) is not true
     or v_recomputed -> 'data' ->> 'processAxisHash'
          is distinct from v_projection.process_axis_hash
     or v_recomputed -> 'data' ->> 'impactAxisHash'
          is distinct from v_projection.impact_axis_hash
     or v_recomputed -> 'data' ->> 'valueGridHash'
          is distinct from v_projection.value_grid_hash
     or v_recomputed -> 'data' ->> 'relationHash'
          is distinct from v_projection.relation_hash
     or v_recomputed -> 'data' ->> 'contentHash'
          is distinct from v_projection.content_hash then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection hashes no longer match the typed persisted rows'
    );
  end if;

  select binding.* into v_existing
  from private.portal_lcia_projection_publications as binding
  where binding.lcia_result_publication_id = v_publication.id
  for update;
  if v_existing.id is not null then
    if v_existing.status = 'finalized'
       and v_existing.revoked_at is null
       and v_existing.projection_id = v_projection.id
       and v_existing.package_id = v_package.id
       and v_existing.package_version = p_package_version
       and v_existing.package_result_hash = p_package_result_hash
       and v_existing.projection_content_hash = p_projection_content_hash
       and v_existing.idempotency_key = v_idempotency_key then
      return jsonb_build_object(
        'ok', true,
        'reused', true,
        'data', jsonb_build_object(
          'projectionPublicationId', v_existing.id,
          'projectionId', v_existing.projection_id,
          'lciaResultPublicationId', v_existing.lcia_result_publication_id,
          'packageId', v_existing.package_id,
          'status', v_existing.status,
          'contentHash', v_existing.projection_content_hash,
          'evidenceHash', v_existing.evidence_hash,
          'finalizedAt', private.portal_timestamp_v1(v_existing.finalized_at)
        )
      );
    end if;
    return api.lcia_result_error(
      'projection_conflict', 409,
      'LCIA result publication is bound to different projection content'
    );
  end if;

  v_evidence_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.publication-evidence.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.id::text,
    v_projection.content_hash,
    v_publication.id::text,
    v_package.id::text,
    v_package.package_version,
    v_package.package_result_hash,
    private.portal_timestamp_v1(v_publication.published_at),
    v_projection.input_manifest_hash,
    v_projection.closure_certificate_hash,
    v_projection.snapshot_hash,
    v_projection.closure_bundle_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text
  );

  insert into private.portal_lcia_projection_publications (
    projection_id,
    lcia_result_publication_id,
    package_id,
    package_version,
    package_result_hash,
    projection_content_hash,
    evidence_hash,
    source_published_at,
    idempotency_key,
    status,
    finalized_by,
    finalized_at
  ) values (
    v_projection.id,
    v_publication.id,
    v_package.id,
    v_package.package_version,
    v_package.package_result_hash,
    v_projection.content_hash,
    v_evidence_hash,
    v_publication.published_at,
    v_idempotency_key,
    'finalized',
    v_actor,
    v_now
  ) returning * into v_binding;

  insert into private.command_audit_log (
    command, actor_user_id, target_table, target_id, target_version, payload
  ) values (
    'cmd_portal_lcia_projection_finalize_publication_v1',
    v_actor,
    'portal_lcia_projection_publications',
    v_binding.id,
    v_binding.package_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'projectionId', v_projection.id,
      'lciaResultPublicationId', v_publication.id,
      'packageId', v_package.id,
      'contentHash', v_projection.content_hash,
      'evidenceHash', v_evidence_hash
    )
  );

  return jsonb_build_object(
    'ok', true,
    'reused', false,
    'data', jsonb_build_object(
      'projectionPublicationId', v_binding.id,
      'projectionId', v_binding.projection_id,
      'lciaResultPublicationId', v_binding.lcia_result_publication_id,
      'packageId', v_binding.package_id,
      'status', v_binding.status,
      'contentHash', v_binding.projection_content_hash,
      'evidenceHash', v_binding.evidence_hash,
      'finalizedAt', private.portal_timestamp_v1(v_binding.finalized_at)
    )
  );
exception
  when unique_violation then
    return api.lcia_result_error(
      'projection_conflict', 409,
      'A conflicting projection publication binding already exists'
    );
end
$_$;

ALTER FUNCTION "private"."portal_lcia_projection_finalize_unchecked_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_finalize_unchecked_v1"("p_projection_id" "uuid", "p_lcia_result_publication_id" "uuid", "p_package_version" "text", "p_package_result_hash" "text", "p_projection_content_hash" "text", "p_idempotency_key" "text", "p_audit" "jsonb") FROM PUBLIC;
