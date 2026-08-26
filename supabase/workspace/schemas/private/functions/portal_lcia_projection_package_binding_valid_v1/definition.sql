CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_package_binding_valid_v1"("p_package_id" "uuid", "p_build_worker_job_id" "uuid", "p_projection_id" "uuid") RETURNS boolean
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $_$
declare
  v_package private.lcia_result_packages%rowtype;
  v_job private.worker_jobs%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_result private.lca_results%rowtype;
  v_latest private.lca_latest_all_unit_results%rowtype;
  v_projection_impacts jsonb;
  v_expected_build_id uuid;
  v_expected_closure_check_id uuid;
  v_expected_eligibility_resolved_at timestamptz;
  v_expected_eligible_input_count integer;
  v_expected_included_input_count integer;
  v_expected_default_impact text;
  v_expected_result_artifact_ref jsonb;
  v_expected_query_artifact_ref jsonb;
begin
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id
    and package.build_worker_job_id = p_build_worker_job_id;
  if v_package.id is null then
    return false;
  end if;

  select job.* into v_job
  from private.worker_jobs as job
  where job.id = p_build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <>
          'lcia_result.package_build.request.v3'
     or v_job.subject_type <> 'lcia_result_build' then
    return false;
  end if;

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
    and projection.build_worker_job_id = p_build_worker_job_id;
  if v_projection.id is null or v_projection.status <> 'prepared' then
    return false;
  end if;

  begin
    v_expected_build_id := nullif(
      v_job.payload_json ->> 'build_id', ''
    )::uuid;
    v_expected_closure_check_id := nullif(
      v_job.payload_json ->> 'closure_check_id', ''
    )::uuid;
    v_expected_eligibility_resolved_at := nullif(
      v_job.payload_json ->> 'eligibility_resolved_at', ''
    )::timestamptz;
    v_expected_eligible_input_count := coalesce(
      (v_job.payload_json ->> 'eligible_input_count')::integer, 0
    );
    v_expected_included_input_count := coalesce(
      (v_job.payload_json ->> 'included_input_count')::integer, 0
    );
  exception
    when invalid_text_representation
      or invalid_datetime_format
      or datetime_field_overflow
      or numeric_value_out_of_range then
      return false;
  end;

  select coalesce(
    jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
    '[]'::jsonb
  ) into v_projection_impacts
  from private.portal_lcia_projection_impact_axis as impact
  where impact.projection_id = v_projection.id;
  v_expected_default_impact := coalesce(
    nullif(btrim(coalesce(
      v_job.payload_json ->> 'default_impact_category', ''
    )), ''),
    v_projection_impacts ->> 0
  );

  select result.* into v_result
  from private.lca_results as result
  where result.id = v_package.result_id;
  if v_result.id is null
     or v_result.job_id is distinct from v_package.build_id
     or v_result.worker_job_id is distinct from v_job.id
     or v_result.snapshot_id is distinct from v_package.snapshot_id then
    return false;
  end if;
  v_expected_result_artifact_ref := jsonb_build_object(
    'artifactUrl', v_result.artifact_url,
    'artifactSha256', v_result.artifact_sha256,
    'artifactByteSize', v_result.artifact_byte_size,
    'artifactFormat', v_result.artifact_format
  );

  if v_package.latest_all_unit_result_id is not null then
    select latest.* into v_latest
    from private.lca_latest_all_unit_results as latest
    where latest.id = v_package.latest_all_unit_result_id;
    if v_latest.id is null
       or v_latest.job_id is distinct from v_package.build_id
       or v_latest.worker_job_id is distinct from v_job.id
       or v_latest.snapshot_id is distinct from v_package.snapshot_id
       or v_latest.result_id is distinct from v_package.result_id
       or v_latest.status <> 'ready' then
      return false;
    end if;
    v_expected_query_artifact_ref := jsonb_build_object(
      'artifactUrl', v_latest.query_artifact_url,
      'artifactSha256', v_latest.query_artifact_sha256,
      'artifactByteSize', v_latest.query_artifact_byte_size,
      'artifactFormat', v_latest.query_artifact_format
    );
  end if;

  return v_package.build_id is not distinct from v_expected_build_id
    and v_package.build_id is not distinct from v_job.subject_id
    and v_package.build_worker_job_id is not distinct from v_job.id
    and private.portal_lcia_public_text_valid_v1(
      v_package.package_version, 256
    )
    and v_package.coverage_mode is not distinct from
      (v_job.payload_json ->> 'coverage_mode')
    and v_package.input_status_filter is not distinct from coalesce(
      v_job.payload_json -> 'input_status_filter',
      '{"state_code":{"between":[100,199]}}'::jsonb
    )
    and v_package.eligibility_definition is not distinct from coalesce(
      v_job.payload_json -> 'eligibility_definition', '{}'::jsonb
    )
    and v_package.eligibility_resolved_at is not distinct from
      v_expected_eligibility_resolved_at
    and v_package.eligible_input_count is not distinct from
      v_expected_eligible_input_count
    and v_package.included_input_count is not distinct from
      v_expected_included_input_count
    and v_package.input_manifest_hash is not distinct from nullif(
      v_job.payload_json ->> 'input_manifest_hash', ''
    )
    and v_package.input_manifest is not distinct from coalesce(
      v_job.payload_json -> 'input_manifest', '{}'::jsonb
    )
    and v_package.snapshot_id::text is not distinct from
      (v_job.payload_json ->> 'snapshot_id')
    and private.portal_lcia_json_object_has_keys_v1(
      v_package.result_artifact_ref,
      array[
        'artifactUrl', 'artifactSha256', 'artifactByteSize', 'artifactFormat'
      ]
    )
    and v_package.result_artifact_ref is not distinct from
      v_expected_result_artifact_ref
    and private.portal_lcia_json_object_has_keys_v1(
      v_package.query_artifact_ref,
      array[
        'artifactUrl', 'artifactSha256', 'artifactByteSize', 'artifactFormat'
      ]
    )
    and (
      v_package.latest_all_unit_result_id is null
      or v_package.query_artifact_ref is not distinct from
           v_expected_query_artifact_ref
    )
    and jsonb_typeof(v_package.artifact_manifest) = 'object'
    and v_package.package_result_hash ~ '^[0-9a-f]{64}$'
    and v_package.package_result_hash is not distinct from
      v_result.artifact_sha256
    and v_package.lcia_method_set is not distinct from coalesce(
      v_job.payload_json -> 'lcia_method_set', '[]'::jsonb
    )
    and v_package.available_impact_categories is not distinct from
      v_projection_impacts
    and v_package.postprocess_manifest is not distinct from coalesce(
      v_job.payload_json -> 'postprocess_manifest',
      '{"postprocess_mode":"skipped"}'::jsonb
    )
    and v_package.default_impact_category is not distinct from
      v_expected_default_impact
    and v_projection_impacts @>
      jsonb_build_array(v_package.default_impact_category)
    and v_package.closure_check_id is not distinct from
      v_expected_closure_check_id
    and v_package.closure_certificate_hash is not distinct from
      (v_job.payload_json ->> 'closure_certificate_hash')
    and v_package.closure_certificate_hash is not distinct from
      v_projection.closure_certificate_hash
    and v_package.closure_snapshot_hash is not distinct from
      (v_job.payload_json ->> 'snapshot_hash')
    and v_package.closure_snapshot_hash is not distinct from
      v_projection.snapshot_hash
    and v_package.status = 'preview_ready'
    and v_package.created_by is not distinct from v_job.requested_by
    and v_projection.input_manifest_hash is not distinct from
      v_package.input_manifest_hash
    and v_projection.input_manifest_hash is not distinct from
      (v_job.payload_json ->> 'input_manifest_hash')
    and v_projection.closure_bundle_hash is not distinct from
      (v_job.payload_json ->> 'closure_bundle_hash')
    and v_projection.snapshot_index_sha256 is not distinct from
      (v_job.payload_json ->> 'snapshot_index_sha256')
    and v_projection.snapshot_build_contract_hash is not distinct from
      (v_job.payload_json ->> 'snapshot_build_contract_hash')
    and v_projection.result_artifact_sha256 is not distinct from
      (v_package.result_artifact_ref ->> 'artifactSha256')
    and v_projection.result_artifact_sha256 is not distinct from
      v_package.package_result_hash
    and v_projection.query_artifact_sha256 is not distinct from
      (v_package.query_artifact_ref ->> 'artifactSha256')
    and v_projection.bundle_content_hash is not distinct from
      (v_package.artifact_manifest ->> 'bundleContentHash')
    and v_projection.bundle_manifest_sha256 is not distinct from
      (v_package.artifact_manifest ->> 'bundleManifestSha256')
    and v_projection.lcia_chunk_set_sha256 is not distinct from
      (v_package.artifact_manifest ->> 'lciaChunkSetSha256')
    and v_projection.id::text is not distinct from
      (v_package.artifact_manifest ->> 'portalProjectionId')
    and v_projection.content_hash is not distinct from
      (v_package.artifact_manifest ->> 'portalProjectionContentHash')
    and (
      not (v_package.artifact_manifest ? 'inputManifestHash')
      or v_package.artifact_manifest ->> 'inputManifestHash'
           is not distinct from v_package.input_manifest_hash
    );
end
$_$;

ALTER FUNCTION "private"."portal_lcia_projection_package_binding_valid_v1"("p_package_id" "uuid", "p_build_worker_job_id" "uuid", "p_projection_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_package_binding_valid_v1"("p_package_id" "uuid", "p_build_worker_job_id" "uuid", "p_projection_id" "uuid") FROM PUBLIC;
