CREATE OR REPLACE FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_package private.lcia_result_packages%rowtype;
  v_job private.worker_jobs%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_current_manifest jsonb;
  v_package_processes jsonb;
  v_current_processes jsonb;
  v_default_impact text;
  v_current_publication private.lcia_result_publications%rowtype;
  v_current_package private.lcia_result_packages%rowtype;
  v_current_process_set_hash text;
  v_publish_plan_hash text;
  v_projection_id uuid;
  v_fields text[];
begin
  if p_package_id is null then
    return api.lcia_result_error(
      'invalid_projection_request', 400,
      'Portal LCIA package identity is required'
    );
  end if;
  select package.* into v_package
  from private.lcia_result_packages as package
  where package.id = p_package_id;
  if v_package.id is null or v_package.status <> 'preview_ready' then
    return api.lcia_result_error(
      'package_not_ready', 400,
      'Package must be preview_ready before publication'
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_package.build_worker_job_id;
  if v_job.id is null
     or v_job.job_kind <> 'lcia_result.package_build'
     or v_job.payload_schema_version <> 'lcia_result.package_build.request.v3'
     or v_job.payload_json ->> 'portalProjectionContractVersion'
          <> 'portal.lcia-projection.v1'
     or v_job.payload_json ->> 'portalProjectionHashContractVersion'
          <> 'portal.lcia-projection.int32be-frame-sha256.v1'
     or v_package.coverage_mode <> 'global_eligible'
     or v_package.included_input_count <> v_package.eligible_input_count
     or v_package.included_input_count < 1
     or jsonb_typeof(v_package.input_manifest -> 'processes') <> 'array'
     or jsonb_typeof(v_package.available_impact_categories) <> 'array'
     or v_package.artifact_manifest ->> 'portalProjectionId'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     or v_package.artifact_manifest ->> 'portalProjectionContentHash'
          !~ '^[0-9a-f]{64}$'
     or coalesce(v_package.package_result_hash, '') !~ '^[0-9a-f]{64}$' then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Only an exact ready global Portal LCIA V3 package can publish'
    );
  end if;
  begin
    v_projection_id := (
      v_package.artifact_manifest ->> 'portalProjectionId'
    )::uuid;
  exception when invalid_text_representation then
    return api.lcia_result_error(
      'projection_package_not_ready', 409,
      'Portal projection identity is invalid'
    );
  end;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_projection_id;
  if v_projection.id is null
     or v_projection.status <> 'prepared'
     or v_projection.build_worker_job_id <> v_package.build_worker_job_id
     or v_projection.content_hash
          <> v_package.artifact_manifest ->> 'portalProjectionContentHash'
     or v_projection.input_manifest_hash <> v_package.input_manifest_hash
     or v_projection.closure_certificate_hash
          <> v_package.closure_certificate_hash
     or v_projection.snapshot_hash <> v_package.closure_snapshot_hash
     or v_projection.result_artifact_sha256
          <> v_package.result_artifact_ref ->> 'artifactSha256'
     or v_projection.query_artifact_sha256
          <> v_package.query_artifact_ref ->> 'artifactSha256'
     or v_projection.bundle_content_hash
          <> v_package.artifact_manifest ->> 'bundleContentHash'
     or v_projection.bundle_manifest_sha256
          <> v_package.artifact_manifest ->> 'bundleManifestSha256'
     or v_projection.lcia_chunk_set_sha256
          <> v_package.artifact_manifest ->> 'lciaChunkSetSha256'
     or v_projection.process_count <> v_package.included_input_count
     or v_projection.impact_count < 1
     or v_projection.impact_count <>
          jsonb_array_length(v_package.available_impact_categories) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Package and prepared Portal projection evidence do not match'
    );
  end if;
  if exists (
    select 1
    from private.portal_lcia_projection_process_axis as process_row
    where process_row.projection_id = v_projection.id
      and (
        v_package.input_manifest -> 'processes' -> process_row.process_index
          ->> 'id' is distinct from process_row.process_id::text
        or v_package.input_manifest -> 'processes' -> process_row.process_index
          ->> 'version' is distinct from process_row.process_version
      )
  ) or v_package.available_impact_categories is distinct from (
    select coalesce(
      jsonb_agg(to_jsonb(impact.method_id::text) order by impact.impact_index),
      '[]'::jsonb
    )
    from private.portal_lcia_projection_impact_axis as impact
    where impact.projection_id = v_projection.id
  ) then
    return api.lcia_result_error(
      'projection_evidence_mismatch', 409,
      'Projection axes do not match the package identities'
    );
  end if;

  v_current_manifest := api.lcia_result_current_eligible_manifest();
  v_package_processes := v_package.input_manifest -> 'processes';
  v_current_processes := v_current_manifest #> '{inputManifest,processes}';
  if jsonb_typeof(v_current_processes) <> 'array'
     or v_package.eligible_input_count <>
          coalesce((v_current_manifest ->> 'eligibleInputCount')::integer, -1)
     or jsonb_array_length(v_package_processes) <>
          v_package.included_input_count
     or jsonb_array_length(v_current_processes) <>
          v_package.included_input_count
     or exists (
       select 1
       from jsonb_array_elements(v_package_processes) as process(value)
       where private.portal_lcia_json_object_has_keys_v1(
         process.value, array['id', 'version']
       ) is not true
         or process.value ->> 'id'
              !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or process.value ->> 'version' !~ '^\d{2}\.\d{2}\.\d{3}$'
     )
     or (
       select count(distinct (process.value ->> 'id', process.value ->> 'version'))
       from jsonb_array_elements(v_package_processes) as process(value)
     ) <> jsonb_array_length(v_package_processes)
     or exists (
       (
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_package_processes) as process(value)
         except
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_current_processes) as process(value)
       )
       union all
       (
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_current_processes) as process(value)
         except
         select process.value ->> 'id', process.value ->> 'version'
         from jsonb_array_elements(v_package_processes) as process(value)
       )
     ) then
    return api.lcia_result_error(
      'package_stale_eligibility', 409,
      'Current eligible Process identities differ from the certified package'
    );
  end if;

  select array[
    'portal.lcia-v3-current-process-set.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    jsonb_array_length(v_current_processes)::text
  ] || coalesce(
    array_agg(field.value order by process.value ->> 'id',
      process.value ->> 'version', field.position),
    '{}'::text[]
  ) into v_fields
  from jsonb_array_elements(v_current_processes) as process(value)
  cross join lateral (
    values (1, process.value ->> 'id'), (2, process.value ->> 'version')
  ) as field(position, value);
  v_current_process_set_hash :=
    private.portal_lcia_projection_sha256_fields_v1(variadic v_fields);

  v_default_impact := coalesce(
    nullif(btrim(coalesce(p_display_default_impact_category, '')), ''),
    v_package.default_impact_category
  );
  if v_default_impact is null
     or private.portal_lcia_public_text_valid_v1(v_default_impact, 512)
          is not true
     or not exists (
       select 1
       from jsonb_array_elements_text(
         v_package.available_impact_categories
       ) as impact(value)
       where impact.value = v_default_impact
     )
     or v_package.result_artifact_ref = '{}'::jsonb then
    return api.lcia_result_error(
      'default_impact_missing', 400,
      'Default impact category or result evidence is unavailable'
    );
  end if;

  select publication.* into v_current_publication
  from private.lcia_result_publications as publication
  where publication.publication_series_key = 'global'
    and publication.publication_channel = 'public'
    and publication.visibility_scope = 'public'
    and publication.is_current
    and publication.status = 'current';
  if v_current_publication.id is not null then
    select package.* into v_current_package
    from private.lcia_result_packages as package
    where package.id = v_current_publication.package_id;
    if v_current_package.id is null
       or v_current_publication.published_at is null then
      return api.lcia_result_error(
        'publication_precondition_invalid', 409,
        'Current publication evidence is incomplete'
      );
    end if;
  end if;

  v_publish_plan_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-v3-package-publish-plan.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_package.id::text,
    v_package.package_version,
    v_package.package_result_hash,
    v_package.input_manifest_hash,
    v_package.closure_certificate_hash,
    v_package.closure_snapshot_hash,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    v_projection.id::text,
    v_projection.content_hash,
    v_projection.process_axis_hash,
    v_projection.impact_axis_hash,
    v_projection.value_grid_hash,
    v_projection.relation_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_default_impact,
    v_current_process_set_hash,
    v_current_publication.id::text,
    v_current_publication.package_id::text,
    v_current_package.package_version,
    case when v_current_publication.published_at is null then null
      else private.portal_timestamp_v1(v_current_publication.published_at) end
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'publishPlanHash', v_publish_plan_hash,
      'package', jsonb_build_object(
        'id', v_package.id,
        'version', v_package.package_version,
        'resultHash', v_package.package_result_hash,
        'inputManifestHash', v_package.input_manifest_hash,
        'closureCertificateHash', v_package.closure_certificate_hash,
        'snapshotHash', v_package.closure_snapshot_hash,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'valueCount', v_projection.expected_value_count
      ),
      'projection', jsonb_build_object(
        'id', v_projection.id,
        'contentHash', v_projection.content_hash,
        'processAxisHash', v_projection.process_axis_hash,
        'impactAxisHash', v_projection.impact_axis_hash,
        'valueGridHash', v_projection.value_grid_hash,
        'relationHash', v_projection.relation_hash
      ),
      'artifacts', jsonb_build_object(
        'bundleContentHash', v_projection.bundle_content_hash,
        'bundleManifestSha256', v_projection.bundle_manifest_sha256,
        'lciaChunkSetSha256', v_projection.lcia_chunk_set_sha256,
        'resultArtifactSha256', v_projection.result_artifact_sha256,
        'queryArtifactSha256', v_projection.query_artifact_sha256
      ),
      'displayDefaultImpactCategory', v_default_impact,
      'currentProcessSetHash', v_current_process_set_hash,
      'currentPublication', case
        when v_current_publication.id is null then 'null'::jsonb
        else jsonb_build_object(
          'publicationId', v_current_publication.id,
          'packageId', v_current_publication.package_id,
          'packageVersion', v_current_package.package_version,
          'publishedAt', private.portal_timestamp_v1(
            v_current_publication.published_at
          )
        )
      end
    )
  );
end
$_$;

ALTER FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_v3_package_publish_prepare_v1"("p_package_id" "uuid", "p_display_default_impact_category" "text") FROM PUBLIC;
