CREATE OR REPLACE FUNCTION "public"."cmd_lcia_result_package_mark_ready_without_closure_recheck"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid" DEFAULT NULL::"uuid", "p_result_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_query_artifact_ref" "jsonb" DEFAULT '{}'::"jsonb", "p_artifact_manifest" "jsonb" DEFAULT '{}'::"jsonb", "p_available_impact_categories" "jsonb" DEFAULT '[]'::"jsonb", "p_default_impact_category" "text" DEFAULT NULL::"text", "p_package_result_hash" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_job public.worker_jobs%rowtype;
  v_result public.lca_results%rowtype;
  v_latest public.lca_latest_all_unit_results%rowtype;
  v_package public.lcia_result_packages%rowtype;
  v_result_artifact_ref jsonb;
  v_query_artifact_ref jsonb;
  v_default_impact text;
begin
  if not public.lcia_result_is_service_request() then
    return public.lcia_result_error('service_role_required', 403, 'Service role is required to mark LCIA result packages ready');
  end if;

  if jsonb_typeof(coalesce(p_result_artifact_ref, '{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_query_artifact_ref, '{}'::jsonb)) <> 'object'
     or jsonb_typeof(coalesce(p_artifact_manifest, '{}'::jsonb)) <> 'object' then
    return public.lcia_result_error('invalid_artifact_payload', 400, 'artifact payloads must be JSON objects');
  end if;

  if jsonb_typeof(coalesce(p_available_impact_categories, '[]'::jsonb)) <> 'array' then
    return public.lcia_result_error('invalid_available_impact_categories', 400, 'available impact categories must be a JSON array');
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_build_worker_job_id
  for update;

  if v_job.id is null or v_job.job_kind <> 'lcia_result.package_build' then
    return public.lcia_result_error('build_worker_job_not_found', 404, 'LCIA result package build worker job not found');
  end if;

  select *
    into v_result
  from public.lca_results
  where id = p_result_id;

  if v_result.id is null then
    return public.lcia_result_error('result_not_found', 404, 'LCIA result artifact row not found');
  end if;

  if p_latest_all_unit_result_id is not null then
    select *
      into v_latest
    from public.lca_latest_all_unit_results
    where id = p_latest_all_unit_result_id;

    if v_latest.id is null then
      return public.lcia_result_error('latest_all_unit_result_not_found', 404, 'Latest all-unit LCIA result row not found');
    end if;
  end if;

  v_result_artifact_ref := case
    when coalesce(p_result_artifact_ref, '{}'::jsonb) <> '{}'::jsonb then p_result_artifact_ref
    else jsonb_strip_nulls(
      jsonb_build_object(
        'artifactUrl', v_result.artifact_url,
        'artifactSha256', v_result.artifact_sha256,
        'artifactByteSize', v_result.artifact_byte_size,
        'artifactFormat', v_result.artifact_format
      )
    )
  end;

  v_query_artifact_ref := case
    when coalesce(p_query_artifact_ref, '{}'::jsonb) <> '{}'::jsonb then p_query_artifact_ref
    when v_latest.id is not null then jsonb_strip_nulls(
      jsonb_build_object(
        'artifactUrl', v_latest.query_artifact_url,
        'artifactSha256', v_latest.query_artifact_sha256,
        'artifactByteSize', v_latest.query_artifact_byte_size,
        'artifactFormat', v_latest.query_artifact_format
      )
    )
    else '{}'::jsonb
  end;

  if v_result_artifact_ref = '{}'::jsonb then
    return public.lcia_result_error('result_artifact_missing', 400, 'LCIA result package requires a persisted result artifact reference');
  end if;

  v_default_impact := coalesce(
    nullif(trim(coalesce(p_default_impact_category, '')), ''),
    nullif(trim(coalesce(v_job.payload_json->>'default_impact_category', '')), '')
  );

  insert into public.lcia_result_packages (
    build_id,
    build_worker_job_id,
    package_version,
    coverage_mode,
    input_status_filter,
    eligibility_definition,
    eligibility_resolved_at,
    eligible_input_count,
    included_input_count,
    input_manifest_hash,
    input_manifest,
    snapshot_id,
    result_id,
    latest_all_unit_result_id,
    result_artifact_ref,
    query_artifact_ref,
    artifact_manifest,
    package_result_hash,
    lcia_method_set,
    available_impact_categories,
    postprocess_manifest,
    default_impact_category,
    status,
    created_by
  )
  values (
    (v_job.payload_json->>'build_id')::uuid,
    v_job.id,
    nullif(trim(coalesce(p_package_version, '')), ''),
    v_job.payload_json->>'coverage_mode',
    coalesce(v_job.payload_json->'input_status_filter', '{"state_code":{"between":[100,199]}}'::jsonb),
    coalesce(v_job.payload_json->'eligibility_definition', '{}'::jsonb),
    coalesce((v_job.payload_json->>'eligibility_resolved_at')::timestamptz, now()),
    coalesce((v_job.payload_json->>'eligible_input_count')::integer, 0),
    coalesce((v_job.payload_json->>'included_input_count')::integer, 0),
    nullif(v_job.payload_json->>'input_manifest_hash', ''),
    coalesce(v_job.payload_json->'input_manifest', '{}'::jsonb),
    p_snapshot_id,
    v_result.id,
    v_latest.id,
    v_result_artifact_ref,
    v_query_artifact_ref,
    coalesce(p_artifact_manifest, '{}'::jsonb),
    nullif(trim(coalesce(p_package_result_hash, '')), ''),
    coalesce(v_job.payload_json->'lcia_method_set', '[]'::jsonb),
    coalesce(p_available_impact_categories, '[]'::jsonb),
    coalesce(v_job.payload_json->'postprocess_manifest', '{"postprocess_mode":"skipped"}'::jsonb),
    v_default_impact,
    'preview_ready',
    v_job.requested_by
  )
  returning *
    into v_package;

  if v_job.status in ('queued', 'running', 'waiting', 'stale') then
    update public.worker_jobs
      set result_ref = coalesce(result_ref, '{}'::jsonb) || jsonb_build_object(
            'packageId', v_package.id,
            'packageVersion', v_package.package_version
          ),
          updated_at = now()
    where id = v_job.id;
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'buildWorkerJobId', v_package.build_worker_job_id,
      'includedInputCount', v_package.included_input_count
    )
  );
exception
  when unique_violation then
    return public.lcia_result_error('package_conflict', 409, 'LCIA result package already exists for this build or package version');
  when foreign_key_violation then
    return public.lcia_result_error('package_reference_invalid', 400, 'LCIA result package references invalid worker, snapshot, or result rows');
  when invalid_text_representation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload contains invalid ids or numeric values');
  when not_null_violation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload is missing required fields');
  when check_violation then
    return public.lcia_result_error('invalid_package_payload', 400, 'LCIA result package payload violates schema constraints');
end;
$$;

ALTER FUNCTION "public"."cmd_lcia_result_package_mark_ready_without_closure_recheck"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_lcia_result_package_mark_ready_without_closure_recheck"("p_build_worker_job_id" "uuid", "p_package_version" "text", "p_snapshot_id" "uuid", "p_result_id" "uuid", "p_latest_all_unit_result_id" "uuid", "p_result_artifact_ref" "jsonb", "p_query_artifact_ref" "jsonb", "p_artifact_manifest" "jsonb", "p_available_impact_categories" "jsonb", "p_default_impact_category" "text", "p_package_result_hash" "text", "p_audit" "jsonb") FROM PUBLIC;
