-- Recover a committed Portal LCIA V3 package when the Worker loses the first
-- response.  This is deliberately an additive replacement of the V3 wrapper:
-- the V1/V2 helper keeps its original definition, owner, configuration, and
-- ACL, while an exact post-conflict readback proves that the committed package
-- is byte-for-byte bound to the same immutable request evidence.

create function private.portal_lcia_projection_package_binding_valid_v1(
  p_package_id uuid,
  p_build_worker_job_id uuid,
  p_projection_id uuid
)
returns boolean
language plpgsql
stable
set search_path = ''
as $function$
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
$function$;

create function private.svc_portal_lcia_projection_package_ready_readback_v1(
  p_build_worker_job_id uuid,
  p_current_lease_token uuid
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_package private.lcia_result_packages%rowtype;
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_projection_id uuid;
  v_projection_content_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  if private.portal_lcia_projection_v3_job_binding_valid_v1(
       p_build_worker_job_id, p_current_lease_token
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;

  select package.* into v_package
  from private.lcia_result_packages as package
  where package.build_worker_job_id = p_build_worker_job_id;
  if v_package.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_not_found', 'status', 404
    );
  end if;

  begin
    v_projection_id := nullif(
      v_package.artifact_manifest ->> 'portalProjectionId', ''
    )::uuid;
  exception when invalid_text_representation then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end;
  v_projection_content_hash := nullif(
    v_package.artifact_manifest ->> 'portalProjectionContentHash', ''
  );

  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = v_projection_id
    and projection.build_worker_job_id = p_build_worker_job_id
    and projection.status = 'prepared'
    and projection.content_hash = v_projection_content_hash;
  if v_projection.id is null
     or private.portal_lcia_projection_package_binding_valid_v1(
       v_package.id, p_build_worker_job_id, v_projection.id
     ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'reused', true,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'buildWorkerJobId', v_package.build_worker_job_id,
      'includedInputCount', v_package.included_input_count,
      'projection', jsonb_build_object(
        'projectionId', v_projection.id,
        'contentHash', v_projection.content_hash,
        'hashContractVersion',
          'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    )
  );
end
$function$;

revoke all on function private.portal_lcia_projection_package_binding_valid_v1(
  uuid, uuid, uuid
) from public, anon, authenticated, service_role;
revoke all on function private.svc_portal_lcia_projection_package_ready_readback_v1(
  uuid, uuid
) from public, anon, authenticated, service_role;
grant execute on function private.svc_portal_lcia_projection_package_ready_readback_v1(
  uuid, uuid
) to service_role;

create or replace function private.svc_portal_lcia_projection_package_mark_ready_v1(
  p_projection_id uuid,
  p_build_worker_job_id uuid,
  p_lease_token uuid,
  p_package_version text,
  p_snapshot_id uuid,
  p_result_id uuid,
  p_latest_all_unit_result_id uuid default null::uuid,
  p_result_artifact_ref jsonb default '{}'::jsonb,
  p_query_artifact_ref jsonb default '{}'::jsonb,
  p_artifact_manifest jsonb default '{}'::jsonb,
  p_available_impact_categories jsonb default '[]'::jsonb,
  p_default_impact_category text default null::text,
  p_package_result_hash text default null::text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_result jsonb;
  v_package private.lcia_result_packages%rowtype;
  v_package_id uuid;
  v_projection_impacts jsonb;
  v_reused boolean := false;
  v_expected_build_id uuid;
  v_expected_closure_check_id uuid;
  v_expected_eligibility_resolved_at timestamptz;
  v_expected_eligible_input_count integer;
  v_expected_included_input_count integer;
  v_expected_default_impact text;
  v_restart_default_impact text;
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

  if coalesce((v_result ->> 'ok')::boolean, false) is true then
    begin
      v_package_id := nullif(v_result -> 'data' ->> 'packageId', '')::uuid;
    exception when invalid_text_representation then
      return jsonb_build_object(
        'ok', false, 'code', 'projection_package_binding_invalid',
        'status', 409
      );
    end;
    select package.* into v_package
    from private.lcia_result_packages as package
    where package.id = v_package_id
    for share;
  elsif v_result ->> 'ok' = 'false'
        and v_result ->> 'code' = 'package_conflict'
        and v_result ->> 'status' = '409' then
    -- A unique violation is ambiguous to the Worker: a prior identical call
    -- may have committed before its response was lost, or an unrelated package
    -- may own one of the unique keys.  Only the exact build/job-version pair is
    -- eligible for reconciliation; all other conflicts remain unchanged.
    v_reused := true;
    select package.* into v_package
    from private.lcia_result_packages as package
    where package.build_worker_job_id = v_job.id
      and package.package_version = p_package_version
    for share;
    if v_package.id is null then
      return v_result;
    end if;
  else
    return v_result;
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
      if v_reused then
        return v_result;
      end if;
      return jsonb_build_object(
        'ok', false, 'code', 'projection_package_binding_invalid',
        'status', 409
      );
  end;
  v_expected_default_impact := coalesce(
    nullif(btrim(coalesce(p_default_impact_category, '')), ''),
    nullif(btrim(coalesce(
      v_job.payload_json ->> 'default_impact_category', ''
    )), '')
  );
  v_restart_default_impact := coalesce(
    nullif(btrim(coalesce(
      v_job.payload_json ->> 'default_impact_category', ''
    )), ''),
    v_projection_impacts ->> 0
  );

  if private.portal_lcia_projection_package_binding_valid_v1(
       v_package.id, v_job.id, v_projection.id
     ) is not true
     or v_expected_default_impact is distinct from v_restart_default_impact
     or v_package.id is null
     or v_package.build_id is distinct from v_expected_build_id
     or v_package.build_id is distinct from v_job.subject_id
     or v_job.subject_type <> 'lcia_result_build'
     or v_package.build_worker_job_id is distinct from v_job.id
     or v_package.package_version is distinct from p_package_version
     or v_package.coverage_mode is distinct from
          (v_job.payload_json ->> 'coverage_mode')
     or v_package.input_status_filter is distinct from coalesce(
          v_job.payload_json -> 'input_status_filter',
          '{"state_code":{"between":[100,199]}}'::jsonb
        )
     or v_package.eligibility_definition is distinct from coalesce(
          v_job.payload_json -> 'eligibility_definition', '{}'::jsonb
        )
     or v_package.eligibility_resolved_at is distinct from
          v_expected_eligibility_resolved_at
     or v_package.eligible_input_count is distinct from
          v_expected_eligible_input_count
     or v_package.included_input_count is distinct from
          v_expected_included_input_count
     or v_package.input_manifest_hash is distinct from nullif(
          v_job.payload_json ->> 'input_manifest_hash', ''
        )
     or v_package.input_manifest is distinct from coalesce(
          v_job.payload_json -> 'input_manifest', '{}'::jsonb
        )
     or v_package.snapshot_id is distinct from p_snapshot_id
     or v_package.result_id is distinct from p_result_id
     or v_package.latest_all_unit_result_id is distinct from
          p_latest_all_unit_result_id
     or v_package.result_artifact_ref is distinct from p_result_artifact_ref
     or v_package.query_artifact_ref is distinct from p_query_artifact_ref
     or v_package.artifact_manifest is distinct from p_artifact_manifest
     or v_package.package_result_hash is distinct from p_package_result_hash
     or v_package.lcia_method_set is distinct from coalesce(
          v_job.payload_json -> 'lcia_method_set', '[]'::jsonb
        )
     or v_package.available_impact_categories is distinct from
          p_available_impact_categories
     or v_package.postprocess_manifest is distinct from coalesce(
          v_job.payload_json -> 'postprocess_manifest',
          '{"postprocess_mode":"skipped"}'::jsonb
        )
     or v_package.default_impact_category is distinct from
          v_expected_default_impact
     or v_package.closure_check_id is distinct from
          v_expected_closure_check_id
     or v_package.closure_certificate_hash is distinct from
          (v_job.payload_json ->> 'closure_certificate_hash')
     or v_package.closure_certificate_hash is distinct from
          v_projection.closure_certificate_hash
     or v_package.closure_snapshot_hash is distinct from
          (v_job.payload_json ->> 'snapshot_hash')
     or v_package.closure_snapshot_hash is distinct from
          v_projection.snapshot_hash
     or v_package.status <> 'preview_ready'
     or v_package.created_by is distinct from v_job.requested_by then
    if v_reused then
      return v_result;
    end if;
    return jsonb_build_object(
      'ok', false, 'code', 'projection_package_binding_invalid', 'status', 409
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'reused', v_reused,
    'data', jsonb_build_object(
      'packageId', v_package.id,
      'packageVersion', v_package.package_version,
      'status', v_package.status,
      'buildWorkerJobId', v_package.build_worker_job_id,
      'includedInputCount', v_package.included_input_count,
      'projection', jsonb_build_object(
        'projectionId', v_projection.id,
        'contentHash', v_projection.content_hash,
        'hashContractVersion',
          'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    )
  );
end
$function$;
