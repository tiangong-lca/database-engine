CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_stage_seal_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_process_count bigint;
  v_impact_count bigint;
  v_value_count bigint;
  v_bad_count bigint;
  v_process_axis_hash text;
  v_impact_axis_hash text;
  v_value_grid_hash text;
  v_relation_hash text;
  v_content_hash text;
  v_fields text[];
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;
  select projection.* into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;
  select job.* into v_job
  from private.worker_jobs as job
  where job.id = v_projection.build_worker_job_id
  for share;
  if v_projection.stage_lease_token is distinct from p_stage_lease_token
     or v_job.status <> 'running'
     or v_job.lease_token is distinct from p_stage_lease_token
     or v_job.lease_expires_at is null
     or v_job.lease_expires_at <= clock_timestamp() then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_lease_invalid', 'status', 409
    );
  end if;
  if v_projection.status = 'prepared' then
    return jsonb_build_object(
      'ok', true,
      'idempotentReplay', true,
      'data', jsonb_build_object(
        'projectionId', v_projection.id,
        'status', v_projection.status,
        'processCount', v_projection.process_count,
        'impactCount', v_projection.impact_count,
        'valueCount', v_projection.expected_value_count,
        'processAxisHash', v_projection.process_axis_hash,
        'impactAxisHash', v_projection.impact_axis_hash,
        'valueGridHash', v_projection.value_grid_hash,
        'relationHash', v_projection.relation_hash,
        'contentHash', v_projection.content_hash,
        'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
      )
    );
  end if;
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (where process_index between 0 and v_projection.process_count - 1)
  into v_process_count, v_bad_count
  from private.portal_lcia_projection_process_axis
  where projection_id = v_projection.id;
  if v_process_count <> v_projection.process_count
     or v_bad_count <> v_projection.process_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (where impact_index between 0 and v_projection.impact_count - 1)
  into v_impact_count, v_bad_count
  from private.portal_lcia_projection_impact_axis
  where projection_id = v_projection.id;
  if v_impact_count <> v_projection.impact_count
     or v_bad_count <> v_projection.impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  select count(*),
         count(*) filter (
           where ordinal = process_index::bigint * v_projection.impact_count::bigint
             + impact_index::bigint + 1
             and ordinal between 1 and v_projection.expected_value_count
         )
  into v_value_count, v_bad_count
  from private.portal_lcia_projection_values
  where projection_id = v_projection.id;
  if v_value_count <> v_projection.expected_value_count
     or v_bad_count <> v_projection.expected_value_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_incomplete', 'status', 409
    );
  end if;

  if jsonb_typeof(v_job.payload_json -> 'input_manifest' -> 'processes')
       <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'input_manifest' -> 'processes')
          <> v_projection.process_count
     or jsonb_typeof(v_job.payload_json -> 'lcia_method_set') <> 'array'
     or jsonb_array_length(v_job.payload_json -> 'lcia_method_set')
          <> v_projection.impact_count then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_process_axis as row
  where row.projection_id = v_projection.id
    and (
      v_job.payload_json -> 'input_manifest' -> 'processes' -> row.process_index
        ->> 'id' is distinct from row.process_id::text
      or v_job.payload_json -> 'input_manifest' -> 'processes'
        -> row.process_index ->> 'version' is distinct from row.process_version
    );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_impact_axis as row
  where row.projection_id = v_projection.id
    and (
      v_job.payload_json -> 'lcia_method_set' -> row.impact_index
        ->> 'id' is distinct from row.method_id::text
      or v_job.payload_json -> 'lcia_method_set' -> row.impact_index
        ->> 'version' is distinct from row.method_version
    );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_process_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.process.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.process_index::text,
        row.process_id::text,
        row.process_version,
        row.reference_flow_id::text,
        row.reference_flow_version,
        row.reference_exchange_internal_id,
        row.reference_flow_amount,
        row.reference_flow_direction,
        row.functional_unit_amount,
        row.functional_unit_unit,
        private.portal_lcia_localized_text_frame_hex_v1(
          row.functional_unit_description
        ),
        row.geography_code,
        row.geography_precision,
        row.reference_year::text,
        row.process_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_impact_axis as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.impact.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.impact_index::text,
        row.method_id::text,
        row.method_version,
        row.impact_id,
        private.portal_lcia_localized_text_frame_hex_v1(row.impact_name),
        row.unit,
        row.method_document_sha256
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select count(*) into v_bad_count
  from private.portal_lcia_projection_values as row
  where row.projection_id = v_projection.id
    and row.record_hash is distinct from
      private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.value.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        row.ordinal::text,
        row.process_index::text,
        row.impact_index::text,
        row.value_text
      );
  if v_bad_count <> 0 then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_evidence_mismatch', 'status', 409
    );
  end if;

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'process-axis',
    v_projection.process_count::text
  ] || coalesce(
    array_agg(field.value order by row.process_index, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_process_axis as row
  cross join lateral (
    values (1, (row.process_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_process_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'impact-axis',
    v_projection.impact_count::text
  ] || coalesce(
    array_agg(field.value order by row.impact_index, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_impact_axis as row
  cross join lateral (
    values (1, (row.impact_index + 1)::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_impact_axis_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  select array[
    'portal.lcia-projection.relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    'value-grid',
    v_projection.expected_value_count::text
  ] || coalesce(
    array_agg(field.value order by row.ordinal, field.position),
    '{}'::text[]
  )
  into v_fields
  from private.portal_lcia_projection_values as row
  cross join lateral (
    values (1, row.ordinal::text), (2, row.record_hash)
  ) as field(position, value)
  where row.projection_id = v_projection.id;
  v_value_grid_hash := private.portal_lcia_projection_sha256_fields_v1(
    variadic v_fields
  );

  v_relation_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.grid-relation.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    'ordinal=processIndex*impactCount+impactIndex+1',
    v_process_axis_hash,
    v_impact_axis_hash,
    v_value_grid_hash
  );
  v_content_hash := private.portal_lcia_projection_sha256_fields_v1(
    'portal.lcia-projection.content.v1',
    'portal.lcia-projection.int32be-frame-sha256.v1',
    v_projection.projection_contract_version,
    v_projection.input_manifest_hash,
    v_projection.closure_certificate_hash,
    v_projection.snapshot_hash,
    v_projection.closure_bundle_hash,
    v_projection.snapshot_index_sha256,
    v_projection.snapshot_build_contract_hash,
    v_projection.bundle_content_hash,
    v_projection.bundle_manifest_sha256,
    v_projection.lcia_chunk_set_sha256,
    v_projection.result_artifact_sha256,
    v_projection.query_artifact_sha256,
    v_projection.process_count::text,
    v_projection.impact_count::text,
    v_projection.expected_value_count::text,
    v_process_axis_hash,
    v_impact_axis_hash,
    v_value_grid_hash,
    v_relation_hash
  );

  update private.portal_lcia_projection_headers
  set status = 'prepared',
      process_axis_hash = v_process_axis_hash,
      impact_axis_hash = v_impact_axis_hash,
      value_grid_hash = v_value_grid_hash,
      relation_hash = v_relation_hash,
      content_hash = v_content_hash,
      prepared_at = clock_timestamp()
  where id = v_projection.id
  returning * into v_projection;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', false,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'status', v_projection.status,
      'processCount', v_projection.process_count,
      'impactCount', v_projection.impact_count,
      'valueCount', v_projection.expected_value_count,
      'processAxisHash', v_projection.process_axis_hash,
      'impactAxisHash', v_projection.impact_axis_hash,
      'valueGridHash', v_projection.value_grid_hash,
      'relationHash', v_projection.relation_hash,
      'contentHash', v_projection.content_hash,
      'hashContractVersion', 'portal.lcia-projection.int32be-frame-sha256.v1'
    )
  );
end
$$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_stage_seal_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_seal_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_seal_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid") TO "service_role";
