CREATE OR REPLACE FUNCTION "private"."svc_portal_lcia_projection_stage_register_batch_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_batch" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_projection private.portal_lcia_projection_headers%rowtype;
  v_job private.worker_jobs%rowtype;
  v_record jsonb;
  v_expected_process jsonb;
  v_expected_method jsonb;
  v_process private.portal_lcia_projection_process_axis%rowtype;
  v_impact private.portal_lcia_projection_impact_axis%rowtype;
  v_value private.portal_lcia_projection_values%rowtype;
  v_process_index integer;
  v_impact_index integer;
  v_ordinal bigint;
  v_decimal text;
  v_record_hash text;
  v_inserted integer := 0;
  v_row_count integer := 0;
  v_batch_count integer;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false, 'code', 'service_role_required', 'status', 403
    );
  end if;

  if p_projection_id is null
     or p_stage_lease_token is null
     or pg_catalog.octet_length(
       pg_catalog.convert_to(p_batch::text, 'UTF8')
     ) > 1048576
     or private.portal_lcia_json_object_has_keys_v1(
       p_batch,
       array['schemaVersion', 'processes', 'impacts', 'values']
     ) is not true
     or p_batch ->> 'schemaVersion' <> 'portal.lcia-projection.batch.v1'
     or jsonb_typeof(p_batch -> 'processes') <> 'array'
     or jsonb_typeof(p_batch -> 'impacts') <> 'array'
     or jsonb_typeof(p_batch -> 'values') <> 'array' then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_batch', 'status', 400
    );
  end if;

  v_batch_count := jsonb_array_length(p_batch -> 'processes')
    + jsonb_array_length(p_batch -> 'impacts')
    + jsonb_array_length(p_batch -> 'values');
  if v_batch_count < 1 or v_batch_count > 500
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'processes') as item(value)
       group by item.value ->> 'processIndex'
       having count(*) > 1
     )
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'impacts') as item(value)
       group by item.value ->> 'impactIndex'
       having count(*) > 1
     )
     or exists (
       select 1
       from jsonb_array_elements(p_batch -> 'values') as item(value)
       group by item.value ->> 'ordinal'
       having count(*) > 1
     ) then
    return jsonb_build_object(
      'ok', false, 'code', 'invalid_projection_batch', 'status', 400
    );
  end if;

  select projection.*
  into v_projection
  from private.portal_lcia_projection_headers as projection
  where projection.id = p_projection_id
  for update;
  if v_projection.id is null then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_found', 'status', 404
    );
  end if;

  select job.*
  into v_job
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
  if v_projection.status <> 'staging' then
    return jsonb_build_object(
      'ok', false, 'code', 'projection_not_staging', 'status', 409
    );
  end if;

  begin
    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'processes') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array[
          'processIndex', 'processId', 'processVersion',
          'referenceFlowId', 'referenceFlowVersion',
          'referenceExchangeInternalId', 'referenceFlowAmount',
          'referenceFlowDirection', 'functionalUnitAmount',
          'functionalUnitUnit', 'functionalUnitDescription',
          'geographyCode', 'geographyPrecision', 'referenceYear',
          'processDocumentSha256'
        ]
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'processIndex') <> 'number'
         or jsonb_typeof(v_record -> 'referenceYear') <> 'number'
         or jsonb_typeof(v_record -> 'processId') <> 'string'
         or jsonb_typeof(v_record -> 'processVersion') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowId') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowVersion') <> 'string'
         or jsonb_typeof(v_record -> 'referenceExchangeInternalId') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowAmount') <> 'string'
         or jsonb_typeof(v_record -> 'referenceFlowDirection') <> 'string'
         or jsonb_typeof(v_record -> 'functionalUnitAmount') <> 'string'
         or jsonb_typeof(v_record -> 'functionalUnitUnit') <> 'string'
         or jsonb_typeof(v_record -> 'geographyCode') <> 'string'
         or jsonb_typeof(v_record -> 'geographyPrecision') <> 'string'
         or jsonb_typeof(v_record -> 'processDocumentSha256') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_process_index := (v_record ->> 'processIndex')::integer;
        v_process.process_id := (v_record ->> 'processId')::uuid;
        v_process.reference_flow_id := (v_record ->> 'referenceFlowId')::uuid;
        v_process.reference_year := (v_record ->> 'referenceYear')::integer;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_decimal := private.portal_canonical_decimal_v1(
        v_record ->> 'functionalUnitAmount'
      );
      v_expected_process := v_job.payload_json
        -> 'input_manifest' -> 'processes' -> v_process_index;
      if v_record ->> 'processIndex' !~ '^(0|[1-9]\d*)$'
         or v_record ->> 'referenceYear' !~ '^(0|[1-9]\d*)$'
         or v_process_index not between 0 and v_projection.process_count - 1
         or v_decimal is distinct from v_record ->> 'functionalUnitAmount'
         or private.portal_canonical_decimal_v1(
              v_record ->> 'referenceFlowAmount'
            ) is distinct from v_record ->> 'referenceFlowAmount'
         or coalesce(v_record ->> 'processVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or coalesce(v_record ->> 'referenceFlowVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or coalesce(v_record ->> 'referenceExchangeInternalId', '')
              !~ '^(0|[1-9]\d{0,5})$'
         or v_record ->> 'referenceFlowDirection' not in ('input', 'output')
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'functionalUnitUnit', 128
            ) is not true
         or private.portal_lcia_localized_text_valid_v1(
              v_record -> 'functionalUnitDescription'
            ) is not true
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'geographyCode', 128
            ) is not true
         or v_record ->> 'geographyPrecision'
              not in ('country', 'province', 'city', 'other', 'unknown')
         or v_process.reference_year not between 0 and 9999
         or coalesce(v_record ->> 'processDocumentSha256', '')
              !~ '^[0-9a-f]{64}$'
         or jsonb_typeof(v_expected_process) <> 'object'
         or v_expected_process ->> 'id'
              is distinct from v_process.process_id::text
         or v_expected_process ->> 'version'
              is distinct from v_record ->> 'processVersion' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.process.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_process_index::text,
        v_process.process_id::text,
        v_record ->> 'processVersion',
        v_process.reference_flow_id::text,
        v_record ->> 'referenceFlowVersion',
        v_record ->> 'referenceExchangeInternalId',
        private.portal_canonical_decimal_v1(v_record ->> 'referenceFlowAmount'),
        v_record ->> 'referenceFlowDirection',
        v_decimal,
        btrim(v_record ->> 'functionalUnitUnit'),
        private.portal_lcia_localized_text_frame_hex_v1(
          v_record -> 'functionalUnitDescription'
        ),
        btrim(v_record ->> 'geographyCode'),
        v_record ->> 'geographyPrecision',
        v_process.reference_year::text,
        v_record ->> 'processDocumentSha256'
      );

      insert into private.portal_lcia_projection_process_axis (
        projection_id, process_index, process_id, process_version,
        reference_flow_id, reference_flow_version,
        reference_exchange_internal_id, reference_flow_amount,
        reference_flow_direction, functional_unit_amount,
        functional_unit_unit, functional_unit_description,
        geography_code, geography_precision, reference_year,
        process_document_sha256, record_hash
      ) values (
        v_projection.id, v_process_index, v_process.process_id,
        v_record ->> 'processVersion', v_process.reference_flow_id,
        v_record ->> 'referenceFlowVersion',
        v_record ->> 'referenceExchangeInternalId',
        private.portal_canonical_decimal_v1(v_record ->> 'referenceFlowAmount'),
        v_record ->> 'referenceFlowDirection', v_decimal,
        btrim(v_record ->> 'functionalUnitUnit'),
        v_record -> 'functionalUnitDescription',
        btrim(v_record ->> 'geographyCode'),
        v_record ->> 'geographyPrecision', v_process.reference_year,
        v_record ->> 'processDocumentSha256', v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_process
      from private.portal_lcia_projection_process_axis as row
      where row.projection_id = v_projection.id
        and row.process_index = v_process_index;
      if v_process.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;

    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'impacts') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array[
          'impactIndex', 'methodId', 'methodVersion', 'impactId',
          'impactName', 'unit', 'methodDocumentSha256'
        ]
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'impactIndex') <> 'number'
         or jsonb_typeof(v_record -> 'methodId') <> 'string'
         or jsonb_typeof(v_record -> 'methodVersion') <> 'string'
         or jsonb_typeof(v_record -> 'impactId') <> 'string'
         or jsonb_typeof(v_record -> 'unit') <> 'string'
         or jsonb_typeof(v_record -> 'methodDocumentSha256') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_impact_index := (v_record ->> 'impactIndex')::integer;
        v_impact.method_id := (v_record ->> 'methodId')::uuid;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_expected_method := v_job.payload_json
        -> 'lcia_method_set' -> v_impact_index;
      if v_record ->> 'impactIndex' !~ '^(0|[1-9]\d*)$'
         or v_impact_index not between 0 and v_projection.impact_count - 1
         or coalesce(v_record ->> 'methodVersion', '')
              !~ '^\d{2}\.\d{2}\.\d{3}$'
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'impactId', 512
            ) is not true
         or private.portal_lcia_localized_text_valid_v1(
              v_record -> 'impactName'
            ) is not true
         or private.portal_lcia_public_text_valid_v1(
              v_record ->> 'unit', 128
            ) is not true
         or coalesce(v_record ->> 'methodDocumentSha256', '')
              !~ '^[0-9a-f]{64}$'
         or jsonb_typeof(v_expected_method) <> 'object'
         or v_expected_method ->> 'id'
              is distinct from v_impact.method_id::text
         or v_expected_method ->> 'version'
              is distinct from v_record ->> 'methodVersion' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.impact.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_impact_index::text,
        v_impact.method_id::text,
        v_record ->> 'methodVersion',
        btrim(v_record ->> 'impactId'),
        private.portal_lcia_localized_text_frame_hex_v1(
          v_record -> 'impactName'
        ),
        btrim(v_record ->> 'unit'),
        v_record ->> 'methodDocumentSha256'
      );
      insert into private.portal_lcia_projection_impact_axis (
        projection_id, impact_index, method_id, method_version,
        impact_id, impact_name, unit, method_document_sha256, record_hash
      ) values (
        v_projection.id, v_impact_index, v_impact.method_id,
        v_record ->> 'methodVersion', btrim(v_record ->> 'impactId'),
        v_record -> 'impactName', btrim(v_record ->> 'unit'),
        v_record ->> 'methodDocumentSha256', v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_impact
      from private.portal_lcia_projection_impact_axis as row
      where row.projection_id = v_projection.id
        and row.impact_index = v_impact_index;
      if v_impact.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;

    for v_record in
      select item.value
      from jsonb_array_elements(p_batch -> 'values') as item(value)
    loop
      if private.portal_lcia_json_object_has_keys_v1(
        v_record,
        array['ordinal', 'processIndex', 'impactIndex', 'value']
      ) is not true then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      if jsonb_typeof(v_record -> 'ordinal') <> 'number'
         or jsonb_typeof(v_record -> 'processIndex') <> 'number'
         or jsonb_typeof(v_record -> 'impactIndex') <> 'number'
         or jsonb_typeof(v_record -> 'value') <> 'string' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;
      begin
        v_ordinal := (v_record ->> 'ordinal')::bigint;
        v_process_index := (v_record ->> 'processIndex')::integer;
        v_impact_index := (v_record ->> 'impactIndex')::integer;
      exception when others then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end;
      v_decimal := private.portal_canonical_decimal_v1(v_record ->> 'value');
      if v_record ->> 'ordinal' !~ '^[1-9]\d*$'
         or v_record ->> 'processIndex' !~ '^(0|[1-9]\d*)$'
         or v_record ->> 'impactIndex' !~ '^(0|[1-9]\d*)$'
         or v_process_index not between 0 and v_projection.process_count - 1
         or v_impact_index not between 0 and v_projection.impact_count - 1
         or v_ordinal < 1
         or v_ordinal <>
              v_process_index::bigint * v_projection.impact_count::bigint
              + v_impact_index::bigint + 1
         or v_decimal is distinct from v_record ->> 'value' then
        raise exception using errcode = 'P2101', message = 'invalid projection batch';
      end if;

      v_record_hash := private.portal_lcia_projection_sha256_fields_v1(
        'portal.lcia-projection.value.v1',
        'portal.lcia-projection.int32be-frame-sha256.v1',
        v_ordinal::text,
        v_process_index::text,
        v_impact_index::text,
        v_decimal
      );
      insert into private.portal_lcia_projection_values (
        projection_id, ordinal, process_index, impact_index,
        value_text, value_numeric, record_hash
      ) values (
        v_projection.id, v_ordinal, v_process_index, v_impact_index,
        v_decimal, v_decimal::numeric, v_record_hash
      ) on conflict do nothing;
      get diagnostics v_row_count = row_count;
      v_inserted := v_inserted + v_row_count;

      select row.* into v_value
      from private.portal_lcia_projection_values as row
      where row.projection_id = v_projection.id
        and row.ordinal = v_ordinal;
      if v_value.record_hash is distinct from v_record_hash then
        raise exception using errcode = 'P2102', message = 'projection batch conflict';
      end if;
    end loop;
  exception
    when sqlstate 'P2101' or invalid_text_representation
      or numeric_value_out_of_range or check_violation
      or foreign_key_violation or not_null_violation then
      return jsonb_build_object(
        'ok', false, 'code', 'invalid_projection_batch', 'status', 400
      );
    when sqlstate 'P2102' or unique_violation then
      return jsonb_build_object(
        'ok', false, 'code', 'projection_batch_conflict', 'status', 409
      );
  end;

  return jsonb_build_object(
    'ok', true,
    'idempotentReplay', v_inserted = 0,
    'data', jsonb_build_object(
      'projectionId', v_projection.id,
      'acceptedRecordCount', v_batch_count,
      'insertedRecordCount', v_inserted
    )
  );
end
$_$;

ALTER FUNCTION "private"."svc_portal_lcia_projection_stage_register_batch_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_batch" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_register_batch_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_batch" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_portal_lcia_projection_stage_register_batch_v1"("p_projection_id" "uuid", "p_stage_lease_token" "uuid", "p_batch" "jsonb") TO "service_role";
