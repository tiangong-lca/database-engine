CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_validate_mapping"("p_actor" "uuid", "p_mapping" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb", "p_expected_ordinal" integer) RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_mapping_keys constant text[] := array[
    'ordinal', 'mapping_id', 'source', 'target', 'compatibility'
  ];
  v_compatibility_keys constant text[] := array[
    'policy_sha256', 'mode', 'confidence', 'flow_property_compatible',
    'unit_group_compatible', 'direction_compatible',
    'compartment_compatible', 'conversion_factor', 'evidence_sha256',
    'flow_schema', 'process_schema_required'
  ];
  v_flow_schema_keys constant text[] := array[
    'status', 'warning_set_sha256'
  ];
  v_mapping_id text;
  v_source_result jsonb;
  v_target_result jsonb;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(
      p_mapping, v_mapping_keys
    )
    or jsonb_typeof(p_mapping->'ordinal') <> 'number'
    or (p_mapping->>'ordinal')::integer <> p_expected_ordinal
    or p_mapping->>'mapping_id' !~ '^[a-f0-9]{64}$'
    or not private.dataset_flow_identity_exact_keys(
      p_mapping->'compatibility', v_compatibility_keys
    )
    or not private.dataset_flow_identity_exact_keys(
      p_mapping #> '{compatibility,flow_schema}', v_flow_schema_keys
    )
    or p_mapping #>> '{compatibility,policy_sha256}'
      is distinct from p_policy->>'policy_sha256'
    or p_mapping #>> '{compatibility,mode}' <> 'identity'
    or p_mapping #>> '{compatibility,confidence}' <> 'approved'
    or p_mapping #>> '{compatibility,conversion_factor}' <> '1'
    or p_mapping #>> '{compatibility,evidence_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_mapping #>> '{compatibility,flow_schema,status}'
      not in ('pass', 'legacy_warning')
    or p_mapping #>> '{compatibility,flow_schema,warning_set_sha256}'
      !~ '^[a-f0-9]{64}$'
    or p_mapping #>> '{compatibility,process_schema_required}' <> 'pass'
    or exists (
      select 1
      from unnest(array[
        'flow_property_compatible', 'unit_group_compatible',
        'direction_compatible', 'compartment_compatible'
      ]) as boolean_field(name)
      where jsonb_typeof(
          p_mapping->'compatibility'->boolean_field.name
        ) <> 'boolean'
        or coalesce((
          p_mapping->'compatibility'->>boolean_field.name
        )::boolean, false) is false
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_SCHEMA_MISMATCH'
    );
  end if;

  v_mapping_id := util.dataset_flow_identity_restricted_sha256_v2(
    p_mapping - 'ordinal' - 'mapping_id'
  );
  if v_mapping_id is distinct from p_mapping->>'mapping_id' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_HASH_MISMATCH'
    );
  end if;

  v_source_result := util.dataset_flow_identity_validate_flow_guard(
    p_actor, p_mapping->'source', false, p_support_snapshots
  );
  if coalesce((v_source_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_SOURCE_REJECTED',
      'details', v_source_result
    );
  end if;

  v_target_result := util.dataset_flow_identity_validate_flow_guard(
    p_actor, p_mapping->'target', true, p_support_snapshots
  );
  if coalesce((v_target_result->>'ok')::boolean, false) is false then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_TARGET_REJECTED',
      'details', v_target_result
    );
  end if;

  if p_mapping #>> '{source,id}' = p_mapping #>> '{target,id}'
    and p_mapping #>> '{source,version}'
      = p_mapping #>> '{target,version}' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_NOOP'
    );
  end if;

  if p_mapping #>> '{source,flow_property_id}'
      is distinct from p_mapping #>> '{target,flow_property_id}'
    or p_mapping #>> '{source,flow_property_version}'
      is distinct from p_mapping #>> '{target,flow_property_version}'
    or p_mapping #>> '{source,unit_group_id}'
      is distinct from p_mapping #>> '{target,unit_group_id}'
    or p_mapping #>> '{source,unit_group_version}'
      is distinct from p_mapping #>> '{target,unit_group_version}' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_MAPPING_NOT_IDENTITY_ONLY'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'mapping_id', v_mapping_id,
    'source', v_source_result,
    'target', v_target_result
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_validate_mapping"("p_actor" "uuid", "p_mapping" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb", "p_expected_ordinal" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_validate_mapping"("p_actor" "uuid", "p_mapping" "jsonb", "p_policy" "jsonb", "p_support_snapshots" "jsonb", "p_expected_ordinal" integer) FROM PUBLIC;
