CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_build_support_set_v2"("p_actor" "uuid", "p_mappings" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_result jsonb := '[]'::jsonb;
  v_claim record;
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload jsonb;
  v_user_id uuid;
  v_state_code integer;
  v_modified_at timestamp with time zone;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_ordinal integer := 0;
  v_validation jsonb;
begin
  for v_claim in
    with endpoint as (
      select guard.value
      from jsonb_array_elements(p_mappings) as mapping(value)
      cross join lateral jsonb_array_elements(jsonb_build_array(
        mapping.value->'source', mapping.value->'target'
      )) as guard(value)
    ), claimed as (
      select 'flowproperties'::text as support_table,
        (value->>'flow_property_id')::uuid as id,
        value->>'flow_property_version' as version
      from endpoint
      union
      select 'unitgroups', (value->>'unit_group_id')::uuid,
        value->>'unit_group_version'
      from endpoint
    )
    select * from claimed order by support_table, id, version
  loop
    v_ordinal := v_ordinal + 1;
    if v_claim.support_table = 'flowproperties' then
      select support.* into v_flowproperty
      from public.flowproperties as support
      where support.id = v_claim.id
        and btrim(support.version::text) = v_claim.version;
      if v_flowproperty.id is null
        or v_flowproperty.json is null or v_flowproperty.json_ordered is null
        or v_flowproperty.json::jsonb
          is distinct from v_flowproperty.json_ordered::jsonb then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_INVALID';
      end if;
      v_payload := v_flowproperty.json_ordered::jsonb;
      v_user_id := v_flowproperty.user_id;
      v_state_code := v_flowproperty.state_code;
      v_modified_at := v_flowproperty.modified_at;
    else
      select support.* into v_unitgroup
      from public.unitgroups as support
      where support.id = v_claim.id
        and btrim(support.version::text) = v_claim.version;
      if v_unitgroup.id is null
        or v_unitgroup.json is null or v_unitgroup.json_ordered is null
        or v_unitgroup.json::jsonb
          is distinct from v_unitgroup.json_ordered::jsonb then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_INVALID';
      end if;
      v_payload := v_unitgroup.json_ordered::jsonb;
      v_user_id := v_unitgroup.user_id;
      v_state_code := v_unitgroup.state_code;
      v_modified_at := v_unitgroup.modified_at;
    end if;
    if v_state_code not in (0, 100)
      or (v_state_code = 0 and v_user_id is distinct from p_actor) then
      raise exception using errcode = '22023',
        message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_VISIBILITY_MISMATCH';
    end if;
    v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
    v_row_sha256 := private.dataset_flow_identity_row_sha256(
      v_claim.id, v_claim.version, v_user_id, v_state_code,
      v_modified_at, v_payload_sha256
    );
    v_result := v_result || jsonb_build_array(jsonb_build_object(
      'ordinal', v_ordinal,
      'table', v_claim.support_table,
      'id', v_claim.id,
      'version', v_claim.version,
      'user_id', v_user_id,
      'state_code', v_state_code,
      'modified_at', v_modified_at,
      'payload_sha256', v_payload_sha256,
      'row_sha256', v_row_sha256
    ));
  end loop;
  v_validation := util.dataset_flow_identity_validate_support_set(
    p_actor, v_result, util.dataset_flow_identity_sha256(v_result)
  );
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_SUPPORT_SET_INVALID';
  end if;
  return v_result;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_build_support_set_v2"("p_actor" "uuid", "p_mappings" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_build_support_set_v2"("p_actor" "uuid", "p_mappings" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_build_support_set_v2"("p_actor" "uuid", "p_mappings" "jsonb") TO "api_internal_executor";
