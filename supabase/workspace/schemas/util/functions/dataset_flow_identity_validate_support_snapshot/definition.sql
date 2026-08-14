CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_validate_support_snapshot"("p_actor" "uuid", "p_snapshot" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_keys constant text[] := array[
    'ordinal', 'table', 'id', 'version', 'user_id', 'state_code',
    'modified_at', 'payload_sha256', 'row_sha256'
  ];
  v_flowproperty public.flowproperties%rowtype;
  v_unitgroup public.unitgroups%rowtype;
  v_payload jsonb;
  v_payload_sha256 text;
  v_row_sha256 text;
  v_live_user_id uuid;
  v_live_state_code integer;
  v_live_modified_at timestamp with time zone;
  v_embedded_id text;
  v_embedded_version text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_snapshot, v_keys)
    or p_snapshot->>'ordinal' !~ '^[1-9][0-9]*$'
    or p_snapshot->>'table' not in ('flowproperties', 'unitgroups')
    or p_snapshot->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_snapshot->>'user_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_snapshot->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or p_snapshot->>'state_code' not in ('0', '100')
    or p_snapshot->>'payload_sha256' !~ '^[a-f0-9]{64}$'
    or p_snapshot->>'row_sha256' !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_snapshot->>'modified_at')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_VALUE_MISMATCH'
    );
  end;

  if p_snapshot->>'table' = 'flowproperties' then
    select support.*
    into v_flowproperty
    from public.flowproperties as support
    where support.id = (p_snapshot->>'id')::uuid
      and btrim(support.version::text) = p_snapshot->>'version';
    if v_flowproperty.id is null
      or v_flowproperty.json is null or v_flowproperty.json_ordered is null
      or v_flowproperty.json::jsonb
        is distinct from v_flowproperty.json_ordered::jsonb then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_JSON_PARITY_MISMATCH'
      );
    end if;
    v_payload := v_flowproperty.json_ordered::jsonb;
    v_live_user_id := v_flowproperty.user_id;
    v_live_state_code := v_flowproperty.state_code;
    v_live_modified_at := v_flowproperty.modified_at;
    v_embedded_id := v_payload #>>
      '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:UUID}';
    v_embedded_version := v_payload #>>
      '{flowPropertyDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}';
  else
    select support.*
    into v_unitgroup
    from public.unitgroups as support
    where support.id = (p_snapshot->>'id')::uuid
      and btrim(support.version::text) = p_snapshot->>'version';
    if v_unitgroup.id is null
      or v_unitgroup.json is null or v_unitgroup.json_ordered is null
      or v_unitgroup.json::jsonb
        is distinct from v_unitgroup.json_ordered::jsonb then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_JSON_PARITY_MISMATCH'
      );
    end if;
    v_payload := v_unitgroup.json_ordered::jsonb;
    v_live_user_id := v_unitgroup.user_id;
    v_live_state_code := v_unitgroup.state_code;
    v_live_modified_at := v_unitgroup.modified_at;
    v_embedded_id := v_payload #>>
      '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}';
    v_embedded_version := v_payload #>>
      '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}';
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
  v_row_sha256 := private.dataset_flow_identity_row_sha256(
    (p_snapshot->>'id')::uuid,
    p_snapshot->>'version',
    v_live_user_id,
    v_live_state_code,
    v_live_modified_at,
    v_payload_sha256
  );
  if v_live_user_id::text is distinct from p_snapshot->>'user_id'
    or v_live_state_code::text is distinct from p_snapshot->>'state_code'
    or v_live_modified_at is distinct from
      (p_snapshot->>'modified_at')::timestamp with time zone
    or (v_live_state_code = 0 and v_live_user_id is distinct from p_actor)
    or v_live_state_code not in (0, 100)
    or v_embedded_id is distinct from p_snapshot->>'id'
    or v_embedded_version is distinct from p_snapshot->>'version'
    or v_payload_sha256 is distinct from p_snapshot->>'payload_sha256'
    or v_row_sha256 is distinct from p_snapshot->>'row_sha256' then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_LIVE_MISMATCH'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'table', p_snapshot->>'table',
    'id', p_snapshot->>'id',
    'version', p_snapshot->>'version',
    'payload_sha256', v_payload_sha256,
    'row_sha256', v_row_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SNAPSHOT_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_validate_support_snapshot"("p_actor" "uuid", "p_snapshot" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_validate_support_snapshot"("p_actor" "uuid", "p_snapshot" "jsonb") FROM PUBLIC;
