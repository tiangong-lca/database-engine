CREATE OR REPLACE FUNCTION "private"."portal_dataset_projection_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_json jsonb;
  v_state_code integer;
  v_modified_at timestamptz;
  v_capabilities jsonb;
begin
  if p_kind not in ('process', 'flow')
     or p_version !~ '^\d{2}\.\d{2}\.\d{3}$' then
    return null;
  end if;
  if p_kind = 'process' then
    select row.json, row.state_code, row.modified_at
    into v_json, v_state_code, v_modified_at
    from public.processes as row
    where row.id = p_id
      and row.version::text = p_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'processDataSet') = 'object'
    limit 1;
  else
    select row.json, row.state_code, row.modified_at
    into v_json, v_state_code, v_modified_at
    from public.flows as row
    where row.id = p_id
      and row.version::text = p_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
    limit 1;
  end if;
  if v_json is null or v_modified_at is null then
    return null;
  end if;
  v_capabilities := private.portal_capabilities_v1(p_kind, v_state_code, v_json);
  return jsonb_build_object(
    'schemaVersion', 'portal.public-dataset.v1',
    'key', jsonb_build_object('kind', p_kind, 'id', p_id::text, 'version', p_version),
    'accessLevel', case when (v_capabilities ->> 'exchangesVisible')::boolean then 'open' else 'metadata_only' end,
    'capabilities', v_capabilities,
    'metadata', private.portal_dataset_metadata_v1(p_kind, v_state_code, v_json),
    'provenance', jsonb_build_object(
      'importBatchId', null,
      'normalizationRuleVersion', null,
      'fieldOrigins', '[]'::jsonb
    ),
    'publication', null,
    'modifiedAt', private.portal_timestamp_v1(v_modified_at)
  );
end
$_$;

ALTER FUNCTION "private"."portal_dataset_projection_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_dataset_projection_v1"("p_kind" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;
