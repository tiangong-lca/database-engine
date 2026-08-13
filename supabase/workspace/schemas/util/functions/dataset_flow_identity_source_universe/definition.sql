CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_source_universe"("p_actor" "uuid", "p_expected_universe" "jsonb", "p_expected_sha256" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_live jsonb;
  v_invalid_count integer;
  v_live_sha256 text;
begin
  if p_actor is null
    or jsonb_typeof(p_expected_universe) <> 'array'
    or jsonb_array_length(p_expected_universe) <> 305
    or p_expected_sha256 !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(p_expected_universe)
      is distinct from p_expected_sha256
    or exists (
      select 1
      from jsonb_array_elements(p_expected_universe)
        with ordinality as item(value, ordinality)
      where not private.dataset_flow_identity_exact_keys(
          item.value,
          array['id', 'version', 'user_id', 'state_code', 'flow_type']
        )
        or item.value->>'id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or item.value->>'version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or item.value->>'user_id' <> p_actor::text
        or item.value->>'state_code' <> '0'
        or item.value->>'flow_type' <> 'Elementary flow'
    ) then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SOURCE_UNIVERSE_SCHEMA_MISMATCH'
    );
  end if;

  select
    coalesce(jsonb_agg(jsonb_build_object(
      'id', flow.id,
      'version', btrim(flow.version::text),
      'user_id', flow.user_id,
      'state_code', flow.state_code,
      'flow_type', flow.json_ordered #>>
        '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
    ) order by flow.id, btrim(flow.version::text)), '[]'::jsonb),
    count(*) filter (
      where flow.json is null
        or flow.json_ordered is null
        or flow.json::jsonb is distinct from flow.json_ordered::jsonb
        or flow.json_ordered #>>
          '{flowDataSet,flowInformation,dataSetInformation,common:UUID}'
          is distinct from flow.id::text
        or flow.json_ordered #>>
          '{flowDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
          is distinct from btrim(flow.version::text)
    )::integer
  into v_live, v_invalid_count
  from public.flows as flow
  where flow.user_id = p_actor
    and flow.state_code = 0
    and flow.json_ordered #>>
      '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}'
      = 'Elementary flow';

  v_live_sha256 := util.dataset_flow_identity_sha256(v_live);
  return jsonb_build_object(
    'ok', v_invalid_count = 0
      and jsonb_array_length(v_live) = 305
      and v_live is not distinct from p_expected_universe
      and v_live_sha256 is not distinct from p_expected_sha256,
    'schema_version', 'dataset-flow-identity-source-universe-proof.v1',
    'expected_count', 305,
    'observed_count', jsonb_array_length(v_live),
    'expected_sha256', p_expected_sha256,
    'observed_sha256', v_live_sha256,
    'invalid_live_row_count', v_invalid_count
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SOURCE_UNIVERSE_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_source_universe"("p_actor" "uuid", "p_expected_universe" "jsonb", "p_expected_sha256" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_source_universe"("p_actor" "uuid", "p_expected_universe" "jsonb", "p_expected_sha256" "text") FROM PUBLIC;
