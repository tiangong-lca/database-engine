CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_mapping_index_v2"("p_mappings" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE STRICT
    SET "search_path" TO ''
    AS $_$
declare
  v_count integer;
  v_distinct_ordinals integer;
  v_distinct_ids integer;
  v_distinct_sources integer;
  v_by_ordinal jsonb;
  v_by_id jsonb;
  v_by_source jsonb;
begin
  if jsonb_typeof(p_mappings) <> 'array'
    or jsonb_array_length(p_mappings) not between 1 and 305 then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_MAPPING_INDEX_ARRAY_INVALID';
  end if;
  select
    count(*)::integer,
    count(distinct item.value->>'ordinal')::integer,
    count(distinct item.value->>'mapping_id')::integer,
    count(distinct (item.value #>> '{source,id}') || '@'
      || (item.value #>> '{source,version}'))::integer,
    jsonb_object_agg(item.value->>'ordinal', item.value),
    jsonb_object_agg(item.value->>'mapping_id', item.value),
    jsonb_object_agg(
      (item.value #>> '{source,id}') || '@'
        || (item.value #>> '{source,version}'), item.value
    )
  into v_count, v_distinct_ordinals, v_distinct_ids, v_distinct_sources,
    v_by_ordinal, v_by_id, v_by_source
  from jsonb_array_elements(p_mappings) as item(value)
  where jsonb_typeof(item.value->'ordinal') = 'number'
    and (item.value->>'ordinal')::numeric between 1 and 305
    and item.value->>'mapping_id' ~ '^[a-f0-9]{64}$';
  if v_count <> jsonb_array_length(p_mappings)
    or v_distinct_ordinals <> v_count
    or v_distinct_ids <> v_count
    or v_distinct_sources <> v_count then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_MAPPING_INDEX_IDENTITY_INVALID';
  end if;
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-mapping-index.v2',
    'mapping_count', v_count,
    'mapping_guard_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(p_mappings),
    'mappings', p_mappings,
    'by_ordinal', v_by_ordinal,
    'by_id', v_by_id,
    'by_source', v_by_source
  );
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_mapping_index_v2"("p_mappings" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_mapping_index_v2"("p_mappings" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_mapping_index_v2"("p_mappings" "jsonb") TO "api_internal_executor";
