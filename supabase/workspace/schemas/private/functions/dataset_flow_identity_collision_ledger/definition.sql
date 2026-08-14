CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_collision_ledger"("p_exchanges" "jsonb", "p_rewrites" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
  with touched as (
    select distinct
      rewrite.value #>> '{target_reference,@refObjectId}' as target_id,
      rewrite.value #>> '{target_reference,@version}' as target_version
    from jsonb_array_elements(p_rewrites) as rewrite(value)
  ), matching as (
    select
      touched.target_id,
      touched.target_version,
      (exchange.ordinality - 1)::integer as exchange_index,
      exchange.value->>'@dataSetInternalID' as internal_id,
      rewrite.value->>'mapping_id' as mapping_id
    from touched
    join lateral jsonb_array_elements(p_exchanges)
      with ordinality as exchange(value, ordinality)
      on exchange.value #>> '{referenceToFlowDataSet,@refObjectId}'
          = touched.target_id
        and exchange.value #>> '{referenceToFlowDataSet,@version}'
          = touched.target_version
    left join lateral (
      select candidate.value
      from jsonb_array_elements(p_rewrites) as candidate(value)
      where (candidate.value->>'exchange_index')::integer
        = exchange.ordinality - 1
      limit 1
    ) as rewrite on true
  ), grouped as (
    select
      matching.target_id,
      matching.target_version,
      count(*)::integer as multiplicity,
      jsonb_agg(matching.exchange_index order by matching.exchange_index)
        as exchange_indexes,
      jsonb_agg(matching.internal_id order by matching.exchange_index)
        as internal_ids,
      jsonb_agg(to_jsonb(matching.mapping_id)
        order by matching.exchange_index) as mapping_ids
    from matching
    group by matching.target_id, matching.target_version
    having count(*) > 1
  )
  select jsonb_build_object(
    'schema_version', 'dataset-flow-identity-collision-ledger.v1',
    'entries', coalesce(jsonb_agg(jsonb_build_object(
      'target_id', grouped.target_id,
      'target_version', grouped.target_version,
      'exchange_indexes', grouped.exchange_indexes,
      'internal_ids', grouped.internal_ids,
      'mapping_ids', grouped.mapping_ids,
      'preserve_rows', true
    ) order by grouped.target_id, grouped.target_version), '[]'::jsonb)
  )
  from grouped
$$;

ALTER FUNCTION "private"."dataset_flow_identity_collision_ledger"("p_exchanges" "jsonb", "p_rewrites" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_collision_ledger"("p_exchanges" "jsonb", "p_rewrites" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_collision_ledger"("p_exchanges" "jsonb", "p_rewrites" "jsonb") TO "api_internal_executor";
