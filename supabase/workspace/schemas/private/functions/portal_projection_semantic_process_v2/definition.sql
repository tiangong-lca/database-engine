CREATE OR REPLACE FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") RETURNS TABLE("id" "uuid", "version" "text", "semantic_distance" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
begin
  if p_query_embedding is null or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using errcode = '22023', message = 'invalid portal request';
  end if;
  return query
  with nearest as materialized (
    select source.id, source.version::text as version,
      source.embedding_ft operator(extensions.<=>) p_query_embedding as distance
    from public.processes as source
    where source.state_code in (100,200)
      and source.embedding_ft is not null
      and exists (
        select 1 from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'process'
          and projection.id = source.id and projection.version = source.version::text
          and projection.state_code in (100,200)
          and (p_filters = '{}'::jsonb
            or private.portal_card_matches_filters_v2(projection.card, p_filters))
        offset 0
      )
    order by source.embedding_ft operator(extensions.<=>) p_query_embedding
    limit 200
  )
  select nearest.id, nearest.version, nearest.distance
  from nearest
  where nearest.distance >= 0::double precision and nearest.distance <= 0.5::double precision
  order by nearest.distance + 0::double precision, nearest.id, nearest.version desc;
end;
$$;

ALTER FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") TO "portal_public_executor";
