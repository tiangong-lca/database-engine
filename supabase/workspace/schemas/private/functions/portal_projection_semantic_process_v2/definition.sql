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
declare
  v_exact_cutover constant integer := 2000;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer;
  v_indexed_probe boolean := false;
begin
  if p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024 then
    raise exception using
      errcode = '22023',
      message = 'invalid portal request';
  end if;

  -- Geography and access level are exact, normalized facts in the
  -- transactionally synchronized facet child.  Additional filters remain a
  -- final canonical card recheck, so this key set is a safe candidate
  -- superset for combined filter requests.
  if (p_filters ? 'geography') and (p_filters ? 'accessLevel') then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'geography' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_geography = p_filters ->> 'geography'
      limit v_exact_cutover + 1
    ) as candidate;
  elsif p_filters ? 'accessLevel' then
    v_indexed_probe := true;
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    into v_candidate_ids, v_candidate_versions
    from (
      select facet.id, facet.version
      from private.portal_catalog_facet_rows_v1 as facet
      where facet.dataset_kind = 'process'
        and facet.state_code in (100, 200)
        and facet.facet_contract_version = 1
        and facet.facet_access_level = p_filters ->> 'accessLevel'
      limit v_exact_cutover + 1
    ) as candidate;
  end if;

  v_candidate_count := coalesce(
    pg_catalog.cardinality(v_candidate_ids),
    0
  );

  if v_indexed_probe
     and v_candidate_count <= v_exact_cutover then
    return query
    with candidate_keys as materialized (
      select
        v_candidate_ids[key.ordinal] as id,
        v_candidate_versions[key.ordinal] as version
      from pg_catalog.generate_subscripts(
        v_candidate_ids,
        1
      ) as key(ordinal)
    ), nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) p_query_embedding
          as distance
      from candidate_keys as candidate
      join public.processes as source
        on source.id = candidate.id
       and source.version::text = candidate.version
      join private.portal_catalog_search_rows_v1 as projection
        on projection.dataset_kind = 'process'
       and projection.id = candidate.id
       and projection.version = candidate.version
      where source.state_code in (100, 200)
        and source.embedding_ft is not null
        and projection.state_code in (100, 200)
        and private.portal_card_matches_filters_v2(
          projection.card,
          p_filters
        )
      -- The no-op addition deliberately prevents the global HNSW index from
      -- satisfying this ORDER BY.  Only the bounded exact-key rows are scored.
      order by
        (
          source.embedding_ft operator(extensions.<=>) p_query_embedding
        ) + 0::double precision,
        source.id,
        source.version::text desc
      limit 200
    )
    select nearest.id, nearest.version, nearest.distance
    from nearest
    where nearest.distance >= 0::double precision
      and nearest.distance <= 0.5::double precision
    order by
      nearest.distance + 0::double precision,
      nearest.id,
      nearest.version desc;
    return;
  end if;

  -- Unfiltered, unsupported-filter-only, and broad indexed-filter requests
  -- retain the predecessor HNSW path byte-for-byte.
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

ALTER FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_projection_semantic_process_v2"("p_query_embedding" "extensions"."vector", "p_filters" "jsonb") TO "api_internal_executor";
