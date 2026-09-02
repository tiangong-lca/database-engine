CREATE OR REPLACE FUNCTION "private"."portal_projection_semantic_flow_v1"("p_query_embedding" "extensions"."vector") RETURNS TABLE("id" "uuid", "version" "text", "semantic_distance" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'relaxed_order'
    SET "hnsw.ef_search" TO '1000'
    SET "hnsw.max_scan_tuples" TO '200000'
    SET "hnsw.scan_mem_multiplier" TO '4'
    SET "enable_sort" TO 'off'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_ids uuid[];
  v_versions text[];
  v_distances double precision[];
  v_source_ids uuid[];
  v_source_versions text[];
  v_source_distances double precision[];
  v_source_rows integer;
begin
  if p_query_embedding is null then
    raise exception using
      errcode = '22023',
      message = 'invalid portal semantic query';
  end if;

  select pg_catalog.array_agg(
      candidate.id
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.version
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    ),
    pg_catalog.array_agg(
      candidate.semantic_distance
      order by candidate.semantic_distance, candidate.id, candidate.version desc
    )
  into v_ids, v_versions, v_distances
  from (
    select approximate.id,
      approximate.version,
      approximate.semantic_distance
    from (
      select flow.id,
        flow.version::text as version,
        flow.embedding_ft operator(extensions.<=>) p_query_embedding
          as semantic_distance
      from public.flows as flow
      where flow.state_code in (100, 200)
        and flow.embedding_ft is not null
        and exists (
          select 1
          from private.portal_catalog_search_rows_v1 as projection
          where projection.dataset_kind = 'flow'
            and projection.id = flow.id
            and projection.version = flow.version::text
            and not exists (
              select 1
              from private.portal_catalog_search_rows_v1 as newer
              where newer.dataset_kind = projection.dataset_kind
                and newer.id = projection.id
                and (
                  newer.version > projection.version
                  or (
                    newer.version = projection.version
                    and newer.modified_at > projection.modified_at
                  )
                  or (
                    newer.version = projection.version
                    and newer.modified_at = projection.modified_at
                    and newer.state_code > projection.state_code
                  )
                )
            )
        )
      order by flow.embedding_ft
        operator(extensions.<=>) p_query_embedding
      limit 5000
    ) as approximate
    where approximate.semantic_distance is not null
      and approximate.semantic_distance >= 0::double precision
    order by approximate.semantic_distance + 0::double precision,
      approximate.id,
      approximate.version desc
    limit 200
  ) as candidate;

  if coalesce(pg_catalog.cardinality(v_ids), 0) >= 200 then
    return query
    select v_ids[candidate.ordinal],
      v_versions[candidate.ordinal],
      v_distances[candidate.ordinal]
    from pg_catalog.generate_subscripts(v_ids, 1)
      as candidate(ordinal)
    where v_distances[candidate.ordinal] <= 0.5::double precision
    order by candidate.ordinal;
    return;
  end if;

  select pg_catalog.array_agg(
      bounded_source.id order by bounded_source.id, bounded_source.version desc
    ),
    pg_catalog.array_agg(
      bounded_source.version
      order by bounded_source.id, bounded_source.version desc
    ),
    pg_catalog.array_agg(
      bounded_source.semantic_distance
      order by bounded_source.id, bounded_source.version desc
    )
  into v_source_ids, v_source_versions, v_source_distances
  from (
    select flow.id,
      flow.version::text as version,
      flow.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.flows as flow
    where flow.state_code in (100, 200)
      and flow.embedding_ft is not null
    limit 200
  ) as bounded_source;

  v_source_rows := coalesce(pg_catalog.cardinality(v_source_ids), 0);

  if v_source_rows < 200 then
    return query
    select v_source_ids[source.ordinal],
      v_source_versions[source.ordinal],
      v_source_distances[source.ordinal]
    from pg_catalog.generate_subscripts(v_source_ids, 1)
      as source(ordinal)
    where v_source_distances[source.ordinal] is not null
      and v_source_distances[source.ordinal] >= 0::double precision
      and v_source_distances[source.ordinal] <= 0.5::double precision
      and exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = v_source_ids[source.ordinal]
          and projection.version = v_source_versions[source.ordinal]
          and not exists (
            select 1
            from private.portal_catalog_search_rows_v1 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
        offset 0
      )
    order by v_source_distances[source.ordinal],
      v_source_ids[source.ordinal],
      v_source_versions[source.ordinal] desc;
    return;
  end if;

  return query
  select exact.*
  from private.portal_projection_semantic_flow_exact_v1(
    p_query_embedding
  ) as exact;
  return;
end
$$;

ALTER FUNCTION "private"."portal_projection_semantic_flow_v1"("p_query_embedding" "extensions"."vector") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_semantic_flow_v1"("p_query_embedding" "extensions"."vector") FROM PUBLIC;
