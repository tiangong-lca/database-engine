CREATE OR REPLACE FUNCTION "private"."portal_projection_semantic_process_exact_v1"("p_query_embedding" "extensions"."vector") RETURNS TABLE("id" "uuid", "version" "text", "semantic_distance" double precision)
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "work_mem" TO '32MB'
    SET "enable_hashjoin" TO 'on'
    SET "enable_nestloop" TO 'off'
    SET "enable_mergejoin" TO 'off'
    SET "enable_sort" TO 'on'
    SET "max_parallel_workers_per_gather" TO '0'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
  with latest_keys as materialized (
    select distinct on (projection.id)
      projection.id,
      projection.version
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
    order by projection.id,
      projection.version desc,
      projection.modified_at desc,
      projection.state_code desc
  ), eligible as materialized (
    select process.id,
      process.version::text as version,
      process.embedding_ft operator(extensions.<=>) p_query_embedding
        as semantic_distance
    from public.processes as process
    join latest_keys as latest
      on latest.id = process.id
     and process.version = latest.version::character(9)
    where process.state_code in (100, 200)
      and process.embedding_ft is not null
  )
  select eligible.id,
    eligible.version,
    eligible.semantic_distance
  from eligible
  where eligible.semantic_distance is not null
    and eligible.semantic_distance >= 0::double precision
    and eligible.semantic_distance <= 0.5::double precision
  order by eligible.semantic_distance,
    eligible.id,
    eligible.version desc
  limit 200
$$;

ALTER FUNCTION "private"."portal_projection_semantic_process_exact_v1"("p_query_embedding" "extensions"."vector") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_semantic_process_exact_v1"("p_query_embedding" "extensions"."vector") FROM PUBLIC;
