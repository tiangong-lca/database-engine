CREATE OR REPLACE FUNCTION "private"."portal_projection_hybrid_candidates_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb") RETURNS TABLE("id" "uuid", "version" "text", "lexical_rank" integer, "semantic_rank" integer, "semantic_distance" double precision, "score" numeric)
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
  with lexical_counts as materialized (
    select match.id, match.version, pg_catalog.count(distinct match.term_ordinal)::integer as hit_count
    from private.catalog_portal_hybrid_pattern_matches_v1(p_kind,p_query_terms) as match
    join private.portal_catalog_search_rows_v1 as projection
      on projection.dataset_kind = p_kind and projection.id = match.id
        and projection.version = match.version
    where projection.state_code in (100,200)
      and (p_filters = '{}'::jsonb or private.portal_card_matches_filters_v2(projection.card,p_filters))
    group by match.id, match.version
  ), lexical_candidates as materialized (
    select * from lexical_counts
    where hit_count > 0
    order by hit_count desc, id, version desc
    limit 200
  ), lexical as materialized (
    select candidate.*,
      pg_catalog.row_number() over(order by hit_count desc,id,version desc)::integer as ordinal
    from lexical_candidates as candidate
  ), semantic as materialized (
    select candidate.*,
      pg_catalog.row_number() over(order by semantic_distance,id,version desc)::integer as ordinal
    from private.portal_projection_semantic_candidates_v2(p_kind,p_query_embedding,p_filters) as candidate
  )
  select coalesce(lexical.id,semantic.id), coalesce(lexical.version,semantic.version),
    lexical.ordinal, semantic.ordinal, semantic.semantic_distance,
    pg_catalog.round(least(1::numeric,greatest(0::numeric,(
      coalesce(0.5::numeric / (60 + lexical.ordinal),0::numeric)
      + coalesce(0.5::numeric / (60 + semantic.ordinal),0::numeric)
    ) * 61::numeric)),12)
  from lexical full outer join semantic
    on semantic.id = lexical.id and semantic.version = lexical.version;
$$;

ALTER FUNCTION "private"."portal_projection_hybrid_candidates_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."portal_projection_hybrid_candidates_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_projection_hybrid_candidates_v2"("p_kind" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_filters" "jsonb") TO "portal_public_executor";
