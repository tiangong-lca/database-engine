CREATE OR REPLACE FUNCTION "private"."next_hybrid_version_keys_v2"("p_kind" "text", "p_query_text" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_match_threshold" double precision, "p_lexical_weight" double precision, "p_semantic_weight" double precision, "p_rrf_k" integer, "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("id" "uuid", "version" "text", "score" double precision, "semantic_route" "text", "semantic_candidate_population" integer, "semantic_fallback_used" boolean)
    LANGUAGE "sql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
  with lexical as materialized (
    select candidate.*
    from private.next_lexical_version_candidates_v2(
      p_kind, p_query_text, p_query_terms, p_residual_filter,
      p_process_type, p_flow_types, p_as_input, p_classification_codes,
      p_elementary_codes, p_data_source, p_state_code, p_team_id
    ) as candidate
    where p_lexical_weight > 0
  ), semantic_raw as materialized (
    select candidate.*
    from private.next_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate
    where p_semantic_weight > 0
  ), semantic_primary as materialized (
    select candidate.*
    from semantic_raw as candidate
    where candidate.distance < 1 - p_match_threshold
  ), semantic_meta as (
    select
      max(candidate.semantic_route) as semantic_route,
      max(candidate.candidate_population) as candidate_population
    from semantic_raw as candidate
  ), primary_fused as materialized (
    select
      coalesce(lexical.id, semantic.id) as id,
      coalesce(lexical.version, semantic.version) as version,
      coalesce(p_lexical_weight / (p_rrf_k + lexical.rank), 0::double precision)
        + coalesce(p_semantic_weight / (p_rrf_k + semantic.rank), 0::double precision)
          as score
    from lexical
    full outer join semantic_primary as semantic
      on semantic.id = lexical.id
     and semantic.version = lexical.version
  ), fallback_fused as materialized (
    select
      coalesce(lexical.id, semantic.id) as id,
      coalesce(lexical.version, semantic.version) as version,
      coalesce(p_lexical_weight / (p_rrf_k + lexical.rank), 0::double precision)
        + coalesce(p_semantic_weight / (p_rrf_k + semantic.rank), 0::double precision)
          as score
    from lexical
    full outer join semantic_raw as semantic
      on semantic.id = lexical.id
     and semantic.version = lexical.version
    where not exists (select 1 from primary_fused)
  ), selected as (
    select primary_fused.*, false as fallback_used from primary_fused
    union all
    select fallback_fused.*, true as fallback_used from fallback_fused
  )
  select
    selected.id,
    selected.version,
    selected.score,
    semantic_meta.semantic_route,
    semantic_meta.candidate_population,
    selected.fallback_used
  from selected
  cross join semantic_meta
  order by selected.score desc, selected.id, selected.version desc;
$$;

ALTER FUNCTION "private"."next_hybrid_version_keys_v2"("p_kind" "text", "p_query_text" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_match_threshold" double precision, "p_lexical_weight" double precision, "p_semantic_weight" double precision, "p_rrf_k" integer, "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_hybrid_version_keys_v2"("p_kind" "text", "p_query_text" "text", "p_query_terms" "text"[], "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_match_threshold" double precision, "p_lexical_weight" double precision, "p_semantic_weight" double precision, "p_rrf_k" integer, "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;
