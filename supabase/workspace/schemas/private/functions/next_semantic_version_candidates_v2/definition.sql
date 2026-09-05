CREATE OR REPLACE FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision, "semantic_route" "text", "candidate_population" integer)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
begin
  if p_data_source in ('tg', 'co') then
    return query
    select candidate.*
    from private.next_public_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_team_id
    ) as candidate;
    return;
  end if;

  if p_data_source in ('my', 'te') then
    return query
    select candidate.*
    from private.next_actor_semantic_version_candidates_v2(
      p_kind, p_query_embedding, p_residual_filter, p_process_type,
      p_flow_types, p_as_input, p_classification_codes, p_elementary_codes,
      p_data_source, p_state_code, p_team_id
    ) as candidate;
    return;
  end if;

  raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
end;
$$;

ALTER FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;
