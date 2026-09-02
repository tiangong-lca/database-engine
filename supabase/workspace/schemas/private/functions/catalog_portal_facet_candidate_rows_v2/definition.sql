CREATE OR REPLACE FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") RETURNS TABLE("dataset_kind" "text", "id" "uuid", "version" "text", "card" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '8s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
begin
  if p_kind = 'process' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'flow' then
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  elsif p_kind = 'all' then
    return query
    select 'process'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'process', p_query, p_exact_id, p_like_pattern
    ) as candidate;
    return query
    select 'flow'::text,
      candidate.id,
      candidate.version,
      candidate.card
    from private.catalog_portal_candidate_rows_v2(
      'flow', p_query, p_exact_id, p_like_pattern
    ) as candidate;
  end if;
end
$$;

ALTER FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_facet_candidate_rows_v2"("p_kind" "text", "p_query" "text", "p_exact_id" "uuid", "p_like_pattern" "text") FROM PUBLIC;
