CREATE OR REPLACE FUNCTION "private"."catalog_portal_hybrid_pattern_matches_v1"("p_kind" "text", "p_query_terms" "text"[]) RETURNS TABLE("id" "uuid", "version" "text", "term_ordinal" integer)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '20s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "row_security" TO 'on'
    AS $$
declare
  v_pattern text;
begin
  for v_ordinal in 1..pg_catalog.cardinality(p_query_terms)
  loop
    v_pattern := '%' || pg_catalog.replace(
      pg_catalog.replace(
        pg_catalog.replace(
          p_query_terms[v_ordinal],
          pg_catalog.chr(92),
          pg_catalog.chr(92) || pg_catalog.chr(92)
        ),
        '%',
        pg_catalog.chr(92) || '%'
      ),
      '_',
      pg_catalog.chr(92) || '_'
    ) || '%';
    if p_kind = 'process' then
      return query
      select pattern.id,
        pattern.version,
        v_ordinal
      from private.catalog_portal_process_pattern_versions_v1(
        v_pattern
      ) as pattern;
    elsif p_kind = 'flow' then
      return query
      select pattern.id,
        pattern.version,
        v_ordinal
      from private.catalog_portal_flow_pattern_versions_v1(
        v_pattern
      ) as pattern;
    end if;
  end loop;
end
$$;

ALTER FUNCTION "private"."catalog_portal_hybrid_pattern_matches_v1"("p_kind" "text", "p_query_terms" "text"[]) OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."catalog_portal_hybrid_pattern_matches_v1"("p_kind" "text", "p_query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."catalog_portal_hybrid_pattern_matches_v1"("p_kind" "text", "p_query_terms" "text"[]) TO "api_internal_executor";
