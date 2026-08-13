CREATE OR REPLACE FUNCTION "private"."pgroonga_escape_query_terms"("query_terms" "text"[]) RETURNS "text"[]
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'extensions', 'pg_temp'
    AS $$
  select coalesce(
    array_agg(extensions.pgroonga_query_escape(normalized_term) order by ord),
    '{}'::text[]
  )
  from (
    select regexp_replace(btrim(raw_term.term), '[[:space:]]+', ' ', 'g') as normalized_term,
           raw_term.ord
    from unnest(coalesce(query_terms, '{}'::text[])) with ordinality as raw_term(term, ord)
    where raw_term.term is not null
      and btrim(raw_term.term) <> ''
  ) terms;
$$;

ALTER FUNCTION "private"."pgroonga_escape_query_terms"("query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."pgroonga_escape_query_terms"("query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."pgroonga_escape_query_terms"("query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."pgroonga_escape_query_terms"("query_terms" "text"[]) TO "api_internal_executor";
