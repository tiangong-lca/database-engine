CREATE OR REPLACE FUNCTION "private"."portal_query_fingerprint_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select encode(
    extensions.digest(
      convert_to(
        jsonb_build_object(
          'kind', p_kind,
          'query', p_query,
          'filters', p_filters,
          'sort', p_sort
        )::text,
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$$;

ALTER FUNCTION "private"."portal_query_fingerprint_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_query_fingerprint_v1"("p_kind" "text", "p_query" "text", "p_filters" "jsonb", "p_sort" "text") FROM PUBLIC;
