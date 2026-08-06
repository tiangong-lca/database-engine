CREATE OR REPLACE FUNCTION "private"."review_scope_checksum_v1"("p_items" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(
        private.review_canonical_json_text_v1(p_items),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$$;

ALTER FUNCTION "private"."review_scope_checksum_v1"("p_items" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_scope_checksum_v1"("p_items" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_scope_checksum_v1"("p_items" "jsonb") TO "api_internal_executor";
