CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_sha256"("p_value" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_value, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_sha256"("p_value" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_sha256"("p_value" "text") FROM PUBLIC;
