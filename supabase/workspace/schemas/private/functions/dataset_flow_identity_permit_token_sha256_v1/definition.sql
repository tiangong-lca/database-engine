CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_permit_token_sha256_v1"("p_token" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select pg_catalog.encode(
    extensions.digest(pg_catalog.convert_to(p_token, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

ALTER FUNCTION "private"."dataset_flow_identity_permit_token_sha256_v1"("p_token" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_permit_token_sha256_v1"("p_token" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_permit_token_sha256_v1"("p_token" "text") TO "api_internal_executor";
