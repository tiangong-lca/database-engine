CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_sha256"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
  select encode(
    extensions.digest(
      convert_to(private.dataset_alias_canonical_jsonb_v1(p_value), 'UTF8'),
      'sha256'
    ),
    'hex'
  )
$$;

ALTER FUNCTION "util"."dataset_flow_identity_sha256"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_sha256"("p_value" "jsonb") FROM PUBLIC;
