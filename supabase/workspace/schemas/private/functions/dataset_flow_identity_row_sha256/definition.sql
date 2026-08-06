CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_row_sha256"("p_id" "uuid", "p_version" "text", "p_user_id" "uuid", "p_state_code" integer, "p_modified_at" timestamp with time zone, "p_payload_sha256" "text") RETURNS "text"
    LANGUAGE "sql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
  select util.dataset_flow_identity_sha256(jsonb_build_object(
    'id', p_id,
    'version', p_version,
    'user_id', p_user_id,
    'state_code', p_state_code,
    'modified_at', p_modified_at,
    'payload_sha256', p_payload_sha256
  ))
$$;

ALTER FUNCTION "private"."dataset_flow_identity_row_sha256"("p_id" "uuid", "p_version" "text", "p_user_id" "uuid", "p_state_code" integer, "p_modified_at" timestamp with time zone, "p_payload_sha256" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_row_sha256"("p_id" "uuid", "p_version" "text", "p_user_id" "uuid", "p_state_code" integer, "p_modified_at" timestamp with time zone, "p_payload_sha256" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_row_sha256"("p_id" "uuid", "p_version" "text", "p_user_id" "uuid", "p_state_code" integer, "p_modified_at" timestamp with time zone, "p_payload_sha256" "text") TO "api_internal_executor";
