CREATE OR REPLACE FUNCTION "public"."cmd_review_lifecycle_error"("p_code" "text", "p_path" "text", "p_ref_table" "text" DEFAULT NULL::"text", "p_ref_id" "uuid" DEFAULT NULL::"uuid", "p_ref_version" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', 409,
    'message', case p_code
      when 'MODEL_DEPENDENCY_NOT_PUBLIC'
        then 'Model dependency is not public'
      when 'MODEL_COMPOSITION_POLICY_GAP'
        then 'Model composition sources do not agree'
      else 'Reference role is not configured'
    end,
    'details', jsonb_build_object(
      'path', p_path,
      'reference', jsonb_strip_nulls(jsonb_build_object(
        'table', p_ref_table,
        'id', p_ref_id,
        'version', p_ref_version
      ))
    )
  )
$$;

ALTER FUNCTION "public"."cmd_review_lifecycle_error"("p_code" "text", "p_path" "text", "p_ref_table" "text", "p_ref_id" "uuid", "p_ref_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_lifecycle_error"("p_code" "text", "p_path" "text", "p_ref_table" "text", "p_ref_id" "uuid", "p_ref_version" "text") FROM PUBLIC;
