CREATE OR REPLACE FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case jsonb_typeof(p_value)
    when 'array' then coalesce(p_value, '[]'::jsonb)
    when 'object' then jsonb_build_array(p_value)
    when 'string' then jsonb_build_array(p_value)
    when 'number' then jsonb_build_array(p_value)
    when 'boolean' then jsonb_build_array(p_value)
    else '[]'::jsonb
  end
$$;

ALTER FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_json_array"("p_value" "jsonb") TO "service_role";
