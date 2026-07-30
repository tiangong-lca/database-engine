CREATE OR REPLACE FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select jsonb_build_object(
    'ok', false,
    'code', p_code,
    'status', p_status,
    'message', p_message
  )
$$;

ALTER FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."lcia_result_error"("p_code" "text", "p_status" integer, "p_message" "text") TO "service_role";
