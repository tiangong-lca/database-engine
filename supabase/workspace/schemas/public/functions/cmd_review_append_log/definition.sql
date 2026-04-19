CREATE OR REPLACE FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_review_json jsonb := coalesce(p_review_json, '{}'::jsonb);
  v_logs jsonb := public.cmd_review_json_array(v_review_json->'logs');
  v_actor_meta jsonb := public.cmd_review_get_actor_meta(p_actor);
  v_log_entry jsonb;
begin
  v_log_entry := jsonb_build_object(
    'action', p_action,
    'time', to_jsonb(now()),
    'user', v_actor_meta
  ) || coalesce(p_extra, '{}'::jsonb);

  return jsonb_set(
    v_review_json,
    '{logs}',
    v_logs || jsonb_build_array(v_log_entry),
    true
  );
end;
$$;

ALTER FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_append_log"("p_review_json" "jsonb", "p_action" "text", "p_actor" "uuid", "p_extra" "jsonb") TO "service_role";
