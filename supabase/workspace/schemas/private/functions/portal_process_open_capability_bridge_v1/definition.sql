CREATE OR REPLACE FUNCTION "private"."portal_process_open_capability_bridge_v1"("p_state_code" integer, "p_json" "jsonb") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select coalesce((
    private.portal_capabilities_v1('process', p_state_code, p_json)
      ->> 'exchangesVisible'
  )::boolean, false)
$$;

ALTER FUNCTION "private"."portal_process_open_capability_bridge_v1"("p_state_code" integer, "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_process_open_capability_bridge_v1"("p_state_code" integer, "p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."portal_process_open_capability_bridge_v1"("p_state_code" integer, "p_json" "jsonb") TO "postgres";
