CREATE OR REPLACE FUNCTION "api"."cmd_lcia_result_build_request"("p_name" "text", "p_processes" "jsonb" DEFAULT NULL::"jsonb", "p_coverage_mode" "text" DEFAULT 'global_eligible'::"text", "p_default_impact_category" "text" DEFAULT NULL::"text", "p_lcia_method_set" "jsonb" DEFAULT '[]'::"jsonb", "p_idempotency_key" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  if coalesce((select require_certificate_for_builds from private.lcia_scope_closure_config where singleton), false) then
    return api.lcia_scope_closure_error('closure_check_required', 409, 'A valid scope-closure certificate is required');
  end if;
  return private.cmd_lcia_result_build_request_legacy(p_name, p_processes, p_coverage_mode, p_default_impact_category, p_lcia_method_set, p_idempotency_key, p_audit);
end;
$$;

ALTER FUNCTION "api"."cmd_lcia_result_build_request"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_result_build_request"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_build_request"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_build_request"("p_name" "text", "p_processes" "jsonb", "p_coverage_mode" "text", "p_default_impact_category" "text", "p_lcia_method_set" "jsonb", "p_idempotency_key" "text", "p_audit" "jsonb") TO "authenticated";
