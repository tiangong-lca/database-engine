CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_role"("p_artifact_type" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select case p_artifact_type
    when 'closure_report_xlsx' then 'closure_report'
    when 'closure_complete_machine_result' then 'complete_machine_result'
    when 'closure_bundle' then 'closure_bundle'
    else null
  end
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_role"("p_artifact_type" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_role"("p_artifact_type" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_role"("p_artifact_type" "text") TO "api_internal_executor";
