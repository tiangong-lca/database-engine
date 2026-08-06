CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_lineage_eligible"("p_check" "private"."lcia_scope_closure_checks", "p_artifact" "private"."worker_job_artifacts", "p_public_artifact_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select case p_public_artifact_role
    when 'closure_report_xlsx' then
      (p_artifact).id = (p_check).report_artifact_id
      and (p_artifact).job_id = (p_check).worker_job_id
      and (p_artifact).artifact_role = 'closure_report'
      and (p_artifact).artifact_type = 'closure_report_xlsx'
    when 'closure_issue_manifest' then
      (p_artifact).id = (p_check).complete_machine_result_artifact_id
      and (p_artifact).artifact_role = 'complete_machine_result'
      and (p_artifact).artifact_type = 'closure_complete_machine_result'
      and (
        (p_artifact).job_id = (p_check).worker_job_id
        or exists (
          select 1
          from private.lcia_scope_closure_checks source_check
          where source_check.id = (p_check).reused_from_check_id
            and source_check.worker_job_id = (p_artifact).job_id
        )
      )
    else false
  end
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_lineage_eligible"("p_check" "private"."lcia_scope_closure_checks", "p_artifact" "private"."worker_job_artifacts", "p_public_artifact_role" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_lineage_eligible"("p_check" "private"."lcia_scope_closure_checks", "p_artifact" "private"."worker_job_artifacts", "p_public_artifact_role" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_lineage_eligible"("p_check" "private"."lcia_scope_closure_checks", "p_artifact" "private"."worker_job_artifacts", "p_public_artifact_role" "text") TO "api_internal_executor";
