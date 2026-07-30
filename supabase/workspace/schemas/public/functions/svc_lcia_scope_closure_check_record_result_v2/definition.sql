CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb" DEFAULT '{}'::"jsonb", "p_issues" "jsonb" DEFAULT '[]'::"jsonb", "p_blocker_codes" "text"[] DEFAULT '{}'::"text"[], "p_report_artifact_id" "uuid" DEFAULT NULL::"uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  if lower(trim(coalesce(p_status, ''))) = 'passed' then
    return public.lcia_scope_closure_error(
      'closure_snapshot_evidence_v3_required', 409,
      'Passed closure checks require database-verified numerical snapshot evidence through record_result_v3'
    );
  end if;
  return public.svc_lcia_scope_closure_check_record_result_v2_legacy(
    p_closure_check_id,
    p_job_id,
    p_lease_token,
    p_status,
    p_scan_completeness,
    p_effective_scope_manifest,
    coalesce(p_evidence, '{}'::jsonb)
      - array['snapshotId', 'snapshotHash', 'snapshotArtifactId',
              'snapshotIndexSha256', 'snapshotBuildContractHash'],
    p_result_summary,
    p_issues,
    p_blocker_codes,
    p_report_artifact_id
  );
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_check_record_result_v2"("p_closure_check_id" "uuid", "p_job_id" "uuid", "p_lease_token" "uuid", "p_status" "text", "p_scan_completeness" "text", "p_effective_scope_manifest" "jsonb", "p_evidence" "jsonb", "p_result_summary" "jsonb", "p_issues" "jsonb", "p_blocker_codes" "text"[], "p_report_artifact_id" "uuid") TO "service_role";
