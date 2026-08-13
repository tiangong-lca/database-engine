CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select
    (p_check).status = 'passed'
    and (p_check).scan_completeness = 'complete'
    and (p_check).certificate_status = 'valid'
    and (p_check).valid_until > now()
    and exists (
      select 1
      from private.worker_job_artifacts report
      cross join private.worker_job_artifacts machine_result
      cross join private.worker_job_artifacts bundle
      where report.id = (p_check).report_artifact_id
        and report.job_id = (p_check).worker_job_id
        and report.artifact_role = 'closure_report'
        and report.lifecycle_state = 'ready'
        and report.expires_at > now()
        and report.storage_bucket is not null
        and report.storage_path is not null
        and report.checksum_sha256 is not null
        and machine_result.id =
          (p_check).complete_machine_result_artifact_id
        and (
          machine_result.job_id = (p_check).worker_job_id
          or exists (
            select 1
            from private.lcia_scope_closure_checks source
            where source.id = (p_check).reused_from_check_id
              and source.worker_job_id = machine_result.job_id
          )
        )
        and machine_result.artifact_role = 'complete_machine_result'
        and machine_result.lifecycle_state = 'ready'
        and machine_result.expires_at > now()
        and machine_result.storage_bucket is not null
        and machine_result.storage_path is not null
        and machine_result.checksum_sha256 is not null
        and private.lcia_scope_closure_bundle_binding_matches(p_check, bundle)
        and bundle.lifecycle_state = 'ready'
        and bundle.expires_at > now()
        and bundle.storage_bucket is not null
        and bundle.storage_path is not null
        and bundle.checksum_sha256 is not null
    )
$$;

ALTER FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_evidence_usable"("p_check" "private"."lcia_scope_closure_checks") TO "api_internal_executor";
