CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_bundle_binding_matches"("p_check" "private"."lcia_scope_closure_checks", "p_bundle" "private"."worker_job_artifacts") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  SELECT coalesce((
    (p_bundle).id = (p_check).closure_bundle_artifact_id
    AND (p_bundle).artifact_role = 'closure_bundle'
    AND (p_bundle).artifact_type = 'closure_bundle'
    AND (p_bundle).checksum_sha256 = (p_check).closure_bundle_hash
    AND (
      (
        (p_check).reused_from_check_id IS NULL
        AND (p_bundle).job_id = (p_check).worker_job_id
        AND coalesce((p_bundle).metadata->>'closureCheckId', '') = (p_check).id::text
      )
      OR EXISTS (
        SELECT 1
        FROM private.lcia_scope_closure_checks source
        WHERE source.id = (p_check).reused_from_check_id
          AND source.reused_from_check_id IS NULL
          AND source.status = 'passed'
          AND source.scan_completeness = 'complete'
          AND source.worker_job_id = (p_bundle).job_id
          AND source.closure_bundle_artifact_id = (p_bundle).id
          AND source.closure_bundle_hash = (p_check).closure_bundle_hash
          AND source.snapshot_id = (p_check).snapshot_id
          AND source.snapshot_hash = (p_check).snapshot_hash
          AND source.snapshot_artifact_id = (p_check).snapshot_artifact_id
          AND source.snapshot_index_sha256 = (p_check).snapshot_index_sha256
          AND source.snapshot_build_contract_hash = (p_check).snapshot_build_contract_hash
          AND source.effective_scope_hash = (p_check).effective_scope_hash
          AND source.requested_scope_hash = (p_check).requested_scope_hash
          AND source.policy_fingerprint = (p_check).policy_fingerprint
          AND source.data_snapshot_token = (p_check).data_snapshot_token
          AND coalesce((p_bundle).metadata->>'closureCheckId', '') = source.id::text
      )
    )
  ), false)
$$;

ALTER FUNCTION "private"."lcia_scope_closure_bundle_binding_matches"("p_check" "private"."lcia_scope_closure_checks", "p_bundle" "private"."worker_job_artifacts") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_bundle_binding_matches"("p_check" "private"."lcia_scope_closure_checks", "p_bundle" "private"."worker_job_artifacts") FROM PUBLIC;
