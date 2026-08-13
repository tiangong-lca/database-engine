CREATE OR REPLACE FUNCTION "util"."list_lca_snapshot_gc_candidates"("p_preview_retention" interval DEFAULT '30 days'::interval, "p_draft_retention" interval DEFAULT '7 days'::interval, "p_as_of" timestamp with time zone DEFAULT "now"(), "p_limit" integer DEFAULT 500, "p_keep_latest_per_scope" integer DEFAULT 1, "p_max_total_bytes" bigint DEFAULT NULL::bigint) RETURNS TABLE("candidate_type" "text", "snapshot_id" "uuid", "snapshot_directory" "text", "bucket_id" "text", "object_name" "text", "storage_bytes" bigint, "reason" "text", "delete_db_snapshot" boolean, "snapshot_status" "text", "snapshot_created_at" timestamp with time zone, "snapshot_updated_at" timestamp with time zone, "effective_expires_at" timestamp with time zone, "object_count" bigint, "snapshot_storage_bytes" bigint, "downstream_active_count" bigint, "downstream_job_count" bigint, "downstream_result_count" bigint, "downstream_cache_count" bigint, "downstream_latest_count" bigint, "downstream_factorization_count" bigint, "downstream_artifact_count" bigint)
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select candidate.*
  from util.list_lca_snapshot_gc_candidates_without_closure_protection(
    p_preview_retention,
    p_draft_retention,
    p_as_of,
    p_limit,
    p_keep_latest_per_scope,
    p_max_total_bytes
  ) candidate
  where not exists (
    select 1
    from private.lcia_scope_closure_checks closure_check
    where closure_check.snapshot_id = candidate.snapshot_id
      and closure_check.status = 'passed'
      and closure_check.scan_completeness = 'complete'
      and closure_check.certificate_status = 'valid'
  )
  and not exists (
    select 1
    from private.lcia_result_packages package
    where package.snapshot_id = candidate.snapshot_id
  )
  and not exists (
    select 1
    from private.lca_results result
    where result.snapshot_id = candidate.snapshot_id
  )
$$;

ALTER FUNCTION "util"."list_lca_snapshot_gc_candidates"("p_preview_retention" interval, "p_draft_retention" interval, "p_as_of" timestamp with time zone, "p_limit" integer, "p_keep_latest_per_scope" integer, "p_max_total_bytes" bigint) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."list_lca_snapshot_gc_candidates"("p_preview_retention" interval, "p_draft_retention" interval, "p_as_of" timestamp with time zone, "p_limit" integer, "p_keep_latest_per_scope" integer, "p_max_total_bytes" bigint) FROM PUBLIC;

GRANT ALL ON FUNCTION "util"."list_lca_snapshot_gc_candidates"("p_preview_retention" interval, "p_draft_retention" interval, "p_as_of" timestamp with time zone, "p_limit" integer, "p_keep_latest_per_scope" integer, "p_max_total_bytes" bigint) TO "service_role";
