CREATE OR REPLACE FUNCTION "api"."svc_lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select private.lca_read_latest_single_solve_result(
    p_requested_by, p_snapshot_id, p_process_index
  )
$$;

ALTER FUNCTION "api"."svc_lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_lca_read_latest_single_solve_result"("p_requested_by" "uuid", "p_snapshot_id" "uuid", "p_process_index" integer) TO "service_role";
