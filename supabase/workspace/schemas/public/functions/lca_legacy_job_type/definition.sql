CREATE OR REPLACE FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case lower(trim(coalesce(p_job_kind, '')))
    when 'lca.solve_one' then 'solve_one'
    when 'lca.solve_batch' then 'solve_batch'
    when 'lca.solve_all_unit' then 'solve_all_unit'
    when 'lca.build_snapshot' then 'build_snapshot'
    when 'lca.contribution_path' then 'analyze_contribution_path'
    when 'lca.factorization_prepare' then 'prepare_factorization'
    when 'lca.snapshot_gc' then 'snapshot_gc'
    when 'lca.result_gc' then 'result_gc'
    else null
  end
$$;

ALTER FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_legacy_job_type"("p_job_kind" "text") TO "service_role";
