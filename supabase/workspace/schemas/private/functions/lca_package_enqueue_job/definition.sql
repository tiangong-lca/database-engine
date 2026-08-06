CREATE OR REPLACE FUNCTION "private"."lca_package_enqueue_job"("p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca package pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use private.worker_enqueue_job with a tidas.* job_kind.';
end;
$$;

ALTER FUNCTION "private"."lca_package_enqueue_job"("p_message" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_package_enqueue_job"("p_message" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_package_enqueue_job"("p_message" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."lca_package_enqueue_job"("p_message" "jsonb") TO "api_internal_executor";
