CREATE OR REPLACE FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") RETURNS bigint
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'queueName', p_queue_name,
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use public.worker_enqueue_job with an lca.* job_kind.';
end;
$$;

ALTER FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") TO "service_role";
