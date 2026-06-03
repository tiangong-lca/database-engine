COMMENT ON FUNCTION "public"."lca_enqueue_job"("p_queue_name" "text", "p_message" "jsonb") IS 'Disabled legacy LCA pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';
