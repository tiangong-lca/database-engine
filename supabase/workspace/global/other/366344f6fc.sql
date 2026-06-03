COMMENT ON FUNCTION "public"."lca_package_enqueue_job"("p_message" "jsonb") IS 'Disabled legacy TIDAS package pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';
