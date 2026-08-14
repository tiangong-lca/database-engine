CREATE OR REPLACE FUNCTION "util"."process_dataset_review_submit_jobs"("batch_size" integer DEFAULT 10, "stale_submitting_seconds" integer DEFAULT 300, "timeout_milliseconds" integer DEFAULT 60000) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  if coalesce(batch_size, 0) <= 0 then
    return;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.process_dataset_review_submit_jobs')) then
    return;
  end if;

  perform util.invoke_edge_function(
    name => 'process_dataset_review_submit_jobs',
    body => jsonb_build_object(
      'batchSize', least(greatest(batch_size, 1), 50),
      'staleSubmittingSeconds', least(greatest(coalesce(stale_submitting_seconds, 300), 1), 3600)
    ),
    timeout_milliseconds => least(greatest(coalesce(timeout_milliseconds, 60000), 1000), 300000)
  );
end;
$$;

ALTER FUNCTION "util"."process_dataset_review_submit_jobs"("batch_size" integer, "stale_submitting_seconds" integer, "timeout_milliseconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."process_dataset_review_submit_jobs"("batch_size" integer, "stale_submitting_seconds" integer, "timeout_milliseconds" integer) FROM PUBLIC;
