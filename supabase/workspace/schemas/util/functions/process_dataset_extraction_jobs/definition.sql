CREATE OR REPLACE FUNCTION "util"."process_dataset_extraction_jobs"("batch_size" integer DEFAULT 5, "visibility_timeout_seconds" integer DEFAULT 300, "max_read_count" integer DEFAULT 5, "timeout_milliseconds" integer DEFAULT ((5 * 60) * 1000)) RETURNS "void"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
begin
  if batch_size <= 0 then
    return;
  end if;

  if not pg_try_advisory_xact_lock(hashtext('util.process_dataset_extraction_jobs')) then
    return;
  end if;

  perform util.invoke_edge_function(
    name => 'process_dataset_extraction_jobs',
    body => jsonb_build_object(
      'batchSize', least(batch_size, 50),
      'visibilityTimeoutSeconds', least(greatest(visibility_timeout_seconds, 1), 3600),
      'maxReadCount', least(greatest(max_read_count, 1), 100)
    ),
    timeout_milliseconds => timeout_milliseconds
  );
end;
$$;

ALTER FUNCTION "util"."process_dataset_extraction_jobs"("batch_size" integer, "visibility_timeout_seconds" integer, "max_read_count" integer, "timeout_milliseconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."process_dataset_extraction_jobs"("batch_size" integer, "visibility_timeout_seconds" integer, "max_read_count" integer, "timeout_milliseconds" integer) FROM PUBLIC;
