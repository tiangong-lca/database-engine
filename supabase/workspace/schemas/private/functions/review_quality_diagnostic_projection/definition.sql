CREATE OR REPLACE FUNCTION "private"."review_quality_diagnostic_projection"("p_job" "private"."worker_jobs") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select jsonb_strip_nulls(jsonb_build_object(
    'runId', (p_job).id,
    'status', (p_job).status,
    'outcome', (p_job).result_json->>'outcome',
    'requestedBy', (p_job).requested_by,
    'requestedAt', (p_job).created_at,
    'startedAt', (p_job).started_at,
    'finishedAt', (p_job).finished_at,
    'updatedAt', (p_job).updated_at,
    'reportSchemaVersion', (p_job).result_schema_version,
    'report', (p_job).result_json,
    'error', case
      when (p_job).error_code is null and (p_job).error_message is null
        then null
      else jsonb_strip_nulls(jsonb_build_object(
        'code', (p_job).error_code,
        'message', (p_job).error_message
      ))
    end
  ))
$$;

ALTER FUNCTION "private"."review_quality_diagnostic_projection"("p_job" "private"."worker_jobs") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_quality_diagnostic_projection"("p_job" "private"."worker_jobs") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_quality_diagnostic_projection"("p_job" "private"."worker_jobs") TO "api_internal_executor";
