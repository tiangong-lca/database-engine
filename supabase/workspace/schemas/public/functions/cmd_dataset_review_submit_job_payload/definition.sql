CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with job as (
    select to_jsonb(p_job) as row_json
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', row_json->>'status',
      'reviewSubmitJobId', row_json->'id',
      'submitWorkerJobId', row_json->'submit_worker_job_id',
      'gateRunId', row_json->'gate_run_id',
      'gateWorkerJobId', row_json->'gate_worker_job_id',
      'datasetRevision', jsonb_build_object(
        'table', row_json->>'dataset_table',
        'id', row_json->'dataset_id',
        'version', row_json->>'dataset_version',
        'revisionChecksum', row_json->>'revision_checksum'
      ),
      'policy', jsonb_build_object(
        'profile', row_json->>'policy_profile',
        'reportSchemaVersion', row_json->>'report_schema_version'
      ),
      'requestedBy', row_json->'requested_by',
      'attemptCount', row_json->'attempt_count',
      'error',
        case
          when row_json->>'last_error_code' is null
            and row_json->>'last_error_message' is null
            and row_json->'last_error_details' is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'code', row_json->>'last_error_code',
              'message', row_json->>'last_error_message',
              'details', row_json->'last_error_details'
            )
          )
        end,
      'result', row_json->'result',
      'submitWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'submit_worker_job_id', '')::uuid
        ),
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = nullif(row_json->>'gate_run_id', '')::uuid
        ),
      'gateWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'gate_worker_job_id', '')::uuid
        ),
      'createdAt', row_json->'created_at',
      'modifiedAt', row_json->'modified_at',
      'completedAt', row_json->'completed_at'
    )
  )
  from job
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_job_payload"("p_job" "anyelement") TO "service_role";
