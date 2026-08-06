CREATE OR REPLACE FUNCTION "private"."worker_job_payload"("p_job" "private"."worker_jobs", "p_include_internal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select coalesce(
    jsonb_object_agg(entry.key, entry.value)
      filter (where entry.value <> 'null'::jsonb),
    '{}'::jsonb
  )
  from jsonb_each(
    jsonb_build_object(
      'id', (p_job).id,
      'jobKind', (p_job).job_kind,
      'workerRuntime', (p_job).worker_runtime,
      'workerQueue', (p_job).worker_queue,
      'priority', (p_job).priority,
      'queueKey', (p_job).queue_key,
      'rootJobId', (p_job).root_job_id,
      'parentJobId', (p_job).parent_job_id,
      'subjectType', (p_job).subject_type,
      'subjectId', (p_job).subject_id,
      'subjectVersion', (p_job).subject_version,
      'requesterType', (p_job).requester_type,
      'requestedBy', (p_job).requested_by,
      'teamId', (p_job).team_id,
      'idempotencyKey', (p_job).idempotency_key,
      'requestHash', (p_job).request_hash,
      'concurrencyKey', (p_job).concurrency_key,
      'status', (p_job).status,
      'phase', (p_job).phase,
      'progress', (p_job).progress,
      'visibility', (p_job).visibility,
      'runAfter', to_jsonb((p_job).run_after),
      'attemptCount', (p_job).attempt_count,
      'maxAttempts', (p_job).max_attempts,
      'leasedBy', case when p_include_internal then (p_job).leased_by else null end,
      'leaseToken', case when p_include_internal then (p_job).lease_token else null end,
      'leaseExpiresAt', case when p_include_internal then to_jsonb((p_job).lease_expires_at) else null end,
      'heartbeatAt', to_jsonb((p_job).heartbeat_at),
      'timeoutAt', to_jsonb((p_job).timeout_at),
      'payloadSchemaVersion', (p_job).payload_schema_version,
      'payload', case when p_include_internal then (p_job).payload_json else null end,
      'payloadRef', case when p_include_internal then (p_job).payload_ref else null end,
      'resultSchemaVersion', (p_job).result_schema_version,
      'result', (p_job).result_json,
      'resultRef', case when p_include_internal then (p_job).result_ref else null end,
      'diagnostics', case when p_include_internal then (p_job).diagnostics else null end,
      'errorCode', (p_job).error_code,
      'errorMessage', (p_job).error_message,
      'errorDetails', case when p_include_internal then (p_job).error_details else null end,
      'blockerCodes', to_jsonb((p_job).blocker_codes),
      'resolutionScope', (p_job).resolution_scope,
      'retryable', (p_job).retryable,
      'createdAt', to_jsonb((p_job).created_at),
      'updatedAt', to_jsonb((p_job).updated_at),
      'startedAt', to_jsonb((p_job).started_at),
      'finishedAt', to_jsonb((p_job).finished_at),
      'expiresAt', to_jsonb((p_job).expires_at),
      'cancelledAt', to_jsonb((p_job).cancelled_at),
      'cancelledBy', (p_job).cancelled_by
    )
  ) as entry(key, value)
$$;

ALTER FUNCTION "private"."worker_job_payload"("p_job" "private"."worker_jobs", "p_include_internal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."worker_job_payload"("p_job" "private"."worker_jobs", "p_include_internal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."worker_job_payload"("p_job" "private"."worker_jobs", "p_include_internal" boolean) TO "service_role";

GRANT ALL ON FUNCTION "private"."worker_job_payload"("p_job" "private"."worker_jobs", "p_include_internal" boolean) TO "api_internal_executor";
