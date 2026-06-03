CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text" DEFAULT NULL::"text") RETURNS "jsonb"
    LANGUAGE "sql" STABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  with worker as (
    select w.*
    from public.worker_jobs as w
    where w.id = (p_run).worker_job_id
    limit 1
  ),
  shaped as (
    select
      worker.id as worker_job_id,
      case
        when p_status_override is not null then p_status_override
        when worker.id is null then (p_run).status
        when worker.status in ('queued', 'waiting', 'stale') then 'queued'
        when worker.status = 'running' then 'running'
        when worker.status = 'blocked' then 'blocked'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'passed'
          then 'passed'
        when worker.status = 'completed'
          and coalesce(worker.result_json->>'status', '') = 'blocked'
          then 'blocked'
        when worker.status in ('completed', 'failed', 'cancelled') then 'error'
        else (p_run).status
      end as effective_status,
      case
        when (p_run).calculator_report is not null then
          jsonb_build_object(
            'schemaVersion',
            coalesce((p_run).report_schema_version, worker.result_schema_version)
          ) || (p_run).calculator_report
        when worker.id is not null
          and jsonb_typeof(worker.result_json->'calculatorReport') = 'object'
          then jsonb_build_object(
            'schemaVersion',
            coalesce(
              worker.result_json #>> '{calculatorReport,schemaVersion}',
              (p_run).report_schema_version,
              worker.result_schema_version
            )
          ) || (worker.result_json->'calculatorReport')
        else null::jsonb
      end as effective_calculator_report,
      coalesce(
        case
          when jsonb_typeof(worker.result_json->'blockingReasons') = 'array'
            then worker.result_json->'blockingReasons'
          else null::jsonb
        end,
        case
          when cardinality(worker.blocker_codes) > 0 then (
            select jsonb_agg(jsonb_build_object('code', code) order by code)
            from unnest(worker.blocker_codes) as code
          )
          else null::jsonb
        end,
        (p_run).blocking_reasons,
        '[]'::jsonb
      ) as effective_blocking_reasons,
      greatest(
        (p_run).modified_at,
        coalesce(worker.updated_at, (p_run).modified_at)
      ) as effective_modified_at,
      coalesce(
        (p_run).completed_at,
        case
          when worker.status in ('completed', 'blocked', 'failed', 'cancelled')
            then worker.finished_at
          else null::timestamptz
        end
      ) as effective_completed_at
    from (select 1) as seed
    left join worker on true
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', shaped.effective_status,
      'gateRunId', (p_run).id,
      'workerJobId', shaped.worker_job_id,
      'workerJob',
        case
          when shaped.worker_job_id is null then null
          else (
            select public.worker_job_payload(worker, false)
            from worker
          )
        end,
      'datasetRevision', jsonb_build_object(
        'table', (p_run).dataset_table,
        'id', (p_run).dataset_id,
        'version', (p_run).dataset_version,
        'revisionChecksum', (p_run).revision_checksum
      ),
      'policy', jsonb_build_object(
        'profile', (p_run).policy_profile,
        'reportSchemaVersion', (p_run).report_schema_version
      ),
      'calculatorReport', shaped.effective_calculator_report,
      'blockingReasons', shaped.effective_blocking_reasons,
      'createdAt', to_jsonb((p_run).created_at),
      'modifiedAt', to_jsonb(shaped.effective_modified_at),
      'completedAt', to_jsonb(shaped.effective_completed_at)
    )
  )
  from shaped
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_payload"("p_run" "public"."dataset_review_submit_gate_runs", "p_status_override" "text") TO "service_role";
