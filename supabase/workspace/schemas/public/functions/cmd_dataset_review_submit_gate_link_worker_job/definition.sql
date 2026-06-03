CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text" DEFAULT 'ensure'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_result jsonb;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_job_id uuid;
  v_status text;
  v_calculator_report jsonb;
  v_blocking_reasons jsonb;
begin
  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id
  for update;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  if v_run.worker_job_id is null and v_run.status in ('queued', 'running') then
    v_worker_result := public.cmd_dataset_review_submit_gate_enqueue_worker_job(
      v_run.dataset_table,
      v_run.dataset_id,
      v_run.dataset_version,
      v_run.revision_checksum,
      v_run.policy_profile,
      v_run.report_schema_version,
      v_run.requested_by,
      v_run.id,
      p_action
    );

    if coalesce((v_worker_result->>'ok')::boolean, false) is false then
      return v_worker_result;
    end if;

    v_worker_job_id := nullif(v_worker_result->'data'->>'id', '')::uuid;

    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_worker_job_id;

    v_status := case
      when v_worker_job.status in ('queued', 'waiting', 'stale') then 'queued'
      when v_worker_job.status = 'running' then 'running'
      when v_worker_job.status = 'blocked' then 'blocked'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'passed'
        then 'passed'
      when v_worker_job.status = 'completed'
        and coalesce(v_worker_job.result_json->>'status', '') = 'blocked'
        then 'blocked'
      when v_worker_job.status in ('completed', 'failed', 'cancelled') then 'error'
      else v_run.status
    end;

    v_calculator_report := case
      when jsonb_typeof(v_worker_job.result_json->'calculatorReport') = 'object'
        then v_worker_job.result_json->'calculatorReport'
      else v_run.calculator_report
    end;

    v_blocking_reasons := coalesce(
      case
        when jsonb_typeof(v_worker_job.result_json->'blockingReasons') = 'array'
          then v_worker_job.result_json->'blockingReasons'
        else null::jsonb
      end,
      v_run.blocking_reasons,
      '[]'::jsonb
    );

    update public.dataset_review_submit_gate_runs
      set worker_job_id = v_worker_job.id,
          status = v_status,
          calculator_report = v_calculator_report,
          blocking_reasons = v_blocking_reasons,
          modified_at = now(),
          completed_at = case
            when v_status in ('passed', 'blocked', 'error') then coalesce(v_worker_job.finished_at, now())
            else completed_at
          end
    where id = v_run.id
    returning *
      into v_run;
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_link_worker_job"("p_gate_run_id" "uuid", "p_action" "text") TO "service_role";
