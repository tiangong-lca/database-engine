CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb" DEFAULT NULL::"jsonb", "p_blocking_reasons" "jsonb" DEFAULT '[]'::"jsonb", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_status text := lower(trim(coalesce(p_status, '')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_status text;
  v_worker_result jsonb;
  v_blocker_codes text[];
begin
  if v_status not in ('passed', 'blocked', 'error') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT_STATUS',
      'status', 400,
      'message', 'result status must be passed, blocked, or error'
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  if p_calculator_report is not null and jsonb_typeof(p_calculator_report) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_REPORT',
      'status', 400,
      'message', 'calculator report must be a JSON object'
    );
  end if;

  if jsonb_typeof(coalesce(p_blocking_reasons, '[]'::jsonb)) <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_BLOCKING_REASONS',
      'status', 400,
      'message', 'blockingReasons must be a JSON array'
    );
  end if;

  if v_status = 'passed' and jsonb_array_length(coalesce(p_blocking_reasons, '[]'::jsonb)) > 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_RESULT',
      'status', 400,
      'message', 'passed gate results cannot include blockingReasons'
    );
  end if;

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

  update public.dataset_review_submit_gate_runs
    set status = v_status,
        calculator_report = p_calculator_report,
        blocking_reasons = coalesce(p_blocking_reasons, '[]'::jsonb),
        report_schema_version = p_report_schema_version,
        modified_at = now(),
        completed_at = now()
  where id = p_gate_run_id
  returning *
    into v_run;

  if v_run.worker_job_id is not null then
    v_worker_status := case
      when v_status = 'passed' then 'completed'
      when v_status = 'blocked' then 'blocked'
      else 'failed'
    end;

    select coalesce(
      array_agg(distinct nullif(reason->>'code', '')) filter (where nullif(reason->>'code', '') is not null),
      '{}'::text[]
    )
      into v_blocker_codes
    from jsonb_array_elements(coalesce(p_blocking_reasons, '[]'::jsonb)) as reason;

    if v_worker_status = 'blocked' and cardinality(v_blocker_codes) = 0 then
      v_blocker_codes := array['review_submit_gate_blocked'];
    end if;

    v_worker_result := jsonb_strip_nulls(
      jsonb_build_object(
        'status', v_status,
        'datasetRevision', jsonb_build_object(
          'table', v_run.dataset_table,
          'id', v_run.dataset_id,
          'version', v_run.dataset_version,
          'revisionChecksum', v_run.revision_checksum
        ),
        'policy', jsonb_build_object(
          'profile', v_run.policy_profile,
          'reportSchemaVersion', p_report_schema_version
        ),
        'calculatorReport', p_calculator_report,
        'blockingReasons', coalesce(p_blocking_reasons, '[]'::jsonb),
        'gateRunId', v_run.id,
        'recordedBy', 'cmd_dataset_review_submit_gate_record_result'
      )
    );

    update public.worker_jobs
      set status = v_worker_status,
          result_json = case
            when v_worker_status in ('completed', 'blocked') then v_worker_result
            else result_json
          end,
          result_schema_version = coalesce(result_schema_version, 'review_submit.gate.result.v1'),
          error_code = case
            when v_worker_status = 'failed' then 'REVIEW_SUBMIT_GATE_ERROR'
            else null
          end,
          error_message = case
            when v_worker_status = 'failed' then 'Review-submit gate failed before review submission'
            else null
          end,
          blocker_codes = case
            when v_worker_status = 'blocked' then v_blocker_codes
            else '{}'::text[]
          end,
          resolution_scope = case
            when v_worker_status = 'blocked' then 'user'
            else null
          end,
          retryable = case
            when v_worker_status = 'failed' then true
            when v_worker_status in ('completed', 'blocked') then false
            else retryable
          end,
          updated_at = now(),
          finished_at = now()
    where id = v_run.worker_job_id;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_run.worker_job_id,
      'legacy_gate_result_recorded',
      v_worker_status,
      jsonb_build_object(
        'gateRunId', v_run.id,
        'gateStatus', v_status,
        'source', 'cmd_dataset_review_submit_gate_record_result'
      )
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate_record_result',
    coalesce(v_actor, v_run.requested_by),
    v_run.dataset_table,
    v_run.dataset_id,
    v_run.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'status', v_status,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_gate_payload(v_run)
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate_record_result"("p_gate_run_id" "uuid", "p_status" "text", "p_calculator_report" "jsonb", "p_blocking_reasons" "jsonb", "p_report_schema_version" "text", "p_audit" "jsonb") TO "service_role";
