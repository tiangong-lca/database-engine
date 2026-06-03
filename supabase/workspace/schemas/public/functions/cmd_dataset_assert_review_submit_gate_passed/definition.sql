CREATE OR REPLACE FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_revision_checksum" "text" DEFAULT NULL::"text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_result_checksum text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object('ok', true);
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if p_gate_run_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_REQUIRED',
      'status', 400,
      'message', 'A passed review-submit gate run is required before process review submission'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = p_gate_run_id;

  if v_run.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit gate run not found'
    );
  end if;

  v_payload := public.cmd_dataset_review_submit_gate_payload(v_run);

  if v_run.dataset_table <> p_table
    or v_run.dataset_id <> p_id
    or v_run.dataset_version <> p_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run belongs to a different dataset revision'
    );
  end if;

  if v_run.revision_checksum <> p_revision_checksum then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_STALE',
      'status', 409,
      'message', 'Review-submit gate run is stale for the submitted dataset revision',
      'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
    );
  end if;

  if v_run.policy_profile <> coalesce(p_policy_profile, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different policy profile',
      'details', jsonb_build_object(
        'expected', p_policy_profile,
        'actual', v_run.policy_profile
      )
    );
  end if;

  if v_run.report_schema_version <> coalesce(p_report_schema_version, '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_MISMATCH',
      'status', 409,
      'message', 'Review-submit gate run used a different report schema version',
      'details', jsonb_build_object(
        'expected', p_report_schema_version,
        'actual', v_run.report_schema_version
      )
    );
  end if;

  if v_run.worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_run.worker_job_id;

    if v_worker_job.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', v_payload
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_run.dataset_table
      or v_worker_job.subject_id <> v_run.dataset_id
      or v_worker_job.subject_version <> v_run.dataset_version
      or v_worker_job.requested_by is distinct from v_run.requested_by
      or v_worker_job.payload_json #>> '{datasetRevision,revisionChecksum}' is distinct from v_run.revision_checksum
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_run.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_run.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this gate run',
        'details', v_payload
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    end if;

    if v_worker_job.status = 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    end if;

    if v_worker_job.status <> 'completed' then
      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', v_payload
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', v_payload
      );
    end if;

    v_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';
    if v_result_checksum is not null and v_result_checksum <> p_revision_checksum then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker result is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'data', v_payload
    );
  end if;

  case v_run.status
    when 'passed' then
      return jsonb_build_object(
        'ok', true,
        'data', v_payload
      );
    when 'blocked' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', v_payload
      );
    when 'queued', 'running' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', v_payload
      );
    when 'error' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate failed before review submission',
        'details', v_payload
      );
    else
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate run is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_gate_payload(v_run, 'stale')
      );
  end case;
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") TO "service_role";

GRANT ALL ON FUNCTION "public"."cmd_dataset_assert_review_submit_gate_passed"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_gate_run_id" "uuid", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text") TO "authenticated";
