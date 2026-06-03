CREATE OR REPLACE FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text" DEFAULT 'review_submit_fast.v1'::"text", "p_report_schema_version" "text" DEFAULT 'review_submit_gate_report.v1'::"text", "p_action" "text" DEFAULT 'ensure'::"text", "p_gate_run_id" "uuid" DEFAULT NULL::"uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_action text := lower(trim(coalesce(p_action, 'ensure')));
  v_run public.dataset_review_submit_gate_runs%rowtype;
  v_existing public.dataset_review_submit_gate_runs%rowtype;
  v_supersedes uuid;
  v_link_result jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in ('processes', 'lifecyclemodels') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review-submit gate'
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

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
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

  if v_action not in ('ensure', 'read', 'rerun') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_GATE_ACTION',
      'status', 400,
      'message', 'action must be ensure, read, or rerun'
    );
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
      'message', 'Only the dataset owner can run or read the review-submit gate'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_existing
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id
    for update;

    if v_existing.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_existing.dataset_table <> p_table
      or v_existing.dataset_id <> p_id
      or v_existing.dataset_version <> p_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run belongs to a different dataset revision'
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(
          v_existing,
          case
            when v_existing.revision_checksum <> p_revision_checksum then 'stale'
            else null
          end
        )
      );
    end if;

    if v_action = 'ensure' and v_existing.revision_checksum = p_revision_checksum then
      return public.cmd_dataset_review_submit_gate_link_worker_job(v_existing.id, 'ensure');
    end if;

    v_supersedes := v_existing.id;
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_gate',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version,
        v_actor::text
      ),
      0
    )
  );

  if v_action in ('ensure', 'read') and p_gate_run_id is null then
    select *
      into v_run
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and revision_checksum = p_revision_checksum
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1
    for update;

    if v_run.id is not null then
      if v_action = 'ensure' then
        return public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, 'ensure');
      end if;

      return jsonb_build_object(
        'ok', true,
        'data', public.cmd_dataset_review_submit_gate_payload(v_run)
      );
    end if;

    if v_action = 'read' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;
  end if;

  if v_action = 'rerun' and p_gate_run_id is null then
    select id
      into v_supersedes
    from public.dataset_review_submit_gate_runs
    where dataset_table = p_table
      and dataset_id = p_id
      and dataset_version = p_version
      and policy_profile = p_policy_profile
      and report_schema_version = p_report_schema_version
      and requested_by = v_actor
    order by created_at desc
    limit 1;
  end if;

  insert into public.dataset_review_submit_gate_runs (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    supersedes_gate_run_id
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    'queued',
    v_actor,
    v_supersedes
  )
  returning *
    into v_run;

  v_link_result := public.cmd_dataset_review_submit_gate_link_worker_job(v_run.id, v_action);

  if coalesce((v_link_result->>'ok')::boolean, false) is false then
    return v_link_result;
  end if;

  select *
    into v_run
  from public.dataset_review_submit_gate_runs
  where id = v_run.id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_gate',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', v_action,
      'gate_run_id', v_run.id,
      'worker_job_id', v_run.worker_job_id,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version,
      'supersedes_gate_run_id', v_supersedes
    )
  );

  return v_link_result;
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "public"."cmd_dataset_review_submit_gate"("p_table" "text", "p_id" "uuid", "p_version" "text", "p_revision_checksum" "text", "p_policy_profile" "text", "p_report_schema_version" "text", "p_action" "text", "p_gate_run_id" "uuid", "p_audit" "jsonb") TO "authenticated";
