CREATE OR REPLACE FUNCTION "util"."admit_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid", "p_plan_sha256" "text", "p_operation_id" "text", "p_reason_code" "text", "p_targets" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "lock_timeout" TO '5s'
    AS $_$
declare
  v_command constant text := 'cmd_dataset_derivative_rebuild_plan_guarded';
  v_schema_version constant text := 'dataset-derivative-rebuild-batch.v1';
  v_target jsonb;
  v_snapshot jsonb;
  v_quarantine jsonb;
  v_action jsonb;
  v_table text;
  v_id uuid;
  v_version text;
  v_expected_json_ordered_sha256 text;
  v_baseline_snapshot_sha256 text;
  v_target_count integer;
  v_flow_count integer;
  v_process_count integer;
  v_ordinal integer;
  v_action_id text;
  v_action_request_sha256 text;
  v_plan_request_sha256 text;
  v_summary_audit_id bigint;
  v_action_audit_id bigint;
  v_request_id uuid;
  v_now timestamp with time zone := pg_catalog.clock_timestamp();
  v_child_ids jsonb := '[]'::jsonb;
  v_normalized_targets jsonb;
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_actor_user_id is null
    or p_batch_id is null
    or p_plan_sha256 is null
    or p_plan_sha256 !~ '^[a-f0-9]{64}$'
    or nullif(btrim(p_operation_id), '') is null
    or octet_length(p_operation_id) > 512
    or nullif(btrim(p_reason_code), '') is null
    or octet_length(p_reason_code) > 512
    or jsonb_typeof(p_targets) is distinct from 'array'
    or jsonb_array_length(p_targets) not between 1 and 50
    or pg_column_size(p_targets) > 131072 then
    raise exception using
      errcode = '22023',
      message = 'Invalid bounded derivative rebuild batch request';
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_targets) as target(value)
    where jsonb_typeof(target.value) is distinct from 'object'
      or not (target.value ?& array[
        'table',
        'id',
        'version',
        'expected_json_ordered_sha256',
        'baseline_snapshot_sha256'
      ])
      or exists (
        select 1
        from jsonb_object_keys(target.value) as target_key(key)
        where target_key.key <> all (array[
          'table',
          'id',
          'version',
          'expected_json_ordered_sha256',
          'baseline_snapshot_sha256'
        ])
      )
      or jsonb_typeof(target.value->'table') is distinct from 'string'
      or target.value->>'table' not in ('flows', 'processes')
      or jsonb_typeof(target.value->'id') is distinct from 'string'
      or (target.value->>'id')
        !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or jsonb_typeof(target.value->'version') is distinct from 'string'
      or (target.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
      or jsonb_typeof(target.value->'expected_json_ordered_sha256')
        is distinct from 'string'
      or (target.value->>'expected_json_ordered_sha256')
        !~ '^[a-f0-9]{64}$'
      or jsonb_typeof(target.value->'baseline_snapshot_sha256')
        is distinct from 'string'
      or (target.value->>'baseline_snapshot_sha256')
        !~ '^[a-f0-9]{64}$'
  ) then
    raise exception using
      errcode = '22023',
      message = 'Derivative rebuild batch targets must match the exact schema';
  end if;

  select
    count(*)::integer,
    count(*) filter (where target.value->>'table' = 'flows')::integer,
    count(*) filter (where target.value->>'table' = 'processes')::integer
  into v_target_count, v_flow_count, v_process_count
  from jsonb_array_elements(p_targets) as target(value);

  if (
    select count(*)
    from (
      select distinct
        target.value->>'table' as target_table,
        (target.value->>'id')::uuid as target_id,
        btrim(target.value->>'version') as target_version
      from jsonb_array_elements(p_targets) as target(value)
    ) as unique_target
  ) <> v_target_count then
    raise exception using
      errcode = '22023',
      message = 'Derivative rebuild batch targets must be unique';
  end if;

  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      p_batch_id::text,
      0
    )
  );

  if exists (
    select 1
    from util.dataset_derivative_rebuild_requests as request
    where request.batch_id = p_batch_id
  ) then
    raise exception using
      errcode = '23505',
      message = 'Derivative rebuild batch id has already been admitted';
  end if;

  -- Full validation pass.  No quarantine, audit, proposal, request, or queue
  -- effect happens until every target has passed this loop.  Stable ordered
  -- row locks below avoid adding a database-wide write lock; the protected
  -- alias caller already owns any stronger closure locks required by its own
  -- primary mutation transaction.
  for v_target in
    select target.value || jsonb_build_object('ordinal', target.ordinality)
    from jsonb_array_elements(p_targets) with ordinality as target(value, ordinality)
    order by
      target.value->>'table',
      (target.value->>'id')::uuid,
      btrim(target.value->>'version')
  loop
    v_table := v_target->>'table';
    v_id := (v_target->>'id')::uuid;
    v_version := btrim(v_target->>'version');
    v_expected_json_ordered_sha256 :=
      v_target->>'expected_json_ordered_sha256';

    if v_table = 'flows' then
      v_flow := null;
      select flow.*
      into v_flow
      from public.flows as flow
      where flow.id = v_id
        and btrim(flow.version::text) = v_version
        and flow.user_id = p_actor_user_id
        and flow.state_code = 0
      for update;
      if v_flow.id is not null then
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_flow);
      else
        v_snapshot := null;
      end if;
    else
      v_process := null;
      select process.*
      into v_process
      from public.processes as process
      where process.id = v_id
        and btrim(process.version::text) = v_version
        and process.user_id = p_actor_user_id
        and process.state_code = 0
      for update;
      if v_process.id is not null then
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
      else
        v_snapshot := null;
      end if;
    end if;

    if v_snapshot is null then
      raise exception using
        errcode = 'P0002',
        message = 'Derivative rebuild batch target is not an owner draft',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;

    if v_snapshot->>'json_sha256'
        is distinct from v_snapshot->>'json_ordered_sha256'
      or v_snapshot->>'json_ordered_sha256'
        is distinct from v_expected_json_ordered_sha256 then
      raise exception using
        errcode = '40001',
        message = 'Derivative rebuild batch desired primary hash drifted',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;

    if exists (
      select 1
      from util.dataset_derivative_rebuild_requests as request
      where request.target_table = v_table
        and request.target_id = v_id
        and request.target_version = v_version
        and request.status not in ('completed', 'stale', 'failed')
    ) then
      raise exception using
        errcode = '55006',
        message = 'Derivative rebuild batch target already has an active fence',
        detail = v_table || ':' || v_id::text || '@' || v_version;
    end if;
  end loop;

  select jsonb_agg(
    jsonb_build_object(
      'table', target.value->>'table',
      'id', (target.value->>'id')::uuid,
      'version', btrim(target.value->>'version'),
      'expected_json_ordered_sha256',
        target.value->>'expected_json_ordered_sha256',
      'baseline_snapshot_sha256',
        target.value->>'baseline_snapshot_sha256'
    )
    order by
      target.value->>'table',
      (target.value->>'id')::uuid,
      btrim(target.value->>'version')
  )
  into v_normalized_targets
  from jsonb_array_elements(p_targets) as target(value);

  v_plan_request_sha256 := util.dataset_derivative_rebuild_sha256(
    jsonb_build_object(
      'schema_version', v_schema_version,
      'batch_id', p_batch_id,
      'plan_sha256', p_plan_sha256,
      'operation_id', btrim(p_operation_id),
      'reason_code', btrim(p_reason_code),
      'targets', v_normalized_targets
    )::text
  );

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  ) values (
    v_command,
    p_actor_user_id,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'plan_summary',
      'schema_version', v_schema_version,
      'batch_id', p_batch_id,
      'plan_sha256', p_plan_sha256,
      'operation_id', btrim(p_operation_id),
      'target_visibility', 'owner_draft',
      'plan_request_sha256', v_plan_request_sha256,
      'action_count', v_target_count,
      'accepted_count', v_target_count,
      'flows', v_flow_count,
      'processes', v_process_count,
      'reason_code', btrim(p_reason_code),
      'hash_algorithm', 'postgres-jsonb-text-sha256'
    )
  )
  returning id into v_summary_audit_id;

  for v_target in
    select target.value || jsonb_build_object('ordinal', target.ordinality)
    from jsonb_array_elements(p_targets) with ordinality as target(value, ordinality)
    order by target.ordinality
  loop
    v_table := v_target->>'table';
    v_id := (v_target->>'id')::uuid;
    v_version := btrim(v_target->>'version');
    v_ordinal := (v_target->>'ordinal')::integer;
    v_expected_json_ordered_sha256 :=
      v_target->>'expected_json_ordered_sha256';
    v_baseline_snapshot_sha256 := v_target->>'baseline_snapshot_sha256';
    v_snapshot := util.dataset_derivative_rebuild_snapshot(
      v_table,
      v_id,
      v_version
    );

    if v_snapshot is null
      or v_snapshot->>'user_id' is distinct from p_actor_user_id::text
      or v_snapshot->>'state_code' is distinct from '0'
      or v_snapshot->>'json_sha256'
        is distinct from v_expected_json_ordered_sha256
      or v_snapshot->>'json_ordered_sha256'
        is distinct from v_expected_json_ordered_sha256 then
      raise exception using
        errcode = '40001',
        message = 'Derivative rebuild batch primary changed after validation';
    end if;

    v_quarantine := util.quarantine_dataset_derivative_rebuild_target(
      v_table,
      v_id,
      v_version
    );
    v_request_id := pg_catalog.gen_random_uuid();
    v_action_id := 'batch:' || v_ordinal::text || ':'
      || v_table || ':' || v_id::text || '@' || v_version;
    v_action := jsonb_build_object(
      'schema_version', 'dataset-derivative-rebuild-batch-action.v1',
      'batch_id', p_batch_id,
      'batch_ordinal', v_ordinal,
      'action_id', v_action_id,
      'action', 'rebuild_derivatives',
      'table', v_table,
      'id', v_id,
      'version', v_version,
      'expected_state_code', 0,
      'expected_json_ordered_sha256', v_expected_json_ordered_sha256,
      'baseline_snapshot_sha256', v_baseline_snapshot_sha256,
      'post_write_snapshot_sha256', v_snapshot->>'snapshot_sha256',
      'components', jsonb_build_array('extracted_md', 'embedding_ft'),
      'reason_code', btrim(p_reason_code)
    );
    v_action_request_sha256 := util.dataset_derivative_rebuild_sha256(
      v_action::text
    );

    insert into private.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    ) values (
      v_command,
      p_actor_user_id,
      v_table,
      v_id,
      v_version,
      jsonb_build_object(
        'record_type', 'action',
        'schema_version', v_schema_version,
        'batch_id', p_batch_id,
        'batch_ordinal', v_ordinal,
        'batch_target_count', v_target_count,
        'request_id', v_request_id,
        'plan_sha256', p_plan_sha256,
        'operation_id', btrim(p_operation_id),
        'action_id', v_action_id,
        'target_visibility', 'owner_draft',
        'expected_snapshot_sha256', v_snapshot->>'snapshot_sha256',
        'expected_json_ordered_sha256', v_expected_json_ordered_sha256,
        'baseline_snapshot_sha256', v_baseline_snapshot_sha256,
        'plan_request_sha256', v_plan_request_sha256,
        'action_request_sha256', v_action_request_sha256,
        'reason_code', btrim(p_reason_code),
        'components', jsonb_build_array('extracted_md', 'embedding_ft'),
        'hash_algorithm', 'postgres-jsonb-text-sha256'
      )
    )
    returning id into v_action_audit_id;

    insert into util.dataset_derivative_rebuild_requests (
      id,
      actor_user_id,
      plan_sha256,
      operation_id,
      action_id,
      target_table,
      target_id,
      target_version,
      expected_snapshot_sha256,
      expected_modified_at,
      expected_json_sha256,
      expected_json_ordered_sha256,
      before_extracted_md_sha256,
      before_embedding_ft_sha256,
      before_embedding_ft_at,
      plan_request_sha256,
      action_request_sha256,
      reason_code,
      status,
      phase,
      admitted_at,
      drain_not_before,
      action_audit_id,
      summary_audit_id,
      quarantined_http_requests,
      quarantined_embedding_jobs,
      quarantined_pending_jobs,
      batch_id,
      batch_ordinal,
      batch_target_count,
      source_baseline_snapshot_sha256
    ) values (
      v_request_id,
      p_actor_user_id,
      p_plan_sha256,
      btrim(p_operation_id),
      v_action_id,
      v_table,
      v_id,
      v_version,
      v_snapshot->>'snapshot_sha256',
      (v_snapshot->>'modified_at')::timestamp with time zone,
      v_snapshot->>'json_sha256',
      v_snapshot->>'json_ordered_sha256',
      v_snapshot->>'extracted_md_sha256',
      v_snapshot->>'embedding_ft_sha256',
      (v_snapshot->>'embedding_ft_at')::timestamp with time zone,
      v_plan_request_sha256,
      v_action_request_sha256,
      btrim(p_reason_code),
      'queued',
      'admitted',
      v_now,
      v_now + interval '420 seconds',
      v_action_audit_id,
      v_summary_audit_id,
      coalesce((v_quarantine->>'http_requests')::integer, 0),
      coalesce((v_quarantine->>'embedding_jobs')::integer, 0),
      coalesce((v_quarantine->>'pending_jobs')::integer, 0),
      p_batch_id,
      v_ordinal,
      v_target_count,
      v_baseline_snapshot_sha256
    );

    v_child_ids := v_child_ids || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_ordinal,
        'table', v_table,
        'id', v_id,
        'version', v_version,
        'request_id', v_request_id,
        'expected_snapshot_sha256', v_snapshot->>'snapshot_sha256'
      )
    );
  end loop;

  return jsonb_build_object(
    'ok', true,
    'schema_version', v_schema_version,
    'batch_id', p_batch_id,
    'plan_sha256', p_plan_sha256,
    'operation_id', btrim(p_operation_id),
    'plan_request_sha256', v_plan_request_sha256,
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'flows', v_flow_count,
    'processes', v_process_count,
    'summary_audit_id', v_summary_audit_id::text,
    'child_request_ids', v_child_ids
  );
exception
  when lock_not_available then
    raise exception using
      errcode = '55P03',
      message = 'Derivative rebuild batch write fence could not be acquired';
end;
$_$;

ALTER FUNCTION "util"."admit_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid", "p_plan_sha256" "text", "p_operation_id" "text", "p_reason_code" "text", "p_targets" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."admit_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid", "p_plan_sha256" "text", "p_operation_id" "text", "p_reason_code" "text", "p_targets" "jsonb") FROM PUBLIC;
