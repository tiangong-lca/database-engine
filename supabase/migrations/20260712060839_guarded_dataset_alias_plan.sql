-- The dimension RPC below was introduced by the preceding migration so that
-- its extensive validation could be tested independently.  It is deliberately
-- not an authenticated API: allowing callers to invoke one dimension would
-- make a two-dimension owner-draft plan externally non-atomic.
revoke all on function public.cmd_dataset_alias_batch_guarded(jsonb)
  from public, anon, authenticated, service_role;

comment on function public.cmd_dataset_alias_batch_guarded(jsonb) is
  'Internal owner-draft dimension executor. Direct API execution is revoked; authenticated callers must use cmd_dataset_alias_plan_guarded so time and length_time commit or roll back together.';

create unique index command_audit_log_guarded_alias_plan_summary_replay_idx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'plan_request_sha256')
  )
  where command = 'cmd_dataset_alias_plan_guarded'
    and target_table is null;

create or replace function public.cmd_dataset_alias_plan_guarded(
  p_plan jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
as $$
declare
  v_actor uuid := auth.uid();
  v_schema_version constant text := 'dataset-alias-plan.v1';
  v_batch_schema_version constant text := 'dataset-alias-batch.v1';
  v_command constant text := 'cmd_dataset_alias_plan_guarded';
  v_plan_sha256 text;
  v_operation_id text;
  v_target_visibility text;
  v_batches jsonb;
  v_time_batch jsonb;
  v_length_time_batch jsonb;
  v_batch jsonb;
  v_dimension text;
  v_expected_dimension text;
  v_expected_factor text;
  v_expected_action_count integer;
  v_action_count integer;
  v_owned_action_count integer;
  v_action_distinct_count integer;
  v_batch_request_sha256 text;
  v_time_batch_request_sha256 text;
  v_length_time_batch_request_sha256 text;
  v_plan_request_sha256 text;
  v_time_result jsonb;
  v_length_time_result jsonb;
  v_batch_result jsonb;
  v_failure_result jsonb;
  v_failed_dimension text;
  v_time_replay boolean;
  v_length_time_replay boolean;
  v_time_summary_audit_id bigint;
  v_length_time_summary_audit_id bigint;
  v_plan_summary_audit_id bigint;
  v_existing_plan_audit_count integer;
  v_batch_proof_count integer;
  v_support_owned boolean;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_plan is not null and pg_column_size(p_plan) > 268435456 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_REQUEST_TOO_LARGE',
      'status', 413,
      'message', 'Alias plan request exceeds the 256 MiB database limit'
    );
  end if;

  if jsonb_typeof(p_plan) is distinct from 'object'
    or not (p_plan ?& array[
      'schema_version',
      'plan_sha256',
      'operation_id',
      'target_visibility',
      'batches'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_plan) as plan_key(key)
      where plan_key.key <> all (array[
        'schema_version',
        'plan_sha256',
        'operation_id',
        'target_visibility',
        'batches'
      ])
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_INVALID_REQUEST',
      'status', 400,
      'message', 'Plan request must match dataset-alias-plan.v1 exactly'
    );
  end if;

  if jsonb_typeof(p_plan->'schema_version') is distinct from 'string'
    or p_plan->>'schema_version' <> v_schema_version
    or jsonb_typeof(p_plan->'plan_sha256') is distinct from 'string'
    or (p_plan->>'plan_sha256') !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_plan->'operation_id') is distinct from 'string'
    or nullif(btrim(p_plan->>'operation_id'), '') is null
    or octet_length(p_plan->>'operation_id') > 512
    or jsonb_typeof(p_plan->'target_visibility') is distinct from 'string'
    or p_plan->>'target_visibility' <> 'owner_draft'
    or jsonb_typeof(p_plan->'batches') is distinct from 'array'
    or jsonb_array_length(p_plan->'batches') <> 2 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_INVALID_REQUEST',
      'status', 400,
      'message', 'Plan identity, owner_draft visibility, and exactly two batches are required'
    );
  end if;

  v_plan_sha256 := p_plan->>'plan_sha256';
  v_operation_id := btrim(p_plan->>'operation_id');
  v_target_visibility := p_plan->>'target_visibility';
  v_batches := p_plan->'batches';
  v_time_batch := v_batches->0;
  v_length_time_batch := v_batches->1;

  -- Requiring a stable order makes the whole-plan request hash deterministic
  -- and prevents callers from substituting two copies of one dimension.
  if jsonb_typeof(v_time_batch) is distinct from 'object'
    or jsonb_typeof(v_length_time_batch) is distinct from 'object'
    or v_time_batch->>'dimension' is distinct from 'time'
    or v_length_time_batch->>'dimension' is distinct from 'length_time' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_INVALID_BATCH_SET',
      'status', 400,
      'message', 'batches must contain time followed by length_time exactly once'
    );
  end if;

  -- Validate enough of both batch envelopes to perform a safe, non-locking
  -- ownership preflight.  The internal executor repeats the complete semantic,
  -- payload, closure, hash, and locked-row validation after the broad locks.
  for v_batch, v_expected_dimension, v_expected_factor, v_expected_action_count in
    select *
    from (values
      (v_time_batch, 'time'::text, '0.00011415525114155251'::text, 25),
      (v_length_time_batch, 'length_time'::text, '1000'::text, 27)
    ) as expected(batch, dimension, factor, action_count)
  loop
    if not (v_batch ?& array[
        'schema_version',
        'plan_sha256',
        'operation_id',
        'batch_id',
        'dimension',
        'factor',
        'target_visibility',
        'target',
        'actions'
      ])
      or exists (
        select 1
        from jsonb_object_keys(v_batch) as batch_key(key)
        where batch_key.key <> all (array[
          'schema_version',
          'plan_sha256',
          'operation_id',
          'batch_id',
          'dimension',
          'factor',
          'target_visibility',
          'target',
          'actions'
        ])
      )
      or jsonb_typeof(v_batch->'schema_version') is distinct from 'string'
      or v_batch->>'schema_version' <> v_batch_schema_version
      or jsonb_typeof(v_batch->'plan_sha256') is distinct from 'string'
      or v_batch->>'plan_sha256' is distinct from v_plan_sha256
      or jsonb_typeof(v_batch->'operation_id') is distinct from 'string'
      or btrim(v_batch->>'operation_id') is distinct from v_operation_id
      or jsonb_typeof(v_batch->'batch_id') is distinct from 'string'
      or nullif(btrim(v_batch->>'batch_id'), '') is null
      or octet_length(v_batch->>'batch_id') > 512
      or jsonb_typeof(v_batch->'dimension') is distinct from 'string'
      or v_batch->>'dimension' is distinct from v_expected_dimension
      or jsonb_typeof(v_batch->'factor') is distinct from 'string'
      or v_batch->>'factor' is distinct from v_expected_factor
      or jsonb_typeof(v_batch->'target_visibility') is distinct from 'string'
      or v_batch->>'target_visibility' is distinct from v_target_visibility
      or jsonb_typeof(v_batch->'target') is distinct from 'object'
      or not ((v_batch->'target') ?& array[
        'flowproperty',
        'unitgroup',
        'source_unitgroup'
      ])
      or exists (
        select 1
        from jsonb_object_keys(v_batch->'target') as target_key(key)
        where target_key.key <> all (array[
          'flowproperty',
          'unitgroup',
          'source_unitgroup'
        ])
      )
      or jsonb_typeof(v_batch->'actions') is distinct from 'array'
      or jsonb_array_length(v_batch->'actions') <> v_expected_action_count
      or exists (
        select 1
        from jsonb_array_elements(v_batch->'actions') as action_item(value)
        where jsonb_typeof(action_item.value) is distinct from 'object'
          or jsonb_typeof(action_item.value->'table') is distinct from 'string'
          or action_item.value->>'table' not in (
            'flowproperties', 'flows', 'processes'
          )
          or jsonb_typeof(action_item.value->'id') is distinct from 'string'
          or (action_item.value->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or jsonb_typeof(action_item.value->'version') is distinct from 'string'
          or (action_item.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
          or jsonb_typeof(action_item.value->'expected_state_code') is distinct from 'number'
          or action_item.value->>'expected_state_code' <> '0'
      ) then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_INVALID_BATCH',
        'status', 400,
        'message', 'Both batch envelopes must match the frozen full-plan contract'
      );
    end if;

    if exists (
      select 1
      from (values
        (v_batch#>'{target,flowproperty}'),
        (v_batch#>'{target,unitgroup}'),
        (v_batch#>'{target,source_unitgroup}')
      ) as support_item(value)
      where jsonb_typeof(support_item.value) is distinct from 'object'
        or jsonb_typeof(support_item.value->'id') is distinct from 'string'
        or (support_item.value->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or jsonb_typeof(support_item.value->'version') is distinct from 'string'
        or (support_item.value->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    ) then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_INVALID_BATCH',
        'status', 400,
        'message', 'Both batch envelopes must carry valid support row keys'
      );
    end if;

    -- This preflight intentionally happens before either dimension can reach
    -- the internal executor's SHARE ROW EXCLUSIVE locks.  It is non-locking,
    -- uniform, and non-disclosing; the executor rechecks after taking locks.
    select
      exists (
        select 1
        from public.flowproperties as target_fp
        where target_fp.id = (v_batch#>>'{target,flowproperty,id}')::uuid
          and target_fp.version::text = v_batch#>>'{target,flowproperty,version}'
          and target_fp.user_id = v_actor
          and target_fp.state_code = 0
      )
      and exists (
        select 1
        from public.unitgroups as target_ug
        where target_ug.id = (v_batch#>>'{target,unitgroup,id}')::uuid
          and target_ug.version::text = v_batch#>>'{target,unitgroup,version}'
          and target_ug.user_id = v_actor
          and target_ug.state_code = 0
      )
      and exists (
        select 1
        from public.unitgroups as source_ug
        where source_ug.id = (v_batch#>>'{target,source_unitgroup,id}')::uuid
          and source_ug.version::text = v_batch#>>'{target,source_unitgroup,version}'
          and source_ug.user_id = v_actor
          and source_ug.state_code = 0
      )
    into v_support_owned;

    select count(*)
    into v_owned_action_count
    from jsonb_array_elements(v_batch->'actions') as action_item(value)
    where (
      action_item.value->>'table' = 'flowproperties'
      and exists (
        select 1
        from public.flowproperties as dataset_row
        where dataset_row.id = (action_item.value->>'id')::uuid
          and dataset_row.version::text = action_item.value->>'version'
          and dataset_row.user_id = v_actor
          and dataset_row.state_code = 0
      )
    ) or (
      action_item.value->>'table' = 'flows'
      and exists (
        select 1
        from public.flows as dataset_row
        where dataset_row.id = (action_item.value->>'id')::uuid
          and dataset_row.version::text = action_item.value->>'version'
          and dataset_row.user_id = v_actor
          and dataset_row.state_code = 0
      )
    ) or (
      action_item.value->>'table' = 'processes'
      and exists (
        select 1
        from public.processes as dataset_row
        where dataset_row.id = (action_item.value->>'id')::uuid
          and dataset_row.version::text = action_item.value->>'version'
          and dataset_row.user_id = v_actor
          and dataset_row.state_code = 0
      )
    );

    if not v_support_owned
      or v_owned_action_count <> v_expected_action_count then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_SCOPE_MISMATCH',
        'status', 409,
        'message', 'Full-plan owner-draft scope does not match the authenticated actor'
      );
    end if;
  end loop;

  if v_time_batch->>'batch_id' = v_length_time_batch->>'batch_id' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_INVALID_BATCH_SET',
      'status', 400,
      'message', 'Batch IDs must be distinct'
    );
  end if;

  select count(distinct (
    action_item.value->>'table',
    action_item.value->>'id',
    action_item.value->>'version'
  ))
  into v_action_distinct_count
  from jsonb_array_elements(v_batches) as batch_item(value)
  cross join lateral jsonb_array_elements(
    batch_item.value->'actions'
  ) as action_item(value);

  if v_action_distinct_count <> 52
    or (v_time_batch#>>'{target,flowproperty,id}') =
      (v_length_time_batch#>>'{target,flowproperty,id}')
    or (v_time_batch#>>'{target,flowproperty,id}') in (
      select action_item.value->>'id'
      from jsonb_array_elements(v_batches) as batch_item(value)
      cross join lateral jsonb_array_elements(
        batch_item.value->'actions'
      ) as action_item(value)
      where action_item.value->>'table' = 'flowproperties'
    )
    or (v_length_time_batch#>>'{target,flowproperty,id}') in (
      select action_item.value->>'id'
      from jsonb_array_elements(v_batches) as batch_item(value)
      cross join lateral jsonb_array_elements(
        batch_item.value->'actions'
      ) as action_item(value)
      where action_item.value->>'table' = 'flowproperties'
    )
    or (
      select count(distinct support_ug.id)
      from (values
        (v_time_batch#>>'{target,unitgroup,id}'),
        (v_time_batch#>>'{target,source_unitgroup,id}'),
        (v_length_time_batch#>>'{target,unitgroup,id}'),
        (v_length_time_batch#>>'{target,source_unitgroup,id}')
      ) as support_ug(id)
    ) <> 4 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_PLAN_OVERLAPPING_BATCHES',
      'status', 400,
      'message', 'The two frozen dimensions must use 52 distinct action rows and disjoint support rows'
    );
  end if;

  v_time_batch_request_sha256 := encode(
    extensions.digest(convert_to(v_time_batch::text, 'UTF8'), 'sha256'),
    'hex'
  );
  v_length_time_batch_request_sha256 := encode(
    extensions.digest(convert_to(v_length_time_batch::text, 'UTF8'), 'sha256'),
    'hex'
  );
  v_plan_request_sha256 := encode(
    extensions.digest(convert_to(p_plan::text, 'UTF8'), 'sha256'),
    'hex'
  );

  -- PL/pgSQL exception blocks are subtransactions.  Any returned batch error
  -- or unexpected second-dimension exception is raised inside this block,
  -- caught outside it, and therefore rolls back the first dimension's rows and
  -- audits before a structured plan failure is returned.
  begin
    v_failed_dimension := 'time';
    v_time_result := public.cmd_dataset_alias_batch_guarded(v_time_batch);
    if coalesce((v_time_result->>'ok')::boolean, false) is not true then
      v_failure_result := v_time_result;
      raise exception using
        errcode = 'P0001',
        message = 'Guarded alias time batch rejected';
    end if;

    v_failed_dimension := 'length_time';
    v_length_time_result :=
      public.cmd_dataset_alias_batch_guarded(v_length_time_batch);
    if coalesce((v_length_time_result->>'ok')::boolean, false) is not true then
      v_failure_result := v_length_time_result;
      raise exception using
        errcode = 'P0001',
        message = 'Guarded alias length_time batch rejected';
    end if;

    if v_time_result->>'dimension' is distinct from 'time'
      or v_time_result->>'target_visibility' is distinct from 'owner_draft'
      or v_time_result->>'batch_request_sha256'
        is distinct from v_time_batch_request_sha256
      or v_time_result->>'row_count' is distinct from '25'
      or v_time_result->>'exchange_count' is distinct from '20'
      or (v_time_result->>'summary_audit_id') !~ '^[0-9]+$'
      or jsonb_typeof(v_time_result->'audit') is distinct from 'array'
      or jsonb_array_length(v_time_result->'audit') <> 25
      or v_length_time_result->>'dimension' is distinct from 'length_time'
      or v_length_time_result->>'target_visibility' is distinct from 'owner_draft'
      or v_length_time_result->>'batch_request_sha256'
        is distinct from v_length_time_batch_request_sha256
      or v_length_time_result->>'row_count' is distinct from '27'
      or v_length_time_result->>'exchange_count' is distinct from '39'
      or (v_length_time_result->>'summary_audit_id') !~ '^[0-9]+$'
      or jsonb_typeof(v_length_time_result->'audit') is distinct from 'array'
      or jsonb_array_length(v_length_time_result->'audit') <> 27 then
      v_failure_result := jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_BATCH_PROOF_MISMATCH',
        'status', 409,
        'message', 'A dimension result did not prove the exact frozen batch'
      );
      raise exception using
        errcode = 'P0001',
        message = 'Guarded alias dimension proof mismatch';
    end if;

    v_time_replay := (v_time_result->>'idempotent_replay')::boolean;
    v_length_time_replay :=
      (v_length_time_result->>'idempotent_replay')::boolean;
    v_time_summary_audit_id :=
      (v_time_result->>'summary_audit_id')::bigint;
    v_length_time_summary_audit_id :=
      (v_length_time_result->>'summary_audit_id')::bigint;

    if v_time_replay is distinct from v_length_time_replay then
      v_failure_result := jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_PARTIAL_STATE',
        'status', 409,
        'message', 'The two dimensions are split between fresh and replay states'
      );
      raise exception using
        errcode = 'P0001',
        message = 'Guarded alias plan partial state';
    end if;

    select count(*)
    into v_batch_proof_count
    from public.command_audit_log as batch_audit
    where batch_audit.id in (
        v_time_summary_audit_id,
        v_length_time_summary_audit_id
      )
      and batch_audit.command = 'cmd_dataset_alias_batch_guarded'
      and batch_audit.actor_user_id = v_actor
      and batch_audit.target_table is null
      and batch_audit.payload->>'record_type' = 'batch_summary'
      and (
        (
          batch_audit.id = v_time_summary_audit_id
          and batch_audit.payload->>'dimension' = 'time'
          and batch_audit.payload->>'batch_request_sha256' =
            v_time_batch_request_sha256
        ) or (
          batch_audit.id = v_length_time_summary_audit_id
          and batch_audit.payload->>'dimension' = 'length_time'
          and batch_audit.payload->>'batch_request_sha256' =
            v_length_time_batch_request_sha256
        )
      );

    if v_batch_proof_count <> 2 then
      v_failure_result := jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_PLAN_BATCH_PROOF_MISMATCH',
        'status', 409,
        'message', 'Dimension audit proofs do not match the exact full plan'
      );
      raise exception using
        errcode = 'P0001',
        message = 'Guarded alias dimension audits mismatch';
    end if;

    select count(*)
    into v_existing_plan_audit_count
    from public.command_audit_log as plan_audit
    where plan_audit.command = v_command
      and plan_audit.actor_user_id = v_actor
      and plan_audit.payload->>'plan_request_sha256' = v_plan_request_sha256;

    v_plan_summary_audit_id := null;
    select plan_audit.id
    into v_plan_summary_audit_id
    from public.command_audit_log as plan_audit
    where plan_audit.command = v_command
      and plan_audit.actor_user_id = v_actor
      and plan_audit.target_table is null
      and plan_audit.target_id is null
      and plan_audit.target_version is null
      and plan_audit.payload->>'record_type' = 'plan_summary'
      and plan_audit.payload->>'schema_version' = v_schema_version
      and plan_audit.payload->>'plan_sha256' = v_plan_sha256
      and plan_audit.payload->>'operation_id' = v_operation_id
      and plan_audit.payload->>'target_visibility' = v_target_visibility
      and plan_audit.payload->>'plan_request_sha256' = v_plan_request_sha256
      and plan_audit.payload->>'hash_algorithm' = 'postgres-jsonb-text-sha256'
      and plan_audit.payload->>'time_batch_request_sha256' =
        v_time_batch_request_sha256
      and plan_audit.payload->>'length_time_batch_request_sha256' =
        v_length_time_batch_request_sha256
      and plan_audit.payload->>'time_batch_summary_audit_id' =
        v_time_summary_audit_id::text
      and plan_audit.payload->>'length_time_batch_summary_audit_id' =
        v_length_time_summary_audit_id::text
      and plan_audit.payload->>'batch_count' = '2'
      and plan_audit.payload->>'row_count' = '52'
      and plan_audit.payload->>'exchange_count' = '59'
      and plan_audit.payload->>'support_owner_user_id' = v_actor::text
    order by plan_audit.id desc
    limit 1;

    if v_time_replay then
      if v_existing_plan_audit_count <> 1
        or v_plan_summary_audit_id is null then
        v_failure_result := jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_PLAN_REPLAY_UNPROVEN',
          'status', 409,
          'message', 'Committed dimensions have no exact full-plan audit proof'
        );
        raise exception using
          errcode = 'P0001',
          message = 'Guarded alias plan replay unproven';
      end if;
    else
      if v_existing_plan_audit_count <> 0
        or v_plan_summary_audit_id is not null then
        v_failure_result := jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_PLAN_AUDIT_STATE_CONFLICT',
          'status', 409,
          'message', 'A plan audit exists while both dimensions are fresh'
        );
        raise exception using
          errcode = 'P0001',
          message = 'Guarded alias plan audit state conflict';
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
        v_command,
        v_actor,
        null,
        null,
        null,
        jsonb_build_object(
          'record_type', 'plan_summary',
          'schema_version', v_schema_version,
          'plan_sha256', v_plan_sha256,
          'operation_id', v_operation_id,
          'target_visibility', v_target_visibility,
          'plan_request_sha256', v_plan_request_sha256,
          'hash_algorithm', 'postgres-jsonb-text-sha256',
          'time_batch_request_sha256', v_time_batch_request_sha256,
          'length_time_batch_request_sha256',
            v_length_time_batch_request_sha256,
          'time_batch_summary_audit_id', v_time_summary_audit_id::text,
          'length_time_batch_summary_audit_id',
            v_length_time_summary_audit_id::text,
          'batch_count', 2,
          'row_count', 52,
          'exchange_count', 59,
          'support_owner_user_id', v_actor
        )
      )
      returning id into v_plan_summary_audit_id;
    end if;
  exception
    when others then
      if v_failure_result is null then
        v_failure_result := jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_PLAN_EXECUTION_FAILED',
          'status', 409,
          'message', 'The full owner-draft plan failed and was rolled back'
        );
      end if;
  end;

  if v_failure_result is not null then
    return v_failure_result || jsonb_build_object(
      'failed_dimension', v_failed_dimension,
      'plan_rolled_back', true
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'schema_version', v_schema_version,
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'target_visibility', v_target_visibility,
    'plan_request_sha256', v_plan_request_sha256,
    'batch_count', 2,
    'row_count', 52,
    'exchange_count', 59,
    'summary_audit_id', v_plan_summary_audit_id::text,
    'batches', jsonb_build_array(v_time_result, v_length_time_result),
    'idempotent_replay', v_time_replay
  );
end;
$$;

alter function public.cmd_dataset_alias_plan_guarded(jsonb)
  owner to postgres;

revoke all on function public.cmd_dataset_alias_plan_guarded(jsonb)
  from public, anon, authenticated, service_role;

grant execute on function public.cmd_dataset_alias_plan_guarded(jsonb)
  to authenticated;

comment on function public.cmd_dataset_alias_plan_guarded(jsonb) is
  'Atomically validates, applies, audits, and replays the exact two-dimension owner_draft alias plan (time 25 rows/20 exchanges plus length_time 27 rows/39 exchanges). It performs a non-locking actor-owned state-code-0 preflight before internal closure locks; either all 52 rows and both batch audit sets commit with one plan proof, or all plan effects roll back.';
