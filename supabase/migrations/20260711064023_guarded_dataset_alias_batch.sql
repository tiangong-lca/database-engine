create or replace function private.dataset_alias_canonical_jsonb_v1(
  p_value jsonb
) returns text
language plpgsql
stable
strict
set search_path = ''
as $$
declare
  v_result text;
begin
  case jsonb_typeof(p_value)
    when 'object' then
      select '{' || coalesce(string_agg(
        to_jsonb(object_item.key)::text
          || ':'
          || private.dataset_alias_canonical_jsonb_v1(object_item.value),
        ',' order by object_item.key
      ), '') || '}'
      into v_result
      from jsonb_each(p_value) as object_item(key, value);
    when 'array' then
      select '[' || coalesce(string_agg(
        private.dataset_alias_canonical_jsonb_v1(array_item.value),
        ',' order by array_item.ordinality
      ), '') || ']'
      into v_result
      from jsonb_array_elements(p_value)
        with ordinality as array_item(value, ordinality);
    else
      v_result := p_value::text;
  end case;

  return v_result;
end;
$$;

alter function private.dataset_alias_canonical_jsonb_v1(jsonb)
  owner to postgres;

revoke all on function private.dataset_alias_canonical_jsonb_v1(jsonb)
  from public, anon, authenticated, service_role;

comment on function private.dataset_alias_canonical_jsonb_v1(jsonb) is
  'Serializes JSON with recursively sorted object keys and compact separators for guarded alias exchange evidence hashing.';

create unique index command_audit_log_guarded_alias_batch_row_replay_idx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'batch_request_sha256'),
    target_table,
    target_id,
    target_version,
    (payload ->> 'action_id')
  )
  where command = 'cmd_dataset_alias_batch_guarded'
    and target_table is not null;

create unique index command_audit_log_guarded_alias_batch_summary_replay_idx
  on public.command_audit_log (
    actor_user_id,
    (payload ->> 'batch_request_sha256')
  )
  where command = 'cmd_dataset_alias_batch_guarded'
    and target_table is null;

create or replace function public.cmd_dataset_alias_batch_guarded(
  p_batch jsonb
) returns jsonb
language plpgsql
security definer
set search_path = ''
set lock_timeout = '5s'
as $$
declare
  v_actor uuid := auth.uid();
  v_schema_version constant text := 'dataset-alias-batch.v1';
  v_command constant text := 'cmd_dataset_alias_batch_guarded';
  v_plan_sha256 text;
  v_operation_id text;
  v_batch_id text;
  v_dimension text;
  v_factor text;
  v_target_visibility text;
  v_target_flowproperty_id uuid;
  v_target_flowproperty_version text;
  v_target_flowproperty_expected_modified_at timestamptz;
  v_target_flowproperty_expected_json_ordered jsonb;
  v_target_unitgroup_id uuid;
  v_target_unitgroup_version text;
  v_target_unitgroup_expected_modified_at timestamptz;
  v_target_unitgroup_expected_json_ordered jsonb;
  v_source_unitgroup_id uuid;
  v_source_unitgroup_version text;
  v_source_unitgroup_expected_modified_at timestamptz;
  v_source_unitgroup_expected_json_ordered jsonb;
  v_target_actual_modified_at timestamptz;
  v_target_actual_json_ordered jsonb;
  v_target_found boolean := false;
  v_source_found boolean := false;
  v_source_flowproperty_id uuid;
  v_source_flowproperty_version text;
  v_source_flowproperty_expected_json_ordered jsonb;
  v_source_flowproperty_name text;
  v_target_flowproperty_name text;
  v_source_unitgroup_name text;
  v_target_unitgroup_name text;
  v_expected_reference jsonb;
  v_units jsonb;
  v_unit jsonb;
  v_unit_count integer;
  v_action_count integer;
  v_flowproperty_count integer;
  v_flow_count integer;
  v_process_count integer;
  v_exchange_count integer;
  v_live_flow_closure_count integer;
  v_live_process_closure_count integer;
  v_live_exchange_closure_count integer;
  v_closure_exact boolean;
  v_forbidden_closure_exists boolean;
  v_request_sha256 text;
  v_action jsonb;
  v_action_id text;
  v_action_table text;
  v_action_uuid uuid;
  v_action_version text;
  v_expected_state_code integer;
  v_expected_modified_at timestamptz;
  v_expected_json_ordered jsonb;
  v_desired_json_ordered jsonb;
  v_expected_root text;
  v_expected_uuid_path text[];
  v_mutation jsonb;
  v_mutation_kind text;
  v_mutation_index integer;
  v_mutation_internal_id text;
  v_mutation_exchange jsonb;
  v_before_reference jsonb;
  v_after_reference jsonb;
  v_before_exchange jsonb;
  v_after_exchange jsonb;
  v_normalized_desired jsonb;
  v_before_mean_text text;
  v_after_mean_text text;
  v_before_resulting_text text;
  v_after_resulting_text text;
  v_exchange_path text[];
  v_actual_state_code integer;
  v_actual_modified_at timestamptz;
  v_actual_json_ordered jsonb;
  v_current_row jsonb;
  v_locked_actions jsonb := '[]'::jsonb;
  v_fresh_count integer := 0;
  v_replay_count integer := 0;
  v_existing_audit_count integer := 0;
  v_prior_audit_id bigint;
  v_summary_audit_id bigint;
  v_audit_id bigint;
  v_audit_rows jsonb := '[]'::jsonb;
  v_before_sha256 text;
  v_after_sha256 text;
  v_target_flowproperty_sha256 text;
  v_target_unitgroup_sha256 text;
  v_source_unitgroup_sha256 text;
  v_committed_modified_at timestamptz;
  v_committed_json_ordered jsonb;
  v_committed_version text;
  v_mismatches text[];
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_batch is not null and pg_column_size(p_batch) > 134217728 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_REQUEST_TOO_LARGE',
      'status', 413,
      'message', 'Alias batch request exceeds the 128 MiB database limit'
    );
  end if;

  if jsonb_typeof(p_batch) is distinct from 'object'
    or not (p_batch ?& array[
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
      from jsonb_object_keys(p_batch) as request_key(key)
      where request_key.key <> all (array[
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
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_REQUEST',
      'status', 400,
      'message', 'Batch request must match dataset-alias-batch.v1 exactly'
    );
  end if;

  if jsonb_typeof(p_batch->'schema_version') is distinct from 'string'
    or p_batch->>'schema_version' <> v_schema_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_REQUEST',
      'status', 400,
      'message', 'Unsupported alias batch schema version'
    );
  end if;

  v_plan_sha256 := p_batch->>'plan_sha256';
  v_operation_id := nullif(btrim(p_batch->>'operation_id'), '');
  v_batch_id := nullif(btrim(p_batch->>'batch_id'), '');
  v_dimension := p_batch->>'dimension';
  v_factor := p_batch->>'factor';
  v_target_visibility := p_batch->>'target_visibility';

  if jsonb_typeof(p_batch->'plan_sha256') is distinct from 'string'
    or v_plan_sha256 is null
    or v_plan_sha256 !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_batch->'operation_id') is distinct from 'string'
    or v_operation_id is null
    or octet_length(v_operation_id) > 512
    or jsonb_typeof(p_batch->'batch_id') is distinct from 'string'
    or v_batch_id is null
    or octet_length(v_batch_id) > 512 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_AUDIT_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'plan_sha256, operation_id, and batch_id are required'
    );
  end if;

  if jsonb_typeof(p_batch->'dimension') is distinct from 'string'
    or v_dimension not in ('time', 'length_time') then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_DIMENSION',
      'status', 400,
      'message', 'dimension must be time or length_time'
    );
  end if;

  if jsonb_typeof(p_batch->'factor') is distinct from 'string'
    or (v_dimension = 'time' and v_factor <> '0.00011415525114155251')
    or (v_dimension = 'length_time' and v_factor <> '1000') then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_FACTOR',
      'status', 400,
      'message', 'factor does not match the frozen dimension conversion'
    );
  end if;

  if jsonb_typeof(p_batch->'target_visibility') is distinct from 'string'
    or v_target_visibility <> 'owner_draft' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_TARGET_VISIBILITY',
      'status', 400,
      'message', 'target_visibility must be owner_draft'
    );
  end if;

  if jsonb_typeof(p_batch->'target') is distinct from 'object'
    or not ((p_batch->'target') ?& array[
      'flowproperty',
      'unitgroup',
      'source_unitgroup'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_batch->'target') as target_key(key)
      where target_key.key <> all (array[
        'flowproperty',
        'unitgroup',
        'source_unitgroup'
      ])
    )
    or jsonb_typeof(p_batch#>'{target,flowproperty}') is distinct from 'object'
    or not ((p_batch#>'{target,flowproperty}') ?& array[
      'id',
      'version',
      'expected_modified_at',
      'expected_json_ordered'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_batch#>'{target,flowproperty}') as fp_key(key)
      where fp_key.key <> all (array[
        'id',
        'version',
        'expected_modified_at',
        'expected_json_ordered'
      ])
    )
    or jsonb_typeof(p_batch#>'{target,unitgroup}') is distinct from 'object'
    or not ((p_batch#>'{target,unitgroup}') ?& array[
      'id',
      'version',
      'expected_modified_at',
      'expected_json_ordered'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_batch#>'{target,unitgroup}') as ug_key(key)
      where ug_key.key <> all (array[
        'id',
        'version',
        'expected_modified_at',
        'expected_json_ordered'
      ])
    )
    or jsonb_typeof(p_batch#>'{target,flowproperty,id}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,flowproperty,version}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,flowproperty,expected_modified_at}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,flowproperty,expected_json_ordered}') is distinct from 'object'
    or pg_column_size(p_batch#>'{target,flowproperty,expected_json_ordered}') > 16777216
    or jsonb_typeof(p_batch#>'{target,unitgroup,id}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,unitgroup,version}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,unitgroup,expected_modified_at}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,unitgroup,expected_json_ordered}') is distinct from 'object'
    or pg_column_size(p_batch#>'{target,unitgroup,expected_json_ordered}') > 16777216
    or jsonb_typeof(p_batch#>'{target,source_unitgroup}') is distinct from 'object'
    or not ((p_batch#>'{target,source_unitgroup}') ?& array[
      'id',
      'version',
      'expected_modified_at',
      'expected_json_ordered'
    ])
    or exists (
      select 1
      from jsonb_object_keys(p_batch#>'{target,source_unitgroup}') as source_ug_key(key)
      where source_ug_key.key <> all (array[
        'id',
        'version',
        'expected_modified_at',
        'expected_json_ordered'
      ])
    )
    or jsonb_typeof(p_batch#>'{target,source_unitgroup,id}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,source_unitgroup,version}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,source_unitgroup,expected_modified_at}') is distinct from 'string'
    or jsonb_typeof(p_batch#>'{target,source_unitgroup,expected_json_ordered}') is distinct from 'object'
    or pg_column_size(p_batch#>'{target,source_unitgroup,expected_json_ordered}') > 16777216
    or (p_batch#>>'{target,flowproperty,id}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or (p_batch#>>'{target,unitgroup,id}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or (p_batch#>>'{target,source_unitgroup,id}') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or (p_batch#>>'{target,flowproperty,version}') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or (p_batch#>>'{target,unitgroup,version}') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or (p_batch#>>'{target,source_unitgroup,version}') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_TARGET',
      'status', 400,
      'message', 'Target support rows require exact UUID, version, modified_at, and payload snapshots'
    );
  end if;

  v_target_flowproperty_id := (p_batch#>>'{target,flowproperty,id}')::uuid;
  v_target_flowproperty_version := p_batch#>>'{target,flowproperty,version}';
  v_target_flowproperty_expected_json_ordered :=
    p_batch#>'{target,flowproperty,expected_json_ordered}';
  v_target_unitgroup_id := (p_batch#>>'{target,unitgroup,id}')::uuid;
  v_target_unitgroup_version := p_batch#>>'{target,unitgroup,version}';
  v_target_unitgroup_expected_json_ordered :=
    p_batch#>'{target,unitgroup,expected_json_ordered}';
  v_source_unitgroup_id := (p_batch#>>'{target,source_unitgroup,id}')::uuid;
  v_source_unitgroup_version := p_batch#>>'{target,source_unitgroup,version}';
  v_source_unitgroup_expected_json_ordered :=
    p_batch#>'{target,source_unitgroup,expected_json_ordered}';

  if v_source_unitgroup_id = v_target_unitgroup_id
    and v_source_unitgroup_version = v_target_unitgroup_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_TARGET',
      'status', 400,
      'message', 'Source and target unit group row keys must differ'
    );
  end if;

  if v_target_flowproperty_expected_json_ordered
      #>> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:UUID}'
      is distinct from v_target_flowproperty_id::text
    or v_target_flowproperty_expected_json_ordered
      #>> '{flowPropertyDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from v_target_flowproperty_version
    or v_target_unitgroup_expected_json_ordered
      #>> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}'
      is distinct from v_target_unitgroup_id::text
    or v_target_unitgroup_expected_json_ordered
      #>> '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from v_target_unitgroup_version
    or v_source_unitgroup_expected_json_ordered
      #>> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:UUID}'
      is distinct from v_source_unitgroup_id::text
    or v_source_unitgroup_expected_json_ordered
      #>> '{unitGroupDataSet,administrativeInformation,publicationAndOwnership,common:dataSetVersion}'
      is distinct from v_source_unitgroup_version then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_TARGET',
      'status', 400,
      'message', 'Target support payload roots and embedded UUID/version identities must match their row keys'
    );
  end if;

  begin
    v_target_flowproperty_expected_modified_at :=
      (p_batch#>>'{target,flowproperty,expected_modified_at}')::timestamptz;
    v_target_unitgroup_expected_modified_at :=
      (p_batch#>>'{target,unitgroup,expected_modified_at}')::timestamptz;
    v_source_unitgroup_expected_modified_at :=
      (p_batch#>>'{target,source_unitgroup,expected_modified_at}')::timestamptz;
  exception
    when invalid_datetime_format or datetime_field_overflow then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_INVALID_TARGET',
        'status', 400,
        'message', 'Target expected_modified_at values must be valid timestamps'
      );
  end;

  if jsonb_typeof(p_batch->'actions') is distinct from 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_ACTIONS',
      'status', 400,
      'message', 'actions must be an array'
    );
  end if;

  v_action_count := jsonb_array_length(p_batch->'actions');

  for v_action in
    select action_item.value
    from jsonb_array_elements(p_batch->'actions') as action_item(value)
  loop
    if jsonb_typeof(v_action) is distinct from 'object'
      or not (v_action ?& array[
        'action_id',
        'action',
        'table',
        'id',
        'version',
        'expected_state_code',
        'expected_modified_at',
        'expected_json_ordered',
        'desired_json_ordered',
        'mutation'
      ])
      or exists (
        select 1
        from jsonb_object_keys(v_action) as action_key(key)
        where action_key.key <> all (array[
          'action_id',
          'action',
          'table',
          'id',
          'version',
          'expected_state_code',
          'expected_modified_at',
          'expected_json_ordered',
          'desired_json_ordered',
          'mutation'
        ])
      )
      or jsonb_typeof(v_action->'action_id') is distinct from 'string'
      or nullif(btrim(v_action->>'action_id'), '') is null
      or octet_length(v_action->>'action_id') > 512
      or jsonb_typeof(v_action->'action') is distinct from 'string'
      or v_action->>'action' <> 'update_json_ordered'
      or jsonb_typeof(v_action->'table') is distinct from 'string'
      or v_action->>'table' not in ('flowproperties', 'flows', 'processes')
      or jsonb_typeof(v_action->'id') is distinct from 'string'
      or (v_action->>'id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
      or jsonb_typeof(v_action->'version') is distinct from 'string'
      or (v_action->>'version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
      or jsonb_typeof(v_action->'expected_state_code') is distinct from 'number'
      or v_action->>'expected_state_code' <> '0'
      or jsonb_typeof(v_action->'expected_modified_at') is distinct from 'string'
      or jsonb_typeof(v_action->'expected_json_ordered') is distinct from 'object'
      or jsonb_typeof(v_action->'desired_json_ordered') is distinct from 'object'
      or jsonb_typeof(v_action->'mutation') is distinct from 'object'
      or pg_column_size(v_action->'expected_json_ordered') > 16777216
      or pg_column_size(v_action->'desired_json_ordered') > 16777216
      or pg_column_size(v_action->'mutation') > 1048576
      or v_action->'expected_json_ordered' = v_action->'desired_json_ordered' then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_INVALID_ACTION',
        'status', 400,
        'message', 'Every action must be an exact allowed draft JSON update'
      );
    end if;

    v_action_table := v_action->>'table';
    v_action_uuid := (v_action->>'id')::uuid;
    v_action_version := v_action->>'version';
    v_expected_root := case v_action_table
      when 'flowproperties' then 'flowPropertyDataSet'
      when 'flows' then 'flowDataSet'
      when 'processes' then 'processDataSet'
    end;
    v_expected_uuid_path := case v_action_table
      when 'flowproperties' then array[
        'flowPropertyDataSet',
        'flowPropertiesInformation',
        'dataSetInformation',
        'common:UUID'
      ]
      when 'flows' then array[
        'flowDataSet',
        'flowInformation',
        'dataSetInformation',
        'common:UUID'
      ]
      when 'processes' then array[
        'processDataSet',
        'processInformation',
        'dataSetInformation',
        'common:UUID'
      ]
    end;

    if not ((v_action->'expected_json_ordered') ? v_expected_root)
      or not ((v_action->'desired_json_ordered') ? v_expected_root)
      or (v_action->'expected_json_ordered') #>> v_expected_uuid_path
        is distinct from v_action_uuid::text
      or (v_action->'desired_json_ordered') #>> v_expected_uuid_path
        is distinct from v_action_uuid::text
      or (v_action->'expected_json_ordered') #>> array[
        v_expected_root,
        'administrativeInformation',
        'publicationAndOwnership',
        'common:dataSetVersion'
      ] is distinct from v_action_version
      or (v_action->'desired_json_ordered') #>> array[
        v_expected_root,
        'administrativeInformation',
        'publicationAndOwnership',
        'common:dataSetVersion'
      ] is distinct from v_action_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_INVALID_ACTION',
        'status', 400,
        'message', 'Action payload root and embedded UUID/version identity must match its table and row key',
        'details', jsonb_build_object('action_id', v_action->>'action_id')
      );
    end if;

    begin
      perform (v_action->>'expected_modified_at')::timestamptz;
    exception
      when invalid_datetime_format or datetime_field_overflow then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_ACTION',
          'status', 400,
          'message', 'expected_modified_at must be a valid timestamp',
          'details', jsonb_build_object('action_id', v_action->>'action_id')
        );
    end;
  end loop;

  if exists (
    select 1
    from jsonb_array_elements(p_batch->'actions') as duplicate_action(value)
    group by duplicate_action.value->>'action_id'
    having count(*) <> 1
  ) or exists (
    select 1
    from jsonb_array_elements(p_batch->'actions') as duplicate_row(value)
    group by
      duplicate_row.value->>'table',
      duplicate_row.value->>'id',
      duplicate_row.value->>'version'
    having count(*) <> 1
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_DUPLICATE_ACTION',
      'status', 400,
      'message', 'action_id and table/id/version row keys must be unique'
    );
  end if;

  select
    count(*) filter (where action_item.value->>'table' = 'flowproperties'),
    count(*) filter (where action_item.value->>'table' = 'flows'),
    count(*) filter (where action_item.value->>'table' = 'processes')
  into v_flowproperty_count, v_flow_count, v_process_count
  from jsonb_array_elements(p_batch->'actions') as action_item(value);

  if (v_dimension = 'time' and (
      v_action_count <> 25
      or v_flowproperty_count <> 1
      or v_flow_count <> 10
      or v_process_count <> 14
    ))
    or (v_dimension = 'length_time' and (
      v_action_count <> 27
      or v_flowproperty_count <> 1
      or v_flow_count <> 13
      or v_process_count <> 13
    )) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_COUNTS',
      'status', 400,
      'message', 'Action counts do not match the frozen dimension scope',
      'details', jsonb_build_object(
        'action_count', v_action_count,
        'flowproperty_count', v_flowproperty_count,
        'flow_count', v_flow_count,
        'process_count', v_process_count
      )
    );
  end if;

  select
    (source_action.value->>'id')::uuid,
    source_action.value->>'version',
    source_action.value->'expected_json_ordered'
  into
    v_source_flowproperty_id,
    v_source_flowproperty_version,
    v_source_flowproperty_expected_json_ordered
  from jsonb_array_elements(p_batch->'actions') as source_action(value)
  where source_action.value->>'table' = 'flowproperties';

  select name_item.value->>'#text'
  into v_source_flowproperty_name
  from jsonb_array_elements(
    case jsonb_typeof(
      v_source_flowproperty_expected_json_ordered
        #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
    )
      when 'array' then
        v_source_flowproperty_expected_json_ordered
          #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      else jsonb_build_array(
        v_source_flowproperty_expected_json_ordered
          #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      )
    end
  ) as name_item(value)
  where coalesce(name_item.value->>'@xml:lang', 'en') = 'en'
  limit 1;

  select name_item.value->>'#text'
  into v_target_flowproperty_name
  from jsonb_array_elements(
    case jsonb_typeof(
      v_target_flowproperty_expected_json_ordered
        #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
    )
      when 'array' then
        v_target_flowproperty_expected_json_ordered
          #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      else jsonb_build_array(
        v_target_flowproperty_expected_json_ordered
          #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      )
    end
  ) as name_item(value)
  where coalesce(name_item.value->>'@xml:lang', 'en') = 'en'
  limit 1;

  select name_item.value->>'#text'
  into v_source_unitgroup_name
  from jsonb_array_elements(
    case jsonb_typeof(
      v_source_unitgroup_expected_json_ordered
        #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
    )
      when 'array' then
        v_source_unitgroup_expected_json_ordered
          #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      else jsonb_build_array(
        v_source_unitgroup_expected_json_ordered
          #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      )
    end
  ) as name_item(value)
  where coalesce(name_item.value->>'@xml:lang', 'en') = 'en'
  limit 1;

  select name_item.value->>'#text'
  into v_target_unitgroup_name
  from jsonb_array_elements(
    case jsonb_typeof(
      v_target_unitgroup_expected_json_ordered
        #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
    )
      when 'array' then
        v_target_unitgroup_expected_json_ordered
          #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      else jsonb_build_array(
        v_target_unitgroup_expected_json_ordered
          #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      )
    end
  ) as name_item(value)
  where coalesce(name_item.value->>'@xml:lang', 'en') = 'en'
  limit 1;

  if nullif(v_source_flowproperty_name, '') is null
    or nullif(v_target_flowproperty_name, '') is null
    or nullif(v_source_unitgroup_name, '') is null
    or nullif(v_target_unitgroup_name, '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
      'status', 400,
      'message', 'Source and target support snapshots require English names'
    );
  end if;

  v_expected_reference := jsonb_build_object(
    '@refObjectId', v_target_unitgroup_id,
    '@type', 'unit group data set',
    '@uri', '../unitgroups/' || v_target_unitgroup_id::text || '.json',
    '@version', v_target_unitgroup_version,
    'common:shortDescription', jsonb_build_object(
      '#text', v_target_unitgroup_name,
      '@xml:lang', 'en'
    )
  );

  if v_target_flowproperty_expected_json_ordered
      #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}'
      is distinct from v_expected_reference then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
      'status', 400,
      'message', 'Target flow property must reference the exact frozen target unit group'
    );
  end if;

  if v_source_unitgroup_expected_json_ordered
      #>> '{unitGroupDataSet,unitGroupInformation,quantitativeReference,referenceToReferenceUnit}'
      is distinct from '1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
      'status', 400,
      'message', 'Source unit group must retain reference unit internal ID 1'
    );
  end if;

  v_units := v_source_unitgroup_expected_json_ordered
    #> '{unitGroupDataSet,units,unit}';
  v_units := case jsonb_typeof(v_units)
    when 'array' then v_units
    when 'object' then jsonb_build_array(v_units)
    else '[]'::jsonb
  end;

  if jsonb_array_length(v_units) <> 1
    or jsonb_typeof(v_units->0) is distinct from 'object'
    or v_units#>>'{0,@dataSetInternalID}' is distinct from '1'
    or v_units#>>'{0,meanValue}' is distinct from '1.0'
    or v_units#>>'{0,name}' is distinct from
      (case v_dimension when 'time' then 'hr' else 'kmy' end) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
      'status', 400,
      'message', 'Source unit group does not match the frozen conversion unit definition'
    );
  end if;

  v_units := v_target_unitgroup_expected_json_ordered
    #> '{unitGroupDataSet,units,unit}';
  v_units := case jsonb_typeof(v_units)
    when 'array' then v_units
    when 'object' then jsonb_build_array(v_units)
    else '[]'::jsonb
  end;
  v_unit_count := 0;

  for v_unit in
    select unit_item.value
    from jsonb_array_elements(v_units) as unit_item(value)
    where unit_item.value->>'name' =
      (case v_dimension when 'time' then 'hr' else 'kmy' end)
  loop
    v_unit_count := v_unit_count + 1;
    if v_unit->>'@dataSetInternalID' is distinct from '4'
      or jsonb_typeof(v_unit->'meanValue') is distinct from 'string'
      or octet_length(v_unit->>'meanValue') > 128
      or v_unit->>'meanValue' !~ '^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
      then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
        'status', 400,
        'message', 'Target unit group conversion unit does not match the frozen factor'
      );
    end if;

    begin
      if (v_unit->>'meanValue')::numeric is distinct from v_factor::numeric then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
          'status', 400,
          'message', 'Target unit group conversion unit does not match the frozen factor'
        );
      end if;
    exception
      when numeric_value_out_of_range or invalid_text_representation then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
          'status', 400,
          'message', 'Target unit group conversion unit must be a finite numeric'
        );
    end;
  end loop;

  if v_unit_count <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_SUPPORT_SNAPSHOT',
      'status', 400,
      'message', 'Target unit group must contain exactly one frozen conversion unit'
    );
  end if;

  v_exchange_count := 0;

  for v_action in
    select action_item.value
    from jsonb_array_elements(p_batch->'actions') as action_item(value)
  loop
    v_action_id := v_action->>'action_id';
    v_action_table := v_action->>'table';
    v_expected_json_ordered := v_action->'expected_json_ordered';
    v_desired_json_ordered := v_action->'desired_json_ordered';
    v_mutation := v_action->'mutation';
    v_mutation_kind := v_mutation->>'kind';

    if v_action_table = 'flowproperties' then
      if not (v_mutation ?& array['kind'])
        or exists (
          select 1
          from jsonb_object_keys(v_mutation) as mutation_key(key)
          where mutation_key.key <> 'kind'
        )
        or jsonb_typeof(v_mutation->'kind') is distinct from 'string'
        or v_mutation_kind <> 'flowproperty_unitgroup_reference' then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_MUTATION',
          'status', 400,
          'message', 'Flow property action requires the exact unit-group-reference mutation contract',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

      v_before_reference := v_expected_json_ordered #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}';
      v_after_reference := v_desired_json_ordered #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}';
      v_expected_reference := jsonb_build_object(
        '@refObjectId', v_source_unitgroup_id,
        '@type', 'unit group data set',
        '@uri', '../unitgroups/' || v_source_unitgroup_id::text || '.json',
        '@version', v_source_unitgroup_version,
        'common:shortDescription', jsonb_build_object(
          '#text', v_source_unitgroup_name,
          '@xml:lang', 'en'
        )
      );

      if jsonb_typeof(v_before_reference) is distinct from 'object'
        or jsonb_typeof(v_after_reference) is distinct from 'object'
        or v_before_reference is distinct from v_expected_reference
        or v_after_reference->>'@refObjectId' is distinct from v_target_unitgroup_id::text
        or v_after_reference->>'@version' is distinct from v_target_unitgroup_version
        or v_after_reference is distinct from
          v_target_flowproperty_expected_json_ordered
            #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}'
        or v_expected_json_ordered #- '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}'
          is distinct from
          v_desired_json_ordered #- '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}' then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
          'status', 400,
          'message', 'Flow property desired payload may replace only its exact source unit group reference with the target reference',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

    elsif v_action_table = 'flows' then
      if not (v_mutation ?& array[
          'kind',
          'flow_property_internal_id',
          'source_flowproperty_id',
          'source_flowproperty_version'
        ])
        or exists (
          select 1
          from jsonb_object_keys(v_mutation) as mutation_key(key)
          where mutation_key.key <> all (array[
            'kind',
            'flow_property_internal_id',
            'source_flowproperty_id',
            'source_flowproperty_version'
          ])
        )
        or jsonb_typeof(v_mutation->'kind') is distinct from 'string'
        or v_mutation_kind <> 'flow_flowproperty_reference'
        or jsonb_typeof(v_mutation->'flow_property_internal_id') is distinct from 'string'
        or v_mutation->>'flow_property_internal_id' <> '1'
        or jsonb_typeof(v_mutation->'source_flowproperty_id') is distinct from 'string'
        or (v_mutation->>'source_flowproperty_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or jsonb_typeof(v_mutation->'source_flowproperty_version') is distinct from 'string'
        or v_mutation->>'source_flowproperty_id' is distinct from v_source_flowproperty_id::text
        or v_mutation->>'source_flowproperty_version' is distinct from v_source_flowproperty_version then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_MUTATION',
          'status', 400,
          'message', 'Flow action requires one exact indexed alias flow-property reference mutation',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

      v_mutation_internal_id := v_mutation->>'flow_property_internal_id';
      v_exchange_path := array[
        'flowDataSet',
        'flowProperties',
        'flowProperty'
      ];
      v_before_exchange := v_expected_json_ordered #> v_exchange_path;
      v_after_exchange := v_desired_json_ordered #> v_exchange_path;
      v_before_reference := v_before_exchange->'referenceToFlowPropertyDataSet';
      v_after_reference := v_after_exchange->'referenceToFlowPropertyDataSet';
      v_expected_reference := jsonb_build_object(
        '@refObjectId', v_source_flowproperty_id,
        '@type', 'flow property data set',
        '@uri', '../flowproperties/' || v_source_flowproperty_id::text || '.json',
        '@version', v_source_flowproperty_version,
        'common:shortDescription', jsonb_build_object(
          '#text', v_source_flowproperty_name,
          '@xml:lang', 'en'
        )
      );

      if jsonb_typeof(v_before_exchange) is distinct from 'object'
        or jsonb_typeof(v_after_exchange) is distinct from 'object'
        or v_before_exchange->>'@dataSetInternalID' is distinct from v_mutation_internal_id
        or v_after_exchange->>'@dataSetInternalID' is distinct from v_mutation_internal_id
        or v_before_exchange->>'meanValue' is null
        or v_after_exchange->>'meanValue' is null
        or octet_length(v_before_exchange->>'meanValue') > 128
        or octet_length(v_after_exchange->>'meanValue') > 128
        or v_before_exchange->>'meanValue' !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$'
        or v_after_exchange->>'meanValue' !~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$'
        or jsonb_typeof(v_before_reference) is distinct from 'object'
        or jsonb_typeof(v_after_reference) is distinct from 'object'
        or v_before_reference is distinct from v_expected_reference
        or v_after_reference is distinct from jsonb_build_object(
          '@refObjectId', v_target_flowproperty_id,
          '@type', 'flow property data set',
          '@uri', '../flowproperties/' || v_target_flowproperty_id::text || '.json',
          '@version', v_target_flowproperty_version,
          'common:shortDescription', jsonb_build_object(
            '#text', v_target_flowproperty_name,
            '@xml:lang', 'en'
          )
        )
        or jsonb_set(
          v_desired_json_ordered,
          array_append(v_exchange_path, 'referenceToFlowPropertyDataSet'),
          v_before_reference,
          false
        ) is distinct from v_expected_json_ordered then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
          'status', 400,
          'message', 'Flow desired payload may replace only the indexed alias flow-property reference',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

      begin
        if (v_before_exchange->>'meanValue')::numeric <> 1::numeric
          or (v_after_exchange->>'meanValue')::numeric <> 1::numeric then
          return jsonb_build_object(
            'ok', false,
            'code', 'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
            'status', 400,
            'message', 'Singleton flow property references must retain meanValue=1',
            'details', jsonb_build_object('action_id', v_action_id)
          );
        end if;
      exception
        when numeric_value_out_of_range or invalid_text_representation then
          return jsonb_build_object(
            'ok', false,
            'code', 'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
            'status', 400,
            'message', 'Singleton flow property meanValue must be a finite numeric',
            'details', jsonb_build_object('action_id', v_action_id)
          );
      end;

    elsif v_action_table = 'processes' then
      if not (v_mutation ?& array['kind', 'exchanges'])
        or exists (
          select 1
          from jsonb_object_keys(v_mutation) as mutation_key(key)
          where mutation_key.key <> all (array['kind', 'exchanges'])
        )
        or jsonb_typeof(v_mutation->'kind') is distinct from 'string'
        or v_mutation_kind <> 'process_exchange_amounts'
        or jsonb_typeof(v_mutation->'exchanges') is distinct from 'array'
        or jsonb_array_length(v_mutation->'exchanges') = 0 then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_MUTATION',
          'status', 400,
          'message', 'Process action requires a non-empty exact exchange mutation list',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

      if exists (
        select 1
        from jsonb_array_elements(v_mutation->'exchanges') as duplicate_exchange(value)
        group by duplicate_exchange.value->>'index'
        having count(*) <> 1
      ) or exists (
        select 1
        from jsonb_array_elements(v_mutation->'exchanges') as duplicate_exchange(value)
        group by duplicate_exchange.value->>'internal_id'
        having count(*) <> 1
      ) then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_INVALID_MUTATION',
          'status', 400,
          'message', 'Process exchange indexes and internal IDs must be unique within an action',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;

      v_normalized_desired := v_desired_json_ordered;

      for v_mutation_exchange in
        select exchange_item.value
        from jsonb_array_elements(v_mutation->'exchanges') as exchange_item(value)
      loop
        if jsonb_typeof(v_mutation_exchange) is distinct from 'object'
          or not (v_mutation_exchange ?& array[
            'index',
            'internal_id',
            'flow_id',
            'flow_version',
            'direction',
            'before_exchange_sha256'
          ])
          or exists (
            select 1
            from jsonb_object_keys(v_mutation_exchange) as exchange_key(key)
            where exchange_key.key <> all (array[
              'index',
              'internal_id',
              'flow_id',
              'flow_version',
              'direction',
              'before_exchange_sha256'
            ])
          )
          or jsonb_typeof(v_mutation_exchange->'index') is distinct from 'number'
          or (v_mutation_exchange->>'index') !~ '^(0|[1-9][0-9]*)$'
          or octet_length(v_mutation_exchange->>'index') > 6
          or jsonb_typeof(v_mutation_exchange->'internal_id') is distinct from 'string'
          or nullif(v_mutation_exchange->>'internal_id', '') is null
          or octet_length(v_mutation_exchange->>'internal_id') > 128
          or jsonb_typeof(v_mutation_exchange->'flow_id') is distinct from 'string'
          or (v_mutation_exchange->>'flow_id') !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or jsonb_typeof(v_mutation_exchange->'flow_version') is distinct from 'string'
          or (v_mutation_exchange->>'flow_version') !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
          or jsonb_typeof(v_mutation_exchange->'direction') is distinct from 'string'
          or v_mutation_exchange->>'direction' not in ('Input', 'Output')
          or jsonb_typeof(v_mutation_exchange->'before_exchange_sha256') is distinct from 'string'
          or (v_mutation_exchange->>'before_exchange_sha256') !~ '^[a-f0-9]{64}$'
          or not exists (
            select 1
            from jsonb_array_elements(p_batch->'actions') as flow_action(value)
            where flow_action.value->>'table' = 'flows'
              and flow_action.value->>'id' = v_mutation_exchange->>'flow_id'
              and flow_action.value->>'version' = v_mutation_exchange->>'flow_version'
          ) then
          return jsonb_build_object(
            'ok', false,
            'code', 'ALIAS_BATCH_INVALID_MUTATION',
            'status', 400,
            'message', 'Every process exchange locator must be exact and reference a flow action in this batch',
            'details', jsonb_build_object('action_id', v_action_id)
          );
        end if;

        v_mutation_index := (v_mutation_exchange->>'index')::integer;
        v_exchange_path := array[
          'processDataSet',
          'exchanges',
          'exchange',
          v_mutation_index::text
        ];
        v_before_exchange := v_expected_json_ordered #> v_exchange_path;
        v_after_exchange := v_desired_json_ordered #> v_exchange_path;

        if jsonb_typeof(v_before_exchange) is distinct from 'object'
          or jsonb_typeof(v_after_exchange) is distinct from 'object'
          or v_before_exchange->>'@dataSetInternalID' is distinct from v_mutation_exchange->>'internal_id'
          or v_after_exchange->>'@dataSetInternalID' is distinct from v_mutation_exchange->>'internal_id'
          or v_before_exchange#>>'{referenceToFlowDataSet,@refObjectId}' is distinct from v_mutation_exchange->>'flow_id'
          or v_before_exchange#>>'{referenceToFlowDataSet,@version}' is distinct from v_mutation_exchange->>'flow_version'
          or v_after_exchange#>>'{referenceToFlowDataSet,@refObjectId}' is distinct from v_mutation_exchange->>'flow_id'
          or v_after_exchange#>>'{referenceToFlowDataSet,@version}' is distinct from v_mutation_exchange->>'flow_version'
          or v_before_exchange->>'exchangeDirection' is distinct from v_mutation_exchange->>'direction'
          or v_after_exchange->>'exchangeDirection' is distinct from v_mutation_exchange->>'direction'
          or encode(
            extensions.digest(
              convert_to(
                private.dataset_alias_canonical_jsonb_v1(v_before_exchange),
                'UTF8'
              ),
              'sha256'
            ),
            'hex'
          ) is distinct from v_mutation_exchange->>'before_exchange_sha256' then
          return jsonb_build_object(
            'ok', false,
            'code', 'ALIAS_BATCH_MUTATION_EVIDENCE_MISMATCH',
            'status', 400,
            'message', 'Process exchange locator or canonical before hash does not match the expected payload',
            'details', jsonb_build_object(
              'action_id', v_action_id,
              'exchange_index', v_mutation_index
            )
          );
        end if;

        v_before_mean_text := v_before_exchange->>'meanAmount';
        v_after_mean_text := v_after_exchange->>'meanAmount';
        v_before_resulting_text := v_before_exchange->>'resultingAmount';
        v_after_resulting_text := v_after_exchange->>'resultingAmount';

        if jsonb_typeof(v_before_exchange->'meanAmount') is distinct from 'string'
          or jsonb_typeof(v_after_exchange->'meanAmount') is distinct from 'string'
          or jsonb_typeof(v_before_exchange->'resultingAmount') is distinct from 'string'
          or jsonb_typeof(v_after_exchange->'resultingAmount') is distinct from 'string'
          or octet_length(v_before_mean_text) > 128
          or octet_length(v_after_mean_text) > 128
          or octet_length(v_before_resulting_text) > 128
          or octet_length(v_after_resulting_text) > 128
          or v_before_mean_text !~ '^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
          or v_after_mean_text !~ '^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
          or v_before_resulting_text !~ '^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
          or v_after_resulting_text !~ '^-?(0|[1-9][0-9]*)([.][0-9]+)?$'
          or v_before_mean_text is distinct from v_before_resulting_text
          or v_after_mean_text is distinct from v_after_resulting_text then
          return jsonb_build_object(
            'ok', false,
            'code', 'ALIAS_BATCH_INVALID_AMOUNT',
            'status', 400,
            'message', 'Listed process exchanges require numeric meanAmount and resultingAmount values',
            'details', jsonb_build_object(
              'action_id', v_action_id,
              'exchange_index', v_mutation_index
            )
          );
        end if;

        begin
          if v_after_mean_text::numeric
              is distinct from v_before_mean_text::numeric * v_factor::numeric
            or v_after_resulting_text::numeric
              is distinct from v_before_resulting_text::numeric * v_factor::numeric then
            return jsonb_build_object(
              'ok', false,
              'code', 'ALIAS_BATCH_INVALID_AMOUNT',
              'status', 400,
              'message', 'Listed process exchange amounts must use the exact frozen factor',
              'details', jsonb_build_object(
                'action_id', v_action_id,
                'exchange_index', v_mutation_index
              )
            );
          end if;
        exception
          when numeric_value_out_of_range or invalid_text_representation then
            return jsonb_build_object(
              'ok', false,
              'code', 'ALIAS_BATCH_INVALID_AMOUNT',
              'status', 400,
              'message', 'Listed process exchange amounts must be finite PostgreSQL numerics',
              'details', jsonb_build_object(
                'action_id', v_action_id,
                'exchange_index', v_mutation_index
              )
            );
        end;

        v_normalized_desired := jsonb_set(
          v_normalized_desired,
          array_append(v_exchange_path, 'meanAmount'),
          v_before_exchange->'meanAmount',
          false
        );
        v_normalized_desired := jsonb_set(
          v_normalized_desired,
          array_append(v_exchange_path, 'resultingAmount'),
          v_before_exchange->'resultingAmount',
          false
        );
        v_exchange_count := v_exchange_count + 1;
      end loop;

      if v_normalized_desired is distinct from v_expected_json_ordered then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
          'status', 400,
          'message', 'Process desired payload may change only listed meanAmount and resultingAmount fields',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;
    end if;
  end loop;

  if (v_dimension = 'time' and v_exchange_count <> 20)
    or (v_dimension = 'length_time' and v_exchange_count <> 39) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_EXCHANGE_COUNT',
      'status', 400,
      'message', 'Listed exchange count does not match the frozen dimension scope',
      'details', jsonb_build_object('exchange_count', v_exchange_count)
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements(p_batch->'actions') as batch_flow(value)
    where batch_flow.value->>'table' = 'flows'
      and not exists (
        select 1
        from jsonb_array_elements(p_batch->'actions') as batch_process(value)
        cross join lateral jsonb_array_elements(
          batch_process.value#>'{mutation,exchanges}'
        ) as process_exchange(value)
        where batch_process.value->>'table' = 'processes'
          and process_exchange.value->>'flow_id' = batch_flow.value->>'id'
          and process_exchange.value->>'flow_version' = batch_flow.value->>'version'
      )
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_INVALID_MUTATION',
      'status', 400,
      'message', 'Every flow action must be referenced by at least one listed process exchange'
    );
  end if;

  v_request_sha256 := encode(
    extensions.digest(convert_to(p_batch::text, 'UTF8'), 'sha256'),
    'hex'
  );
  v_target_flowproperty_sha256 := encode(
    extensions.digest(
      convert_to(v_target_flowproperty_expected_json_ordered::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );
  v_target_unitgroup_sha256 := encode(
    extensions.digest(
      convert_to(v_target_unitgroup_expected_json_ordered::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );
  v_source_unitgroup_sha256 := encode(
    extensions.digest(
      convert_to(v_source_unitgroup_expected_json_ordered::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  -- Stable table-level write locks close the phantom window between the
  -- support-parent and exact flow/process closure scans and the batch commit.
  -- This command is intentionally a short, one-dimension transaction with a
  -- 5 second limit.
  lock table public.flowproperties, public.flows, public.processes
    in share row exclusive mode;

  v_current_row := null;
  v_target_actual_modified_at := null;
  v_target_actual_json_ordered := null;

  select
    target_fp.modified_at,
    target_fp.json_ordered::jsonb,
    to_jsonb(target_fp)
  into
    v_target_actual_modified_at,
    v_target_actual_json_ordered,
    v_current_row
  from public.flowproperties as target_fp
  where target_fp.id = v_target_flowproperty_id
    and target_fp.version = v_target_flowproperty_version
    and target_fp.user_id = v_actor
    and target_fp.state_code = 0
  for share of target_fp;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_target_actual_modified_at is distinct from v_target_flowproperty_expected_modified_at
    or v_target_actual_json_ordered is distinct from v_target_flowproperty_expected_json_ordered then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_TARGET_PRECONDITION_FAILED',
      'status', 409,
      'message', 'Target flow property changed after the alias batch was planned',
      'details', jsonb_build_object('target', 'flowproperty')
    );
  end if;

  v_target_found := false;
  v_source_found := false;

  for v_current_row in
    select to_jsonb(support_ug)
    from public.unitgroups as support_ug
    where support_ug.user_id = v_actor
      and support_ug.state_code = 0
      and (
        (
          support_ug.id = v_target_unitgroup_id
          and support_ug.version = v_target_unitgroup_version
        ) or (
          support_ug.id = v_source_unitgroup_id
          and support_ug.version = v_source_unitgroup_version
        )
      )
    order by support_ug.id, support_ug.version
    for share of support_ug
  loop
    if (v_current_row->>'id')::uuid = v_target_unitgroup_id
      and btrim(v_current_row->>'version') = v_target_unitgroup_version then
      v_target_found := true;
      if (v_current_row->>'modified_at')::timestamptz
          is distinct from v_target_unitgroup_expected_modified_at
        or v_current_row->'json_ordered'
          is distinct from v_target_unitgroup_expected_json_ordered then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_TARGET_PRECONDITION_FAILED',
          'status', 409,
          'message', 'Target unit group changed after the alias batch was planned',
          'details', jsonb_build_object('target', 'unitgroup')
        );
      end if;
    elsif (v_current_row->>'id')::uuid = v_source_unitgroup_id
      and btrim(v_current_row->>'version') = v_source_unitgroup_version then
      v_source_found := true;
      if (v_current_row->>'modified_at')::timestamptz
          is distinct from v_source_unitgroup_expected_modified_at
        or v_current_row->'json_ordered'
          is distinct from v_source_unitgroup_expected_json_ordered then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_SOURCE_SUPPORT_PRECONDITION_FAILED',
          'status', 409,
          'message', 'Source unit group changed after the alias batch was planned'
        );
      end if;
    end if;
  end loop;

  if not v_target_found then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if not v_source_found then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  for v_action in
    select action_item.value
    from jsonb_array_elements(p_batch->'actions') as action_item(value)
    order by
      case action_item.value->>'table'
        when 'flowproperties' then 1
        when 'flows' then 2
        when 'processes' then 3
      end,
      (action_item.value->>'id')::uuid,
      action_item.value->>'version'
  loop
    v_action_table := v_action->>'table';
    v_action_uuid := (v_action->>'id')::uuid;
    v_action_version := v_action->>'version';
    v_current_row := null;
    v_actual_state_code := null;
    v_actual_modified_at := null;
    v_actual_json_ordered := null;

    execute format(
      'select t.state_code,
              t.modified_at,
              t.json_ordered::jsonb,
              to_jsonb(t)
         from public.%I as t
        where t.id = $1
          and t.version = $2
          and t.user_id = $3
          and t.state_code = 0
        for update of t',
      v_action_table
    )
      into
        v_actual_state_code,
        v_actual_modified_at,
        v_actual_json_ordered,
        v_current_row
      using v_action_uuid, v_action_version, v_actor;

    if v_current_row is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_DATASET_NOT_FOUND',
        'status', 404,
        'message', 'Dataset not found',
        'details', jsonb_build_object('action_id', v_action->>'action_id')
      );
    end if;

    v_locked_actions := v_locked_actions || jsonb_build_array(
      v_action || jsonb_build_object(
        '_actual_state_code', v_actual_state_code,
        '_actual_modified_at', v_actual_modified_at,
        '_actual_json_ordered', v_actual_json_ordered
      )
    );
  end loop;

  -- A private support row must not already participate in a public, foreign,
  -- or otherwise non-draft parent closure. The source flow-property closure
  -- is checked exactly below because those flows are rewritten. Target-FP and
  -- source/target-UG parents may include additional same-owner draft rows that
  -- are outside this mutation, but no mixed-visibility parent is allowed.
  select
    exists (
      select 1
      from public.flows as support_parent_flow
      cross join lateral jsonb_array_elements(
        case jsonb_typeof(
          support_parent_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        )
          when 'array' then support_parent_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
          when 'object' then jsonb_build_array(
            support_parent_flow.json_ordered::jsonb
              #> '{flowDataSet,flowProperties,flowProperty}'
          )
          else '[]'::jsonb
        end
      ) as support_parent_property(value)
      where (
          (
            support_parent_property.value
              #>> '{referenceToFlowPropertyDataSet,@refObjectId}' =
              v_source_flowproperty_id::text
            and support_parent_property.value
              #>> '{referenceToFlowPropertyDataSet,@version}' =
              v_source_flowproperty_version
          ) or (
            support_parent_property.value
              #>> '{referenceToFlowPropertyDataSet,@refObjectId}' =
              v_target_flowproperty_id::text
            and support_parent_property.value
              #>> '{referenceToFlowPropertyDataSet,@version}' =
              v_target_flowproperty_version
          )
        )
        and (
          support_parent_flow.user_id is distinct from v_actor
          or support_parent_flow.state_code is distinct from 0
        )
    ) or exists (
      select 1
      from public.flowproperties as support_parent_fp
      where (
          (
            support_parent_fp.json_ordered::jsonb
              #>> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}' =
              v_source_unitgroup_id::text
            and support_parent_fp.json_ordered::jsonb
              #>> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}' =
              v_source_unitgroup_version
          ) or (
            support_parent_fp.json_ordered::jsonb
              #>> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@refObjectId}' =
              v_target_unitgroup_id::text
            and support_parent_fp.json_ordered::jsonb
              #>> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup,@version}' =
              v_target_unitgroup_version
          )
        )
        and (
          support_parent_fp.user_id is distinct from v_actor
          or support_parent_fp.state_code is distinct from 0
        )
    )
  into v_forbidden_closure_exists;

  if v_forbidden_closure_exists then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Live owner-draft closure does not match the exact submitted batch'
    );
  end if;

  with submitted_flows as (
    select
      (flow_action.value->>'id')::uuid as id,
      flow_action.value->>'version' as version,
      flow_action.value#>>'{mutation,flow_property_internal_id}' as internal_id
    from jsonb_array_elements(p_batch->'actions') as flow_action(value)
    where flow_action.value->>'table' = 'flows'
  ),
  live_flows as (
    select
      dataset_flow.id,
      btrim(dataset_flow.version::text) as version,
      dataset_flow.user_id,
      dataset_flow.state_code,
      flow_property.value->>'@dataSetInternalID' as internal_id
    from public.flows as dataset_flow
    cross join lateral jsonb_array_elements(
      case jsonb_typeof(
        dataset_flow.json_ordered::jsonb
          #> '{flowDataSet,flowProperties,flowProperty}'
      )
        when 'array' then dataset_flow.json_ordered::jsonb
          #> '{flowDataSet,flowProperties,flowProperty}'
        when 'object' then jsonb_build_array(
          dataset_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        )
        else '[]'::jsonb
      end
    ) as flow_property(value)
    where flow_property.value
        #>> '{referenceToFlowPropertyDataSet,@refObjectId}' =
        v_source_flowproperty_id::text
      and flow_property.value
        #>> '{referenceToFlowPropertyDataSet,@version}' =
        v_source_flowproperty_version
  )
  select
    count(*) filter (
      where live_flows.user_id = v_actor
        and live_flows.state_code = 0
    ),
    coalesce(bool_and(
      case
        when live_flows.user_id = v_actor
          and live_flows.state_code = 0 then exists (
            select 1
            from submitted_flows
            where submitted_flows.id = live_flows.id
              and submitted_flows.version = live_flows.version
              and submitted_flows.internal_id = live_flows.internal_id
          )
        else true
      end
    ), true),
    coalesce(bool_or(
      live_flows.user_id is distinct from v_actor
        or live_flows.state_code is distinct from 0
    ), false)
  into
    v_live_flow_closure_count,
    v_closure_exact,
    v_forbidden_closure_exists
  from live_flows;

  if v_forbidden_closure_exists
    or v_live_flow_closure_count not in (0, v_flow_count)
    or not v_closure_exact then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Live owner-draft closure does not match the exact submitted batch'
    );
  end if;

  with submitted_flows as (
    select
      flow_action.value->>'id' as id,
      flow_action.value->>'version' as version
    from jsonb_array_elements(p_batch->'actions') as flow_action(value)
    where flow_action.value->>'table' = 'flows'
  ),
  live_exchanges as (
    select
      dataset_process.id as process_id,
      btrim(dataset_process.version::text) as process_version,
      exchange_item.ordinality - 1 as exchange_index,
      exchange_item.value->>'@dataSetInternalID' as internal_id,
      exchange_item.value#>>'{referenceToFlowDataSet,@refObjectId}' as flow_id,
      exchange_item.value#>>'{referenceToFlowDataSet,@version}' as flow_version,
      exchange_item.value->>'exchangeDirection' as direction,
      dataset_process.user_id,
      dataset_process.state_code
    from public.processes as dataset_process
    cross join lateral jsonb_array_elements(
      case jsonb_typeof(
        dataset_process.json_ordered::jsonb
          #> '{processDataSet,exchanges,exchange}'
      )
        when 'array' then dataset_process.json_ordered::jsonb
          #> '{processDataSet,exchanges,exchange}'
        when 'object' then jsonb_build_array(
          dataset_process.json_ordered::jsonb
            #> '{processDataSet,exchanges,exchange}'
        )
        else '[]'::jsonb
      end
    ) with ordinality as exchange_item(value, ordinality)
    join submitted_flows
      on submitted_flows.id =
        exchange_item.value#>>'{referenceToFlowDataSet,@refObjectId}'
      and submitted_flows.version =
        exchange_item.value#>>'{referenceToFlowDataSet,@version}'
  )
  select
    count(distinct (
      live_exchanges.process_id,
      live_exchanges.process_version
    )) filter (
      where live_exchanges.user_id = v_actor
        and live_exchanges.state_code = 0
    ),
    count(*) filter (
      where live_exchanges.user_id = v_actor
        and live_exchanges.state_code = 0
    ),
    coalesce(bool_and(
      case
        when live_exchanges.user_id = v_actor
          and live_exchanges.state_code = 0 then exists (
            select 1
            from jsonb_array_elements(p_batch->'actions') as process_action(value)
            cross join lateral jsonb_array_elements(
              process_action.value#>'{mutation,exchanges}'
            ) as submitted_exchange(value)
            where process_action.value->>'table' = 'processes'
              and process_action.value->>'id' = live_exchanges.process_id::text
              and process_action.value->>'version' = live_exchanges.process_version
              and (submitted_exchange.value->>'index')::integer = live_exchanges.exchange_index
              and submitted_exchange.value->>'internal_id' = live_exchanges.internal_id
              and submitted_exchange.value->>'flow_id' = live_exchanges.flow_id
              and submitted_exchange.value->>'flow_version' = live_exchanges.flow_version
              and submitted_exchange.value->>'direction' = live_exchanges.direction
          )
        else true
      end
    ), true),
    coalesce(bool_or(
      live_exchanges.user_id is distinct from v_actor
        or live_exchanges.state_code is distinct from 0
    ), false)
  into
    v_live_process_closure_count,
    v_live_exchange_closure_count,
    v_closure_exact,
    v_forbidden_closure_exists
  from live_exchanges;

  if v_forbidden_closure_exists
    or v_live_process_closure_count <> v_process_count
    or v_live_exchange_closure_count <> v_exchange_count
    or not v_closure_exact then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_CLOSURE_MISMATCH',
      'status', 409,
      'message', 'Live owner-draft closure does not match the exact submitted batch'
    );
  end if;

  for v_action in
    select locked_item.value
    from jsonb_array_elements(v_locked_actions) as locked_item(value)
  loop
    v_action_id := v_action->>'action_id';
    v_action_table := v_action->>'table';
    v_action_uuid := (v_action->>'id')::uuid;
    v_action_version := v_action->>'version';
    v_expected_state_code := (v_action->>'expected_state_code')::integer;
    v_expected_modified_at := (v_action->>'expected_modified_at')::timestamptz;
    v_expected_json_ordered := v_action->'expected_json_ordered';
    v_desired_json_ordered := v_action->'desired_json_ordered';
    v_actual_state_code := (v_action->>'_actual_state_code')::integer;
    v_actual_modified_at := (v_action->>'_actual_modified_at')::timestamptz;
    v_actual_json_ordered := v_action->'_actual_json_ordered';
    v_before_sha256 := encode(
      extensions.digest(convert_to(v_expected_json_ordered::text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_after_sha256 := encode(
      extensions.digest(convert_to(v_desired_json_ordered::text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_prior_audit_id := null;

    select audit_log.id
    into v_prior_audit_id
    from public.command_audit_log as audit_log
    where audit_log.command = v_command
      and audit_log.actor_user_id = v_actor
      and audit_log.target_table = v_action_table
      and audit_log.target_id = v_action_uuid
      and audit_log.target_version = v_action_version
      and audit_log.payload->>'record_type' = 'row'
      and audit_log.payload->>'schema_version' = v_schema_version
      and audit_log.payload->>'plan_sha256' = v_plan_sha256
      and audit_log.payload->>'operation_id' = v_operation_id
      and audit_log.payload->>'batch_id' = v_batch_id
      and audit_log.payload->>'dimension' = v_dimension
      and audit_log.payload->>'factor' = v_factor
      and audit_log.payload->>'target_visibility' = v_target_visibility
      and audit_log.payload->>'batch_request_sha256' = v_request_sha256
      and audit_log.payload->>'hash_algorithm' = 'postgres-jsonb-text-sha256'
      and audit_log.payload->>'exchange_evidence_hash_algorithm' = 'sorted-key-compact-json-v1-sha256'
      and audit_log.payload->>'action_id' = v_action_id
      and audit_log.payload->>'expected_state_code' = v_expected_state_code::text
      and audit_log.payload->'expected_modified_at' = to_jsonb(v_expected_modified_at)
      and audit_log.payload->>'expected_json_ordered_sha256' = v_before_sha256
      and audit_log.payload->>'desired_json_ordered_sha256' = v_after_sha256
      and audit_log.payload->>'mutation_sha256' = encode(
        extensions.digest(convert_to((v_action->'mutation')::text, 'UTF8'), 'sha256'),
        'hex'
      )
      and audit_log.payload->'committed_modified_at' = to_jsonb(v_actual_modified_at)
    order by audit_log.id desc
    limit 1;

    if v_actual_state_code = v_expected_state_code
      and v_actual_modified_at is not distinct from v_expected_modified_at
      and v_actual_json_ordered = v_expected_json_ordered then
      if v_prior_audit_id is not null then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_AUDIT_STATE_CONFLICT',
          'status', 409,
          'message', 'A matching committed audit exists while a row is still at its before state',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;
      v_fresh_count := v_fresh_count + 1;
    elsif v_actual_state_code = v_expected_state_code
      and v_actual_json_ordered = v_desired_json_ordered then
      if v_prior_audit_id is null then
        return jsonb_build_object(
          'ok', false,
          'code', 'ALIAS_BATCH_REPLAY_UNPROVEN',
          'status', 409,
          'message', 'A desired-state row has no exact committed batch audit proof',
          'details', jsonb_build_object('action_id', v_action_id)
        );
      end if;
      v_replay_count := v_replay_count + 1;
      v_audit_rows := v_audit_rows || jsonb_build_array(jsonb_build_object(
        'action_id', v_action_id,
        'table', v_action_table,
        'id', v_action_uuid,
        'version', v_action_version,
        'audit_id', v_prior_audit_id::text
      ));
    else
      v_mismatches := array[]::text[];
      if v_actual_state_code is distinct from v_expected_state_code then
        v_mismatches := array_append(v_mismatches, 'state_code');
      end if;
      if v_actual_modified_at is distinct from v_expected_modified_at then
        v_mismatches := array_append(v_mismatches, 'modified_at');
      end if;
      if v_actual_json_ordered is distinct from v_expected_json_ordered then
        v_mismatches := array_append(v_mismatches, 'json_ordered');
      end if;

      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_PRECONDITION_FAILED',
        'status', 409,
        'message', 'A dataset changed after the alias batch was planned',
        'details', jsonb_build_object(
          'action_id', v_action_id,
          'mismatches', to_jsonb(v_mismatches)
        )
      );
    end if;
  end loop;

  v_summary_audit_id := null;

  select audit_log.id
  into v_summary_audit_id
  from public.command_audit_log as audit_log
  where audit_log.command = v_command
    and audit_log.actor_user_id = v_actor
    and audit_log.target_table is null
    and audit_log.target_id is null
    and audit_log.target_version is null
    and audit_log.payload->>'record_type' = 'batch_summary'
    and audit_log.payload->>'schema_version' = v_schema_version
    and audit_log.payload->>'plan_sha256' = v_plan_sha256
    and audit_log.payload->>'operation_id' = v_operation_id
    and audit_log.payload->>'batch_id' = v_batch_id
    and audit_log.payload->>'dimension' = v_dimension
    and audit_log.payload->>'factor' = v_factor
    and audit_log.payload->>'target_visibility' = v_target_visibility
    and audit_log.payload->>'batch_request_sha256' = v_request_sha256
    and audit_log.payload->>'hash_algorithm' = 'postgres-jsonb-text-sha256'
    and audit_log.payload->>'exchange_evidence_hash_algorithm' = 'sorted-key-compact-json-v1-sha256'
    and audit_log.payload->>'batch_action_count' = v_action_count::text
    and audit_log.payload->>'exchange_count' = v_exchange_count::text
    and audit_log.payload->>'target_flowproperty_sha256' = v_target_flowproperty_sha256
    and audit_log.payload->>'target_unitgroup_sha256' = v_target_unitgroup_sha256
    and audit_log.payload->>'source_unitgroup_sha256' = v_source_unitgroup_sha256
    and audit_log.payload->>'target_flowproperty_expected_state_code' = '0'
    and audit_log.payload->>'target_unitgroup_expected_state_code' = '0'
    and audit_log.payload->>'source_unitgroup_expected_state_code' = '0'
    and audit_log.payload->>'support_owner_user_id' = v_actor::text
  order by audit_log.id desc
  limit 1;

  select count(*)
  into v_existing_audit_count
  from public.command_audit_log as audit_log
  where audit_log.command = v_command
    and audit_log.actor_user_id = v_actor
    and audit_log.payload->>'batch_request_sha256' = v_request_sha256;

  if v_replay_count = v_action_count then
    if v_summary_audit_id is null
      or v_existing_audit_count <> v_action_count + 1 then
      return jsonb_build_object(
        'ok', false,
        'code', 'ALIAS_BATCH_REPLAY_UNPROVEN',
        'status', 409,
        'message', 'Committed audit cardinality does not match the exact batch'
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'command', v_command,
      'dimension', v_dimension,
      'target_visibility', v_target_visibility,
      'batch_id', v_batch_id,
      'batch_request_sha256', v_request_sha256,
      'row_count', v_action_count,
      'exchange_count', v_exchange_count,
      'summary_audit_id', v_summary_audit_id::text,
      'audit', v_audit_rows,
      'idempotent_replay', true
    );
  end if;

  if v_fresh_count <> v_action_count then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_PARTIAL_STATE',
      'status', 409,
      'message', 'Batch rows are split between before and committed states'
    );
  end if;

  if v_existing_audit_count <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'ALIAS_BATCH_AUDIT_STATE_CONFLICT',
      'status', 409,
      'message', 'Audit rows already exist for a batch whose datasets remain at the before state'
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
    v_command,
    v_actor,
    null,
    null,
    null,
    jsonb_build_object(
      'record_type', 'batch_summary',
      'schema_version', v_schema_version,
      'plan_sha256', v_plan_sha256,
      'operation_id', v_operation_id,
      'batch_id', v_batch_id,
      'dimension', v_dimension,
      'factor', v_factor,
      'target_visibility', v_target_visibility,
      'batch_request_sha256', v_request_sha256,
      'hash_algorithm', 'postgres-jsonb-text-sha256',
      'exchange_evidence_hash_algorithm', 'sorted-key-compact-json-v1-sha256',
      'batch_action_count', v_action_count,
      'flowproperty_count', v_flowproperty_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'exchange_count', v_exchange_count,
      'target_flowproperty_id', v_target_flowproperty_id,
      'target_flowproperty_version', v_target_flowproperty_version,
      'target_flowproperty_expected_modified_at', v_target_flowproperty_expected_modified_at,
      'target_flowproperty_expected_state_code', 0,
      'target_flowproperty_sha256', v_target_flowproperty_sha256,
      'target_unitgroup_id', v_target_unitgroup_id,
      'target_unitgroup_version', v_target_unitgroup_version,
      'target_unitgroup_expected_modified_at', v_target_unitgroup_expected_modified_at,
      'target_unitgroup_expected_state_code', 0,
      'target_unitgroup_sha256', v_target_unitgroup_sha256,
      'source_unitgroup_id', v_source_unitgroup_id,
      'source_unitgroup_version', v_source_unitgroup_version,
      'source_unitgroup_expected_modified_at', v_source_unitgroup_expected_modified_at,
      'source_unitgroup_expected_state_code', 0,
      'source_unitgroup_sha256', v_source_unitgroup_sha256,
      'support_owner_user_id', v_actor
    )
  )
  returning id into v_summary_audit_id;

  for v_action in
    select locked_item.value
    from jsonb_array_elements(v_locked_actions) as locked_item(value)
  loop
    v_action_id := v_action->>'action_id';
    v_action_table := v_action->>'table';
    v_action_uuid := (v_action->>'id')::uuid;
    v_action_version := v_action->>'version';
    v_expected_state_code := (v_action->>'expected_state_code')::integer;
    v_expected_modified_at := (v_action->>'expected_modified_at')::timestamptz;
    v_expected_json_ordered := v_action->'expected_json_ordered';
    v_desired_json_ordered := v_action->'desired_json_ordered';
    v_before_sha256 := encode(
      extensions.digest(convert_to(v_expected_json_ordered::text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_after_sha256 := encode(
      extensions.digest(convert_to(v_desired_json_ordered::text, 'UTF8'), 'sha256'),
      'hex'
    );
    v_committed_modified_at := null;
    v_committed_json_ordered := null;
    v_committed_version := null;

    execute format(
      'update public.%I as t
          set json_ordered = $1::json,
              modified_at = now()
        where t.id = $2
          and t.version = $3
          and t.user_id = $4
          and t.state_code = $5
          and t.modified_at is not distinct from $6
          and t.json_ordered::jsonb is not distinct from $7
      returning t.modified_at, t.json_ordered::jsonb, t.version::text',
      v_action_table
    )
      into
        v_committed_modified_at,
        v_committed_json_ordered,
        v_committed_version
      using
        v_desired_json_ordered,
        v_action_uuid,
        v_action_version,
        v_actor,
        v_expected_state_code,
        v_expected_modified_at,
        v_expected_json_ordered;

    if v_committed_modified_at is null
      or v_committed_json_ordered is distinct from v_desired_json_ordered
      or v_committed_version is distinct from v_action_version then
      raise exception using
        errcode = 'P0001',
        message = format(
          'Guarded alias batch lost its locked update precondition for action %s',
          v_action_id
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
      v_command,
      v_actor,
      v_action_table,
      v_action_uuid,
      v_action_version,
      jsonb_build_object(
        'record_type', 'row',
        'schema_version', v_schema_version,
        'plan_sha256', v_plan_sha256,
        'operation_id', v_operation_id,
        'batch_id', v_batch_id,
        'dimension', v_dimension,
        'factor', v_factor,
        'target_visibility', v_target_visibility,
        'batch_request_sha256', v_request_sha256,
        'hash_algorithm', 'postgres-jsonb-text-sha256',
        'exchange_evidence_hash_algorithm', 'sorted-key-compact-json-v1-sha256',
        'batch_action_count', v_action_count,
        'target_flowproperty_id', v_target_flowproperty_id,
        'target_flowproperty_version', v_target_flowproperty_version,
        'target_unitgroup_id', v_target_unitgroup_id,
        'target_unitgroup_version', v_target_unitgroup_version,
        'source_unitgroup_id', v_source_unitgroup_id,
        'source_unitgroup_version', v_source_unitgroup_version,
        'action_id', v_action_id,
        'action', 'update_json_ordered',
        'mutation_sha256', encode(
          extensions.digest(convert_to((v_action->'mutation')::text, 'UTF8'), 'sha256'),
          'hex'
        ),
        'expected_state_code', v_expected_state_code,
        'expected_modified_at', v_expected_modified_at,
        'expected_json_ordered_sha256', v_before_sha256,
        'desired_json_ordered_sha256', v_after_sha256,
        'committed_modified_at', v_committed_modified_at
      )
    )
    returning id into v_audit_id;

    v_audit_rows := v_audit_rows || jsonb_build_array(jsonb_build_object(
      'action_id', v_action_id,
      'table', v_action_table,
      'id', v_action_uuid,
      'version', v_action_version,
      'audit_id', v_audit_id::text
    ));
  end loop;

  select count(*)
  into v_existing_audit_count
  from public.command_audit_log as audit_log
  where audit_log.command = v_command
    and audit_log.actor_user_id = v_actor
    and audit_log.payload->>'batch_request_sha256' = v_request_sha256;

  if v_existing_audit_count <> v_action_count + 1 then
    raise exception using
      errcode = 'P0001',
      message = 'Guarded alias batch committed an incomplete audit set';
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', v_command,
    'dimension', v_dimension,
    'target_visibility', v_target_visibility,
    'batch_id', v_batch_id,
    'batch_request_sha256', v_request_sha256,
    'row_count', v_action_count,
    'exchange_count', v_exchange_count,
    'summary_audit_id', v_summary_audit_id::text,
    'audit', v_audit_rows,
    'idempotent_replay', false
  );
end;
$$;

alter function public.cmd_dataset_alias_batch_guarded(jsonb)
  owner to postgres;

revoke all on function public.cmd_dataset_alias_batch_guarded(jsonb)
  from public, anon, authenticated, service_role;

grant execute on function public.cmd_dataset_alias_batch_guarded(jsonb)
  to authenticated;

comment on function public.cmd_dataset_alias_batch_guarded(jsonb) is
  'Atomically rewrites one frozen owner_draft FP/UG alias dimension batch; source support, target support, flows, and processes must all be state-code-0 rows owned by the authenticated actor, with exact closure checks and audit-proven replay.';
