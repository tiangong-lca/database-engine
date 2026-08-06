CREATE OR REPLACE FUNCTION "util"."read_dataset_alias_execution_primary_closure"("p_actor" "uuid", "p_plan" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_batch_count integer := 0;
  v_action_count integer := 0;
  v_distinct_action_count integer := 0;
  v_flowproperty_count integer := 0;
  v_flow_count integer := 0;
  v_process_count integer := 0;
  v_support_count integer := 0;
  v_flowproperty_support_count integer := 0;
  v_unitgroup_support_count integer := 0;
  v_source_unitgroup_support_count integer := 0;
  v_invalid_action_count integer := 0;
  v_invalid_support_count integer := 0;
  v_action_evidence jsonb := '[]'::jsonb;
  v_support_evidence jsonb := '[]'::jsonb;
  v_proof_material jsonb;
  v_live_closure boolean := false;
begin
  if p_actor is null
    or jsonb_typeof(p_plan) is distinct from 'object'
    or jsonb_typeof(p_plan->'batches') is distinct from 'array' then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-alias-primary-closure.v1',
      'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_INVALID_INPUT',
      'live_closure_proof', false
    );
  end if;

  select jsonb_array_length(p_plan->'batches')
  into v_batch_count;

  select
    count(*)::integer,
    count(distinct (
      action_item.value->>'table',
      action_item.value->>'id',
      action_item.value->>'version'
    ))::integer,
    count(*) filter (
      where action_item.value->>'table' = 'flowproperties'
    )::integer,
    count(*) filter (
      where action_item.value->>'table' = 'flows'
    )::integer,
    count(*) filter (
      where action_item.value->>'table' = 'processes'
    )::integer
  into
    v_action_count,
    v_distinct_action_count,
    v_flowproperty_count,
    v_flow_count,
    v_process_count
  from jsonb_array_elements(p_plan->'batches') as batch_item(value)
  cross join lateral jsonb_array_elements(
    batch_item.value->'actions'
  ) as action_item(value);

  if v_batch_count <> 2
    or v_action_count <> 52
    or v_distinct_action_count <> 52
    or v_flowproperty_count <> 2
    or v_flow_count <> 23
    or v_process_count <> 27
    or exists (
      select 1
      from jsonb_array_elements(p_plan->'batches') as batch_item(value)
      cross join lateral jsonb_array_elements(
        batch_item.value->'actions'
      ) as action_item(value)
      where jsonb_typeof(action_item.value) is distinct from 'object'
        or action_item.value->>'table' not in (
          'flowproperties', 'flows', 'processes'
        )
        or (action_item.value->>'id')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or (action_item.value->>'version')
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or jsonb_typeof(action_item.value->'desired_json_ordered')
          is distinct from 'object'
    ) then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-alias-primary-closure.v1',
      'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_PLAN_SHAPE_MISMATCH',
      'live_closure_proof', false,
      'batch_count', v_batch_count,
      'action_count', v_action_count,
      'distinct_action_count', v_distinct_action_count,
      'flowproperty_count', v_flowproperty_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count
    );
  end if;

  with actions as (
    select
      batch_item.ordinality::integer as batch_ordinality,
      action_item.ordinality::integer as action_ordinality,
      batch_item.value->>'dimension' as dimension,
      action_item.value->>'action_id' as action_id,
      action_item.value->>'table' as table_name,
      (action_item.value->>'id')::uuid as id,
      action_item.value->>'version' as version,
      action_item.value->'desired_json_ordered' as desired_json_ordered
    from jsonb_array_elements(p_plan->'batches') with ordinality
      as batch_item(value, ordinality)
    cross join lateral jsonb_array_elements(
      batch_item.value->'actions'
    ) with ordinality as action_item(value, ordinality)
  ),
  live as (
    select
      action.*,
      dataset_row.id is not null as row_found,
      dataset_row.user_id as live_user_id,
      dataset_row.state_code as live_state_code,
      dataset_row.json::jsonb as live_json,
      dataset_row.json_ordered::jsonb as live_json_ordered
    from actions as action
    left join public.flowproperties as dataset_row
      on dataset_row.id = action.id
     and dataset_row.version::text = action.version
    where action.table_name = 'flowproperties'

    union all

    select
      action.*,
      dataset_row.id is not null,
      dataset_row.user_id,
      dataset_row.state_code,
      dataset_row.json::jsonb,
      dataset_row.json_ordered::jsonb
    from actions as action
    left join public.flows as dataset_row
      on dataset_row.id = action.id
     and dataset_row.version::text = action.version
    where action.table_name = 'flows'

    union all

    select
      action.*,
      dataset_row.id is not null,
      dataset_row.user_id,
      dataset_row.state_code,
      dataset_row.json::jsonb,
      dataset_row.json_ordered::jsonb
    from actions as action
    left join public.processes as dataset_row
      on dataset_row.id = action.id
     and dataset_row.version::text = action.version
    where action.table_name = 'processes'
  ),
  evidence as (
    select
      live.*,
      (
        row_found
        and live_user_id = p_actor
        and live_state_code = 0
        and live_json is not distinct from desired_json_ordered
        and live_json_ordered is not distinct from desired_json_ordered
      ) as valid
    from live
  ),
  hashed_evidence as (
    select
      evidence.*,
      util.dataset_alias_execution_sha256(
        desired_json_ordered::text
      ) as desired_json_ordered_sha256
    from evidence
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'batch_ordinality', batch_ordinality,
          'action_ordinality', action_ordinality,
          'dimension', dimension,
          'action_id', action_id,
          'table', table_name,
          'id', id,
          'version', version,
          'row_found', row_found,
          'owner_matches', live_user_id = p_actor,
          'state_code_matches', live_state_code = 0,
          'json_matches', live_json is not distinct from desired_json_ordered,
          'json_ordered_matches',
            live_json_ordered is not distinct from desired_json_ordered,
          'desired_json_ordered_sha256', desired_json_ordered_sha256,
          'live_json_sha256', case
            when not row_found then null
            when live_json is not distinct from desired_json_ordered then
              desired_json_ordered_sha256
            else util.dataset_alias_execution_sha256(live_json::text)
          end,
          'live_json_ordered_sha256', case
            when not row_found then null
            when live_json_ordered is not distinct from desired_json_ordered then
              desired_json_ordered_sha256
            else util.dataset_alias_execution_sha256(
              live_json_ordered::text
            )
          end,
          'valid', valid
        ) order by batch_ordinality, action_ordinality
      ),
      '[]'::jsonb
    ),
    count(*) filter (where not valid)::integer
  into v_action_evidence, v_invalid_action_count
  from hashed_evidence;

  select
    count(*)::integer,
    count(*) filter (
      where support_item.role = 'flowproperty'
    )::integer,
    count(*) filter (
      where support_item.role = 'unitgroup'
    )::integer,
    count(*) filter (
      where support_item.role = 'source_unitgroup'
    )::integer
  into
    v_support_count,
    v_flowproperty_support_count,
    v_unitgroup_support_count,
    v_source_unitgroup_support_count
  from jsonb_array_elements(p_plan->'batches') as batch_item(value)
  cross join lateral (values
    ('flowproperty'::text, batch_item.value#>'{target,flowproperty}'),
    ('unitgroup'::text, batch_item.value#>'{target,unitgroup}'),
    ('source_unitgroup'::text,
      batch_item.value#>'{target,source_unitgroup}')
  ) as support_item(role, snapshot);

  if v_support_count <> 6
    or v_flowproperty_support_count <> 2
    or v_unitgroup_support_count <> 2
    or v_source_unitgroup_support_count <> 2
    or exists (
      select 1
      from jsonb_array_elements(p_plan->'batches') as batch_item(value)
      cross join lateral (values
        ('flowproperty'::text, batch_item.value#>'{target,flowproperty}'),
        ('unitgroup'::text, batch_item.value#>'{target,unitgroup}'),
        ('source_unitgroup'::text,
          batch_item.value#>'{target,source_unitgroup}')
      ) as support_item(role, snapshot)
      where jsonb_typeof(support_item.snapshot) is distinct from 'object'
        or not (support_item.snapshot ?& array[
          'id', 'version', 'expected_modified_at', 'expected_json_ordered'
        ])
        or exists (
          select 1
          from jsonb_object_keys(support_item.snapshot)
            as support_key(key)
          where support_key.key <> all (array[
            'id', 'version', 'expected_modified_at',
            'expected_json_ordered'
          ])
        )
        or (support_item.snapshot->>'id')
          !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or (support_item.snapshot->>'version')
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or jsonb_typeof(support_item.snapshot->'expected_modified_at')
          is distinct from 'string'
        or jsonb_typeof(support_item.snapshot->'expected_json_ordered')
          is distinct from 'object'
    ) then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-alias-primary-closure.v1',
      'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_SUPPORT_SHAPE_MISMATCH',
      'live_closure_proof', false,
      'support_reference_count', v_support_count
    );
  end if;

  with supports as (
    select
      batch_item.ordinality::integer as batch_ordinality,
      batch_item.value->>'dimension' as dimension,
      support_item.role,
      case
        when support_item.role = 'flowproperty' then 'flowproperties'
        else 'unitgroups'
      end as table_name,
      (support_item.snapshot->>'id')::uuid as id,
      support_item.snapshot->>'version' as version,
      (support_item.snapshot->>'expected_modified_at')::timestamptz
        as expected_modified_at,
      support_item.snapshot->'expected_json_ordered'
        as expected_json_ordered
    from jsonb_array_elements(p_plan->'batches') with ordinality
      as batch_item(value, ordinality)
    cross join lateral (values
      ('flowproperty'::text, batch_item.value#>'{target,flowproperty}'),
      ('unitgroup'::text, batch_item.value#>'{target,unitgroup}'),
      ('source_unitgroup'::text,
        batch_item.value#>'{target,source_unitgroup}')
    ) as support_item(role, snapshot)
  ),
  live as (
    select
      support.*,
      dataset_row.id is not null as row_found,
      dataset_row.user_id as live_user_id,
      dataset_row.state_code as live_state_code,
      dataset_row.modified_at as live_modified_at,
      dataset_row.json::jsonb as live_json,
      dataset_row.json_ordered::jsonb as live_json_ordered
    from supports as support
    left join public.flowproperties as dataset_row
      on dataset_row.id = support.id
     and dataset_row.version::text = support.version
    where support.table_name = 'flowproperties'

    union all

    select
      support.*,
      dataset_row.id is not null,
      dataset_row.user_id,
      dataset_row.state_code,
      dataset_row.modified_at,
      dataset_row.json::jsonb,
      dataset_row.json_ordered::jsonb
    from supports as support
    left join public.unitgroups as dataset_row
      on dataset_row.id = support.id
     and dataset_row.version::text = support.version
    where support.table_name = 'unitgroups'
  ),
  evidence as (
    select
      live.*,
      (
        row_found
        and live_user_id = p_actor
        and live_state_code = 0
        and live_modified_at is not distinct from expected_modified_at
        and live_json is not distinct from expected_json_ordered
        and live_json_ordered is not distinct from expected_json_ordered
      ) as valid
    from live
  ),
  hashed_evidence as (
    select
      evidence.*,
      util.dataset_alias_execution_sha256(
        expected_json_ordered::text
      ) as expected_json_ordered_sha256
    from evidence
  )
  select
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'batch_ordinality', batch_ordinality,
          'dimension', dimension,
          'role', role,
          'table', table_name,
          'id', id,
          'version', version,
          'row_found', row_found,
          'owner_matches', live_user_id = p_actor,
          'state_code_matches', live_state_code = 0,
          'modified_at_matches',
            live_modified_at is not distinct from expected_modified_at,
          'json_matches', live_json is not distinct from expected_json_ordered,
          'json_ordered_matches',
            live_json_ordered is not distinct from expected_json_ordered,
          'expected_modified_at', expected_modified_at,
          'live_modified_at', live_modified_at,
          'expected_json_ordered_sha256', expected_json_ordered_sha256,
          'live_json_sha256', case
            when not row_found then null
            when live_json is not distinct from expected_json_ordered then
              expected_json_ordered_sha256
            else util.dataset_alias_execution_sha256(live_json::text)
          end,
          'live_json_ordered_sha256', case
            when not row_found then null
            when live_json_ordered is not distinct from expected_json_ordered then
              expected_json_ordered_sha256
            else util.dataset_alias_execution_sha256(
              live_json_ordered::text
            )
          end,
          'valid', valid
        ) order by
          batch_ordinality,
          case role
            when 'flowproperty' then 1
            when 'unitgroup' then 2
            else 3
          end
      ),
      '[]'::jsonb
    ),
    count(*) filter (where not valid)::integer
  into v_support_evidence, v_invalid_support_count
  from hashed_evidence;

  v_live_closure :=
    v_invalid_action_count = 0
    and v_invalid_support_count = 0;

  v_proof_material := jsonb_build_object(
    'schema_version', 'dataset-alias-primary-closure.v1',
    'actor_user_id', p_actor,
    'batch_count', v_batch_count,
    'action_count', v_action_count,
    'distinct_action_count', v_distinct_action_count,
    'flowproperty_count', v_flowproperty_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'support_reference_count', v_support_count,
    'flowproperty_support_count', v_flowproperty_support_count,
    'unitgroup_support_count', v_unitgroup_support_count,
    'source_unitgroup_support_count', v_source_unitgroup_support_count,
    'invalid_action_count', v_invalid_action_count,
    'invalid_support_count', v_invalid_support_count,
    'action_evidence', v_action_evidence,
    'support_evidence', v_support_evidence,
    'live_closure_proof', v_live_closure
  );

  return v_proof_material || jsonb_build_object(
    'ok', v_live_closure,
    'row_count', case when v_live_closure then 52 else null end,
    'exchange_count', case when v_live_closure then 59 else null end,
    'live_closure_proof_sha256',
      util.dataset_alias_execution_artifact_sha256(v_proof_material)
  );
exception
  when others then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-alias-primary-closure.v1',
      'code', 'ALIAS_EXECUTION_PRIMARY_CLOSURE_READ_FAILED',
      'live_closure_proof', false
    );
end;
$_$;

ALTER FUNCTION "util"."read_dataset_alias_execution_primary_closure"("p_actor" "uuid", "p_plan" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."read_dataset_alias_execution_primary_closure"("p_actor" "uuid", "p_plan" "jsonb") FROM PUBLIC;
