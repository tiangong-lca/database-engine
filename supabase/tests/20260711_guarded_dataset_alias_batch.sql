begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.alias_entity_id(
  p_dimension text,
  p_kind text,
  p_index integer default 0
) returns uuid
language sql
immutable
as $$
  select (
    case p_dimension
      when 'time' then case p_kind
        when 'source_unitgroup' then 'c2100000'
        when 'target_unitgroup' then 'c2200000'
        when 'source_flowproperty' then 'c3100000'
        when 'target_flowproperty' then 'c3200000'
        when 'flow' then 'c4000000'
        when 'process' then 'c5000000'
      end
      when 'length_time' then case p_kind
        when 'source_unitgroup' then 'd2100000'
        when 'target_unitgroup' then 'd2200000'
        when 'source_flowproperty' then 'd3100000'
        when 'target_flowproperty' then 'd3200000'
        when 'flow' then 'd4000000'
        when 'process' then 'd5000000'
      end
    end
    || '-0000-0000-0000-'
    || lpad(p_index::text, 12, '0')
  )::uuid;
$$;

create or replace function pg_temp.alias_unit_name(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension when 'time' then 'hr' else 'kmy' end;
$$;

create or replace function pg_temp.alias_factor(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension
    when 'time' then '0.00011415525114155251'
    else '1000'
  end;
$$;

create or replace function pg_temp.alias_source_fp_name(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension when 'time' then 'Amount in hr' else 'Amount in kmy' end;
$$;

create or replace function pg_temp.alias_target_fp_name(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension when 'time' then 'Time' else 'Length*time' end;
$$;

create or replace function pg_temp.alias_source_ug_name(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension when 'time' then 'Units of hr' else 'Units of kmy' end;
$$;

create or replace function pg_temp.alias_target_ug_name(p_dimension text)
returns text
language sql
immutable
as $$
  select case p_dimension when 'time' then 'Units of time' else 'Units of length*time' end;
$$;

create or replace function pg_temp.alias_reference(
  p_type text,
  p_folder text,
  p_id uuid,
  p_version text,
  p_name text
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    '@refObjectId', p_id,
    '@type', p_type,
    '@uri', '../' || p_folder || '/' || p_id::text || '.json',
    '@version', p_version,
    'common:shortDescription', jsonb_build_object(
      '#text', p_name,
      '@xml:lang', 'en'
    )
  );
$$;

create or replace function pg_temp.alias_unitgroup_payload(
  p_dimension text,
  p_target boolean
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'unitGroupDataSet', jsonb_build_object(
      'unitGroupInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.alias_entity_id(
            p_dimension,
            case when p_target then 'target_unitgroup' else 'source_unitgroup' end
          ),
          'common:name', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en',
            '#text', case when p_target
              then pg_temp.alias_target_ug_name(p_dimension)
              else pg_temp.alias_source_ug_name(p_dimension)
            end
          ))
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnit', '1'
        )
      ),
      'units', jsonb_build_object(
        'unit', jsonb_build_array(jsonb_build_object(
          '@dataSetInternalID', case when p_target then '4' else '1' end,
          'meanValue', case when p_target
            then case p_dimension
              when 'time' then '0.00011415525114155251'
              else '1000.0'
            end
            else '1.0'
          end,
          'name', pg_temp.alias_unit_name(p_dimension)
        ))
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', case when p_target
            then '01.00.000'
            else '00.00.000'
          end
        )
      )
    )
  );
$$;

create or replace function pg_temp.alias_flowproperty_payload(
  p_dimension text,
  p_target boolean
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'flowPropertyDataSet', jsonb_build_object(
      'flowPropertiesInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.alias_entity_id(
            p_dimension,
            case when p_target then 'target_flowproperty' else 'source_flowproperty' end
          ),
          'common:name', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en',
            '#text', case when p_target
              then pg_temp.alias_target_fp_name(p_dimension)
              else pg_temp.alias_source_fp_name(p_dimension)
            end
          ))
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnitGroup', pg_temp.alias_reference(
            'unit group data set',
            'unitgroups',
            pg_temp.alias_entity_id(
              p_dimension,
              case when p_target then 'target_unitgroup' else 'source_unitgroup' end
            ),
            case when p_target then '01.00.000' else '00.00.000' end,
            case when p_target
              then pg_temp.alias_target_ug_name(p_dimension)
              else pg_temp.alias_source_ug_name(p_dimension)
            end
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', case when p_target
            then '01.00.000'
            else '00.00.000'
          end
        )
      )
    )
  );
$$;

create or replace function pg_temp.alias_flow_after_payload(p_dimension text)
returns jsonb
language sql
immutable
as $$
  select jsonb_set(
    pg_temp.alias_flowproperty_payload(p_dimension, false),
    '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}',
    pg_temp.alias_flowproperty_payload(p_dimension, true)
      #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}',
    false
  );
$$;

create or replace function pg_temp.alias_dataset_flow_payload(
  p_dimension text,
  p_flow_index integer,
  p_after boolean
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'flowDataSet', jsonb_build_object(
      'flowInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.alias_entity_id(p_dimension, 'flow', p_flow_index),
          'name', jsonb_build_object(
            'baseName', jsonb_build_array(jsonb_build_object(
              '@xml:lang', 'en',
              '#text', 'alias flow'
            ))
          )
        )
      ),
      'flowProperties', jsonb_build_object(
        'flowProperty', jsonb_build_object(
          '@dataSetInternalID', '1',
          'meanValue', '1.0',
          'referenceToFlowPropertyDataSet', pg_temp.alias_reference(
            'flow property data set',
            'flowproperties',
            pg_temp.alias_entity_id(
              p_dimension,
              case when p_after then 'target_flowproperty' else 'source_flowproperty' end
            ),
            case when p_after then '01.00.000' else '00.00.000' end,
            case when p_after
              then pg_temp.alias_target_fp_name(p_dimension)
              else pg_temp.alias_source_fp_name(p_dimension)
            end
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  );
$$;

create or replace function pg_temp.alias_process_exchange_count(
  p_dimension text,
  p_process_index integer
) returns integer
language sql
immutable
as $$
  select case p_dimension
    when 'time' then case when p_process_index <= 6 then 2 else 1 end
    else 3
  end;
$$;

create or replace function pg_temp.alias_process_payload(
  p_dimension text,
  p_process_index integer,
  p_after boolean
) returns jsonb
language sql
immutable
as $$
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'processInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.alias_entity_id(
            p_dimension,
            'process',
            p_process_index
          ),
          'name', jsonb_build_object(
            'baseName', jsonb_build_array(jsonb_build_object(
              '@xml:lang', 'en',
              '#text', 'alias process ' || p_process_index::text
            ))
          )
        )
      ),
      'exchanges', jsonb_build_object(
        'exchange', (
          select jsonb_agg(jsonb_build_object(
            '@dataSetInternalID', ((p_process_index - 1) * 3 + exchange_index)::text,
            'exchangeDirection', case
              when ((p_process_index + exchange_index) % 2) = 0 then 'Input'
              else 'Output'
            end,
            'meanAmount', case when p_after
              then (10::numeric * pg_temp.alias_factor(p_dimension)::numeric)::text
              else '10.0'
            end,
            'resultingAmount', case when p_after
              then (10::numeric * pg_temp.alias_factor(p_dimension)::numeric)::text
              else '10.0'
            end,
            'referenceToFlowDataSet', jsonb_build_object(
              '@refObjectId', pg_temp.alias_entity_id(
                p_dimension,
                'flow',
                (((p_process_index - 1) * 3 + exchange_index - 1)
                  % case p_dimension when 'time' then 10 else 13 end) + 1
              ),
              '@type', 'flow data set',
              '@uri', '../flows/' || pg_temp.alias_entity_id(
                p_dimension,
                'flow',
                (((p_process_index - 1) * 3 + exchange_index - 1)
                  % case p_dimension when 'time' then 10 else 13 end) + 1
              )::text || '.json',
              '@version', '01.00.000'
            )
          ) order by exchange_index)
          from generate_series(
            1,
            pg_temp.alias_process_exchange_count(p_dimension, p_process_index)
          ) as exchange_index
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', '01.00.000'
        )
      )
    )
  );
$$;

create or replace function pg_temp.alias_process_mutation(
  p_dimension text,
  p_process_index integer
) returns jsonb
language sql
immutable
security definer
set search_path = ''
as $$
  with expected as (
    select pg_temp.alias_process_payload(
      p_dimension,
      p_process_index,
      false
    ) as payload
  )
  select jsonb_build_object(
    'kind', 'process_exchange_amounts',
    'exchanges', (
      select jsonb_agg(jsonb_build_object(
        'index', exchange_ordinality - 1,
        'internal_id', exchange_item.value->>'@dataSetInternalID',
        'flow_id', exchange_item.value#>>'{referenceToFlowDataSet,@refObjectId}',
        'flow_version', exchange_item.value#>>'{referenceToFlowDataSet,@version}',
        'direction', exchange_item.value->>'exchangeDirection',
        'before_exchange_sha256', encode(
          extensions.digest(
            convert_to(
              private.dataset_alias_canonical_jsonb_v1(exchange_item.value),
              'UTF8'
            ),
            'sha256'
          ),
          'hex'
        )
      ) order by exchange_ordinality)
      from expected
      cross join lateral jsonb_array_elements(
        expected.payload#>'{processDataSet,exchanges,exchange}'
      ) with ordinality as exchange_item(value, exchange_ordinality)
    )
  );
$$;

create or replace function pg_temp.alias_batch(
  p_dimension text,
  p_batch_id text
) returns jsonb
language plpgsql
stable
as $$
declare
  v_actions jsonb := '[]'::jsonb;
  v_index integer;
  v_flow_count integer := case p_dimension when 'time' then 10 else 13 end;
  v_process_count integer := case p_dimension when 'time' then 14 else 13 end;
begin
  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'action_id', 'flowproperty-alias',
    'action', 'update_json_ordered',
    'table', 'flowproperties',
    'id', pg_temp.alias_entity_id(p_dimension, 'source_flowproperty'),
    'version', '00.00.000',
    'expected_state_code', 0,
    'expected_modified_at', '2026-07-11 00:00:00+00',
    'expected_json_ordered', pg_temp.alias_flowproperty_payload(p_dimension, false),
    'desired_json_ordered', pg_temp.alias_flow_after_payload(p_dimension),
    'mutation', jsonb_build_object(
      'kind', 'flowproperty_unitgroup_reference'
    )
  ));

  for v_index in 1..v_flow_count loop
    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'action_id', 'flow-' || lpad(v_index::text, 2, '0'),
      'action', 'update_json_ordered',
      'table', 'flows',
      'id', pg_temp.alias_entity_id(p_dimension, 'flow', v_index),
      'version', '01.00.000',
      'expected_state_code', 0,
      'expected_modified_at', '2026-07-11 00:00:00+00',
      'expected_json_ordered', pg_temp.alias_dataset_flow_payload(
        p_dimension,
        v_index,
        false
      ),
      'desired_json_ordered', pg_temp.alias_dataset_flow_payload(
        p_dimension,
        v_index,
        true
      ),
      'mutation', jsonb_build_object(
        'kind', 'flow_flowproperty_reference',
        'flow_property_internal_id', '1',
        'source_flowproperty_id', pg_temp.alias_entity_id(
          p_dimension,
          'source_flowproperty'
        ),
        'source_flowproperty_version', '00.00.000'
      )
    ));
  end loop;

  for v_index in 1..v_process_count loop
    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'action_id', 'process-' || lpad(v_index::text, 2, '0'),
      'action', 'update_json_ordered',
      'table', 'processes',
      'id', pg_temp.alias_entity_id(p_dimension, 'process', v_index),
      'version', '01.00.000',
      'expected_state_code', 0,
      'expected_modified_at', '2026-07-11 00:00:00+00',
      'expected_json_ordered', pg_temp.alias_process_payload(
        p_dimension,
        v_index,
        false
      ),
      'desired_json_ordered', pg_temp.alias_process_payload(
        p_dimension,
        v_index,
        true
      ),
      'mutation', pg_temp.alias_process_mutation(p_dimension, v_index)
    ));
  end loop;

  return jsonb_build_object(
    'schema_version', 'dataset-alias-batch.v1',
    'plan_sha256', repeat(case p_dimension when 'time' then 'a' else 'b' end, 64),
    'operation_id', 'maintenance-alias-' || p_dimension,
    'batch_id', p_batch_id,
    'dimension', p_dimension,
    'factor', pg_temp.alias_factor(p_dimension),
    'target_visibility', 'owner_draft',
    'target', jsonb_build_object(
      'flowproperty', jsonb_build_object(
        'id', pg_temp.alias_entity_id(p_dimension, 'target_flowproperty'),
        'version', '01.00.000',
        'expected_modified_at', '2026-07-11 00:00:00+00',
        'expected_json_ordered', pg_temp.alias_flowproperty_payload(p_dimension, true)
      ),
      'unitgroup', jsonb_build_object(
        'id', pg_temp.alias_entity_id(p_dimension, 'target_unitgroup'),
        'version', '01.00.000',
        'expected_modified_at', '2026-07-11 00:00:00+00',
        'expected_json_ordered', pg_temp.alias_unitgroup_payload(p_dimension, true)
      ),
      'source_unitgroup', jsonb_build_object(
        'id', pg_temp.alias_entity_id(p_dimension, 'source_unitgroup'),
        'version', '00.00.000',
        'expected_modified_at', '2026-07-11 00:00:00+00',
        'expected_json_ordered', pg_temp.alias_unitgroup_payload(p_dimension, false)
      )
    ),
    'actions', v_actions
  );
end;
$$;

select plan(63);

alter table public.flowproperties
  disable trigger zz_flowproperties_extracted_text_sync_trigger;
alter table public.flowproperties
  disable trigger flowproperties_set_modified_at_trigger;
alter table public.unitgroups
  disable trigger zz_unitgroups_extracted_text_sync_trigger;
alter table public.unitgroups
  disable trigger unitgroups_set_modified_at_trigger;
alter table public.flows
  disable trigger flow_dataset_extraction_trigger_insert;
alter table public.flows
  disable trigger flow_embedding_ft_on_extract_md_update;
alter table public.flows
  disable trigger flow_extract_md_trigger_update;
alter table public.flows
  disable trigger zz_flows_extracted_text_sync_trigger;
alter table public.flows
  disable trigger flows_set_modified_at_trigger;
alter table public.processes
  disable trigger process_embedding_ft_on_extract_md_update;
alter table public.processes
  disable trigger process_extract_md_trigger_insert;
alter table public.processes
  disable trigger process_extract_md_trigger_update;
alter table public.processes
  disable trigger zz_processes_extracted_text_sync_trigger;
alter table public.processes
  disable trigger processes_set_modified_at_trigger;

select ok(
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_alias_batch_guarded(jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'anon',
    'public.cmd_dataset_alias_batch_guarded(jsonb)',
    'execute'
  )
  and not has_function_privilege(
    'service_role',
    'public.cmd_dataset_alias_batch_guarded(jsonb)',
    'execute'
  ),
  'guarded alias batch is executable only by authenticated callers'
);

select ok(
  strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'security definer'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'set search_path to '''''
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'set lock_timeout to ''5s'''
  ) > 0,
  'guarded alias batch hardens its definer search path and lock timeout'
);

select ok(
  strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'and t.user_id = $3'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'for update of t'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'and t.state_code = 0'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'and target_fp.user_id = v_actor'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'where support_ug.user_id = v_actor'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'order by support_ug.id, support_ug.version'
  ) > 0
  and strpos(
    lower(pg_get_functiondef('public.cmd_dataset_alias_batch_guarded(jsonb)'::regprocedure)),
    'lock table public.flows, public.processes in share row exclusive mode'
  ) > 0,
  'guarded alias batch uses stable closure locks and owner-scoped row locks'
);

select ok(
  to_regclass('public.command_audit_log_guarded_alias_batch_row_replay_idx') is not null
  and to_regclass('public.command_audit_log_guarded_alias_batch_summary_replay_idx') is not null
  and (
    select bool_and(index_meta.indisvalid and index_meta.indisready)
    from pg_catalog.pg_index as index_meta
    where index_meta.indexrelid in (
      'public.command_audit_log_guarded_alias_batch_row_replay_idx'::regclass,
      'public.command_audit_log_guarded_alias_batch_summary_replay_idx'::regclass
    )
  ),
  'guarded alias replay indexes are valid and ready'
);

select set_config('request.jwt.claim.role', 'authenticated', true);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    'c1000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'alias-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"c1000000-0000-0000-0000-000000000001"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    'c1000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'alias-other@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"c1000000-0000-0000-0000-000000000002"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.unitgroups (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
select
  pg_temp.alias_entity_id(dimension, kind),
  case kind when 'target_unitgroup' then '01.00.000' else '00.00.000' end,
  pg_temp.alias_unitgroup_payload(
    dimension,
    kind = 'target_unitgroup'
  ),
  'c1000000-0000-0000-0000-000000000001'::uuid,
  0,
  true,
  '2026-07-11 00:00:00+00'
from (values ('time'), ('length_time')) as dimensions(dimension)
cross join (values ('source_unitgroup'), ('target_unitgroup')) as kinds(kind);

insert into public.flowproperties (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
select
  pg_temp.alias_entity_id(dimension, kind),
  case kind when 'target_flowproperty' then '01.00.000' else '00.00.000' end,
  pg_temp.alias_flowproperty_payload(
    dimension,
    kind = 'target_flowproperty'
  ),
  'c1000000-0000-0000-0000-000000000001'::uuid,
  0,
  true,
  '2026-07-11 00:00:00+00'
from (values ('time'), ('length_time')) as dimensions(dimension)
cross join (values ('source_flowproperty'), ('target_flowproperty')) as kinds(kind);

insert into public.flows (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
select
  pg_temp.alias_entity_id(dimension, 'flow', flow_index),
  '01.00.000',
  pg_temp.alias_dataset_flow_payload(dimension, flow_index, false),
  'c1000000-0000-0000-0000-000000000001',
  0,
  true,
  '2026-07-11 00:00:00+00'
from (values ('time', 10), ('length_time', 13)) as dimensions(dimension, flow_count)
cross join lateral generate_series(1, dimensions.flow_count) as flow_index;

insert into public.processes (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
select
  pg_temp.alias_entity_id(dimension, 'process', process_index),
  '01.00.000',
  pg_temp.alias_process_payload(dimension, process_index, false),
  'c1000000-0000-0000-0000-000000000001',
  0,
  true,
  '2026-07-11 00:00:00+00'
from (values ('time', 14), ('length_time', 13)) as dimensions(dimension, process_count)
cross join lateral generate_series(1, dimensions.process_count) as process_index;

alter table public.command_audit_log
  add constraint command_audit_log_test_force_alias_batch_row_failure
  check (
    not (
      payload->>'record_type' = 'row'
      and payload->>'batch_id' = 'length-time-force-audit-failure'
      and payload->>'action_id' = 'process-13'
    )
  );

set local role authenticated;
select set_config('request.jwt.claim.sub', '', true);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  )->>'code',
  'AUTH_REQUIRED',
  'guarded alias batch requires an authenticated actor identity'
);

select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-invalid-target-visibility'),
      '{target_visibility}',
      '"public"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_TARGET_VISIBILITY',
  'guarded alias batch accepts only the explicit owner_draft target visibility'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-missing-target-visibility')
      - 'target_visibility'
  )->>'code',
  'ALIAS_BATCH_INVALID_REQUEST',
  'guarded alias batch requires target_visibility in the exact request shape'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-invalid-factor'),
      '{factor}',
      '"1"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_FACTOR',
  'guarded alias batch rejects any factor outside the frozen dimension conversion'
);

reset role;

insert into public.flows (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'flow', 999),
  '01.00.000',
  jsonb_set(
    pg_temp.alias_dataset_flow_payload('time', 999, false),
    '{flowDataSet,flowProperties,flowProperty}',
    jsonb_build_array(
      pg_temp.alias_dataset_flow_payload('time', 999, false)
        #> '{flowDataSet,flowProperties,flowProperty}'
    ),
    false
  ),
  'c1000000-0000-0000-0000-000000000001',
  0,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-extra-flow-closure')
  )->>'code',
  'ALIAS_BATCH_CLOSURE_MISMATCH',
  'guarded alias batch rejects an omitted legacy array-shaped owner flow that still references the source flow property'
);

reset role;

delete from public.flows
where id = pg_temp.alias_entity_id('time', 'flow', 999)
  and version = '01.00.000';

insert into public.flows (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'flow', 997),
  '01.00.000',
  pg_temp.alias_dataset_flow_payload('time', 997, false),
  'c1000000-0000-0000-0000-000000000001',
  100,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-flow-closure')
  ),
  jsonb_build_object(
    'ok', false,
    'code', 'ALIAS_BATCH_CLOSURE_MISMATCH',
    'status', 409,
    'message', 'Live owner-draft closure does not match the exact submitted batch'
  ),
  'guarded alias batch rejects an omitted public flow reference without disclosing closure details'
);

reset role;

delete from public.flows
where id = pg_temp.alias_entity_id('time', 'flow', 997)
  and version = '01.00.000';

insert into public.flows (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'flow', 998),
  '01.00.000',
  pg_temp.alias_dataset_flow_payload('time', 998, false),
  'c1000000-0000-0000-0000-000000000002',
  0,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-flow-closure')
  )->>'code',
  'ALIAS_BATCH_CLOSURE_MISMATCH',
  'guarded alias batch rejects an omitted foreign-owner flow reference without disclosing closure details'
);

reset role;

delete from public.flows
where id = pg_temp.alias_entity_id('time', 'flow', 998)
  and version = '01.00.000';

insert into public.processes (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'process', 999),
  '01.00.000',
  pg_temp.alias_process_payload('time', 999, false),
  'c1000000-0000-0000-0000-000000000001',
  0,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-extra-process-closure')
  )->>'code',
  'ALIAS_BATCH_CLOSURE_MISMATCH',
  'guarded alias batch rejects an omitted owner process exchange that references an affected flow'
);

reset role;

delete from public.processes
where id = pg_temp.alias_entity_id('time', 'process', 999)
  and version = '01.00.000';

insert into public.processes (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'process', 997),
  '01.00.000',
  pg_temp.alias_process_payload('time', 997, false),
  'c1000000-0000-0000-0000-000000000001',
  100,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-process-closure')
  )->>'code',
  'ALIAS_BATCH_CLOSURE_MISMATCH',
  'guarded alias batch rejects an omitted public process reference without disclosing closure details'
);

reset role;

delete from public.processes
where id = pg_temp.alias_entity_id('time', 'process', 997)
  and version = '01.00.000';

insert into public.processes (
  id,
  version,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at
)
values (
  pg_temp.alias_entity_id('time', 'process', 998),
  '01.00.000',
  pg_temp.alias_process_payload('time', 998, false),
  'c1000000-0000-0000-0000-000000000002',
  0,
  true,
  '2026-07-11 00:00:00+00'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-process-closure')
  )->>'code',
  'ALIAS_BATCH_CLOSURE_MISMATCH',
  'guarded alias batch rejects an omitted foreign-owner process reference without disclosing closure details'
);

reset role;

delete from public.processes
where id = pg_temp.alias_entity_id('time', 'process', 998)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-bad-count') #- '{actions,24}'
  )->>'code',
  'ALIAS_BATCH_INVALID_COUNTS',
  'guarded alias batch rejects a dimension with the wrong row count'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-bad-table'),
      '{actions,1,table}',
      '"unitgroups"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_ACTION',
  'guarded alias batch rejects tables outside the exact allowlist'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-target-drift'),
      '{target,flowproperty,expected_modified_at}',
      '"2026-07-11 00:00:01+00"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_TARGET_PRECONDITION_FAILED',
  'guarded alias batch rejects a drifted owner-draft target snapshot'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-source-support-drift'),
      '{target,source_unitgroup,expected_modified_at}',
      '"2026-07-11 00:00:01+00"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_SOURCE_SUPPORT_PRECONDITION_FAILED',
  'guarded alias batch rejects drift in the locked source conversion support'
);

reset role;

update public.flowproperties
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'target_flowproperty')
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-target-flowproperty')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public target flow property without disclosing it'
);

reset role;

update public.flowproperties
set state_code = 0,
    user_id = 'c1000000-0000-0000-0000-000000000002'
where id = pg_temp.alias_entity_id('time', 'target_flowproperty')
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-target-flowproperty')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a foreign target flow property without disclosing it'
);

reset role;

update public.flowproperties
set user_id = 'c1000000-0000-0000-0000-000000000001'
where id = pg_temp.alias_entity_id('time', 'target_flowproperty')
  and version = '01.00.000';

update public.unitgroups
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'target_unitgroup')
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-target-unitgroup')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public target unit group without disclosing it'
);

reset role;

update public.unitgroups
set state_code = 0,
    user_id = 'c1000000-0000-0000-0000-000000000002'
where id = pg_temp.alias_entity_id('time', 'target_unitgroup')
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-target-unitgroup')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a foreign target unit group without disclosing it'
);

reset role;

update public.unitgroups
set user_id = 'c1000000-0000-0000-0000-000000000001'
where id = pg_temp.alias_entity_id('time', 'target_unitgroup')
  and version = '01.00.000';

update public.unitgroups
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'source_unitgroup')
  and version = '00.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-source-unitgroup')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public source unit group without disclosing it'
);

reset role;

update public.unitgroups
set state_code = 0,
    user_id = 'c1000000-0000-0000-0000-000000000002'
where id = pg_temp.alias_entity_id('time', 'source_unitgroup')
  and version = '00.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-source-unitgroup')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a foreign source unit group without disclosing it'
);

reset role;

update public.unitgroups
set user_id = 'c1000000-0000-0000-0000-000000000001'
where id = pg_temp.alias_entity_id('time', 'source_unitgroup')
  and version = '00.00.000';

update public.flowproperties
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'source_flowproperty')
  and version = '00.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-source-flowproperty')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public source flow property action'
);

reset role;

update public.flowproperties
set state_code = 0
where id = pg_temp.alias_entity_id('time', 'source_flowproperty')
  and version = '00.00.000';

update public.flows
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'flow', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-flow')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public flow action'
);

reset role;

update public.flows
set state_code = 0
where id = pg_temp.alias_entity_id('time', 'flow', 1)
  and version = '01.00.000';

update public.processes
set state_code = 100
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-public-process')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a public process action'
);

reset role;

update public.processes
set state_code = 0,
    user_id = 'c1000000-0000-0000-0000-000000000002'
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-foreign-process')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'owner_draft rejects a foreign process action without disclosing it'
);

reset role;

update public.processes
set user_id = 'c1000000-0000-0000-0000-000000000001'
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
  ),
  '0',
  'visibility, ownership, closure, and drift rejections leave no partial audit rows'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-fp-extra-change'),
      '{actions,0,desired_json_ordered,flowPropertyDataSet,extra}',
      'true'::jsonb,
      true
    )
  )->>'code',
  'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
  'flow property desired payload cannot change outside the exact unit-group reference'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-embedded-uuid-mismatch'),
      '{actions,0,desired_json_ordered,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:UUID}',
      '"ffffffff-ffff-ffff-ffff-ffffffffffff"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_ACTION',
  'guarded alias batch binds desired embedded UUID identity to the exact row key'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-flow-ref-extra'),
      '{actions,1,desired_json_ordered,flowDataSet,flowProperties,flowProperty,referenceToFlowPropertyDataSet,extra}',
      'true'::jsonb,
      true
    )
  )->>'code',
  'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
  'flow desired reference must exactly match the canonical target reference object'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-process-extra-change'),
      '{actions,11,desired_json_ordered,processDataSet,extra}',
      'true'::jsonb,
      true
    )
  )->>'code',
  'ALIAS_BATCH_MUTATION_SCOPE_VIOLATION',
  'process desired payload cannot change outside listed exchange amounts'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-bad-factor-amount'),
      '{actions,11,desired_json_ordered,processDataSet,exchanges,exchange,0,meanAmount}',
      '"9.0"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_AMOUNT',
  'process desired amount must use the exact frozen factor and preserve the amount pair'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-bad-exchange-evidence'),
      '{actions,11,mutation,exchanges,0,direction}',
      '"Output"'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_MUTATION_EVIDENCE_MISMATCH',
  'process exchange locator evidence must match the exact before exchange'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-bad-exchange-hash'),
      '{actions,11,mutation,exchanges,0,before_exchange_sha256}',
      to_jsonb(repeat('f', 64))
    )
  )->>'code',
  'ALIAS_BATCH_MUTATION_EVIDENCE_MISMATCH',
  'process exchange evidence hash is recomputed with sorted-key compact canonical JSON'
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    jsonb_set(
      pg_temp.alias_batch('time', 'time-oversized-exchange-index'),
      '{actions,11,mutation,exchanges,0,index}',
      '1000000'::jsonb
    )
  )->>'code',
  'ALIAS_BATCH_INVALID_MUTATION',
  'process exchange index is bounded before its integer cast'
);

reset role;

update public.processes
set modified_at = '2026-07-11 00:00:01+00'
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-row-drift')
  )->>'code',
  'ALIAS_BATCH_PRECONDITION_FAILED',
  'guarded alias batch rejects any locked row modified_at drift'
);

reset role;

update public.processes
set modified_at = '2026-07-11 00:00:00+00'
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

update public.flows
set user_id = 'c1000000-0000-0000-0000-000000000002'
where id = pg_temp.alias_entity_id('time', 'flow', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-other-owner')
  )->>'code',
  'ALIAS_BATCH_DATASET_NOT_FOUND',
  'guarded alias batch does not disclose another owner row'
);

reset role;

update public.flows
set user_id = 'c1000000-0000-0000-0000-000000000001'
where id = pg_temp.alias_entity_id('time', 'flow', 1)
  and version = '01.00.000';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select throws_ok(
  $$
    select public.cmd_dataset_alias_batch_guarded(
      pg_temp.alias_batch('length_time', 'length-time-force-audit-failure')
    )
  $$,
  '23514',
  null,
  'forced row-audit failure aborts the guarded alias batch statement'
);

reset role;

select is(
  (
    select count(*)::text
    from public.flowproperties
    where id = pg_temp.alias_entity_id('length_time', 'source_flowproperty')
      and json_ordered::jsonb = pg_temp.alias_flowproperty_payload('length_time', false)
  )
  || ':' || (
    select count(*)::text
    from public.flows
    where id::text like 'd4000000-%'
      and json_ordered::jsonb = pg_temp.alias_dataset_flow_payload(
        'length_time',
        right(id::text, 12)::integer,
        false
      )
  )
  || ':' || (
    select count(*)::text
    from public.processes
    where id::text like 'd5000000-%'
      and json_ordered::jsonb = pg_temp.alias_process_payload(
        'length_time',
        right(id::text, 12)::integer,
        false
      )
  ),
  '1:13:13',
  'forced row-audit failure rolls back every prior row update in the dimension'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'length-time-force-audit-failure'
  ),
  '0',
  'forced row-audit failure also rolls back the batch summary audit'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select ok(
  committed.result @> '{"ok":true,"target_visibility":"owner_draft","idempotent_replay":false,"row_count":25,"exchange_count":20}'::jsonb
    and jsonb_typeof(committed.result->'summary_audit_id') = 'string'
    and not exists (
      select 1
      from jsonb_array_elements(committed.result->'audit') as audit_item(value)
      where jsonb_typeof(audit_item.value->'audit_id') <> 'string'
    ),
  'time dimension commits atomically and returns lossless decimal-string audit IDs: '
    || committed.result::text
)
from (
  select public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  ) as result
) as committed;

reset role;

select is(
  (
    select count(*)::text
    from public.flowproperties
    where id = pg_temp.alias_entity_id('time', 'source_flowproperty')
      and state_code = 0
      and json_ordered::jsonb = pg_temp.alias_flow_after_payload('time')
  )
  || ':' || (
    select count(*)::text
    from public.flows
    where id::text like 'c4000000-%'
      and state_code = 0
      and json_ordered::jsonb = pg_temp.alias_dataset_flow_payload(
        'time',
        right(id::text, 12)::integer,
        true
      )
  )
  || ':' || (
    select count(*)::text
    from public.processes
    where id::text like 'c5000000-%'
      and state_code = 0
      and json_ordered::jsonb = pg_temp.alias_process_payload(
        'time',
        right(id::text, 12)::integer,
        true
      )
  ),
  '1:10:14',
  'time dimension changes exact desired payloads while every dataset remains state_code=0'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'time-success'
  ),
  '26',
  'time commit writes one summary plus one correlated audit for each row'
);

select is(
  (
    select count(*) filter (where target_table is null)::text
      || ':' || count(*) filter (where target_table is not null)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'time-success'
      and payload->>'hash_algorithm' = 'postgres-jsonb-text-sha256'
      and payload->>'target_visibility' = 'owner_draft'
  ),
  '1:25',
  'time audit set contains one exact summary and 25 row proofs'
);

savepoint alias_mixed_partial_state;

update public.processes
set json_ordered = pg_temp.alias_process_payload('time', 1, false)::json,
    modified_at = '2026-07-11 00:00:00+00'
where id = pg_temp.alias_entity_id('time', 'process', 1)
  and version = '01.00.000';

delete from public.command_audit_log
where command = 'cmd_dataset_alias_batch_guarded'
  and payload->>'batch_id' = 'time-success'
  and payload->>'action_id' = 'process-01';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  )->>'code',
  'ALIAS_BATCH_PARTIAL_STATE',
  'replay rejects a mixed before/desired batch even when the remaining desired rows retain exact audits'
);

reset role;
rollback to savepoint alias_mixed_partial_state;
release savepoint alias_mixed_partial_state;

savepoint alias_missing_row_audit;

delete from public.command_audit_log
where command = 'cmd_dataset_alias_batch_guarded'
  and payload->>'batch_id' = 'time-success'
  and payload->>'action_id' = 'process-01';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  )->>'code',
  'ALIAS_BATCH_REPLAY_UNPROVEN',
  'replay rejects a desired-state row when its exact row audit is missing'
);

reset role;
rollback to savepoint alias_missing_row_audit;
release savepoint alias_missing_row_audit;

savepoint alias_tampered_row_audit;

update public.command_audit_log
set payload = jsonb_set(
  payload,
  '{desired_json_ordered_sha256}',
  to_jsonb(repeat('f', 64)),
  false
)
where command = 'cmd_dataset_alias_batch_guarded'
  and payload->>'batch_id' = 'time-success'
  and payload->>'action_id' = 'process-01';

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  )->>'code',
  'ALIAS_BATCH_REPLAY_UNPROVEN',
  'replay rejects a row audit whose semantic hash was tampered'
);

reset role;
rollback to savepoint alias_tampered_row_audit;
release savepoint alias_tampered_row_audit;

savepoint alias_missing_summary_audit;

delete from public.command_audit_log
where command = 'cmd_dataset_alias_batch_guarded'
  and payload->>'batch_id' = 'time-success'
  and target_table is null;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select is(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  )->>'code',
  'ALIAS_BATCH_REPLAY_UNPROVEN',
  'replay rejects a complete desired row set when the exact summary audit is missing'
);

reset role;
rollback to savepoint alias_missing_summary_audit;
release savepoint alias_missing_summary_audit;

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select ok(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  ) @> '{"ok":true,"target_visibility":"owner_draft","idempotent_replay":true,"row_count":25,"exchange_count":20}'::jsonb,
  'identical time request replays from one summary and every exact row audit'
);

select ok(
  jsonb_typeof(replay.result->'summary_audit_id') = 'string'
  and not exists (
    select 1
    from jsonb_array_elements(replay.result->'audit') as audit_item(value)
    where jsonb_typeof(audit_item.value->'audit_id') <> 'string'
  ),
  'replay responses expose bigint audit IDs as lossless decimal strings'
)
from (
  select public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('time', 'time-success')
  ) as result
) as replay;

reset role;

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'time-success'
  ),
  '26',
  'time replay does not duplicate summary or row audits'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select ok(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('length_time', 'length-time-success')
  ) @> '{"ok":true,"target_visibility":"owner_draft","idempotent_replay":false,"row_count":27,"exchange_count":39}'::jsonb,
  'length_time dimension commits all 27 rows and 39 exchange mutations atomically'
);

reset role;

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'length-time-success'
  ),
  '28',
  'length_time commit writes one summary plus 27 row audits'
);

select is(
  (
    select count(*)::text
    from public.processes
    where id::text like 'd5000000-%'
      and state_code = 0
      and json_ordered::jsonb = pg_temp.alias_process_payload(
        'length_time',
        right(id::text, 12)::integer,
        true
      )
  ),
  '13',
  'length_time commit scales only the desired amount pairs in all 13 processes'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'c1000000-0000-0000-0000-000000000001',
  true
);

select ok(
  public.cmd_dataset_alias_batch_guarded(
    pg_temp.alias_batch('length_time', 'length-time-success')
  ) @> '{"ok":true,"target_visibility":"owner_draft","idempotent_replay":true,"row_count":27,"exchange_count":39}'::jsonb,
  'identical length_time request is audit-proven replay'
);

reset role;

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and payload->>'batch_id' = 'length-time-success'
  ),
  '28',
  'length_time replay does not duplicate audit rows'
);

select ok(
  (
    select bool_and(jsonb_typeof(payload->'committed_modified_at') = 'string')
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and target_table is not null
  ),
  'row audits bind replay to each committed modified_at value'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and target_table is null
      and payload ?& array[
        'target_flowproperty_sha256',
        'target_unitgroup_sha256',
        'source_unitgroup_sha256',
        'exchange_count'
      ]
      and payload->>'target_visibility' = 'owner_draft'
      and payload->>'support_owner_user_id' = 'c1000000-0000-0000-0000-000000000001'
      and payload->>'target_flowproperty_expected_state_code' = '0'
      and payload->>'target_unitgroup_expected_state_code' = '0'
      and payload->>'source_unitgroup_expected_state_code' = '0'
  ),
  '2',
  'batch summaries freeze target/source support snapshots and exchange cardinality'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_dataset_alias_batch_guarded'
      and target_table is not null
      and payload->>'mutation_sha256' ~ '^[a-f0-9]{64}$'
      and payload->>'expected_json_ordered_sha256' ~ '^[a-f0-9]{64}$'
      and payload->>'desired_json_ordered_sha256' ~ '^[a-f0-9]{64}$'
      and payload->>'target_visibility' = 'owner_draft'
  ),
  '52',
  'all row audits contain DB-computed mutation, before, and desired semantic hashes'
);

select is(
  (
    select count(*)::text
    from public.unitgroups
    where id in (
      pg_temp.alias_entity_id('time', 'source_unitgroup'),
      pg_temp.alias_entity_id('time', 'target_unitgroup'),
      pg_temp.alias_entity_id('length_time', 'source_unitgroup'),
      pg_temp.alias_entity_id('length_time', 'target_unitgroup')
    )
      and user_id = 'c1000000-0000-0000-0000-000000000001'
      and state_code = 0
      and modified_at = '2026-07-11 00:00:00+00'
  ),
  '4',
  'guarded alias batches lock but never modify source or target unit groups'
);

select is(
  (
    select count(*)::text
    from public.flowproperties
    where id in (
      pg_temp.alias_entity_id('time', 'target_flowproperty'),
      pg_temp.alias_entity_id('length_time', 'target_flowproperty')
    )
      and user_id = 'c1000000-0000-0000-0000-000000000001'
      and state_code = 0
      and modified_at = '2026-07-11 00:00:00+00'
  ),
  '2',
  'guarded alias batches lock but never modify owner-draft target flow properties'
);

select * from finish();
rollback;
