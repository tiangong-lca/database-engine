-- Disposable hosted-Preview fixture for the protected owner-draft alias E2E.
--
-- The Node runner replaces each whole {{..._SQL}} token with a validated SQL
-- literal.  This script is deliberately committed setup: run the matching
-- cleanup script for the same actor/scenario/request after every scenario.
-- Never run this file against persistent dev or production.

begin;

set local search_path = extensions, public, auth;
set local lock_timeout = '5s';
set local statement_timeout = '120s';

create temporary table preview_alias_fixture_config (
  actor_user_id uuid primary key,
  actor_email text not null,
  scenario_id text not null,
  scenario_kind text not null,
  request_id uuid not null,
  preview_ref text not null,
  preview_url text not null,
  service_role_key text not null,
  vault_description text not null,
  approved_at_utc text not null,
  fixture_modified_at timestamptz not null
) on commit drop;

insert into preview_alias_fixture_config values (
  ({{ACTOR_UUID_SQL}})::uuid,
  lower(btrim(({{ACTOR_EMAIL_SQL}})::text)),
  btrim(({{SCENARIO_NAMESPACE_SQL}})::text),
  btrim(({{SCENARIO_KIND_SQL}})::text),
  ({{REQUEST_ID_SQL}})::uuid,
  lower(btrim(({{PREVIEW_REF_SQL}})::text)),
  btrim(({{PREVIEW_URL_SQL}})::text),
  ({{SERVICE_ROLE_KEY_SQL}})::text,
  'database-engine#262 protected-alias-preview-e2e scenario='
    || btrim(({{SCENARIO_NAMESPACE_SQL}})::text)
    || ' request=' || ({{REQUEST_ID_SQL}})::uuid::text,
  '2026-07-16T00:00:00Z',
  '2026-07-15T00:00:00Z'::timestamptz
);

create or replace function pg_temp.preview_alias_uuid(p_name text)
returns uuid
language sql
stable
strict
as $$
  select extensions.uuid_generate_v5(
    extensions.uuid_ns_url(),
    'database-engine#262/' || config.scenario_id || '/' || p_name
  )
  from pg_temp.preview_alias_fixture_config as config
$$;

create or replace function pg_temp.preview_alias_entity_id(
  p_dimension text,
  p_kind text,
  p_index integer default 0
) returns uuid
language sql
stable
as $$
  select pg_temp.preview_alias_uuid(
    p_dimension || '/' || p_kind || '/' || p_index::text
  )
$$;

do $fixture_gate$
declare
  config pg_temp.preview_alias_fixture_config%rowtype;
  server_context jsonb;
begin
  select * into strict config from pg_temp.preview_alias_fixture_config;

  if config.actor_email !~ '^[^[:space:]@]+@[^[:space:]@]+$'
    or octet_length(config.actor_email) > 320
    or config.scenario_id !~ '^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$'
    or config.scenario_kind !~ '^[a-z0-9][a-z0-9_-]{1,63}$'
    or config.preview_ref !~ '^[a-z0-9][a-z0-9-]{3,127}$'
    or config.preview_ref = 'qgzvkongdjqiiamzbbts'
    or config.preview_url is distinct from
      'https://' || config.preview_ref || '.supabase.co'
    or config.service_role_key is distinct from btrim(config.service_role_key)
    or config.service_role_key ~ '[[:space:]]'
    or octet_length(config.service_role_key) not between 20 and 16384
    or config.vault_description is distinct from
      'database-engine#262 protected-alias-preview-e2e scenario='
        || config.scenario_id || ' request=' || config.request_id::text then
    raise exception 'invalid protected alias Preview fixture parameters';
  end if;

  if to_regprocedure('public.cmd_dataset_alias_execution_preflight_guarded(jsonb)') is null
    or to_regprocedure('public.cmd_dataset_alias_plan_guarded(jsonb)') is null
    or to_regprocedure('util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)') is null
    or to_regprocedure('util.dataset_derivative_rebuild_snapshot(text,uuid,text)') is null then
    raise exception 'protected alias production toolchain is incomplete';
  end if;

  if exists (
    select 1
    from vault.secrets as secret
    where secret.name in ('project_url', 'project_secret_key')
  ) then
    raise exception
      'fixture requires both branch-local Vault secret names to be absent';
  end if;

  perform vault.create_secret(
    config.preview_url,
    'project_url',
    config.vault_description
  );
  perform vault.create_secret(
    config.service_role_key,
    'project_secret_key',
    config.vault_description
  );

  if (
    select count(*)
    from vault.decrypted_secrets as secret
    where secret.name in ('project_url', 'project_secret_key')
      and secret.description is not distinct from config.vault_description
      and (
        (secret.name = 'project_url'
          and secret.decrypted_secret is not distinct from config.preview_url)
        or (secret.name = 'project_secret_key'
          and secret.decrypted_secret is not distinct from config.service_role_key)
      )
  ) is distinct from 2 then
    raise exception 'fixture Vault secret creation did not seal both exact values';
  end if;

  server_context := util.dataset_alias_execution_server_context();
  if server_context->>'environment' is distinct from 'preview'
    or server_context->>'project_ref' is distinct from config.preview_ref then
    raise exception
      'fixture requires trusted Preview ref %, got %',
      config.preview_ref,
      server_context;
  end if;

  if not exists (
    select 1 from auth.users as actor
    where actor.id = config.actor_user_id
      and lower(btrim(actor.email)) = config.actor_email
  ) then
    raise exception 'fixture actor UUID/email is not an exact auth.users identity';
  end if;

  if not has_table_privilege('service_role', 'public.command_audit_log', 'SELECT') then
    raise exception 'service_role cannot read the sealed fixture artifact';
  end if;

  if exists (
    select 1 from public.command_audit_log as audit
    where audit.command = 'preview_e2e_protected_alias_fixture'
      and audit.actor_user_id = config.actor_user_id
      and audit.target_table = 'preview_e2e_protected_alias'
      and audit.target_id = config.request_id
  ) or exists (
    select 1 from util.dataset_alias_execution_preflights
    where id = config.request_id
  ) or exists (
    select 1 from util.dataset_alias_execution_requests
    where id = config.request_id
  ) then
    raise exception 'fixture request already has durable residue; cleanup first';
  end if;

  if exists (
    select 1 from public.flowproperties
    where id in (
      pg_temp.preview_alias_entity_id('time', 'source_flowproperty'),
      pg_temp.preview_alias_entity_id('time', 'target_flowproperty'),
      pg_temp.preview_alias_entity_id('length_time', 'source_flowproperty'),
      pg_temp.preview_alias_entity_id('length_time', 'target_flowproperty')
    )
  ) or exists (
    select 1 from public.unitgroups
    where id in (
      pg_temp.preview_alias_entity_id('time', 'source_unitgroup'),
      pg_temp.preview_alias_entity_id('time', 'target_unitgroup'),
      pg_temp.preview_alias_entity_id('length_time', 'source_unitgroup'),
      pg_temp.preview_alias_entity_id('length_time', 'target_unitgroup')
    )
  ) or exists (
    select 1 from public.flows
    where id in (
      select pg_temp.preview_alias_entity_id(dimension, 'flow', ordinal)
      from (values ('time'::text, 10), ('length_time'::text, 13)) as dimensions(dimension, row_count)
      cross join lateral generate_series(1, dimensions.row_count) as ordinal
    )
  ) or exists (
    select 1 from public.processes
    where id in (
      select pg_temp.preview_alias_entity_id(dimension, 'process', ordinal)
      from (values ('time'::text, 14), ('length_time'::text, 13)) as dimensions(dimension, row_count)
      cross join lateral generate_series(1, dimensions.row_count) as ordinal
    )
  ) then
    raise exception 'scenario-derived fixture keys already exist; refusing overlap';
  end if;
end;
$fixture_gate$;

create or replace function pg_temp.preview_alias_factor(p_dimension text)
returns text language sql immutable as $$
  select case p_dimension
    when 'time' then '0.00011415525114155251'
    when 'length_time' then '1000'
  end
$$;

create or replace function pg_temp.preview_alias_reference(
  p_type text,
  p_folder text,
  p_id uuid,
  p_version text,
  p_name text
) returns jsonb language sql immutable as $$
  select jsonb_build_object(
    '@refObjectId', p_id,
    '@type', p_type,
    '@uri', '../' || p_folder || '/' || p_id::text || '.json',
    '@version', p_version,
    'common:shortDescription', jsonb_build_object('#text', p_name, '@xml:lang', 'en')
  )
$$;

create or replace function pg_temp.preview_alias_unitgroup_payload(
  p_dimension text,
  p_target boolean
) returns jsonb language sql stable as $$
  select jsonb_build_object(
    'unitGroupDataSet', jsonb_build_object(
      'unitGroupInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_alias_entity_id(
            p_dimension,
            case when p_target then 'target_unitgroup' else 'source_unitgroup' end
          ),
          'common:name', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en',
            '#text', case
              when p_dimension = 'time' and p_target then 'Units of time'
              when p_dimension = 'time' then 'Units of hr'
              when p_target then 'Units of length*time'
              else 'Units of kmy'
            end
          ))
        ),
        'quantitativeReference', jsonb_build_object('referenceToReferenceUnit', '1')
      ),
      'units', jsonb_build_object('unit', jsonb_build_array(jsonb_build_object(
        '@dataSetInternalID', case when p_target then '4' else '1' end,
        'meanValue', case
          when not p_target then '1.0'
          when p_dimension = 'time' then '0.00011415525114155251'
          else '1000.0'
        end,
        'name', case when p_dimension = 'time' then 'hr' else 'kmy' end
      ))),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', case when p_target then '01.00.000' else '00.00.000' end
        )
      )
    )
  )
$$;

create or replace function pg_temp.preview_alias_flowproperty_payload(
  p_dimension text,
  p_target boolean
) returns jsonb language sql stable as $$
  select jsonb_build_object(
    'flowPropertyDataSet', jsonb_build_object(
      'flowPropertiesInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_alias_entity_id(
            p_dimension,
            case when p_target then 'target_flowproperty' else 'source_flowproperty' end
          ),
          'common:name', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en',
            '#text', case
              when p_dimension = 'time' and p_target then 'Time'
              when p_dimension = 'time' then 'Amount in hr'
              when p_target then 'Length*time'
              else 'Amount in kmy'
            end
          ))
        ),
        'quantitativeReference', jsonb_build_object(
          'referenceToReferenceUnitGroup', pg_temp.preview_alias_reference(
            'unit group data set',
            'unitgroups',
            pg_temp.preview_alias_entity_id(
              p_dimension,
              case when p_target then 'target_unitgroup' else 'source_unitgroup' end
            ),
            case when p_target then '01.00.000' else '00.00.000' end,
            case
              when p_dimension = 'time' and p_target then 'Units of time'
              when p_dimension = 'time' then 'Units of hr'
              when p_target then 'Units of length*time'
              else 'Units of kmy'
            end
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object(
          'common:dataSetVersion', case when p_target then '01.00.000' else '00.00.000' end
        )
      )
    )
  )
$$;

create or replace function pg_temp.preview_alias_flowproperty_after(p_dimension text)
returns jsonb language sql stable as $$
  select jsonb_set(
    pg_temp.preview_alias_flowproperty_payload(p_dimension, false),
    '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}',
    pg_temp.preview_alias_flowproperty_payload(p_dimension, true)
      #> '{flowPropertyDataSet,flowPropertiesInformation,quantitativeReference,referenceToReferenceUnitGroup}',
    false
  )
$$;

create or replace function pg_temp.preview_alias_flow_payload(
  p_dimension text,
  p_ordinal integer,
  p_after boolean
) returns jsonb language sql stable as $$
  select jsonb_build_object(
    'flowDataSet', jsonb_build_object(
      'flowInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_alias_entity_id(p_dimension, 'flow', p_ordinal),
          'name', jsonb_build_object('baseName', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en', '#text', 'Preview protected alias flow'
          )))
        )
      ),
      'flowProperties', jsonb_build_object(
        'flowProperty', jsonb_build_object(
          '@dataSetInternalID', '1',
          'meanValue', '1.0',
          'referenceToFlowPropertyDataSet', pg_temp.preview_alias_reference(
            'flow property data set',
            'flowproperties',
            pg_temp.preview_alias_entity_id(
              p_dimension,
              case when p_after then 'target_flowproperty' else 'source_flowproperty' end
            ),
            case when p_after then '01.00.000' else '00.00.000' end,
            case
              when p_dimension = 'time' and p_after then 'Time'
              when p_dimension = 'time' then 'Amount in hr'
              when p_after then 'Length*time'
              else 'Amount in kmy'
            end
          )
        )
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object('common:dataSetVersion', '01.00.000')
      )
    )
  )
$$;

create or replace function pg_temp.preview_alias_target_exchange_count(
  p_dimension text,
  p_ordinal integer
) returns integer language sql immutable as $$
  select case
    when p_dimension = 'time' and p_ordinal <= 6 then 2
    when p_dimension = 'time' then 1
    else 3
  end
$$;

create or replace function pg_temp.preview_alias_unrelated_exchange_count(
  p_dimension text,
  p_ordinal integer
) returns integer language sql immutable as $$
  select case
    when (case when p_dimension = 'time' then p_ordinal else p_ordinal + 14 end) <= 12 then 12
    else 11
  end
$$;

create or replace function pg_temp.preview_alias_process_payload(
  p_dimension text,
  p_ordinal integer,
  p_after boolean
) returns jsonb language sql stable as $$
  with counts as (
    select
      pg_temp.preview_alias_target_exchange_count(p_dimension, p_ordinal) as target_count,
      pg_temp.preview_alias_unrelated_exchange_count(p_dimension, p_ordinal) as unrelated_count,
      case when p_dimension = 'time' then 10 else 13 end as flow_count
  ), exchange_rows as (
    select
      target_ordinal as position,
      jsonb_build_object(
        '@dataSetInternalID', 'target-' || p_dimension || '-' || p_ordinal || '-' || target_ordinal,
        'exchangeDirection', case when (p_ordinal + target_ordinal) % 2 = 0 then 'Input' else 'Output' end,
        'meanAmount', case when p_after
          then (10::numeric * pg_temp.preview_alias_factor(p_dimension)::numeric)::text
          else '10.0'
        end,
        'resultingAmount', case when p_after
          then (10::numeric * pg_temp.preview_alias_factor(p_dimension)::numeric)::text
          else '10.0'
        end,
        'referenceToFlowDataSet', jsonb_build_object(
          '@refObjectId', pg_temp.preview_alias_entity_id(
            p_dimension,
            'flow',
            (((p_ordinal - 1) * 3 + target_ordinal - 1) % counts.flow_count) + 1
          ),
          '@type', 'flow data set',
          '@uri', '../flows/' || pg_temp.preview_alias_entity_id(
            p_dimension,
            'flow',
            (((p_ordinal - 1) * 3 + target_ordinal - 1) % counts.flow_count) + 1
          )::text || '.json',
          '@version', '01.00.000'
        )
      ) as exchange
    from counts
    cross join lateral generate_series(1, counts.target_count) as target_ordinal
    union all
    select
      counts.target_count + unrelated_ordinal,
      jsonb_build_object(
        '@dataSetInternalID', 'unrelated-' || p_dimension || '-' || p_ordinal || '-' || unrelated_ordinal,
        'exchangeDirection', case when unrelated_ordinal % 2 = 0 then 'Input' else 'Output' end,
        'meanAmount', '7.0',
        'resultingAmount', '7.0',
        'referenceToFlowDataSet', jsonb_build_object(
          '@refObjectId', pg_temp.preview_alias_uuid('unrelated-flow'),
          '@type', 'flow data set',
          '@uri', '../flows/' || pg_temp.preview_alias_uuid('unrelated-flow')::text || '.json',
          '@version', '99.99.999'
        )
      )
    from counts
    cross join lateral generate_series(1, counts.unrelated_count) as unrelated_ordinal
  )
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'processInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'common:UUID', pg_temp.preview_alias_entity_id(p_dimension, 'process', p_ordinal),
          'name', jsonb_build_object('baseName', jsonb_build_array(jsonb_build_object(
            '@xml:lang', 'en', '#text', 'Preview protected alias process ' || p_ordinal
          )))
        )
      ),
      'exchanges', jsonb_build_object(
        'exchange', (select jsonb_agg(exchange order by position) from exchange_rows)
      ),
      'administrativeInformation', jsonb_build_object(
        'publicationAndOwnership', jsonb_build_object('common:dataSetVersion', '01.00.000')
      )
    )
  )
$$;

create or replace function pg_temp.preview_alias_process_mutation(
  p_dimension text,
  p_ordinal integer
) returns jsonb language sql stable as $$
  with expected as (
    select pg_temp.preview_alias_process_payload(p_dimension, p_ordinal, false) as payload
  )
  select jsonb_build_object(
    'kind', 'process_exchange_amounts',
    'exchanges', jsonb_agg(jsonb_build_object(
      'index', exchange_item.ordinality - 1,
      'internal_id', exchange_item.value->>'@dataSetInternalID',
      'flow_id', exchange_item.value #>> '{referenceToFlowDataSet,@refObjectId}',
      'flow_version', exchange_item.value #>> '{referenceToFlowDataSet,@version}',
      'direction', exchange_item.value->>'exchangeDirection',
      'before_exchange_sha256', encode(
        extensions.digest(
          convert_to(private.dataset_alias_canonical_jsonb_v1(exchange_item.value), 'UTF8'),
          'sha256'
        ),
        'hex'
      )
    ) order by exchange_item.ordinality)
  )
  from expected
  cross join lateral jsonb_array_elements(
    expected.payload #> '{processDataSet,exchanges,exchange}'
  ) with ordinality as exchange_item(value, ordinality)
  where exchange_item.ordinality <= pg_temp.preview_alias_target_exchange_count(p_dimension, p_ordinal)
$$;

create or replace function pg_temp.preview_alias_plan_sha256()
returns text language sql stable as $$
  select util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
    'contract', 'database-engine-20260711-owner-draft-alias',
    'scenario_id', config.scenario_id,
    'request_id', config.request_id
  ))
  from pg_temp.preview_alias_fixture_config as config
$$;

create or replace function pg_temp.preview_alias_operation_id()
returns text language sql stable as $$
  select 'preview-e2e-protected-alias-' || config.request_id::text
  from pg_temp.preview_alias_fixture_config as config
$$;

create or replace function pg_temp.preview_alias_batch(p_dimension text)
returns jsonb language plpgsql stable as $$
declare
  actions jsonb := '[]'::jsonb;
  ordinal integer;
  flow_count integer := case when p_dimension = 'time' then 10 else 13 end;
  process_count integer := case when p_dimension = 'time' then 14 else 13 end;
  modified_at text := '2026-07-15 00:00:00+00';
begin
  actions := actions || jsonb_build_array(jsonb_build_object(
    'action_id', 'flowproperty-alias',
    'action', 'update_json_ordered',
    'table', 'flowproperties',
    'id', pg_temp.preview_alias_entity_id(p_dimension, 'source_flowproperty'),
    'version', '00.00.000',
    'expected_state_code', 0,
    'expected_modified_at', modified_at,
    'expected_json_ordered', pg_temp.preview_alias_flowproperty_payload(p_dimension, false),
    'desired_json_ordered', pg_temp.preview_alias_flowproperty_after(p_dimension),
    'mutation', jsonb_build_object('kind', 'flowproperty_unitgroup_reference')
  ));

  for ordinal in 1..flow_count loop
    actions := actions || jsonb_build_array(jsonb_build_object(
      'action_id', 'flow-' || lpad(ordinal::text, 2, '0'),
      'action', 'update_json_ordered',
      'table', 'flows',
      'id', pg_temp.preview_alias_entity_id(p_dimension, 'flow', ordinal),
      'version', '01.00.000',
      'expected_state_code', 0,
      'expected_modified_at', modified_at,
      'expected_json_ordered', pg_temp.preview_alias_flow_payload(p_dimension, ordinal, false),
      'desired_json_ordered', pg_temp.preview_alias_flow_payload(p_dimension, ordinal, true),
      'mutation', jsonb_build_object(
        'kind', 'flow_flowproperty_reference',
        'flow_property_internal_id', '1',
        'source_flowproperty_id', pg_temp.preview_alias_entity_id(p_dimension, 'source_flowproperty'),
        'source_flowproperty_version', '00.00.000'
      )
    ));
  end loop;

  for ordinal in 1..process_count loop
    actions := actions || jsonb_build_array(jsonb_build_object(
      'action_id', 'process-' || lpad(ordinal::text, 2, '0'),
      'action', 'update_json_ordered',
      'table', 'processes',
      'id', pg_temp.preview_alias_entity_id(p_dimension, 'process', ordinal),
      'version', '01.00.000',
      'expected_state_code', 0,
      'expected_modified_at', modified_at,
      'expected_json_ordered', pg_temp.preview_alias_process_payload(p_dimension, ordinal, false),
      'desired_json_ordered', pg_temp.preview_alias_process_payload(p_dimension, ordinal, true),
      'mutation', pg_temp.preview_alias_process_mutation(p_dimension, ordinal)
    ));
  end loop;

  return jsonb_build_object(
    'schema_version', 'dataset-alias-batch.v1',
    'plan_sha256', pg_temp.preview_alias_plan_sha256(),
    'operation_id', pg_temp.preview_alias_operation_id(),
    'batch_id', p_dimension || '-' || (select request_id::text from pg_temp.preview_alias_fixture_config),
    'dimension', p_dimension,
    'factor', pg_temp.preview_alias_factor(p_dimension),
    'target_visibility', 'owner_draft',
    'target', jsonb_build_object(
      'flowproperty', jsonb_build_object(
        'id', pg_temp.preview_alias_entity_id(p_dimension, 'target_flowproperty'),
        'version', '01.00.000',
        'expected_modified_at', modified_at,
        'expected_json_ordered', pg_temp.preview_alias_flowproperty_payload(p_dimension, true)
      ),
      'unitgroup', jsonb_build_object(
        'id', pg_temp.preview_alias_entity_id(p_dimension, 'target_unitgroup'),
        'version', '01.00.000',
        'expected_modified_at', modified_at,
        'expected_json_ordered', pg_temp.preview_alias_unitgroup_payload(p_dimension, true)
      ),
      'source_unitgroup', jsonb_build_object(
        'id', pg_temp.preview_alias_entity_id(p_dimension, 'source_unitgroup'),
        'version', '00.00.000',
        'expected_modified_at', modified_at,
        'expected_json_ordered', pg_temp.preview_alias_unitgroup_payload(p_dimension, false)
      )
    ),
    'actions', actions
  );
end;
$$;

create or replace function pg_temp.preview_alias_plan_uncached()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-plan.v1',
    'plan_sha256', pg_temp.preview_alias_plan_sha256(),
    'operation_id', pg_temp.preview_alias_operation_id(),
    'target_visibility', 'owner_draft',
    'batches', jsonb_build_array(
      pg_temp.preview_alias_batch('time'),
      pg_temp.preview_alias_batch('length_time')
    )
  )
$$;

create temporary table preview_alias_plan_cache (
  singleton boolean primary key default true check (singleton),
  payload jsonb not null
) on commit drop;

insert into pg_temp.preview_alias_plan_cache (payload)
select pg_temp.preview_alias_plan_uncached();

create or replace function pg_temp.preview_alias_plan()
returns jsonb language sql stable as $$
  select payload
  from pg_temp.preview_alias_plan_cache
  where singleton
$$;

-- Trigger-free fixture insertion avoids producing derivative work before the
-- protected preflight.  The production RPCs and functions are not replaced.
set local session_replication_role = replica;

insert into public.unitgroups (
  id, version, json, json_ordered, user_id, state_code, rule_verification, modified_at
)
select
  pg_temp.preview_alias_entity_id(dimension, kind),
  case when kind = 'target_unitgroup' then '01.00.000' else '00.00.000' end,
  pg_temp.preview_alias_unitgroup_payload(dimension, kind = 'target_unitgroup'),
  pg_temp.preview_alias_unitgroup_payload(dimension, kind = 'target_unitgroup')::json,
  config.actor_user_id,
  0,
  true,
  config.fixture_modified_at
from (values ('time'::text), ('length_time'::text)) as dimensions(dimension)
cross join (values ('source_unitgroup'::text), ('target_unitgroup'::text)) as kinds(kind)
cross join pg_temp.preview_alias_fixture_config as config;

insert into public.flowproperties (
  id, version, json, json_ordered, user_id, state_code, rule_verification, modified_at
)
select
  pg_temp.preview_alias_entity_id(dimension, kind),
  case when kind = 'target_flowproperty' then '01.00.000' else '00.00.000' end,
  pg_temp.preview_alias_flowproperty_payload(dimension, kind = 'target_flowproperty'),
  pg_temp.preview_alias_flowproperty_payload(dimension, kind = 'target_flowproperty')::json,
  config.actor_user_id,
  0,
  true,
  config.fixture_modified_at
from (values ('time'::text), ('length_time'::text)) as dimensions(dimension)
cross join (values ('source_flowproperty'::text), ('target_flowproperty'::text)) as kinds(kind)
cross join pg_temp.preview_alias_fixture_config as config;

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code, rule_verification,
  modified_at, extracted_text, extracted_md, embedding_ft, embedding_ft_at
)
select
  pg_temp.preview_alias_entity_id(dimensions.dimension, 'flow', ordinal),
  '01.00.000',
  pg_temp.preview_alias_flow_payload(dimensions.dimension, ordinal, false),
  pg_temp.preview_alias_flow_payload(dimensions.dimension, ordinal, false)::json,
  config.actor_user_id,
  0,
  true,
  config.fixture_modified_at,
  'Preview protected alias flow extracted text ' || ordinal,
  'Preview protected alias flow Markdown ' || ordinal,
  null,
  null
from (values ('time'::text, 10), ('length_time'::text, 13)) as dimensions(dimension, row_count)
cross join lateral generate_series(1, dimensions.row_count) as ordinal
cross join pg_temp.preview_alias_fixture_config as config;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code, rule_verification,
  modified_at, extracted_text, extracted_md, embedding_ft, embedding_ft_at
)
select
  pg_temp.preview_alias_entity_id(dimensions.dimension, 'process', ordinal),
  '01.00.000',
  pg_temp.preview_alias_process_payload(dimensions.dimension, ordinal, false),
  pg_temp.preview_alias_process_payload(dimensions.dimension, ordinal, false)::json,
  config.actor_user_id,
  0,
  true,
  config.fixture_modified_at,
  'Preview protected alias process extracted text ' || ordinal,
  'Preview protected alias process Markdown ' || ordinal,
  null,
  null
from (values ('time'::text, 14), ('length_time'::text, 13)) as dimensions(dimension, row_count)
cross join lateral generate_series(1, dimensions.row_count) as ordinal
cross join pg_temp.preview_alias_fixture_config as config;

set local session_replication_role = origin;

create or replace function pg_temp.preview_alias_expected()
returns jsonb language sql immutable as $$
  select jsonb_build_object(
    'action_count', 52,
    'batch_count', 2,
    'exchange_count', 59,
    'amount_field_count', 118,
    'unrelated_exchange_count', 309,
    'audit_count', 55,
    'flowproperty_count', 2,
    'flow_count', 23,
    'process_count', 27,
    'derivative_target_count', 50
  )
$$;

create or replace function pg_temp.preview_alias_action_keys()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'dimension', batch.value->>'dimension',
    'action_id', action.value->>'action_id',
    'table', action.value->>'table',
    'id', action.value->>'id',
    'version', action.value->>'version'
  ) order by batch.ordinality, action.ordinality)
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') with ordinality as batch(value, ordinality)
  cross join lateral jsonb_array_elements(batch.value->'actions') with ordinality as action(value, ordinality)
$$;

create or replace function pg_temp.preview_alias_target_keys()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'table', action.value->>'table',
    'id', action.value->>'id',
    'version', action.value->>'version'
  ) order by action.value->>'table', action.value->>'id', action.value->>'version')
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
  where action.value->>'table' in ('flows', 'processes')
$$;

create or replace function pg_temp.preview_alias_support_keys()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'dimension', batch.value->>'dimension',
    'role', support.role,
    'table', case when support.role = 'flowproperty' then 'flowproperties' else 'unitgroups' end,
    'id', batch.value #>> array['target', support.role, 'id'],
    'version', batch.value #>> array['target', support.role, 'version']
  ) order by batch.ordinality, support.ordinality)
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') with ordinality as batch(value, ordinality)
  cross join lateral (values
    ('flowproperty'::text, 1),
    ('unitgroup'::text, 2),
    ('source_unitgroup'::text, 3)
  ) as support(role, ordinality)
$$;

create or replace function pg_temp.preview_alias_before_material()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'table', action.value->>'table',
    'id', action.value->>'id',
    'version', action.value->>'version',
    'sha256', util.dataset_alias_execution_artifact_sha256(action.value->'expected_json_ordered')
  ) order by action.value->>'table', action.value->>'id', action.value->>'version')
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
$$;

create or replace function pg_temp.preview_alias_desired_material()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'table', action.value->>'table',
    'id', action.value->>'id',
    'version', action.value->>'version',
    'sha256', util.dataset_alias_execution_artifact_sha256(action.value->'desired_json_ordered')
  ) order by action.value->>'table', action.value->>'id', action.value->>'version')
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
$$;

create or replace function pg_temp.preview_alias_exchange_material()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'dimension', batch.value->>'dimension',
    'process_id', action.value->>'id',
    'process_version', action.value->>'version',
    'index', exchange.value->'index',
    'internal_id', exchange.value->>'internal_id',
    'flow_id', exchange.value->>'flow_id',
    'flow_version', exchange.value->>'flow_version',
    'direction', exchange.value->>'direction',
    'before_exchange_sha256', exchange.value->>'before_exchange_sha256',
    'desired_mean_amount', action.value #>> array[
      'desired_json_ordered', 'processDataSet', 'exchanges', 'exchange',
      (exchange.value->>'index'), 'meanAmount'
    ],
    'desired_resulting_amount', action.value #>> array[
      'desired_json_ordered', 'processDataSet', 'exchanges', 'exchange',
      (exchange.value->>'index'), 'resultingAmount'
    ]
  ) order by action.value->>'id', (exchange.value->>'index')::integer)
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
  cross join lateral jsonb_array_elements(action.value #> '{mutation,exchanges}') as exchange(value)
  where action.value->>'table' = 'processes'
$$;

create or replace function pg_temp.preview_alias_unrelated_material()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'process_id', action.value->>'id',
    'process_version', action.value->>'version',
    'index', exchange.ordinality - 1,
    'internal_id', exchange.value->>'@dataSetInternalID',
    'exchange_sha256', util.dataset_alias_execution_artifact_sha256(exchange.value)
  ) order by action.value->>'id', exchange.ordinality)
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') as batch(value)
  cross join lateral jsonb_array_elements(batch.value->'actions') as action(value)
  cross join lateral jsonb_array_elements(
    action.value #> '{expected_json_ordered,processDataSet,exchanges,exchange}'
  ) with ordinality as exchange(value, ordinality)
  where action.value->>'table' = 'processes'
    and exchange.value #>> '{referenceToFlowDataSet,@refObjectId}' = pg_temp.preview_alias_uuid('unrelated-flow')::text
$$;

create or replace function pg_temp.preview_alias_baseline_hashes_uncached()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'table', target.value->>'table',
    'id', target.value->>'id',
    'version', target.value->>'version',
    'baseline_snapshot_sha256', snapshot.value->>'snapshot_sha256'
  ) order by target.value->>'table', target.value->>'id', target.value->>'version')
  from jsonb_array_elements(pg_temp.preview_alias_target_keys()) as target(value)
  cross join lateral (
    select util.dataset_derivative_rebuild_snapshot(
      target.value->>'table',
      (target.value->>'id')::uuid,
      target.value->>'version'
    ) as value
  ) as snapshot
$$;

create temporary table preview_alias_baseline_hashes_cache (
  singleton boolean primary key default true check (singleton),
  payload jsonb not null
) on commit drop;

insert into pg_temp.preview_alias_baseline_hashes_cache (payload)
select pg_temp.preview_alias_baseline_hashes_uncached();

create or replace function pg_temp.preview_alias_baseline_hashes()
returns jsonb language sql stable as $$
  select payload
  from pg_temp.preview_alias_baseline_hashes_cache
  where singleton
$$;

create or replace function pg_temp.preview_alias_derivative_targets()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'table', baseline.value->>'table',
    'id', baseline.value->>'id',
    'version', baseline.value->>'version',
    'user_id', config.actor_user_id,
    'state_code', 0,
    'baseline_snapshot_sha256', baseline.value->>'baseline_snapshot_sha256'
  ) order by baseline.value->>'table', baseline.value->>'id', baseline.value->>'version')
  from jsonb_array_elements(pg_temp.preview_alias_baseline_hashes()) as baseline(value)
  cross join pg_temp.preview_alias_fixture_config as config
$$;

create or replace function pg_temp.preview_alias_support_material()
returns jsonb language sql stable as $$
  select jsonb_agg(jsonb_build_object(
    'dimension', batch.value->>'dimension',
    'role', support.role,
    'table', case when support.role = 'flowproperty' then 'flowproperties' else 'unitgroups' end,
    'id', batch.value #>> array['target', support.role, 'id'],
    'version', batch.value #>> array['target', support.role, 'version'],
    'expected_modified_at', batch.value #>> array['target', support.role, 'expected_modified_at'],
    'expected_json_ordered_sha256', util.dataset_alias_execution_artifact_sha256(
      batch.value #> array['target', support.role, 'expected_json_ordered']
    )
  ) order by batch.ordinality, support.ordinality)
  from jsonb_array_elements(pg_temp.preview_alias_plan()->'batches') with ordinality as batch(value, ordinality)
  cross join lateral (values
    ('flowproperty'::text, 1),
    ('unitgroup'::text, 2),
    ('source_unitgroup'::text, 3)
  ) as support(role, ordinality)
$$;

create or replace function pg_temp.preview_alias_base_bindings_uncached()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'plan_file_sha256', util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
      'kind', 'plan-file', 'plan', pg_temp.preview_alias_plan()
    )),
    'freeze_file_sha256', util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
      'kind', 'freeze-file', 'scenario_id', config.scenario_id, 'request_id', config.request_id
    )),
    'approval_file_sha256', util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
      'kind', 'approval-file', 'scenario_id', config.scenario_id, 'request_id', config.request_id
    )),
    'approval_text_sha256', util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
      'kind', 'approval-text', 'scenario_id', config.scenario_id, 'request_id', config.request_id
    )),
    'alias_plan_request_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_plan()),
    'before_hash_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_before_material()),
    'desired_hash_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_desired_material()),
    'exchange_rewrite_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_exchange_material()),
    'support_snapshot_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_support_material()),
    'derivative_baseline_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_baseline_hashes()),
    'derivative_target_set_sha256', util.dataset_alias_execution_artifact_sha256((
      select jsonb_agg(jsonb_build_object(
        'table', target.value->>'table',
        'id', target.value->>'id',
        'version', target.value->>'version',
        'user_id', target.value->>'user_id',
        'state_code', 0
      ) order by target.value->>'table', target.value->>'id', target.value->>'version')
      from jsonb_array_elements(pg_temp.preview_alias_derivative_targets()) as target(value)
    )),
    'toolchain_evidence_sha256', util.dataset_alias_execution_artifact_sha256(jsonb_build_object(
      'alias_plan_guarded', util.dataset_alias_execution_sha256(pg_get_functiondef(
        'public.cmd_dataset_alias_plan_guarded(jsonb)'::regprocedure
      )),
      'derivative_batch', util.dataset_alias_execution_sha256(pg_get_functiondef(
        'util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)'::regprocedure
      )),
      'protected_preflight', util.dataset_alias_execution_sha256(pg_get_functiondef(
        'public.cmd_dataset_alias_execution_preflight_guarded(jsonb)'::regprocedure
      ))
    ))
  )
  from pg_temp.preview_alias_fixture_config as config
$$;

create temporary table preview_alias_base_bindings_cache (
  singleton boolean primary key default true check (singleton),
  payload jsonb not null
) on commit drop;

insert into pg_temp.preview_alias_base_bindings_cache (payload)
select pg_temp.preview_alias_base_bindings_uncached();

create or replace function pg_temp.preview_alias_base_bindings()
returns jsonb language sql stable as $$
  select payload
  from pg_temp.preview_alias_base_bindings_cache
  where singleton
$$;

create or replace function pg_temp.preview_alias_freeze_without_sha()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-freeze.v1',
    'environment', 'preview',
    'project_ref', config.preview_ref,
    'account', jsonb_build_object('user_id', config.actor_user_id, 'email', config.actor_email),
    'target_visibility', 'owner_draft',
    'plan', jsonb_build_object(
      'plan_file_sha256', base->>'plan_file_sha256',
      'plan_sha256', pg_temp.preview_alias_plan_sha256(),
      'operation_id', pg_temp.preview_alias_operation_id()
    ),
    'sets', jsonb_build_object(
      'alias_plan_request_sha256', base->>'alias_plan_request_sha256',
      'before_hash_set_sha256', base->>'before_hash_set_sha256',
      'desired_hash_set_sha256', base->>'desired_hash_set_sha256',
      'exchange_rewrite_set_sha256', base->>'exchange_rewrite_set_sha256',
      'support_snapshot_set_sha256', base->>'support_snapshot_set_sha256',
      'derivative_baseline_set_sha256', base->>'derivative_baseline_set_sha256',
      'derivative_target_set_sha256', base->>'derivative_target_set_sha256',
      'toolchain_evidence_sha256', base->>'toolchain_evidence_sha256'
    ),
    'expected', pg_temp.preview_alias_expected(),
    'derivative_targets', pg_temp.preview_alias_derivative_targets(),
    'policy', jsonb_build_object(
      'state_code_changes', 0, 'save_draft', 0, 'deletes', 0,
      'rebuild_derivatives', 0, 'unitgroup_actions', 0,
      'person_distance_actions', 0, 'max_admit_posts', 1,
      'automatic_retry', false
    ),
    'freeze_sha256', ''
  )
  from pg_temp.preview_alias_fixture_config as config
  cross join lateral (select pg_temp.preview_alias_base_bindings() as base) as bindings
$$;

create or replace function pg_temp.preview_alias_freeze_sha256()
returns text language sql stable as $$
  select util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_freeze_without_sha())
$$;

create or replace function pg_temp.preview_alias_approval_without_sha()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-approval.v1',
    'approved_at_utc', config.approved_at_utc,
    'environment', 'preview',
    'project_ref', config.preview_ref,
    'account', jsonb_build_object('user_id', config.actor_user_id, 'email', config.actor_email),
    'target_visibility', 'owner_draft',
    'plan_sha256', pg_temp.preview_alias_plan_sha256(),
    'operation_id', pg_temp.preview_alias_operation_id(),
    'plan_file_sha256', base->>'plan_file_sha256',
    'freeze_file_sha256', base->>'freeze_file_sha256',
    'freeze_sha256', pg_temp.preview_alias_freeze_sha256(),
    'approval_text_sha256', base->>'approval_text_sha256',
    'max_admit_posts', 1,
    'automatic_retry', false,
    'approval_identity_sha256', ''
  )
  from pg_temp.preview_alias_fixture_config as config
  cross join lateral (select pg_temp.preview_alias_base_bindings() as base) as bindings
$$;

create or replace function pg_temp.preview_alias_bindings_uncached()
returns jsonb language sql stable as $$
  select base
    || jsonb_build_object(
      'freeze_sha256', pg_temp.preview_alias_freeze_sha256(),
      'approval_identity_sha256', util.dataset_alias_execution_artifact_sha256(
        pg_temp.preview_alias_approval_without_sha()
      )
    )
  from (select pg_temp.preview_alias_base_bindings() as base) as bindings
$$;

create temporary table preview_alias_bindings_cache (
  singleton boolean primary key default true check (singleton),
  payload jsonb not null
) on commit drop;

insert into pg_temp.preview_alias_bindings_cache (payload)
select pg_temp.preview_alias_bindings_uncached();

create or replace function pg_temp.preview_alias_bindings()
returns jsonb language sql stable as $$
  select payload
  from pg_temp.preview_alias_bindings_cache
  where singleton
$$;

create or replace function pg_temp.preview_alias_freeze()
returns jsonb language sql stable as $$
  select jsonb_set(
    pg_temp.preview_alias_freeze_without_sha(),
    '{freeze_sha256}',
    to_jsonb(pg_temp.preview_alias_freeze_sha256()),
    false
  )
$$;

create or replace function pg_temp.preview_alias_approval()
returns jsonb language sql stable as $$
  select jsonb_set(
    pg_temp.preview_alias_approval_without_sha(),
    '{approval_identity_sha256}',
    to_jsonb(pg_temp.preview_alias_bindings()->>'approval_identity_sha256'),
    false
  )
$$;

create or replace function pg_temp.preview_alias_preflight_request()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'schema_version', 'dataset-alias-execution-preflight.v1',
    'request_id', config.request_id,
    'environment', 'preview',
    'project_ref', config.preview_ref,
    'actor', jsonb_build_object('user_id', config.actor_user_id, 'email', config.actor_email),
    'target_visibility', 'owner_draft',
    'plan', pg_temp.preview_alias_plan(),
    'freeze', pg_temp.preview_alias_freeze(),
    'approval', pg_temp.preview_alias_approval(),
    'bindings', pg_temp.preview_alias_bindings(),
    'expected', pg_temp.preview_alias_expected(),
    'derivative_targets', pg_temp.preview_alias_derivative_targets()
  )
  from pg_temp.preview_alias_fixture_config as config
$$;

create or replace function pg_temp.preview_alias_manifest_uncached()
returns jsonb language sql stable as $$
  select jsonb_build_object(
    'schema_version', 'protected-alias-preview-fixture.v1',
    'scenario_id', config.scenario_id,
    'scenario_kind', config.scenario_kind,
    'request_id', config.request_id,
    'preflight_request', pg_temp.preview_alias_preflight_request(),
    'expected', pg_temp.preview_alias_expected(),
    'action_keys', pg_temp.preview_alias_action_keys(),
    'target_keys', pg_temp.preview_alias_target_keys(),
    'support_keys', pg_temp.preview_alias_support_keys(),
    'baseline_hashes', pg_temp.preview_alias_baseline_hashes(),
    'before_hash_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_before_material()),
    'desired_hash_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_desired_material()),
    'unrelated_exchange_set_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_unrelated_material()),
    'unrelated_exchange_count', jsonb_array_length(pg_temp.preview_alias_unrelated_material()),
    'hashes', pg_temp.preview_alias_bindings() || jsonb_build_object(
      'exchange_material_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_exchange_material()),
      'support_material_sha256', util.dataset_alias_execution_artifact_sha256(pg_temp.preview_alias_support_material())
    ),
    'business_rollback_fault', jsonb_build_object(
      'table', 'flowproperties',
      'id', pg_temp.preview_alias_entity_id('time', 'source_flowproperty'),
      'version', '00.00.000'
    )
  )
  from pg_temp.preview_alias_fixture_config as config
$$;

create temporary table preview_alias_manifest_cache (
  singleton boolean primary key default true check (singleton),
  payload jsonb not null
) on commit drop;

insert into pg_temp.preview_alias_manifest_cache (payload)
select pg_temp.preview_alias_manifest_uncached();

create or replace function pg_temp.preview_alias_manifest()
returns jsonb language sql stable as $$
  select payload
  from pg_temp.preview_alias_manifest_cache
  where singleton
$$;

do $fixture_assertions$
declare
  manifest jsonb := pg_temp.preview_alias_manifest();
  plan_payload jsonb := pg_temp.preview_alias_plan();
begin
  if jsonb_array_length(manifest->'action_keys') is distinct from 52
    or jsonb_array_length(manifest->'target_keys') is distinct from 50
    or jsonb_array_length(manifest->'support_keys') is distinct from 6
    or jsonb_array_length(manifest->'baseline_hashes') is distinct from 50
    or jsonb_array_length(pg_temp.preview_alias_exchange_material()) is distinct from 59
    or jsonb_array_length(pg_temp.preview_alias_unrelated_material()) is distinct from 309
    or (select count(*) from public.flowproperties where user_id = (select actor_user_id from pg_temp.preview_alias_fixture_config)
        and id in (
          pg_temp.preview_alias_entity_id('time', 'source_flowproperty'),
          pg_temp.preview_alias_entity_id('length_time', 'source_flowproperty')
        )) is distinct from 2
    or (select count(*) from public.flows where user_id = (select actor_user_id from pg_temp.preview_alias_fixture_config)
        and exists (select 1 from jsonb_array_elements(manifest->'target_keys') key
                    where key->>'table' = 'flows' and (key->>'id')::uuid = flows.id)) is distinct from 23
    or (select count(*) from public.processes where user_id = (select actor_user_id from pg_temp.preview_alias_fixture_config)
        and exists (select 1 from jsonb_array_elements(manifest->'target_keys') key
                    where key->>'table' = 'processes' and (key->>'id')::uuid = processes.id)) is distinct from 27 then
    raise exception 'protected alias fixture cardinality/hash manifest mismatch';
  end if;

  if exists (
    select 1 from jsonb_array_elements(manifest->'baseline_hashes') as baseline(value)
    where baseline.value->>'baseline_snapshot_sha256' !~ '^[a-f0-9]{64}$'
  ) or plan_payload->>'schema_version' is distinct from 'dataset-alias-plan.v1'
    or (manifest #>> '{preflight_request,environment}') is distinct from 'preview'
    or (manifest #>> '{preflight_request,project_ref}') is distinct from
      (select preview_ref from pg_temp.preview_alias_fixture_config) then
    raise exception 'protected alias fixture sealing failed';
  end if;
end;
$fixture_assertions$;

insert into public.command_audit_log (
  command,
  actor_user_id,
  target_table,
  target_id,
  target_version,
  payload
)
select
  'preview_e2e_protected_alias_fixture',
  config.actor_user_id,
  'preview_e2e_protected_alias',
  config.request_id,
  '00.00.001',
  pg_temp.preview_alias_manifest()
from pg_temp.preview_alias_fixture_config as config;

-- Emit standard TAP without installing a test-only extension on the hosted
-- branch.  The hard assertions above make every reported line fail-closed.
select tap
from (values
  ('TAP version 13'),
  ('1..9'),
  ('ok 1 - fixture setup does not pre-create a protected preflight'),
  ('ok 2 - 52 exact alias actions are sealed'),
  ('ok 3 - 23 flow and 27 process derivative keys are sealed'),
  ('ok 4 - six support occurrences are sealed'),
  ('ok 5 - 59 target exchanges are sealed'),
  ('ok 6 - 309 unrelated exchanges are preserved'),
  ('ok 7 - 50 live derivative baselines are sealed'),
  ('ok 8 - one service-readable sealed manifest is committed'),
  ('ok 9 - exact branch-local Vault execution secrets are committed')
) as fixture_tap(tap);

commit;
