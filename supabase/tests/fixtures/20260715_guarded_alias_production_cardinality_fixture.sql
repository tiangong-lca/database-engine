-- Disposable fixture for the guarded dataset-alias closure benchmark.
--
-- This file is intentionally not standalone and must never be loaded by a seed.
-- Include it only from the sibling benchmark after BEGIN and after setting:
--   database_engine.benchmark_target = local | preview
--   application_name = database-engine-254-guarded-alias-benchmark

do $fixture_gate$
begin
  if current_setting('database_engine.benchmark_target', true) is null
    or current_setting('database_engine.benchmark_target', true)
      not in ('local', 'preview')
    or current_setting('application_name', true)
      <> 'database-engine-254-guarded-alias-benchmark' then
    raise exception
      'guarded alias cardinality fixture is restricted to an explicit local or disposable Preview benchmark';
  end if;

  if to_regprocedure('private.dataset_alias_jsonb_array_v1(jsonb)') is null
    or to_regclass('public.flows_json_ordered_alias_flowproperty_gin_idx') is null
    or to_regclass('public.processes_json_ordered_alias_exchange_gin_idx') is null then
    raise exception
      'issue #254 helper and candidate indexes must exist before loading the benchmark fixture';
  end if;

  if exists (
      select 1
      from public.flows
      where id::text like 'f2540000-0000-4000-8000-%'
    ) or exists (
      select 1
      from public.processes
      where id::text like 'f2540000-0000-4001-8000-%'
    ) or exists (
      select 1
      from public.flowproperties
      where id::text like 'f2540000-0000-4f00-8000-%'
    ) then
    raise exception
      'fixed issue #254 benchmark UUID range already exists; refusing to overlap non-fixture data';
  end if;
end;
$fixture_gate$;

create temporary table guarded_alias_benchmark_meta (
  benchmark_owner uuid not null,
  source_flowproperty_id uuid not null,
  target_flowproperty_id uuid not null,
  filler_flowproperty_id uuid not null,
  baseline_flowproperties bigint not null,
  baseline_flows bigint not null,
  baseline_flow_property_nodes bigint not null,
  baseline_processes bigint not null,
  baseline_exchange_nodes bigint not null,
  inserted_flowproperties bigint not null,
  inserted_flows bigint not null,
  inserted_flow_property_nodes bigint not null,
  inserted_processes bigint not null,
  inserted_exchange_nodes bigint not null
) on commit drop;

with physical_baseline as (
  select
    (select count(*)::bigint from public.flowproperties) as flowproperties,
    (select count(*)::bigint from public.flows) as flows,
    (
      select coalesce(sum(jsonb_array_length(
        private.dataset_alias_jsonb_array_v1(
          dataset_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        )
      )), 0)::bigint
      from public.flows as dataset_flow
    ) as flow_property_nodes,
    (select count(*)::bigint from public.processes) as processes,
    (
      select coalesce(sum(jsonb_array_length(
        private.dataset_alias_jsonb_array_v1(
          dataset_process.json_ordered::jsonb
            #> '{processDataSet,exchanges,exchange}'
        )
      )), 0)::bigint
      from public.processes as dataset_process
    ) as exchange_nodes
), required_rows as (
  select
    *,
    greatest(3::bigint, 483::bigint - flowproperties) as flowproperty_inserts,
    greatest(
      24::bigint,
      132185::bigint - flows,
      ceil(greatest(0::bigint, 132259::bigint - flow_property_nodes) / 2.0)::bigint
    ) as flow_inserts,
    greatest(
      27::bigint,
      42369::bigint - processes,
      ceil(greatest(59::bigint, 837020::bigint - exchange_nodes) / 40.0)::bigint
    ) as process_inserts
  from physical_baseline
)
insert into guarded_alias_benchmark_meta (
  benchmark_owner,
  source_flowproperty_id,
  target_flowproperty_id,
  filler_flowproperty_id,
  baseline_flowproperties,
  baseline_flows,
  baseline_flow_property_nodes,
  baseline_processes,
  baseline_exchange_nodes,
  inserted_flowproperties,
  inserted_flows,
  inserted_flow_property_nodes,
  inserted_processes,
  inserted_exchange_nodes
)
select
  'f2540000-0000-4002-8000-000000000001'::uuid,
  'f2540000-0000-4f00-8000-000000000001'::uuid,
  'f2540000-0000-4f00-8000-000000000002'::uuid,
  'f2540000-0000-4f00-8000-000000000003'::uuid,
  flowproperties,
  flows,
  flow_property_nodes,
  processes,
  exchange_nodes,
  flowproperty_inserts,
  flow_inserts,
  greatest(
    flow_inserts,
    23::bigint,
    132259::bigint - flow_property_nodes
  ),
  process_inserts,
  greatest(59::bigint, 837020::bigint - exchange_nodes)
from required_rows;

set local session_replication_role = replica;

with fixture as (
  select *
  from guarded_alias_benchmark_meta
), generated as (
  select
    series_no,
    (
      'f2540000-0000-4f00-8000-' ||
      lpad(to_hex(series_no), 12, '0')
    )::uuid as id
  from fixture
  cross join lateral generate_series(
    1::bigint,
    fixture.inserted_flowproperties
  ) as generated_flowproperty(series_no)
), payloads as (
  select
    id,
    jsonb_build_object(
      'flowPropertyDataSet', jsonb_build_object(
        'flowPropertiesInformation', jsonb_build_object(
          'dataSetInformation', jsonb_build_object(
            'UUID of flow property data set', id::text
          ),
          'quantitativeReference', jsonb_build_object(
            'referenceToReferenceUnitGroup', jsonb_build_object(
              '@refObjectId', 'f2540000-0000-4f10-8000-000000000001',
              '@version', '00.00.001'
            )
          )
        ),
        'administrativeInformation', jsonb_build_object(
          'publicationAndOwnership', jsonb_build_object(
            'common:dataSetVersion', '00.00.001'
          )
        )
      )
    ) as payload
  from generated
)
insert into public.flowproperties (
  id,
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at
)
select
  payloads.id,
  payloads.payload,
  payloads.payload::json,
  fixture.benchmark_owner,
  0,
  '00.00.001',
  '2026-07-15 00:00:00+00'::timestamptz
from payloads
cross join guarded_alias_benchmark_meta as fixture;

with fixture as (
  select
    *,
    inserted_flow_property_nodes - inserted_flows as extra_property_nodes
  from guarded_alias_benchmark_meta
), generated as (
  select
    series_no,
    (
      'f2540000-0000-4000-8000-' ||
      lpad(to_hex(series_no), 12, '0')
    )::uuid as id,
    1
      + (fixture.extra_property_nodes / fixture.inserted_flows)
      + case
          when series_no <= fixture.extra_property_nodes % fixture.inserted_flows
            then 1
          else 0
        end as property_count
  from fixture
  cross join lateral generate_series(
    1::bigint,
    fixture.inserted_flows
  ) as generated_flow(series_no)
), payloads as (
  select
    generated.id,
    jsonb_build_object(
      'flowDataSet', jsonb_build_object(
        'administrativeInformation', jsonb_build_object(
          'publicationAndOwnership', jsonb_build_object(
            'common:dataSetVersion', '00.00.001'
          )
        ),
        'flowProperties', jsonb_build_object(
          'flowProperty', flow_properties.items
        )
      )
    ) as payload
  from generated
  cross join guarded_alias_benchmark_meta as fixture
  cross join lateral (
    select jsonb_agg(
      jsonb_build_object(
        '@dataSetInternalID', format(
          'f254-flow-%s-property-%s',
          generated.series_no,
          property_no
        ),
        'referenceToFlowPropertyDataSet', jsonb_build_object(
          '@refObjectId', case
            when generated.series_no <= 23 and property_no = 1
              then fixture.source_flowproperty_id::text
            else fixture.filler_flowproperty_id::text
          end,
          '@version', '00.00.001'
        )
      ) order by property_no
    ) as items
    from generate_series(
      1::bigint,
      generated.property_count
    ) as generated_property(property_no)
  ) as flow_properties
)
insert into public.flows (
  id,
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at
)
select
  payloads.id,
  payloads.payload,
  payloads.payload::json,
  fixture.benchmark_owner,
  0,
  '00.00.001',
  '2026-07-15 00:00:00+00'::timestamptz
from payloads
cross join guarded_alias_benchmark_meta as fixture;

with fixture as (
  select
    *,
    inserted_exchange_nodes - 59 as filler_exchange_nodes
  from guarded_alias_benchmark_meta
), generated as (
  select
    series_no,
    (
      'f2540000-0000-4001-8000-' ||
      lpad(to_hex(series_no), 12, '0')
    )::uuid as id,
    case
      when series_no <= 5 then 3::bigint
      when series_no <= 27 then 2::bigint
      else 0::bigint
    end as target_exchange_count,
    (fixture.filler_exchange_nodes / fixture.inserted_processes)
      + case
          when series_no <= fixture.filler_exchange_nodes % fixture.inserted_processes
            then 1
          else 0
        end as filler_exchange_count
  from fixture
  cross join lateral generate_series(
    1::bigint,
    fixture.inserted_processes
  ) as generated_process(series_no)
), payloads as (
  select
    generated.id,
    jsonb_build_object(
      'processDataSet', jsonb_build_object(
        'administrativeInformation', jsonb_build_object(
          'publicationAndOwnership', jsonb_build_object(
            'common:dataSetVersion', '00.00.001'
          )
        ),
        'exchanges', jsonb_build_object(
          'exchange', exchanges.items
        )
      )
    ) as payload
  from generated
  cross join lateral (
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          '@dataSetInternalID', format(
            'f254-process-%s-exchange-%s',
            generated.series_no,
            exchange_no
          ),
          'exchangeDirection', case
            when exchange_no % 2 = 0 then 'Output'
            else 'Input'
          end,
          'referenceToFlowDataSet', jsonb_build_object(
            '@refObjectId', case
              when exchange_no <= generated.target_exchange_count then (
                'f2540000-0000-4000-8000-' ||
                lpad(
                  to_hex(
                    (
                      (
                        (generated.series_no - 1) * 3
                        + exchange_no - 1
                      ) % 23
                    ) + 1
                  ),
                  12,
                  '0'
                )
              )
              else 'f2540000-0000-4000-8000-000000000018'
            end,
            '@version', '00.00.001'
          )
        ) order by exchange_no
      ),
      '[]'::jsonb
    ) as items
    from generate_series(
      1::bigint,
      generated.target_exchange_count + generated.filler_exchange_count
    ) as generated_exchange(exchange_no)
  ) as exchanges
)
insert into public.processes (
  id,
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  modified_at
)
select
  payloads.id,
  payloads.payload,
  payloads.payload::json,
  fixture.benchmark_owner,
  0,
  '00.00.001',
  '2026-07-15 00:00:00+00'::timestamptz
from payloads
cross join guarded_alias_benchmark_meta as fixture;

set local session_replication_role = origin;

analyze public.flowproperties;
analyze public.flows;
analyze public.processes;
