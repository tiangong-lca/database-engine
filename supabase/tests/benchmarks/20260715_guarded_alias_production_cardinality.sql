\set ON_ERROR_STOP on
\timing on

-- Required safety attestation:
--   -v benchmark_target=local
--   -v benchmark_target=preview
-- Never run this profile against persistent dev or production.
\if :{?benchmark_target}
\else
  \echo 'ERROR: pass -v benchmark_target=local or -v benchmark_target=preview'
  \quit 3
\endif

\if :{?explain_output}
\else
  \set explain_output '/tmp/database-engine-254-guarded-alias-explain.json'
\endif

begin;

select set_config(
  'database_engine.benchmark_target',
  :'benchmark_target',
  true
);
select set_config(
  'application_name',
  'database-engine-254-guarded-alias-benchmark',
  true
);

set local jit = off;
set local track_io_timing = on;
set local max_parallel_workers_per_gather = 0;

-- Bulk fixture construction after a GIN index already exists would otherwise
-- build an artificial pending list that is absent when production indexes are
-- created over existing rows. These reloptions and every fixture row roll back.
alter index public.flows_json_ordered_alias_flowproperty_gin_idx
  set (fastupdate = off);
alter index public.processes_json_ordered_alias_exchange_gin_idx
  set (fastupdate = off);

\ir ../fixtures/20260715_guarded_alias_production_cardinality_fixture.sql

create temporary table guarded_alias_benchmark_metrics on commit drop as
with physical_counts as (
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
), submitted_flows as materialized (
  select
    (
      'f2540000-0000-4000-8000-' ||
      lpad(to_hex(series_no), 12, '0')
    )::uuid as id,
    '00.00.001'::text as version
  from generate_series(1::bigint, 23::bigint) as submitted(series_no)
), candidate_flows as materialized (
  select dataset_flow.id, dataset_flow.version
  from public.flows as dataset_flow
  where private.dataset_alias_jsonb_array_v1(
      dataset_flow.json_ordered::jsonb
        #> '{flowDataSet,flowProperties,flowProperty}'
    ) @> jsonb_build_array(jsonb_build_object(
      'referenceToFlowPropertyDataSet',
      jsonb_build_object(
        '@refObjectId', 'f2540000-0000-4f00-8000-000000000001',
        '@version', '00.00.001'
      )
    ))
), support_references as materialized (
  select *
  from (values
    ('f2540000-0000-4f00-8000-000000000001', '00.00.001'),
    ('f2540000-0000-4f00-8000-000000000002', '00.00.001')
  ) as support_reference(id, version)
), support_parent_flow_keys as materialized (
  select distinct candidate_flow.id, candidate_flow.version
  from support_references as support_reference
  cross join lateral (
    select dataset_flow.id, dataset_flow.version
    from public.flows as dataset_flow
    where private.dataset_alias_jsonb_array_v1(
        dataset_flow.json_ordered::jsonb
          #> '{flowDataSet,flowProperties,flowProperty}'
      ) @> jsonb_build_array(jsonb_build_object(
        'referenceToFlowPropertyDataSet',
        jsonb_build_object(
          '@refObjectId', support_reference.id,
          '@version', support_reference.version
        )
      ))
  ) as candidate_flow
), candidate_processes as materialized (
  select distinct candidate_process.id, candidate_process.version
  from submitted_flows
  cross join lateral (
    select dataset_process.id, dataset_process.version
    from public.processes as dataset_process
    where private.dataset_alias_jsonb_array_v1(
        dataset_process.json_ordered::jsonb
          #> '{processDataSet,exchanges,exchange}'
      ) @> jsonb_build_array(jsonb_build_object(
        'referenceToFlowDataSet',
        jsonb_build_object(
          '@refObjectId', submitted_flows.id::text,
          '@version', submitted_flows.version
        )
      ))
  ) as candidate_process
), live_target_exchanges as (
  select
    dataset_process.id,
    exchange_item.value
  from candidate_processes
  cross join lateral (
    select candidate_process.id, candidate_process.json_ordered
    from public.processes as candidate_process
    where candidate_process.id = candidate_processes.id
      and candidate_process.version = candidate_processes.version
    limit 1
  ) as dataset_process
  cross join lateral jsonb_array_elements(
    private.dataset_alias_jsonb_array_v1(
      dataset_process.json_ordered::jsonb
        #> '{processDataSet,exchanges,exchange}'
    )
  ) as exchange_item(value)
  join submitted_flows
    on submitted_flows.id::text =
      exchange_item.value #>> '{referenceToFlowDataSet,@refObjectId}'
    and submitted_flows.version =
      exchange_item.value #>> '{referenceToFlowDataSet,@version}'
)
select
  physical_counts.*,
  (select count(*)::bigint from candidate_flows) as flow_candidates,
  (select count(*)::bigint from support_parent_flow_keys)
    as support_parent_candidates,
  (select count(*)::bigint from candidate_processes) as process_candidates,
  (select count(*)::bigint from live_target_exchanges) as target_exchange_nodes
from physical_counts;

do $assert_cardinality$
declare
  metrics guarded_alias_benchmark_metrics%rowtype;
begin
  select * into strict metrics
  from guarded_alias_benchmark_metrics;

  if metrics.flowproperties < 483
    or metrics.flows < 132185
    or metrics.flow_property_nodes < 132259
    or metrics.processes < 42369
    or metrics.exchange_nodes < 837020 then
    raise exception
      'physical fixture lower bound failed: flowproperties %, flows %/% nodes, processes %/% nodes',
      metrics.flowproperties,
      metrics.flows,
      metrics.flow_property_nodes,
      metrics.processes,
      metrics.exchange_nodes;
  end if;

  if metrics.flow_candidates <> 23
    or metrics.support_parent_candidates <> 23
    or metrics.process_candidates <> 27
    or metrics.target_exchange_nodes <> 59 then
    raise exception
      'candidate fixture mismatch: flows %, support parents %, processes %, target exchanges %',
      metrics.flow_candidates,
      metrics.support_parent_candidates,
      metrics.process_candidates,
      metrics.target_exchange_nodes;
  end if;
end;
$assert_cardinality$;

table guarded_alias_benchmark_meta;
table guarded_alias_benchmark_metrics;

create or replace function pg_temp.capture_guarded_alias_explain(
  query_sql text
) returns jsonb
language plpgsql
as $capture$
declare
  captured_plan jsonb;
begin
  execute
    'explain (analyze, buffers, wal, settings, format json) ' || query_sql
    into captured_plan;
  return captured_plan;
end;
$capture$;

create temporary table guarded_alias_benchmark_plans (
  profile text primary key,
  plan jsonb not null
) on commit drop;

insert into guarded_alias_benchmark_plans (profile, plan)
values (
  'flow-candidate-and-exact-recheck',
  pg_temp.capture_guarded_alias_explain($flow_query$
    with candidate_flow_keys as materialized (
      select dataset_flow.id, dataset_flow.version
      from public.flows as dataset_flow
      where private.dataset_alias_jsonb_array_v1(
          dataset_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        ) @> jsonb_build_array(jsonb_build_object(
          'referenceToFlowPropertyDataSet',
          jsonb_build_object(
            '@refObjectId', 'f2540000-0000-4f00-8000-000000000001',
            '@version', '00.00.001'
          )
        ))
    ), live_flows as (
      select dataset_flow.id, flow_property.value
      from candidate_flow_keys
      cross join lateral (
        select candidate_flow.id, candidate_flow.json_ordered
        from public.flows as candidate_flow
        where candidate_flow.id = candidate_flow_keys.id
          and candidate_flow.version = candidate_flow_keys.version
        limit 1
      ) as dataset_flow
      cross join lateral jsonb_array_elements(
        private.dataset_alias_jsonb_array_v1(
          dataset_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        )
      ) as flow_property(value)
      where flow_property.value
          #>> '{referenceToFlowPropertyDataSet,@refObjectId}' =
          'f2540000-0000-4f00-8000-000000000001'
        and flow_property.value
          #>> '{referenceToFlowPropertyDataSet,@version}' = '00.00.001'
    )
    select count(*)
    from live_flows
  $flow_query$)
);

insert into guarded_alias_benchmark_plans (profile, plan)
values (
  'support-parent-candidate-and-exact-recheck',
  pg_temp.capture_guarded_alias_explain($support_parent_query$
    with support_references as materialized (
      select *
      from (values
        ('f2540000-0000-4f00-8000-000000000001', '00.00.001'),
        ('f2540000-0000-4f00-8000-000000000002', '00.00.001')
      ) as support_reference(id, version)
    ), support_parent_flow_keys as materialized (
      select distinct candidate_flow.id, candidate_flow.version
      from support_references as support_reference
      cross join lateral (
        select dataset_flow.id, dataset_flow.version
        from public.flows as dataset_flow
        where private.dataset_alias_jsonb_array_v1(
            dataset_flow.json_ordered::jsonb
              #> '{flowDataSet,flowProperties,flowProperty}'
          ) @> jsonb_build_array(jsonb_build_object(
            'referenceToFlowPropertyDataSet',
            jsonb_build_object(
              '@refObjectId', support_reference.id,
              '@version', support_reference.version
            )
          ))
      ) as candidate_flow
    ), live_support_parents as (
      select
        support_parent_flow.id,
        support_parent_flow.user_id,
        support_parent_flow.state_code,
        support_parent_property.value
      from support_parent_flow_keys
      cross join lateral (
        select
          candidate_parent.id,
          candidate_parent.json_ordered,
          candidate_parent.user_id,
          candidate_parent.state_code
        from public.flows as candidate_parent
        where candidate_parent.id = support_parent_flow_keys.id
          and candidate_parent.version = support_parent_flow_keys.version
        limit 1
      ) as support_parent_flow
      cross join lateral jsonb_array_elements(
        private.dataset_alias_jsonb_array_v1(
          support_parent_flow.json_ordered::jsonb
            #> '{flowDataSet,flowProperties,flowProperty}'
        )
      ) as support_parent_property(value)
      join support_references as support_reference
        on support_reference.id = support_parent_property.value
          #>> '{referenceToFlowPropertyDataSet,@refObjectId}'
        and support_reference.version = support_parent_property.value
          #>> '{referenceToFlowPropertyDataSet,@version}'
    )
    select
      count(*),
      count(*) filter (
        where user_id is distinct from
            'f2540000-0000-4002-8000-000000000001'::uuid
          or state_code is distinct from 0
      )
    from live_support_parents
  $support_parent_query$)
);

insert into guarded_alias_benchmark_plans (profile, plan)
values (
  'process-candidate-and-exact-recheck',
  pg_temp.capture_guarded_alias_explain($process_query$
    with submitted_flows as materialized (
      select
        (
          'f2540000-0000-4000-8000-' ||
          lpad(to_hex(series_no), 12, '0')
        )::uuid as id,
        '00.00.001'::text as version
      from generate_series(1::bigint, 23::bigint) as submitted(series_no)
    ), candidate_process_keys as materialized (
      select distinct candidate_process.id, candidate_process.version
      from submitted_flows
      cross join lateral (
        select dataset_process.id, dataset_process.version
        from public.processes as dataset_process
        where private.dataset_alias_jsonb_array_v1(
            dataset_process.json_ordered::jsonb
              #> '{processDataSet,exchanges,exchange}'
          ) @> jsonb_build_array(jsonb_build_object(
            'referenceToFlowDataSet',
            jsonb_build_object(
              '@refObjectId', submitted_flows.id::text,
              '@version', submitted_flows.version
            )
          ))
      ) as candidate_process
    ), live_exchanges as (
      select dataset_process.id, exchange_item.value
      from candidate_process_keys
      cross join lateral (
        select candidate_process.id, candidate_process.json_ordered
        from public.processes as candidate_process
        where candidate_process.id = candidate_process_keys.id
          and candidate_process.version = candidate_process_keys.version
        limit 1
      ) as dataset_process
      cross join lateral jsonb_array_elements(
        private.dataset_alias_jsonb_array_v1(
          dataset_process.json_ordered::jsonb
            #> '{processDataSet,exchanges,exchange}'
        )
      ) as exchange_item(value)
      join submitted_flows
        on submitted_flows.id::text =
          exchange_item.value #>> '{referenceToFlowDataSet,@refObjectId}'
        and submitted_flows.version =
          exchange_item.value #>> '{referenceToFlowDataSet,@version}'
    )
    select count(distinct id), count(*)
    from live_exchanges
  $process_query$)
);

create temporary table guarded_alias_benchmark_plan_nodes on commit drop as
with recursive plan_nodes as (
  select
    benchmark_plan.profile,
    benchmark_plan.plan #> '{0,Plan}' as node,
    array[]::text[] as ancestor_node_types
  from guarded_alias_benchmark_plans as benchmark_plan

  union all

  select
    plan_nodes.profile,
    child_plan.value,
    plan_nodes.ancestor_node_types || (plan_nodes.node->>'Node Type')
  from plan_nodes
  cross join lateral jsonb_array_elements(
    coalesce(plan_nodes.node->'Plans', '[]'::jsonb)
  ) as child_plan(value)
)
select *
from plan_nodes;

do $assert_plans$
begin
  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile = 'flow-candidate-and-exact-recheck'
        and plan_node.node->>'Index Name' =
          'flows_json_ordered_alias_flowproperty_gin_idx'
    ) then
    raise exception
      'flow candidate profile did not use flows_json_ordered_alias_flowproperty_gin_idx';
  end if;

  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile =
          'support-parent-candidate-and-exact-recheck'
        and plan_node.node->>'Index Name' =
          'flows_json_ordered_alias_flowproperty_gin_idx'
    ) then
    raise exception
      'support-parent profile did not use flows_json_ordered_alias_flowproperty_gin_idx';
  end if;

  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile = 'process-candidate-and-exact-recheck'
        and plan_node.node->>'Index Name' =
          'processes_json_ordered_alias_exchange_gin_idx'
    ) then
    raise exception
      'process candidate profile did not use processes_json_ordered_alias_exchange_gin_idx';
  end if;

  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile = 'flow-candidate-and-exact-recheck'
        and plan_node.node->>'Relation Name' = 'flows'
        and plan_node.node->>'Alias' = 'candidate_flow'
        and plan_node.node->>'Index Name' = 'flows_pkey'
        and plan_node.node->>'Node Type' = 'Index Scan'
        and (plan_node.node->>'Actual Loops')::numeric = 23
        and plan_node.node->>'Index Cond' like '%candidate_flow_keys.id%'
        and plan_node.node->>'Index Cond' like
          '%candidate_flow_keys.version%'
    ) then
    raise exception
      'flow live exact recheck was not 23 candidate-driven flows_pkey point lookups';
  end if;

  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile =
          'support-parent-candidate-and-exact-recheck'
        and plan_node.node->>'Relation Name' = 'flows'
        and plan_node.node->>'Alias' = 'candidate_parent'
        and plan_node.node->>'Index Name' = 'flows_pkey'
        and plan_node.node->>'Node Type' = 'Index Scan'
        and (plan_node.node->>'Actual Loops')::numeric = 23
        and plan_node.node->>'Index Cond' like
          '%support_parent_flow_keys.id%'
        and plan_node.node->>'Index Cond' like
          '%support_parent_flow_keys.version%'
    ) then
    raise exception
      'support-parent live exact recheck was not 23 candidate-driven flows_pkey point lookups';
  end if;

  if not exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where plan_node.profile = 'process-candidate-and-exact-recheck'
        and plan_node.node->>'Relation Name' = 'processes'
        and plan_node.node->>'Alias' = 'candidate_process'
        and plan_node.node->>'Index Name' = 'processes_pkey'
        and plan_node.node->>'Node Type' = 'Index Scan'
        and (plan_node.node->>'Actual Loops')::numeric = 27
        and plan_node.node->>'Index Cond' like
          '%candidate_process_keys.id%'
        and plan_node.node->>'Index Cond' like
          '%candidate_process_keys.version%'
    ) then
    raise exception
      'process live exact recheck was not 27 candidate-driven processes_pkey point lookups';
  end if;

  if exists (
      select 1
      from guarded_alias_benchmark_plan_nodes as plan_node
      where (
          (
            plan_node.profile = 'flow-candidate-and-exact-recheck'
            and plan_node.node->>'Relation Name' = 'flows'
            and plan_node.node->>'Alias' = 'candidate_flow'
            and (
              plan_node.node->>'Node Type' = 'Seq Scan'
              or (
                'Hash' = any(plan_node.ancestor_node_types)
                and (
                  plan_node.node->>'Index Name' is distinct from 'flows_pkey'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%candidate_flow_keys.id%'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%candidate_flow_keys.version%'
                )
              )
            )
          ) or (
            plan_node.profile =
              'support-parent-candidate-and-exact-recheck'
            and plan_node.node->>'Relation Name' = 'flows'
            and plan_node.node->>'Alias' = 'candidate_parent'
            and (
              plan_node.node->>'Node Type' = 'Seq Scan'
              or (
                'Hash' = any(plan_node.ancestor_node_types)
                and (
                  plan_node.node->>'Index Name' is distinct from 'flows_pkey'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%support_parent_flow_keys.id%'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%support_parent_flow_keys.version%'
                )
              )
            )
          ) or (
            plan_node.profile = 'process-candidate-and-exact-recheck'
            and plan_node.node->>'Relation Name' = 'processes'
            and plan_node.node->>'Alias' = 'candidate_process'
            and (
              plan_node.node->>'Node Type' = 'Seq Scan'
              or (
                'Hash' = any(plan_node.ancestor_node_types)
                and (
                  plan_node.node->>'Index Name' is distinct from
                    'processes_pkey'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%candidate_process_keys.id%'
                  or coalesce(plan_node.node->>'Index Cond', '') not like
                    '%candidate_process_keys.version%'
                )
              )
            )
          )
        )
    ) then
    raise exception
      'a live exact relookup used a sequential scan or fed a full-table Hash input';
  end if;

  if exists (
      select 1
      from guarded_alias_benchmark_plans as benchmark_plan
      where benchmark_plan.plan #>> '{0,Settings,enable_hashjoin}' = 'off'
    ) then
    raise exception
      'benchmark must prove candidate-driven point lookups without disabling Hash Join';
  end if;
end;
$assert_plans$;

\pset format unaligned
\pset tuples_only on
select jsonb_object_agg(profile, plan order by profile)
from guarded_alias_benchmark_plans
\g :explain_output
\pset format aligned
\pset tuples_only off

select
  profile,
  (plan #>> '{0,Planning Time}')::numeric as planning_ms,
  (plan #>> '{0,Execution Time}')::numeric as execution_ms,
  encode(
    extensions.digest(convert_to(plan::text, 'UTF8'), 'sha256'),
    'hex'
  ) as explain_sha256,
  :'explain_output' as explain_json_path
from guarded_alias_benchmark_plans
order by profile;

rollback;

-- ANALYZE is intentionally repeated after rollback so a disposable Preview used
-- for the profile does not retain fixture-inflated table statistics.
analyze public.flowproperties;
analyze public.flows;
analyze public.processes;
