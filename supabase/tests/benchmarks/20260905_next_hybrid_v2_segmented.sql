-- Run only through scripts/benchmark_next_hybrid_v2.mjs.
-- The runner supplies rollback-only local fixtures before this profile.
set local application_name = 'database-engine-624-next-hybrid-v2-segmented';
set local statement_timeout = '240s';
set local pg_stat_statements.track = 'all';

-- Keep the exact 2,000/2,001 boundary populations disjoint. The broad
-- Process type has 2 fixture rows plus 2,501 rows below; broad Flow Product
-- has 2 fixture rows plus both classification boundary populations.
insert into public.processes(
  id, version, state_code, json, json_ordered, user_id, team_id,
  modified_at, extracted_md, search_text, embedding_ft, rule_verification
)
select
  md5('next-v2-process-boundary-2000:' || n)::uuid,
  '01.00.000',
  100,
  document,
  document::json,
  null,
  null,
  '2026-09-05 17:00:00+00'::timestamptz,
  'LOCAL NEXT V2 SEGMENTED BENCHMARK',
  array['NextV2BenchNeedle', 'ProcessBoundary2000'],
  pg_temp.next_v2_vector(1, (n::numeric / 100000)::real)::extensions.vector(1024),
  true
from pg_catalog.generate_series(1, 2000) as sequence(n)
cross join lateral (
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'modellingAndValidation', jsonb_build_object(
        'LCIMethodAndAllocation', jsonb_build_object(
          'typeOfDataSet', 'Partly terminated system'
        )
      )
    )
  ) as document
) as fixture;

insert into public.processes(
  id, version, state_code, json, json_ordered, user_id, team_id,
  modified_at, extracted_md, search_text, embedding_ft, rule_verification
)
select
  md5('next-v2-process-overflow-2001:' || n)::uuid,
  '01.00.000',
  100,
  document,
  document::json,
  null,
  null,
  '2026-09-05 17:00:00+00'::timestamptz,
  'LOCAL NEXT V2 SEGMENTED BENCHMARK',
  array['NextV2BenchNeedle', 'ProcessOverflow2001'],
  pg_temp.next_v2_vector(1, (n::numeric / 100000)::real)::extensions.vector(1024),
  true
from pg_catalog.generate_series(1, 2001) as sequence(n)
cross join lateral (
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'modellingAndValidation', jsonb_build_object(
        'LCIMethodAndAllocation', jsonb_build_object(
          'typeOfDataSet', 'Unit process, single operation'
        )
      )
    )
  ) as document
) as fixture;

insert into public.processes(
  id, version, state_code, json, json_ordered, user_id, team_id,
  modified_at, extracted_md, search_text, embedding_ft, rule_verification
)
select
  md5('next-v2-process-broad:' || n)::uuid,
  '01.00.000',
  100,
  document,
  document::json,
  null,
  null,
  '2026-09-05 17:00:00+00'::timestamptz,
  'LOCAL NEXT V2 SEGMENTED BENCHMARK',
  array['NextV2BenchNeedle', 'ProcessBroad'],
  pg_temp.next_v2_vector(1, (n::numeric / 100000)::real)::extensions.vector(1024),
  true
from pg_catalog.generate_series(1, 2501) as sequence(n)
cross join lateral (
  select jsonb_build_object(
    'processDataSet', jsonb_build_object(
      'modellingAndValidation', jsonb_build_object(
        'LCIMethodAndAllocation', jsonb_build_object(
          'typeOfDataSet', 'LCI result'
        )
      )
    )
  ) as document
) as fixture;

insert into public.flows(
  id, version, state_code, json, json_ordered, user_id, team_id,
  modified_at, extracted_md, search_text, embedding_ft, rule_verification
)
select
  md5('next-v2-flow-boundary-2000:' || n)::uuid,
  '01.00.000',
  100,
  document,
  document::json,
  null,
  null,
  '2026-09-05 17:00:00+00'::timestamptz,
  'LOCAL NEXT V2 SEGMENTED BENCHMARK',
  array['NextV2BenchNeedle', 'FlowBoundary2000'],
  pg_temp.next_v2_vector(1, (n::numeric / 100000)::real)::extensions.vector(1024),
  true
from pg_catalog.generate_series(1, 2000) as sequence(n)
cross join lateral (
  select jsonb_build_object(
    'flowDataSet', jsonb_build_object(
      'modellingAndValidation', jsonb_build_object(
        'LCIMethod', jsonb_build_object('typeOfDataSet', 'Product flow')
      ),
      'flowInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'classificationInformation', jsonb_build_object(
            'common:classification', jsonb_build_object(
              'common:class', jsonb_build_array(
                jsonb_build_object('@classId', 'C-BOUNDARY-2000')
              )
            )
          )
        )
      )
    )
  ) as document
) as fixture;

insert into public.flows(
  id, version, state_code, json, json_ordered, user_id, team_id,
  modified_at, extracted_md, search_text, embedding_ft, rule_verification
)
select
  md5('next-v2-flow-overflow-2001:' || n)::uuid,
  '01.00.000',
  100,
  document,
  document::json,
  null,
  null,
  '2026-09-05 17:00:00+00'::timestamptz,
  'LOCAL NEXT V2 SEGMENTED BENCHMARK',
  array['NextV2BenchNeedle', 'FlowOverflow2001'],
  pg_temp.next_v2_vector(1, (n::numeric / 100000)::real)::extensions.vector(1024),
  true
from pg_catalog.generate_series(1, 2001) as sequence(n)
cross join lateral (
  select jsonb_build_object(
    'flowDataSet', jsonb_build_object(
      'modellingAndValidation', jsonb_build_object(
        'LCIMethod', jsonb_build_object('typeOfDataSet', 'Product flow')
      ),
      'flowInformation', jsonb_build_object(
        'dataSetInformation', jsonb_build_object(
          'classificationInformation', jsonb_build_object(
            'common:classification', jsonb_build_object(
              'common:class', jsonb_build_array(
                jsonb_build_object('@classId', 'C-OVERFLOW-2001')
              )
            )
          )
        )
      )
    )
  ) as document
) as fixture;

analyze public.processes;
analyze public.flows;
analyze private.next_hybrid_public_candidates_v2;

create temporary table next_v2_route_cases(
  kind text not null,
  case_name text not null,
  process_type text,
  flow_types text[] not null default '{}',
  classification_codes text[] not null default '{}',
  expected_route text not null,
  actual_population integer,
  primary key(kind, case_name)
);

insert into next_v2_route_cases(
  kind, case_name, process_type, flow_types, classification_codes, expected_route,
  actual_population
) values
  ('process', 'zero', 'Avoided product system', '{}', '{}', 'exact', 0),
  ('process', 'small', 'Unit process, black box', '{}', '{}', 'exact', 1),
  ('process', 'boundary_2000', 'Partly terminated system', '{}', '{}', 'exact', 2000),
  ('process', 'overflow_2001', 'Unit process, single operation', '{}', '{}', 'hnsw', 2001),
  ('process', 'broad', 'LCI result', '{}', '{}', 'hnsw', 2503),
  ('process', 'unfiltered', null, '{}', '{}', 'hnsw', 6505),
  ('flow', 'zero', null, '{}', array['C-ZERO'], 'exact', 0),
  ('flow', 'small', null, '{}', array['C-ONE'], 'exact', 1),
  ('flow', 'boundary_2000', null, '{}', array['C-BOUNDARY-2000'], 'exact', 2000),
  ('flow', 'overflow_2001', null, '{}', array['C-OVERFLOW-2001'], 'hnsw', 2001),
  ('flow', 'broad', null, array['Product flow'], '{}', 'hnsw', 4003),
  ('flow', 'unfiltered', null, '{}', '{}', 'hnsw', 4004);

create temporary table next_v2_route_samples(
  kind text,
  case_name text,
  sample integer,
  elapsed_ms double precision,
  result_count integer,
  result_sha256 text,
  observed_route text,
  reported_population integer
);
grant select on next_v2_route_cases to api_internal_executor;
grant insert on next_v2_route_samples to api_internal_executor;

create function pg_temp.measure_next_v2_routes()
returns void
language plpgsql
set search_path = ''
as $$
declare
  route_case record;
  sample_no integer;
  started timestamptz;
  matched integer;
  result_sha256 text;
  route text;
  reported integer;
  exact_before bigint;
  exact_after bigint;
  hnsw_before bigint;
  hnsw_after bigint;
begin
  for route_case in
    select * from pg_temp.next_v2_route_cases order by kind, case_name
  loop
    for sample_no in 1..6 loop
      select
        coalesce(sum(calls) filter (
          where query like '%with candidate_keys as materialized%'
            and query like '%public.' || case route_case.kind
              when 'process' then 'processes' else 'flows' end || ' as source%'
        ), 0),
        coalesce(sum(calls) filter (
          where query like '%with nearest as materialized%'
            and query like '%public.' || case route_case.kind
              when 'process' then 'processes' else 'flows' end || ' as source%'
        ), 0)
      into exact_before, hnsw_before
      from extensions.pg_stat_statements;

      started := pg_catalog.clock_timestamp();
      select
        count(*)::integer,
        pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
          coalesce(pg_catalog.string_agg(
            candidate.id::text || '@' || candidate.version,
            ',' order by candidate.rank
          ), ''),
          'UTF8'
        ), 'sha256'), 'hex'),
        max(candidate.semantic_route),
        max(candidate.candidate_population)
      into matched, result_sha256, route, reported
      from private.next_public_semantic_version_candidates_v2(
        route_case.kind,
        pg_temp.next_v2_vector(1, 0)::extensions.vector,
        '{}',
        route_case.process_type,
        route_case.flow_types,
        false,
        route_case.classification_codes,
        '{}',
        'tg',
        null
      ) as candidate;

      select
        coalesce(sum(calls) filter (
          where query like '%with candidate_keys as materialized%'
            and query like '%public.' || case route_case.kind
              when 'process' then 'processes' else 'flows' end || ' as source%'
        ), 0),
        coalesce(sum(calls) filter (
          where query like '%with nearest as materialized%'
            and query like '%public.' || case route_case.kind
              when 'process' then 'processes' else 'flows' end || ' as source%'
        ), 0)
      into exact_after, hnsw_after
      from extensions.pg_stat_statements;

      route := coalesce(
        route,
        case
          when route_case.actual_population = 0
            and route_case.expected_route = 'exact' then 'exact'
          when exact_after = exact_before + 1 and hnsw_after = hnsw_before
            then 'exact'
          when hnsw_after = hnsw_before + 1 and exact_after = exact_before
            then 'hnsw'
          else 'ambiguous'
        end
      );
      reported := coalesce(
        reported,
        case when route_case.actual_population = 0 then 0 else null end
      );

      insert into pg_temp.next_v2_route_samples values (
        route_case.kind,
        route_case.case_name,
        sample_no,
        extract(epoch from pg_catalog.clock_timestamp() - started) * 1000,
        matched,
        result_sha256,
        route,
        reported
      );
    end loop;
  end loop;
end;
$$;
grant execute on function pg_temp.measure_next_v2_routes()
  to api_internal_executor;

create temporary table next_v2_comparison_samples(
  kind text,
  revision text,
  sample integer,
  elapsed_ms double precision,
  result_count integer
);
grant insert on next_v2_comparison_samples to api_internal_executor;

create function pg_temp.measure_next_v2_comparison()
returns void
language plpgsql
set search_path = ''
as $$
declare
  sample_no integer;
  started timestamptz;
  matched integer;
begin
  for sample_no in 1..6 loop
    started := pg_catalog.clock_timestamp();
    select count(*)::integer into matched
    from private.semantic_process_version_candidates_v1(
      pg_temp.next_v2_vector(1, 0),
      '{"processDataSet":{"modellingAndValidation":{"LCIMethodAndAllocation":{"typeOfDataSet":"Unit process, black box"}}}}',
      0.5, 200, 'tg'
    );
    insert into pg_temp.next_v2_comparison_samples values (
      'process', 'v1_filtered_semantic', sample_no,
      extract(epoch from pg_catalog.clock_timestamp() - started) * 1000,
      matched
    );

    started := pg_catalog.clock_timestamp();
    select count(*)::integer into matched
    from private.next_public_semantic_version_candidates_v2(
      'process', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', 'Unit process, black box', '{}', false, '{}', '{}', 'tg', null
    );
    insert into pg_temp.next_v2_comparison_samples values (
      'process', 'v2_filtered_semantic', sample_no,
      extract(epoch from pg_catalog.clock_timestamp() - started) * 1000,
      matched
    );

    started := pg_catalog.clock_timestamp();
    select count(*)::integer into matched
    from private.semantic_flow_version_candidates_v1(
      pg_temp.next_v2_vector(1, 0),
      '{"classification":[{"scope":"classification","code":"C-ONE"}]}',
      0.5, 200, 'tg'
    );
    insert into pg_temp.next_v2_comparison_samples values (
      'flow', 'v1_filtered_semantic', sample_no,
      extract(epoch from pg_catalog.clock_timestamp() - started) * 1000,
      matched
    );

    started := pg_catalog.clock_timestamp();
    select count(*)::integer into matched
    from private.next_public_semantic_version_candidates_v2(
      'flow', pg_temp.next_v2_vector(1, 0)::extensions.vector,
      '{}', null, '{}', false, array['C-ONE'], '{}', 'tg', null
    );
    insert into pg_temp.next_v2_comparison_samples values (
      'flow', 'v2_filtered_semantic', sample_no,
      extract(epoch from pg_catalog.clock_timestamp() - started) * 1000,
      matched
    );
  end loop;
end;
$$;
grant execute on function pg_temp.measure_next_v2_comparison()
  to api_internal_executor;

grant api_internal_executor, next_public_search_executor to postgres;
set local role api_internal_executor;
select pg_temp.measure_next_v2_routes();
select pg_temp.measure_next_v2_comparison();
reset role;

create temporary table next_v2_plans(
  kind text,
  shape text,
  plan jsonb
);
grant insert on next_v2_plans to next_public_search_executor;

create function pg_temp.next_v2_plan(p_kind text, p_shape text)
returns jsonb
language plpgsql
set search_path = ''
as $$
declare
  plan jsonb;
begin
  if p_kind = 'process' and p_shape = 'exact' then
    execute $query$
      explain (analyze, buffers, format json)
      select candidate.id, candidate.version
      from private.next_hybrid_public_candidates_v2 as candidate
      where candidate.dataset_kind = 'process'
        and candidate.state_code = 100
        and candidate.dataset_type = 'Unit process, black box'
      limit 2001
    $query$ into plan;
  elsif p_kind = 'flow' and p_shape = 'exact' then
    execute $query$
      explain (analyze, buffers, format json)
      select candidate.id, candidate.version
      from private.next_hybrid_public_candidates_v2 as candidate
      where candidate.dataset_kind = 'flow'
        and candidate.state_code = 100
        and candidate.classification_codes && array['C-ONE']
      limit 2001
    $query$ into plan;
  elsif p_kind = 'process' and p_shape = 'team' then
    execute $query$
      explain (analyze, buffers, format json)
      select candidate.id, candidate.version
      from private.next_hybrid_public_candidates_v2 as candidate
      where candidate.dataset_kind = 'process'
        and candidate.state_code = 100
        and candidate.team_id = '62000000-0000-4000-8000-000000000903'
      limit 2001
    $query$ into plan;
  elsif p_kind = 'flow' and p_shape = 'team' then
    execute $query$
      explain (analyze, buffers, format json)
      select candidate.id, candidate.version
      from private.next_hybrid_public_candidates_v2 as candidate
      where candidate.dataset_kind = 'flow'
        and candidate.state_code = 100
        and candidate.team_id = '62000000-0000-4000-8000-000000000903'
      limit 2001
    $query$ into plan;
  elsif p_kind = 'process' and p_shape = 'hnsw' then
    execute $query$
      explain (analyze, buffers, format json)
      select source.id, source.version,
        source.embedding_ft operator(extensions.<=>)
          pg_temp.next_v2_vector(1, 0)::extensions.vector as distance
      from public.processes as source
      where source.embedding_ft is not null and source.state_code = 100
      order by source.embedding_ft operator(extensions.<=>)
        pg_temp.next_v2_vector(1, 0)::extensions.vector
      limit 200
    $query$ into plan;
  elsif p_kind = 'flow' and p_shape = 'hnsw' then
    execute $query$
      explain (analyze, buffers, format json)
      select source.id, source.version,
        source.embedding_ft operator(extensions.<=>)
          pg_temp.next_v2_vector(1, 0)::extensions.vector as distance
      from public.flows as source
      where source.embedding_ft is not null and source.state_code = 100
      order by source.embedding_ft operator(extensions.<=>)
        pg_temp.next_v2_vector(1, 0)::extensions.vector
      limit 200
    $query$ into plan;
  elsif p_kind = 'process' and p_shape = 'lexical' then
    execute $query$
      explain (analyze, buffers, format json)
      select source.id, source.version,
        extensions.pgroonga_score(source.tableoid, source.ctid) as score
      from public.processes as source
      where source.state_code = 100
        and source.search_text operator(extensions.&@~|) array['NextV2BenchNeedle']
      order by score desc, source.modified_at desc, source.id, source.version desc
      limit 200
    $query$ into plan;
  elsif p_kind = 'flow' and p_shape = 'lexical' then
    execute $query$
      explain (analyze, buffers, format json)
      select source.id, source.version,
        extensions.pgroonga_score(source.tableoid, source.ctid) as score
      from public.flows as source
      where source.state_code = 100
        and source.search_text operator(extensions.&@~|) array['NextV2BenchNeedle']
      order by score desc, source.modified_at desc, source.id, source.version desc
      limit 200
    $query$ into plan;
  else
    raise exception 'invalid Next V2 benchmark plan shape';
  end if;
  return plan;
end;
$$;
grant execute on function pg_temp.next_v2_plan(text, text)
  to next_public_search_executor;
grant execute on function pg_temp.next_v2_vector(real, real)
  to next_public_search_executor;

set local role next_public_search_executor;
set local plan_cache_mode = 'force_custom_plan';
set local hnsw.iterative_scan = 'strict_order';
set local hnsw.ef_search = 200;
set local hnsw.max_scan_tuples = 20000;
set local hnsw.scan_mem_multiplier = 2;
set local jit = off;
insert into pg_temp.next_v2_plans
select kind, shape, pg_temp.next_v2_plan(kind, shape)
from pg_catalog.unnest(array['process', 'flow']) as kinds(kind)
cross join pg_catalog.unnest(array['exact', 'team', 'hnsw', 'lexical']) as shapes(shape);
reset role;

with route_rollup as (
  select
    route_case.kind,
    route_case.case_name,
    route_case.expected_route,
    route_case.actual_population,
    case when count(distinct sample.observed_route) = 1
      then min(sample.observed_route) else 'mixed' end as observed_route,
    max(sample.reported_population) as reported_population,
    max(sample.result_count) as result_count,
    case when count(distinct sample.result_sha256) = 1
      then min(sample.result_sha256) else null end as result_sha256,
    max(sample.elapsed_ms) filter (where sample.sample = 1) as first_ms,
    percentile_cont(0.5) within group (order by sample.elapsed_ms)
      filter (where sample.sample > 1) as repeat_p50_ms,
    max(sample.elapsed_ms) filter (where sample.sample > 1) as repeat_max_ms
  from pg_temp.next_v2_route_cases as route_case
  join pg_temp.next_v2_route_samples as sample using(kind, case_name)
  group by route_case.kind, route_case.case_name,
    route_case.expected_route, route_case.actual_population
), routes as (
  select kind, jsonb_object_agg(case_name, jsonb_build_object(
    'expectedRoute', expected_route,
    'observedRoute', observed_route,
    'actualPopulation', actual_population,
    'candidatePopulation', reported_population,
    'resultCount', result_count,
    'resultSha256', result_sha256,
    'firstObservedMs', round(first_ms::numeric, 3),
    'repeatP50Ms', round(repeat_p50_ms::numeric, 3),
    'repeatMaxMs', round(repeat_max_ms::numeric, 3)
  )) as value
  from route_rollup
  group by kind
), comparison as (
  select kind, revision, jsonb_build_object(
    'firstObservedMs', round(max(elapsed_ms) filter (where sample = 1)::numeric, 3),
    'repeatP50Ms', round((
      percentile_cont(0.5) within group (order by elapsed_ms)
        filter (where sample > 1)
    )::numeric, 3),
    'resultCount', max(result_count)
  ) as value
  from pg_temp.next_v2_comparison_samples
  group by kind, revision
), comparisons as (
  select kind, jsonb_object_agg(revision, value) as value
  from comparison
  group by kind
), plans as (
  select kind,
    jsonb_path_query_array(jsonb_agg(plan), '$.**."Index Name"') as index_names,
    jsonb_path_query_array(jsonb_agg(plan), '$.**."Node Type"') as node_types,
    jsonb_object_agg(
      shape,
      jsonb_path_query_array(plan, '$.**."Index Name"')
    ) as indexes_by_shape,
    jsonb_object_agg(
      shape,
      jsonb_path_query_array(plan, '$.**."Node Type"')
    ) as node_types_by_shape,
    jsonb_object_agg(shape, jsonb_build_object(
      'executionMs', plan #> '{0,Execution Time}',
      'sharedHitBlocks', plan #> '{0,Plan,Shared Hit Blocks}',
      'sharedReadBlocks', plan #> '{0,Plan,Shared Read Blocks}'
    )) as metrics
  from pg_temp.next_v2_plans
  group by kind
)
select jsonb_build_object(
  'benchmark', 'next-hybrid-v2-segmented.v1',
  'profile', 'isolated-synthetic',
  'kind', routes.kind,
  'samplesPerCase', 6,
  'routes', routes.value,
  'filteredSemanticComparison', comparisons.value,
  'planIndexNames', plans.index_names,
  'planNodeTypes', plans.node_types,
  'planIndexesByShape', plans.indexes_by_shape,
  'planNodeTypesByShape', plans.node_types_by_shape,
  'planMetrics', plans.metrics
)
from routes
join comparisons using(kind)
join plans using(kind)
order by routes.kind;

rollback;
