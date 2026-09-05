-- Run ONLY via scripts/benchmark_hybrid_versions.mjs on its isolated local stack.
-- Its transaction/fixtures come from the version-search regression, not real users.
set local statement_timeout='120s';
set local application_name='database-engine-600-hybrid-comparison';

do $$
declare k text; n integer; identity uuid;
begin
  foreach k in array array['process','flow'] loop
    for n in 1..5000 loop
      identity := md5(k || ':comparison:' || n)::uuid;
      perform pg_temp.version_fixture(k,identity,'01.00.000',100,
        case when n<=40 then 'Refined copper production comparison' else 'Background aggregate comparison' end,
        case when n<=40 then pg_temp.portal_versions_vector(1,(n::numeric/200)::real)
          else pg_temp.portal_versions_vector((n::numeric/10000)::real,1) end,'ZZB');
      if n<=20 then
        perform pg_temp.version_fixture(k,identity,'01.00.001',100,
          'Unrelated newer process comparison',pg_temp.portal_versions_vector(0,1),'ZZB');
      end if;
    end loop;
  end loop;

end $$;
analyze public.processes;
analyze public.flows;
analyze private.portal_catalog_search_rows_v1;
create temporary table comparison_samples(kind text,revision text,sample integer,elapsed_ms double precision,payload jsonb);
create temporary table process_route_cases(
  case_name text primary key,
  filters jsonb not null,
  expected_route text not null,
  candidate_population integer
);
insert into process_route_cases(case_name,filters,expected_route) values
  ('zero','{"geography":"zz0"}','exact'),
  ('selective','{"geography":"zz6"}','exact'),
  ('boundary_2000','{"geography":"zx2k"}','exact'),
  ('overflow_2001','{"geography":"zx2k1"}','hnsw'),
  ('broad','{"geography":"zzb"}','hnsw'),
  ('unfiltered','{}','hnsw');
update process_route_cases as route_case
set candidate_population = case
  when route_case.filters ? 'geography' then (
    select count(*)::integer
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'process'
      and facet.state_code in (100,200)
      and facet.facet_contract_version = 1
      and facet.facet_geography = route_case.filters ->> 'geography'
  )
  else null
end;
create temporary table process_route_samples(
  case_name text,
  sample integer,
  elapsed_ms double precision,
  result_count integer,
  observed_route text
);
create function pg_temp.measure_process_routes(p_case_names text[])
returns void
language plpgsql
set search_path=''
as $$
declare
  route_case record;
  n integer;
  started timestamptz;
  elapsed double precision;
  matched integer;
  exact_calls_before bigint;
  exact_calls_after bigint;
  hnsw_calls_before bigint;
  hnsw_calls_after bigint;
  observed text;
begin
  for route_case in
    select *
    from pg_temp.process_route_cases
    where case_name = any(p_case_names)
    order by case_name
  loop
    for n in 1..6 loop
      select
        coalesce(sum(statements.calls) filter(
          where statements.query like 'with candidate_keys as materialized%'
            and statements.query like '%public.processes as source%'
        ),0),
        coalesce(sum(statements.calls) filter(
          where statements.query like 'with nearest as materialized%'
            and statements.query like '%public.processes as source%'
        ),0)
      into exact_calls_before,hnsw_calls_before
      from extensions.pg_stat_statements as statements;

      started := clock_timestamp();
      select count(*)::integer
      into matched
      from private.portal_projection_semantic_process_v2(
        pg_temp.portal_versions_vector(1,0),
        route_case.filters
      );
      elapsed := extract(epoch from clock_timestamp()-started)*1000;

      select
        coalesce(sum(statements.calls) filter(
          where statements.query like 'with candidate_keys as materialized%'
            and statements.query like '%public.processes as source%'
        ),0),
        coalesce(sum(statements.calls) filter(
          where statements.query like 'with nearest as materialized%'
            and statements.query like '%public.processes as source%'
        ),0)
      into exact_calls_after,hnsw_calls_after
      from extensions.pg_stat_statements as statements;

      observed := case
        when exact_calls_after = exact_calls_before + 1
         and hnsw_calls_after = hnsw_calls_before then 'exact'
        when exact_calls_after = exact_calls_before
         and hnsw_calls_after = hnsw_calls_before + 1 then 'hnsw'
        else 'ambiguous'
      end;
      insert into pg_temp.process_route_samples
      values(route_case.case_name,n,elapsed,matched,observed);
    end loop;
  end loop;
end $$;
grant insert on comparison_samples to anon;
grant execute on function pg_temp.portal_versions_vector_text(real,real) to anon;
set local role anon;

do $$
declare k text; revision text; n integer; started timestamptz; result jsonb;
begin
  foreach k in array array['process','flow'] loop
    foreach revision in array array['v1','v2'] loop
      for n in 1..20 loop
        started := clock_timestamp();
        if revision='v1' then
          result := api.portal_hybrid_search_v1(k,array['refined copper'],
            pg_temp.portal_versions_vector_text(1,0),'{"geography":"zzb"}',20);
        else
          result := api.portal_hybrid_search_v2(k,array['refined copper'],
            pg_temp.portal_versions_vector_text(1,0),'{"geography":"zzb"}',20,null);
        end if;
        insert into comparison_samples values(k,revision,n,
          extract(epoch from clock_timestamp()-started)*1000,result);
      end loop;
    end loop;
  end loop;
end $$;

reset role;
create function pg_temp.version_hnsw_plan(p_kind text,p_filters jsonb) returns jsonb
language plpgsql set search_path='' as $$
declare v_plan jsonb; v_table text;
begin
  v_table := case p_kind when 'process' then 'processes' when 'flow' then 'flows' end;
  if v_table is null then raise exception 'invalid benchmark kind'; end if;
  execute pg_catalog.format($query$
    explain(analyze,buffers,format json)
    with nearest as materialized (
      select source.id,source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $1 as distance
      from public.%I as source
      where source.state_code in (100,200) and source.embedding_ft is not null
        and exists(
          select 1 from private.portal_catalog_search_rows_v1 as projection
          where projection.dataset_kind=$2 and projection.id=source.id
            and projection.version=source.version::text and projection.state_code in (100,200)
            and private.portal_card_matches_filters_v2(projection.card,$3)
          offset 0
        )
      order by source.embedding_ft operator(extensions.<=>) $1 limit 200
    )
    select * from nearest where distance>=0 and distance<=0.5
    order by distance+0::double precision,id,version desc
  $query$,v_table) into v_plan
    using pg_temp.portal_versions_vector(1,0),p_kind,p_filters;
  return v_plan;
end $$;
grant execute on function pg_temp.version_hnsw_plan(text,jsonb)
  to api_internal_executor,portal_public_executor;
grant execute on function pg_temp.portal_versions_vector(real,real)
  to api_internal_executor,portal_public_executor;
grant execute on function pg_temp.portal_versions_vector_text(real,real)
  to api_internal_executor,portal_public_executor;
create temporary table comparison_plans(kind text,shape text,plan jsonb);
grant insert on comparison_plans to api_internal_executor,portal_public_executor;
grant api_internal_executor,portal_public_executor to postgres;
set local role portal_public_executor;
set local plan_cache_mode='force_custom_plan';
set local hnsw.iterative_scan='strict_order';
set local hnsw.ef_search=200;
set local hnsw.max_scan_tuples=20000;
set local hnsw.scan_mem_multiplier=2;
set local jit=off;
insert into comparison_plans values
  ('process','broad',pg_temp.version_hnsw_plan('process','{"geography":"zzb"}')),
  ('process','unfiltered',pg_temp.version_hnsw_plan('process','{}')),
  ('flow','broad',pg_temp.version_hnsw_plan('flow','{"geography":"zzb"}')),
  ('flow','unfiltered',pg_temp.version_hnsw_plan('flow','{}'));
reset role;
set local pg_stat_statements.track='all';
select pg_temp.measure_process_routes(
  array['zero','selective','broad','unfiltered']
);

-- Add the two cutoff populations only after broad/unfiltered plan and timing
-- evidence has been captured, so their synthetic graph topology cannot skew
-- the retained HNSW baseline. All boundary vectors remain inside threshold.
do $$
declare n integer;
begin
  for n in 1..2000 loop
    perform pg_temp.version_fixture(
      'process',
      md5('process:adaptive-2000:' || n)::uuid,
      '01.00.000',
      100,
      'Adaptive Process exact boundary',
      pg_temp.portal_versions_vector(1,(n::numeric/100000)::real),
      'ZX2K'
    );
  end loop;
  for n in 1..2001 loop
    perform pg_temp.version_fixture(
      'process',
      md5('process:adaptive-2001:' || n)::uuid,
      '01.00.000',
      100,
      'Adaptive Process HNSW boundary',
      pg_temp.portal_versions_vector(1,(n::numeric/100000)::real),
      'ZX2K1'
    );
  end loop;
end $$;
analyze public.processes;
analyze private.portal_catalog_search_rows_v1;
analyze private.portal_catalog_facet_rows_v1;
update process_route_cases as route_case
set candidate_population = case
  when route_case.filters ? 'geography' then (
    select count(*)::integer
    from private.portal_catalog_facet_rows_v1 as facet
    where facet.dataset_kind = 'process'
      and facet.state_code in (100,200)
      and facet.facet_contract_version = 1
      and facet.facet_geography = route_case.filters ->> 'geography'
  )
  else null
end;
select pg_temp.measure_process_routes(
  array['boundary_2000','overflow_2001']
);

with samples as (
  select kind,revision,
    percentile_cont(0.5) within group(order by elapsed_ms) as p50,
    percentile_cont(0.95) within group(order by elapsed_ms) as p95,
    max(elapsed_ms) as maximum
  from comparison_samples group by kind,revision
), timing as (
  select kind,jsonb_object_agg(revision,jsonb_build_object(
    'p50Ms',round(p50::numeric,3),'p95Ms',round(p95::numeric,3),'maxMs',round(maximum::numeric,3))) as value
  from samples group by kind
), keys as (
  select kind,revision,item.ordinality as position,item.value -> 'key' as key
  from comparison_samples,jsonb_array_elements(payload -> 'items') with ordinality as item(value,ordinality)
  where sample=1
), ranking_overlap as (
  select kind,jsonb_object_agg('top' || n,
    (select count(*) from keys as a join keys as b on a.kind=b.kind and a.key=b.key
      where a.kind=kind_value.kind and a.revision='v1' and b.revision='v2' and a.position<=n and b.position<=n)) as value
  from unnest(array['process','flow']) as kind_value(kind) cross join unnest(array[10,20]) as n group by kind
), historical as (
  select kind,revision,count(*) as hits
  from keys where key ->> 'version'='01.00.000'
    and exists(select 1 from generate_series(1,20) as n where key ->> 'id'=md5(kind || ':comparison:' || n)::uuid::text)
  group by kind,revision
), plan_values as (
  select kind,shape,jsonb_build_object(
    'executorRole','portal_public_executor',
    'indexNames',jsonb_path_query_array(plan,'$.**."Index Name"'),
    'executionMs',plan #> '{0,Execution Time}',
    'sharedHitBlocks',plan #> '{0,Plan,Shared Hit Blocks}',
    'sharedReadBlocks',plan #> '{0,Plan,Shared Read Blocks}',
    'tempReadBlocks',plan #> '{0,Plan,Temp Read Blocks}',
    'tempWrittenBlocks',plan #> '{0,Plan,Temp Written Blocks}'
  ) as value
  from comparison_plans
), plan_summary as (
  select kind,jsonb_object_agg(shape,value) as value
  from plan_values
  group by kind
), route_timing as (
  select
    route_case.case_name,
    route_case.expected_route,
    route_case.candidate_population,
    max(route_sample.result_count) as result_count,
    case when count(distinct route_sample.observed_route)=1
      then min(route_sample.observed_route) else 'mixed' end as observed_route,
    max(route_sample.elapsed_ms) filter(where route_sample.sample=1) as first_ms,
    percentile_cont(0.5) within group(order by route_sample.elapsed_ms)
      filter(where route_sample.sample>1) as repeat_p50_ms,
    max(route_sample.elapsed_ms) filter(where route_sample.sample>1) as repeat_max_ms
  from process_route_cases as route_case
  join process_route_samples as route_sample using(case_name)
  group by route_case.case_name,route_case.expected_route,
    route_case.candidate_population
), route_summary as (
  select jsonb_object_agg(case_name,jsonb_build_object(
    'expectedRoute',expected_route,
    'observedRoute',observed_route,
    'candidatePopulation',candidate_population,
    'resultCount',case when expected_route='exact'
      then result_count else null end,
    'firstMs',round(first_ms::numeric,3),
    'repeatP50Ms',round(repeat_p50_ms::numeric,3),
    'repeatMaxMs',round(repeat_max_ms::numeric,3)
  )) as value
  from route_timing
)
select jsonb_build_object(
  'benchmark','hybrid-version-comparison.v1',
  'profile','isolated-synthetic','kind',timing.kind,
  'comparisonRowsPerKind',5020,
  'publicVersionRows',(select count(*) from private.portal_catalog_search_rows_v1 where dataset_kind=timing.kind),
  'samplesPerRevision',20,'timings',timing.value,'topOverlapCounts',ranking_overlap.value,
  'historicalTop20V1',coalesce((select hits from historical where historical.kind=timing.kind and revision='v1'),0),
  'historicalTop20V2',coalesce((select hits from historical where historical.kind=timing.kind and revision='v2'),0),
  'v2Plan',plan_summary.value -> 'broad',
  'v2UnfilteredPlan',plan_summary.value -> 'unfiltered',
  'adaptiveRouteTimings',case when timing.kind='process'
    then route_summary.value else null end
)
from timing
join ranking_overlap using(kind)
join plan_summary using(kind)
cross join route_summary;
rollback;
