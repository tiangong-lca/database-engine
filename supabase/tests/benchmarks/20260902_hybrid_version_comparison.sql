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
create function pg_temp.version_hnsw_plan(p_kind text) returns jsonb
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
            and private.portal_card_matches_filters_v2(projection.card,'{"geography":"zzb"}')
          offset 0
        )
      order by source.embedding_ft operator(extensions.<=>) $1 limit 200
    )
    select * from nearest where distance>=0 and distance<=0.5
    order by distance+0::double precision,id,version desc
  $query$,v_table) into v_plan using pg_temp.portal_versions_vector(1,0),p_kind;
  return v_plan;
end $$;
grant execute on function pg_temp.version_hnsw_plan(text)
  to api_internal_executor,portal_public_executor;
grant execute on function pg_temp.portal_versions_vector(real,real)
  to api_internal_executor,portal_public_executor;
grant execute on function pg_temp.portal_versions_vector_text(real,real)
  to api_internal_executor,portal_public_executor;
create temporary table comparison_plans(kind text,plan jsonb);
grant insert on comparison_plans to api_internal_executor,portal_public_executor;
grant api_internal_executor,portal_public_executor to postgres;
set local role api_internal_executor;
set local plan_cache_mode='force_custom_plan';
set local hnsw.iterative_scan='strict_order';
set local hnsw.ef_search=200;
set local hnsw.max_scan_tuples=20000;
set local hnsw.scan_mem_multiplier=2;
set local jit=off;
insert into comparison_plans values('process',pg_temp.version_hnsw_plan('process'));
reset role;
set local role portal_public_executor;
set local plan_cache_mode='force_custom_plan';
set local hnsw.iterative_scan='strict_order';
set local hnsw.ef_search=200;
set local hnsw.max_scan_tuples=20000;
set local hnsw.scan_mem_multiplier=2;
set local jit=off;
insert into comparison_plans values('flow',pg_temp.version_hnsw_plan('flow'));
reset role;

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
)
select jsonb_build_object(
  'benchmark','hybrid-version-comparison.v1',
  'profile','isolated-synthetic','kind',timing.kind,
  'comparisonRowsPerKind',5020,
  'publicVersionRows',(select count(*) from private.portal_catalog_search_rows_v1 where dataset_kind=timing.kind),
  'samplesPerRevision',20,'timings',timing.value,'topOverlapCounts',ranking_overlap.value,
  'historicalTop20V1',coalesce((select hits from historical where historical.kind=timing.kind and revision='v1'),0),
  'historicalTop20V2',coalesce((select hits from historical where historical.kind=timing.kind and revision='v2'),0),
  'v2Plan',jsonb_build_object(
    'executorRole',case timing.kind
      when 'flow' then 'portal_public_executor'
      else 'api_internal_executor'
    end,
    'indexNames',jsonb_path_query_array(comparison_plans.plan,'$.**."Index Name"'),
    'executionMs',comparison_plans.plan #> '{0,Execution Time}',
    'sharedHitBlocks',comparison_plans.plan #> '{0,Plan,Shared Hit Blocks}',
    'sharedReadBlocks',comparison_plans.plan #> '{0,Plan,Shared Read Blocks}',
    'tempReadBlocks',comparison_plans.plan #> '{0,Plan,Temp Read Blocks}',
    'tempWrittenBlocks',comparison_plans.plan #> '{0,Plan,Temp Written Blocks}'
  )
)
from timing join ranking_overlap using(kind) join comparison_plans using(kind);
rollback;
