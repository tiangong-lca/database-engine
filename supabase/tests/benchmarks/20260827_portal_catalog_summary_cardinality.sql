\set ON_ERROR_STOP on
\timing on

-- Required operator attestation:
--   -v benchmark_target=local
-- This profile inserts only rollback-scoped private Portal projection rows in
-- an isolated local Supabase project. Never run it against Preview, persistent
-- Dev, or production.
\if :{?benchmark_target}
\else
  \echo 'ERROR: pass -v benchmark_target=local'
  \quit 3
\endif

\if :{?benchmark_samples}
\else
  \set benchmark_samples 20
\endif

\if :{?process_rows}
\else
  \set process_rows 17299
\endif

\if :{?flow_rows}
\else
  \set flow_rows 108947
\endif

select
  :'benchmark_target' = 'local'
  and :'benchmark_samples'::integer = 20
  and :'process_rows'::integer = 17299
  and :'flow_rows'::integer = 108947
  as benchmark_profile_valid
\gset
\if :benchmark_profile_valid
\else
  \echo 'ERROR: use the exact 20-sample 17,299/108,947 release profile'
  \quit 3
\endif

select case
  when pg_catalog.inet_server_addr() is null then true
  when pg_catalog.family(pg_catalog.inet_server_addr()) = 4 then
    pg_catalog.inet_server_addr() << '127.0.0.0/8'::inet
  else pg_catalog.inet_server_addr() = '::1'::inet
end as benchmark_server_is_local
\gset
\if :benchmark_server_is_local
\else
  \echo 'ERROR: benchmark database address is not local'
  \quit 3
\endif

begin;
set local search_path = public, extensions, pg_temp;
set local statement_timeout = '15min';
set local lock_timeout = '5s';
set local jit = off;
set local work_mem = '32MB';
set local max_parallel_workers_per_gather = 0;
set local track_io_timing = on;
select pg_catalog.set_config(
  'application_name',
  'database-engine-533-portal-summary-local',
  true
);

do $empty_projection_guard$
begin
  if (select count(*) from private.portal_catalog_search_rows_v1) <> 0
     or (select count(*) from private.portal_catalog_facet_rows_v1) <> 0 then
    raise exception 'Portal summary benchmark requires an empty local projection'
      using errcode = '55000';
  end if;
end
$empty_projection_guard$;

grant api_internal_executor to postgres;
set local role api_internal_executor;

with generated as (
  select
    'process'::text as dataset_kind,
    ('53310000-0000-4000-8000-' ||
      pg_catalog.lpad(series.ordinal::text, 12, '0'))::uuid as id,
    series.ordinal,
    'Summary Process ' || series.ordinal::text as label
  from pg_catalog.generate_series(
    1, :'process_rows'::integer
  ) as series(ordinal)
  union all
  select
    'flow'::text,
    ('53320000-0000-4000-8000-' ||
      pg_catalog.lpad(series.ordinal::text, 12, '0'))::uuid,
    series.ordinal,
    'Summary Flow ' || series.ordinal::text
  from pg_catalog.generate_series(
    1, :'flow_rows'::integer
  ) as series(ordinal)
)
insert into private.portal_catalog_search_rows_v1 (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card,
  document,
  projection_contract_version
)
select
  generated.dataset_kind,
  generated.id,
  '01.00.000',
  case when generated.ordinal % 2 = 0 then 200 else 100 end,
  '2026-08-27 00:00:00+00'::timestamptz +
    generated.ordinal * interval '1 millisecond',
  pg_catalog.jsonb_build_object(
    'accessLevel', 'metadata_only',
    'capabilities', pg_catalog.jsonb_build_object(
      'metadataVisible', true,
      'exchangesVisible', false,
      'lciaVisible', false,
      'publicArtifactVisible', false,
      'citationVisible', true,
      'policyVersion', 'portal-capability-policy.v1',
      'reasonCodes', pg_catalog.jsonb_build_array('metadata_visible')
    ),
    'names', pg_catalog.jsonb_build_array(
      pg_catalog.jsonb_build_object(
        'language', 'en',
        'value', generated.label
      )
    ),
    'summary', '[]'::jsonb,
    'geography', pg_catalog.jsonb_build_object(
      'code', null,
      'label', '[]'::jsonb,
      'precision', 'unknown'
    ),
    'referenceYear', null,
    'processSubtype', null,
    'source', 'summary benchmark',
    'classifications', '[]'::jsonb,
    'casNumber', null,
    'document', pg_catalog.lower(generated.label)
  ),
  pg_catalog.lower(generated.label),
  1
from generated;

reset role;

analyze private.portal_catalog_search_rows_v1;
analyze private.portal_catalog_facet_rows_v1;

select
  (
    select count(*)
    from private.portal_catalog_search_rows_v1
  ) = :'process_rows'::integer + :'flow_rows'::integer
  and (
    select count(*)
    from private.portal_catalog_facet_rows_v1
  ) = :'process_rows'::integer + :'flow_rows'::integer
  as benchmark_cardinality_ok
\gset
\if :benchmark_cardinality_ok
\else
  \echo 'ERROR: Portal summary benchmark projection cardinality mismatch'
  \quit 3
\endif

create temporary table portal_summary_writer_probe (
  dataset_kind text not null,
  id uuid not null,
  version text not null,
  state_code integer not null,
  modified_at timestamptz not null,
  card jsonb not null,
  primary key (dataset_kind, id, version)
) on commit drop;

insert into portal_summary_writer_probe (
  dataset_kind,
  id,
  version,
  state_code,
  modified_at,
  card
)
select projection.dataset_kind,
  projection.id,
  projection.version,
  projection.state_code,
  projection.modified_at,
  case projection.dataset_kind
    when 'flow' then pg_catalog.jsonb_set(
      projection.card,
      '{casNumber}',
      '"50-00-0"'::jsonb,
      false
    )
    else pg_catalog.jsonb_set(
      projection.card,
      '{classifications}',
      '[{"system":"benchmark","code":"SUMMARY-WRITER","label":[{"language":"en","value":"Writer classification"}]}]'::jsonb,
      false
    )
  end
from private.portal_catalog_search_rows_v1 as projection;

create temporary table portal_summary_writer_timings (
  profile text not null,
  sample integer not null,
  elapsed_ms numeric not null,
  primary key (profile, sample)
) on commit drop;

create temporary table portal_summary_index_build (
  profile text primary key,
  elapsed_ms numeric not null,
  index_bytes bigint not null
) on commit drop;

create function pg_temp.measure_portal_summary_writes(
  p_profile text,
  p_samples integer
)
returns void
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_sample integer;
  v_started_at timestamptz;
  v_flow_id uuid;
  v_process_id uuid;
begin
  for v_sample in 1..p_samples loop
    v_flow_id := ('53320000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_process_id := ('53310000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_started_at := pg_catalog.clock_timestamp();
    update pg_temp.portal_summary_writer_probe
    set card = pg_catalog.jsonb_set(
      card,
      '{casNumber}',
      '"50-00-0"'::jsonb,
      false
    )
    where dataset_kind = 'flow'
      and id = v_flow_id;
    update pg_temp.portal_summary_writer_probe
    set card = pg_catalog.jsonb_set(
      card,
      '{casNumber}',
      'null'::jsonb,
      false
    )
    where dataset_kind = 'flow'
      and id = v_flow_id;
    update pg_temp.portal_summary_writer_probe
    set card = pg_catalog.jsonb_set(
      card,
      '{classifications}',
      '[{"system":"benchmark","code":"SUMMARY-WRITER","label":[{"language":"en","value":"Writer classification"}]}]'::jsonb,
      false
    )
    where dataset_kind = 'process'
      and id = v_process_id;
    update pg_temp.portal_summary_writer_probe
    set card = pg_catalog.jsonb_set(
      card,
      '{classifications}',
      '[]'::jsonb,
      false
    )
    where dataset_kind = 'process'
      and id = v_process_id;
    insert into pg_temp.portal_summary_writer_timings (
      profile,
      sample,
      elapsed_ms
    ) values (
      p_profile,
      v_sample,
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000
    );
  end loop;
end
$function$;

grant portal_public_executor to postgres;

select pg_temp.measure_portal_summary_writes(
  'baseline-without-index', 50
);

do $build_portal_catalog_summary_eligibility_index$
declare
  v_started_at timestamptz := pg_catalog.clock_timestamp();
begin
  create index portal_catalog_summary_eligibility_writer_probe_idx
  on pg_temp.portal_summary_writer_probe (
    dataset_kind,
    id,
    version desc,
    modified_at desc,
    state_code desc
  )
  where (
      dataset_kind = 'flow'
      and pg_catalog.jsonb_typeof(card -> 'casNumber') = 'string'
      and card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
    ) or (
      pg_catalog.jsonb_typeof(card -> 'classifications') = 'array'
      and pg_catalog.jsonb_array_length(card -> 'classifications') > 0
    );

  insert into pg_temp.portal_summary_index_build (
    profile,
    elapsed_ms,
    index_bytes
  ) values (
    'combined-eligibility-index',
    extract(epoch from pg_catalog.clock_timestamp() - v_started_at) * 1000,
    pg_catalog.pg_relation_size(
      'pg_temp.portal_catalog_summary_eligibility_writer_probe_idx'
    )
  );
end
$build_portal_catalog_summary_eligibility_index$;

analyze pg_temp.portal_summary_writer_probe;

select pg_temp.measure_portal_summary_writes(
  'combined-eligibility-index', 50
);

do $build_portal_catalog_flow_cas_index$
declare
  v_started_at timestamptz := pg_catalog.clock_timestamp();
begin
  create index portal_catalog_search_flow_cas_writer_probe_idx
  on pg_temp.portal_summary_writer_probe (
    ((card ->> 'casNumber')),
    id,
    version desc,
    modified_at desc,
    state_code desc
  )
  where dataset_kind = 'flow'
    and pg_catalog.jsonb_typeof(card -> 'casNumber') = 'string'
    and card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
    and pg_catalog.length(card ->> 'casNumber') between 7 and 12;

  insert into pg_temp.portal_summary_index_build (
    profile,
    elapsed_ms,
    index_bytes
  ) values (
    'flow-cas-index',
    extract(epoch from pg_catalog.clock_timestamp() - v_started_at) * 1000,
    pg_catalog.pg_relation_size(
      'pg_temp.portal_catalog_search_flow_cas_writer_probe_idx'
    )
  );
end
$build_portal_catalog_flow_cas_index$;

analyze pg_temp.portal_summary_writer_probe;

select pg_temp.measure_portal_summary_writes(
  'combined-plus-flow-cas-index', 50
);

select writer.profile,
  pg_catalog.round(pg_catalog.min(writer.elapsed_ms), 3) as minimum_ms,
  pg_catalog.round(pg_catalog.avg(writer.elapsed_ms), 3) as average_ms,
  pg_catalog.round(
    (
      pg_catalog.percentile_cont(0.95) within group (
        order by writer.elapsed_ms
      )
    )::numeric,
    3
  ) as p95_ms,
  pg_catalog.round(pg_catalog.max(writer.elapsed_ms), 3) as maximum_ms
from portal_summary_writer_timings as writer
group by writer.profile
order by writer.profile;

select index_build.profile,
  pg_catalog.round(index_build.elapsed_ms, 3) as build_ms,
  index_build.index_bytes
from portal_summary_index_build as index_build
order by index_build.profile;

do $portal_summary_writer_performance_guard$
declare
  v_baseline_p95 numeric;
  v_indexed_p95 numeric;
  v_cas_indexed_p95 numeric;
  v_max_build_ms numeric;
  v_total_index_bytes bigint;
begin
  select pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  into v_baseline_p95
  from portal_summary_writer_timings as writer
  where writer.profile = 'baseline-without-index';

  select pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  into v_indexed_p95
  from portal_summary_writer_timings as writer
  where writer.profile = 'combined-eligibility-index';

  select pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  into v_cas_indexed_p95
  from portal_summary_writer_timings as writer
  where writer.profile = 'combined-plus-flow-cas-index';

  select pg_catalog.max(index_build.elapsed_ms),
    pg_catalog.sum(index_build.index_bytes)
  into v_max_build_ms,
    v_total_index_bytes
  from portal_summary_index_build as index_build;

  if v_baseline_p95 is null
     or v_indexed_p95 is null
     or v_cas_indexed_p95 is null
     or v_max_build_ms is null
     or v_total_index_bytes is null
     or v_indexed_p95 > greatest(5::numeric, v_baseline_p95 * 3)
     or v_cas_indexed_p95 > greatest(5::numeric, v_baseline_p95 * 3)
     or v_max_build_ms > 5000
     or v_total_index_bytes > 64 * 1024 * 1024 then
    raise exception 'Portal summary index writer/build budget failed'
      using errcode = '54000';
  end if;
end
$portal_summary_writer_performance_guard$;

create temporary table portal_summary_timings (
  profile text not null,
  sample integer not null,
  elapsed_ms numeric not null,
  response_bytes integer not null,
  example_count integer not null,
  primary key (profile, sample)
) on commit drop;

create temporary table portal_summary_example_timings (
  profile text not null,
  sample integer not null,
  query_kind text not null,
  dataset_kind text not null,
  elapsed_ms numeric not null,
  item_count integer not null,
  primary key (profile, query_kind, sample)
) on commit drop;

create function pg_temp.measure_portal_summary(
  p_profile text,
  p_samples integer
)
returns void
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_sample integer;
  v_started_at timestamptz;
  v_payload jsonb;
begin
  perform api.portal_catalog_summary_v1();
  for v_sample in 1..p_samples loop
    v_started_at := pg_catalog.clock_timestamp();
    v_payload := api.portal_catalog_summary_v1();
    insert into pg_temp.portal_summary_timings (
      profile,
      sample,
      elapsed_ms,
      response_bytes,
      example_count
    ) values (
      p_profile,
      v_sample,
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000,
      pg_catalog.octet_length(v_payload::text),
      pg_catalog.jsonb_array_length(v_payload -> 'examples')
    );
  end loop;
end
$function$;

create function pg_temp.measure_portal_summary_classification_example(
  p_profile text,
  p_samples integer
)
returns void
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_sample integer;
  v_started_at timestamptz;
  v_summary jsonb;
  v_example jsonb;
  v_page jsonb;
begin
  v_summary := api.portal_catalog_summary_v1();
  select example.value
  into v_example
  from pg_catalog.jsonb_array_elements(v_summary -> 'examples') as example(value)
  where example.value ->> 'queryKind' = 'classification';
  if v_example is null
     or pg_catalog.length(v_example ->> 'query') < 4 then
    raise exception 'Portal summary classification benchmark example is missing or broad'
      using errcode = '54000';
  end if;

  for v_sample in 1..p_samples loop
    v_started_at := pg_catalog.clock_timestamp();
    if v_example ->> 'datasetKind' = 'process' then
      v_page := private.portal_search_v1(
        'process', v_example ->> 'query', '{}'::jsonb, 'relevance', null, 50
      );
    else
      v_page := private.portal_search_v1(
        'flow', v_example ->> 'query', '{}'::jsonb, 'relevance', null, 50
      );
    end if;
    insert into pg_temp.portal_summary_example_timings (
      profile,
      sample,
      query_kind,
      dataset_kind,
      elapsed_ms,
      item_count
    ) values (
      p_profile,
      v_sample,
      'classification',
      v_example ->> 'datasetKind',
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000,
      pg_catalog.jsonb_array_length(v_page -> 'items')
    );
  end loop;
end
$function$;

create function pg_temp.measure_portal_summary_cas_example(
  p_profile text,
  p_samples integer
)
returns void
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_sample integer;
  v_started_at timestamptz;
  v_example jsonb;
  v_page jsonb;
begin
  select example.value
  into v_example
  from pg_catalog.jsonb_array_elements(
    api.portal_catalog_summary_v1() -> 'examples'
  ) as example(value)
  where example.value ->> 'queryKind' = 'cas';
  if v_example is null
     or v_example ->> 'datasetKind' <> 'flow'
     or not private.portal_catalog_summary_valid_cas_v1(
       v_example ->> 'query'
     ) then
    raise exception 'Portal summary CAS benchmark example is missing or invalid'
      using errcode = '54000';
  end if;

  for v_sample in 1..p_samples loop
    v_started_at := pg_catalog.clock_timestamp();
    v_page := private.portal_search_v1(
      'flow', v_example ->> 'query', '{}'::jsonb, 'relevance', null, 50
    );
    insert into pg_temp.portal_summary_example_timings (
      profile,
      sample,
      query_kind,
      dataset_kind,
      elapsed_ms,
      item_count
    ) values (
      p_profile,
      v_sample,
      'cas',
      'flow',
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000,
      pg_catalog.jsonb_array_length(v_page -> 'items')
    );
  end loop;
end
$function$;

grant execute on function pg_temp.measure_portal_summary_classification_example(
  text,
  integer
) to portal_public_executor;
grant execute on function pg_temp.measure_portal_summary_cas_example(
  text,
  integer
) to portal_public_executor;
grant insert, select on table pg_temp.portal_summary_example_timings
to portal_public_executor;

select pg_temp.measure_portal_summary(
  'no-cas-classification-evidence', :'benchmark_samples'::integer
);

set local role api_internal_executor;
update private.portal_catalog_search_rows_v1
set card = pg_catalog.jsonb_set(
  card,
  '{casNumber}',
  '"50-00-0"'::jsonb,
  false
)
where dataset_kind = 'flow'
  and id = (
    select id
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'flow'
    order by id desc
    limit 1
  );

update private.portal_catalog_search_rows_v1
set document = document || E'\nsummary-tail',
  card = pg_catalog.jsonb_set(
    pg_catalog.jsonb_set(
      card,
      '{classifications}',
      '[{"system":"benchmark","code":"SUMMARY-TAIL","label":[{"language":"en","value":"Tail classification"}]}]'::jsonb,
      false
    ),
    '{document}',
    pg_catalog.to_jsonb(document || E'\nsummary-tail'),
    false
  )
where dataset_kind = 'process'
  and id = (
    select id
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
    order by id desc
    limit 1
  );
reset role;

select pg_temp.measure_portal_summary(
  'tail-evidence', :'benchmark_samples'::integer
);
set local role portal_public_executor;
select pg_temp.measure_portal_summary_classification_example(
  'tail-evidence', :'benchmark_samples'::integer
);
select pg_temp.measure_portal_summary_cas_example(
  'tail-evidence', :'benchmark_samples'::integer
);
reset role;

set local role api_internal_executor;
update private.portal_catalog_search_rows_v1
set card = pg_catalog.jsonb_set(
  card,
  '{casNumber}',
  '"50-00-0"'::jsonb,
  false
)
where dataset_kind = 'flow'
  and id = (
    select id
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'flow'
    order by id
    limit 1
  );

update private.portal_catalog_search_rows_v1
set document = document || E'\nsummary-normal',
  card = pg_catalog.jsonb_set(
    pg_catalog.jsonb_set(
      card,
      '{classifications}',
      '[{"system":"benchmark","code":"SUMMARY-NORMAL","label":[{"language":"en","value":"Normal classification"}]}]'::jsonb,
      false
    ),
    '{document}',
    pg_catalog.to_jsonb(document || E'\nsummary-normal'),
    false
  )
where dataset_kind = 'process'
  and id = (
    select id
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
    order by id
    limit 1
  );
reset role;

select pg_temp.measure_portal_summary(
  'normal', :'benchmark_samples'::integer
);
set local role portal_public_executor;
select pg_temp.measure_portal_summary_classification_example(
  'normal', :'benchmark_samples'::integer
);
select pg_temp.measure_portal_summary_cas_example(
  'normal', :'benchmark_samples'::integer
);
reset role;

select
  profile,
  pg_catalog.round(pg_catalog.min(elapsed_ms), 3) as minimum_ms,
  pg_catalog.round(pg_catalog.avg(elapsed_ms), 3) as average_ms,
  pg_catalog.round(
    (
      pg_catalog.percentile_cont(0.95) within group (order by elapsed_ms)
    )::numeric,
    3
  ) as p95_ms,
  pg_catalog.round(pg_catalog.max(elapsed_ms), 3) as maximum_ms,
  pg_catalog.max(response_bytes) as maximum_response_bytes,
  pg_catalog.min(example_count) as minimum_examples,
  pg_catalog.max(example_count) as maximum_examples
from portal_summary_timings
group by profile
order by profile;

do $portal_summary_performance_guard$
declare
  v_profile record;
begin
  for v_profile in
    select profile,
      pg_catalog.percentile_cont(0.95) within group (
        order by elapsed_ms
      ) as p95_ms,
      pg_catalog.max(response_bytes) as maximum_response_bytes,
      pg_catalog.min(example_count) as minimum_examples,
      pg_catalog.max(example_count) as maximum_examples
    from portal_summary_timings
    group by profile
  loop
    if v_profile.p95_ms > 250
       or v_profile.maximum_response_bytes > 16384
       or v_profile.minimum_examples <> v_profile.maximum_examples
       or (
         v_profile.profile = 'no-cas-classification-evidence'
         and v_profile.minimum_examples <> 1
       )
       or (
         v_profile.profile <> 'no-cas-classification-evidence'
         and v_profile.minimum_examples <> 3
       ) then
      raise exception 'Portal summary benchmark failed for %', v_profile.profile
        using errcode = '54000';
    end if;
  end loop;
end
$portal_summary_performance_guard$;

select
  profile,
  query_kind,
  pg_catalog.min(dataset_kind) as dataset_kind,
  pg_catalog.round(
    (
      pg_catalog.percentile_cont(0.95) within group (order by elapsed_ms)
    )::numeric,
    3
  ) as p95_ms,
  pg_catalog.round(pg_catalog.max(elapsed_ms), 3) as maximum_ms,
  pg_catalog.min(item_count) as minimum_items,
  pg_catalog.max(item_count) as maximum_items
from portal_summary_example_timings
group by profile, query_kind
order by profile, query_kind;

do $portal_summary_example_performance_guard$
declare
  v_profile record;
begin
  for v_profile in
    select profile,
      query_kind,
      pg_catalog.min(dataset_kind) as minimum_kind,
      pg_catalog.max(dataset_kind) as maximum_kind,
      pg_catalog.percentile_cont(0.95) within group (
        order by elapsed_ms
      ) as p95_ms,
      pg_catalog.max(elapsed_ms) as maximum_ms,
      pg_catalog.min(item_count) as minimum_items
    from portal_summary_example_timings
    group by profile, query_kind
  loop
    if (
         v_profile.query_kind = 'classification'
         and (
           v_profile.minimum_kind <> 'process'
           or v_profile.maximum_kind <> 'process'
         )
       )
       or (
         v_profile.query_kind = 'cas'
         and (
           v_profile.minimum_kind <> 'flow'
           or v_profile.maximum_kind <> 'flow'
         )
       )
       or v_profile.query_kind not in ('classification', 'cas')
       or v_profile.p95_ms > 2000
       or v_profile.maximum_ms > 8000
       or v_profile.minimum_items < 1 then
      raise exception 'Portal summary example benchmark failed for %', v_profile.profile
        using errcode = '54000';
    end if;
  end loop;
end
$portal_summary_example_performance_guard$;

explain (analyze, buffers, settings, format text)
select distinct on (facet.dataset_kind, facet.id)
  facet.dataset_kind,
  facet.id,
  facet.version,
  facet.modified_at
from private.portal_catalog_facet_rows_v1 as facet
where facet.facet_contract_version = 1
order by facet.dataset_kind,
  facet.id,
  facet.version desc,
  facet.modified_at desc,
  facet.state_code desc;

grant portal_public_executor to postgres;
set local role portal_public_executor;

explain (analyze, buffers, settings, format text)
with latest as materialized (
  select distinct on (facet.dataset_kind, facet.id)
    facet.dataset_kind,
    facet.id,
    facet.version,
    facet.modified_at,
    facet.state_code
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.facet_contract_version = 1
  order by facet.dataset_kind,
    facet.id,
    facet.version desc,
    facet.modified_at desc,
    facet.state_code desc
)
select candidate.id,
  candidate.version,
  candidate.card ->> 'casNumber' as cas_number
from private.portal_catalog_search_rows_v1 as candidate
join latest
  on latest.dataset_kind = candidate.dataset_kind
 and latest.id = candidate.id
 and latest.version = candidate.version
where candidate.dataset_kind = 'flow'
  and pg_catalog.jsonb_typeof(candidate.card -> 'casNumber') = 'string'
  and candidate.card ->> 'casNumber' ~ '^[0-9]{2,7}-[0-9]{2}-[0-9]$'
  and private.portal_catalog_summary_valid_cas_v1(
    candidate.card ->> 'casNumber'
  )
order by candidate.id,
  candidate.version desc,
  candidate.modified_at desc,
  candidate.state_code desc
limit 1;

explain (analyze, buffers, settings, format text)
with latest as materialized (
  select distinct on (facet.dataset_kind, facet.id)
    facet.dataset_kind,
    facet.id,
    facet.version,
    facet.modified_at,
    facet.state_code
  from private.portal_catalog_facet_rows_v1 as facet
  where facet.facet_contract_version = 1
  order by facet.dataset_kind,
    facet.id,
    facet.version desc,
    facet.modified_at desc,
    facet.state_code desc
)
select candidate.dataset_kind,
  candidate.id,
  candidate.version,
  pg_catalog.btrim(classification.value ->> 'code') as classification_code
from private.portal_catalog_search_rows_v1 as candidate
join latest
  on latest.dataset_kind = candidate.dataset_kind
 and latest.id = candidate.id
 and latest.version = candidate.version
cross join lateral pg_catalog.jsonb_array_elements(
  candidate.card -> 'classifications'
) with ordinality as classification(value, ordinality)
where candidate.dataset_kind = 'process'
  and pg_catalog.jsonb_typeof(candidate.card -> 'classifications') = 'array'
  and pg_catalog.jsonb_array_length(candidate.card -> 'classifications') > 0
  and pg_catalog.jsonb_typeof(classification.value) = 'object'
  and pg_catalog.jsonb_typeof(classification.value -> 'code') = 'string'
  and nullif(pg_catalog.btrim(classification.value ->> 'code'), '') is not null
order by candidate.id,
  candidate.version desc,
  candidate.modified_at desc,
  candidate.state_code desc,
  classification.ordinality,
  pg_catalog.btrim(classification.value ->> 'code') collate pg_catalog."C"
limit 1;

explain (analyze, buffers, settings, format text)
select api.portal_catalog_summary_v1();

set local role portal_public_executor;
explain (analyze, buffers, settings, format text)
select private.portal_search_v1(
  'process',
  (
    select example.value ->> 'query'
    from pg_catalog.jsonb_array_elements(
      api.portal_catalog_summary_v1() -> 'examples'
    ) as example(value)
    where example.value ->> 'queryKind' = 'classification'
  ),
  '{}'::jsonb,
  'relevance',
  null,
  50
);

explain (analyze, buffers, settings, format text)
select private.portal_search_v1(
  'flow',
  (
    select example.value ->> 'query'
    from pg_catalog.jsonb_array_elements(
      api.portal_catalog_summary_v1() -> 'examples'
    ) as example(value)
    where example.value ->> 'queryKind' = 'cas'
  ),
  '{}'::jsonb,
  'relevance',
  null,
  50
);
reset role;

rollback;
