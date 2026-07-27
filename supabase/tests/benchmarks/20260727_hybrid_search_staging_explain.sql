\set ON_ERROR_STOP on
\timing on

-- Required operator attestation:
--   -v benchmark_target=staging
--   -v expected_project_ref=fotofiyqnuyvgtotswie
--   -v explain_output=/absolute/private/path/hybrid-search-explain.log
-- This profile is read-only. Never point it at production.
\if :{?benchmark_target}
\else
  \echo 'ERROR: pass -v benchmark_target=staging'
  \quit 3
\endif

\if :{?expected_project_ref}
\else
  \echo 'ERROR: pass -v expected_project_ref=fotofiyqnuyvgtotswie'
  \quit 3
\endif

\if :{?explain_output}
\else
  \echo 'ERROR: pass a private absolute path with -v explain_output=...'
  \quit 3
\endif

select
  :'benchmark_target' = 'staging'
  and :'expected_project_ref' = 'fotofiyqnuyvgtotswie'
  as benchmark_attestation_ok
\gset

\if :benchmark_attestation_ok
\else
  \echo 'ERROR: this profile is pinned to staging ref fotofiyqnuyvgtotswie'
  \quit 3
\endif

begin;
set transaction read only;
set local search_path = public, extensions, pg_temp;

select set_config(
  'application_name',
  'database-engine-292-hybrid-search-staging-explain',
  true
);

set local statement_timeout = '60s';
set local jit = off;
set local track_io_timing = on;
set local hnsw.iterative_scan = 'strict_order';

select
  to_regclass('public.processes_embedding_ft_tg_hnsw_idx') is not null
  and (
    select routine.proconfig @> array[
      'plan_cache_mode=force_custom_plan',
      'hnsw.iterative_scan=strict_order'
    ]
    from pg_proc routine
    where routine.oid =
      'private.semantic_process_candidates(text,text,double precision,integer,text)'::regprocedure
  )
  and (
    select routine.proconfig @> array[
      'plan_cache_mode=force_custom_plan',
      'hnsw.iterative_scan=strict_order'
    ]
    from pg_proc routine
    where routine.oid =
      'private.semantic_flow_candidates(text,text,double precision,integer,text)'::regprocedure
  ) as optimized_migration_installed
\gset

\if :optimized_migration_installed
\else
  \echo 'ERROR: issue #292 migration is not installed on this target'
  \quit 4
\endif

with process_sample as (
  select id, embedding_ft, extracted_text
  from public.processes
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
  order by id
  limit 1
), flow_sample as (
  select id, embedding_ft, extracted_text
  from public.flows
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
    and json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'
      ->>'typeOfDataSet' = 'Product flow'
  order by id
  limit 1
)
select jsonb_build_object(
  'project_ref', :'expected_project_ref',
  'postgres_version', current_setting('server_version'),
  'pgvector_version', (
    select extension.extversion
    from pg_extension extension
    where extension.extname = 'vector'
  ),
  'parameters', jsonb_build_object(
    'data_source', 'tg',
    'match_threshold', 0.5,
    'match_count', 20,
    'weights', jsonb_build_array(0.3, 0.2, 0.5),
    'rrf_k', 10,
    'page_size', 10,
    'page_current', 1
  ),
  'process_sample_sha256', (
    select encode(digest(id::text, 'sha256'), 'hex')
    from process_sample
  ),
  'flow_sample_sha256', (
    select encode(digest(id::text, 'sha256'), 'hex')
    from flow_sample
  ),
  'vector_dimensions', jsonb_build_object(
    'processes', (select vector_dims(embedding_ft) from process_sample),
    'flows', (select vector_dims(embedding_ft) from flow_sample)
  )
) as redacted_benchmark_metadata;

\pset format unaligned
\pset tuples_only on
\o :explain_output

\qecho profile=process-semantic-tg-empty-json
explain (analyze, buffers, settings, wal, summary, format json)
with sample as materialized (
  select embedding_ft
  from public.processes
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
  order by id
  limit 1
), candidates as materialized (
  select
    process.id as candidate_id,
    process.embedding_ft <=> (
      select embedding_ft from sample
    ) as candidate_distance
  from public.processes process
  where process.embedding_ft is not null
    and process.state_code = 100
  order by process.embedding_ft <=> (
    select embedding_ft from sample
  )
  limit 200
)
select
  rank() over (order by candidate_distance)::bigint,
  candidate_id,
  candidate_distance
from candidates
where candidate_distance < 0.5
order by candidate_distance
limit 20;

\qecho profile=flow-semantic-tg-empty-json
explain (analyze, buffers, settings, wal, summary, format json)
with sample as materialized (
  select embedding_ft
  from public.flows
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
    and json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'
      ->>'typeOfDataSet' = 'Product flow'
  order by id
  limit 1
), candidates as materialized (
  select
    flow.id as candidate_id,
    flow.embedding_ft <=> (
      select embedding_ft from sample
    ) as candidate_distance
  from public.flows flow
  where flow.embedding_ft is not null
    and flow.state_code = 100
  order by flow.embedding_ft <=> (
    select embedding_ft from sample
  )
  limit 200
)
select
  rank() over (order by candidate_distance)::bigint,
  candidate_id,
  candidate_distance
from candidates
where candidate_distance < 0.5
order by candidate_distance
limit 20;

\qecho profile=flow-semantic-tg-product-filter
explain (analyze, buffers, settings, wal, summary, format json)
with sample as materialized (
  select embedding_ft
  from public.flows
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
    and json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'
      ->>'typeOfDataSet' = 'Product flow'
  order by id
  limit 1
), candidates as materialized (
  select
    flow.id as candidate_id,
    flow.embedding_ft <=> (
      select embedding_ft from sample
    ) as candidate_distance
  from public.flows flow
  where flow.embedding_ft is not null
    and flow.state_code = 100
    and flow.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'
      ->>'typeOfDataSet' = 'Product flow'
  order by flow.embedding_ft <=> (
    select embedding_ft from sample
  )
  limit 200
)
select
  rank() over (order by candidate_distance)::bigint,
  candidate_id,
  candidate_distance
from candidates
where candidate_distance < 0.5
order by candidate_distance
limit 20;

\qecho profile=process-hybrid-rpc-defaults
explain (analyze, buffers, settings, wal, summary, format json)
with sample as materialized (
  select
    array_to_string(
      (regexp_split_to_array(btrim(extracted_text), '[[:space:]]+'))[1:3],
      ' '
    ) as query_text,
    (regexp_split_to_array(
      btrim(extracted_text),
      '[[:space:]]+'
    ))[1:3] as query_terms,
    embedding_ft::text as query_embedding
  from public.processes
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
  order by id
  limit 1
)
select hybrid_result.*
from sample
cross join lateral public.hybrid_search_processes(
  sample.query_text,
  sample.query_embedding,
  '{}',
  0.5,
  20,
  0.3,
  0.2,
  0.5,
  10,
  'tg',
  10,
  1,
  sample.query_terms
) hybrid_result;

\qecho profile=flow-hybrid-rpc-product-filter
explain (analyze, buffers, settings, wal, summary, format json)
with sample as materialized (
  select
    array_to_string(
      (regexp_split_to_array(btrim(extracted_text), '[[:space:]]+'))[1:3],
      ' '
    ) as query_text,
    (regexp_split_to_array(
      btrim(extracted_text),
      '[[:space:]]+'
    ))[1:3] as query_terms,
    embedding_ft::text as query_embedding
  from public.flows
  where state_code = 100
    and embedding_ft is not null
    and nullif(btrim(extracted_text), '') is not null
    and json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'
      ->>'typeOfDataSet' = 'Product flow'
  order by id
  limit 1
)
select hybrid_result.*
from sample
cross join lateral public.hybrid_search_flows(
  sample.query_text,
  sample.query_embedding,
  '{"flowType":"Product flow"}',
  0.5,
  20,
  0.3,
  0.2,
  0.5,
  10,
  'tg',
  10,
  1,
  sample.query_terms
) hybrid_result;

\o
\pset format aligned
\pset tuples_only off

select :'explain_output' as explain_output;

rollback;
