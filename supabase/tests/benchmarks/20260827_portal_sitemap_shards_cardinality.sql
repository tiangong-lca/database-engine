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
  'database-engine-539-portal-sitemap-local',
  true
);

do $empty_projection_guard$
begin
  if (select count(*) from private.portal_catalog_search_rows_v1) <> 0
     or (select count(*) from private.portal_catalog_facet_rows_v1) <> 0
     or (select count(*) from private.portal_sitemap_rows_v1) <> 0 then
    raise exception
      'Portal sitemap benchmark requires an empty local projection'
      using errcode = '55000';
  end if;
end
$empty_projection_guard$;

grant api_internal_executor to postgres;
set local role api_internal_executor;

with generated as (
  select
    'process'::text as dataset_kind,
    ('53910000-0000-4000-8000-' ||
      pg_catalog.lpad(series.ordinal::text, 12, '0'))::uuid as id,
    series.ordinal,
    'Sitemap Process ' || series.ordinal::text as label
  from pg_catalog.generate_series(
    1, :'process_rows'::integer
  ) as series(ordinal)
  union all
  select
    'flow'::text,
    ('53920000-0000-4000-8000-' ||
      pg_catalog.lpad(series.ordinal::text, 12, '0'))::uuid,
    series.ordinal,
    'Sitemap Flow ' || series.ordinal::text
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
    'source', 'sitemap benchmark',
    'classifications', '[]'::jsonb,
    'casNumber', null,
    'document', pg_catalog.lower(generated.label)
  ),
  pg_catalog.lower(generated.label),
  1
from generated;

reset role;
revoke api_internal_executor from postgres;

analyze private.portal_catalog_search_rows_v1;
analyze private.portal_catalog_facet_rows_v1;
analyze private.portal_sitemap_rows_v1;

select
  (
    select count(*)
    from private.portal_catalog_search_rows_v1
  ) = :'process_rows'::integer + :'flow_rows'::integer
  and (
    select count(*)
    from private.portal_sitemap_rows_v1
  ) = :'process_rows'::integer + :'flow_rows'::integer
  and (
    select count(*)
    from private.portal_sitemap_rows_v1
    where dataset_kind = 'process'
  ) = :'process_rows'::integer
  and (
    select count(*)
    from private.portal_sitemap_rows_v1
    where dataset_kind = 'flow'
  ) = :'flow_rows'::integer
  as benchmark_cardinality_ok
\gset
\if :benchmark_cardinality_ok
\else
  \echo 'ERROR: Portal sitemap benchmark projection cardinality mismatch'
  \quit 3
\endif

-- Measure the real incremental exact-version sync trigger on the existing projection
-- writer path. Every sample restores the original timestamp, so disabling the
-- new trigger for the baseline leaves no projection drift.
create temporary table portal_sitemap_trigger_writer_timings (
  profile text not null,
  sample integer not null,
  elapsed_ms numeric not null,
  primary key (profile, sample)
) on commit drop;

create function pg_temp.measure_portal_sitemap_trigger_writes(
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
    v_flow_id := ('53920000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_process_id := ('53910000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_started_at := pg_catalog.clock_timestamp();

    update private.portal_catalog_search_rows_v1
    set modified_at = modified_at + interval '1 microsecond'
    where dataset_kind = 'flow'
      and id = v_flow_id;
    update private.portal_catalog_search_rows_v1
    set modified_at = modified_at - interval '1 microsecond'
    where dataset_kind = 'flow'
      and id = v_flow_id;
    update private.portal_catalog_search_rows_v1
    set modified_at = modified_at + interval '1 microsecond'
    where dataset_kind = 'process'
      and id = v_process_id;
    update private.portal_catalog_search_rows_v1
    set modified_at = modified_at - interval '1 microsecond'
    where dataset_kind = 'process'
      and id = v_process_id;

    insert into pg_temp.portal_sitemap_trigger_writer_timings (
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

grant insert on table portal_sitemap_trigger_writer_timings
  to api_internal_executor;
grant execute on function
  pg_temp.measure_portal_sitemap_trigger_writes(text, integer)
  to api_internal_executor;

alter table private.portal_catalog_facet_rows_v1
  disable trigger portal_sitemap_rows_sync_v1;
grant api_internal_executor to postgres;
set local role api_internal_executor;
select pg_temp.measure_portal_sitemap_trigger_writes(
  'baseline-without-version-sync',
  :'benchmark_samples'::integer
);
reset role;
revoke api_internal_executor from postgres;
alter table private.portal_catalog_facet_rows_v1
  enable trigger portal_sitemap_rows_sync_v1;

grant api_internal_executor to postgres;
set local role api_internal_executor;
select pg_temp.measure_portal_sitemap_trigger_writes(
  'with-version-sync',
  :'benchmark_samples'::integer
);
reset role;
revoke api_internal_executor from postgres;

select writer.profile,
  pg_catalog.count(*) as samples,
  pg_catalog.round(pg_catalog.min(writer.elapsed_ms), 3) as minimum_ms,
  pg_catalog.round(pg_catalog.avg(writer.elapsed_ms), 3) as average_ms,
  pg_catalog.round((
    pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  )::numeric, 3) as p95_ms,
  pg_catalog.round(pg_catalog.max(writer.elapsed_ms), 3) as maximum_ms
from portal_sitemap_trigger_writer_timings as writer
group by writer.profile
order by writer.profile;

do $portal_sitemap_trigger_writer_guard$
declare
  v_baseline_p95 numeric;
  v_enabled_p95 numeric;
begin
  select pg_catalog.percentile_cont(0.95) within group (
    order by writer.elapsed_ms
  ) into v_baseline_p95
  from portal_sitemap_trigger_writer_timings as writer
  where writer.profile = 'baseline-without-version-sync';

  select pg_catalog.percentile_cont(0.95) within group (
    order by writer.elapsed_ms
  ) into v_enabled_p95
  from portal_sitemap_trigger_writer_timings as writer
  where writer.profile = 'with-version-sync';

  if v_baseline_p95 is null
     or v_enabled_p95 is null
     or v_enabled_p95 > greatest(
       5::numeric,
       v_baseline_p95 * 3
     )
     or exists (
       select 1
       from private.portal_sitemap_rows_v1 as latest
       join private.portal_catalog_facet_rows_v1 as facet
         on facet.dataset_kind = latest.dataset_kind
        and facet.id = latest.id
        and facet.version = latest.version
       where latest.modified_at <> facet.modified_at
     ) then
    raise exception 'Portal sitemap version-sync writer budget failed'
      using errcode = '54000';
  end if;
end
$portal_sitemap_trigger_writer_guard$;

-- Rebuild the exact narrow index on a temporary copy so build bytes and
-- incremental writer cost can be measured without changing durable schema.
create temporary table portal_sitemap_writer_probe (
  dataset_kind text not null,
  id uuid not null,
  version text not null,
  modified_at timestamptz not null,
  shard_no smallint not null,
  contract_version smallint not null,
  primary key (dataset_kind, id, version)
) on commit drop;

insert into portal_sitemap_writer_probe (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
)
select
  projection.dataset_kind,
  projection.id,
  projection.version,
  projection.modified_at,
  projection.shard_no,
  projection.contract_version
from private.portal_sitemap_rows_v1 as projection;

create temporary table portal_sitemap_writer_timings (
  profile text not null,
  sample integer not null,
  elapsed_ms numeric not null,
  primary key (profile, sample)
) on commit drop;

create temporary table portal_sitemap_index_build (
  elapsed_ms numeric not null,
  index_bytes bigint not null
) on commit drop;

create function pg_temp.measure_portal_sitemap_writes(
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
    v_flow_id := ('53920000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_process_id := ('53910000-0000-4000-8000-' ||
      pg_catalog.lpad(v_sample::text, 12, '0'))::uuid;
    v_started_at := pg_catalog.clock_timestamp();

    update pg_temp.portal_sitemap_writer_probe
    set modified_at = modified_at + interval '1 microsecond'
    where dataset_kind = 'flow'
      and id = v_flow_id
      and version = '01.00.000';
    update pg_temp.portal_sitemap_writer_probe
    set modified_at = modified_at - interval '1 microsecond'
    where dataset_kind = 'flow'
      and id = v_flow_id
      and version = '01.00.000';
    update pg_temp.portal_sitemap_writer_probe
    set modified_at = modified_at + interval '1 microsecond'
    where dataset_kind = 'process'
      and id = v_process_id
      and version = '01.00.000';
    update pg_temp.portal_sitemap_writer_probe
    set modified_at = modified_at - interval '1 microsecond'
    where dataset_kind = 'process'
      and id = v_process_id
      and version = '01.00.000';

    insert into pg_temp.portal_sitemap_writer_timings (
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

select pg_temp.measure_portal_sitemap_writes(
  'baseline-without-sitemap-index',
  :'benchmark_samples'::integer
);

do $build_portal_sitemap_writer_probe_index$
declare
  v_started_at timestamptz := pg_catalog.clock_timestamp();
begin
  create index portal_sitemap_writer_probe_idx
  on pg_temp.portal_sitemap_writer_probe (
    shard_no,
    contract_version,
    dataset_kind,
    id,
    version desc,
    modified_at desc
  );

  insert into pg_temp.portal_sitemap_index_build (
    elapsed_ms,
    index_bytes
  ) values (
    extract(epoch from pg_catalog.clock_timestamp() - v_started_at) * 1000,
    pg_catalog.pg_relation_size(
      'pg_temp.portal_sitemap_writer_probe_idx'::regclass
    )
  );
end
$build_portal_sitemap_writer_probe_index$;

analyze pg_temp.portal_sitemap_writer_probe;

select pg_temp.measure_portal_sitemap_writes(
  'with-sitemap-index',
  :'benchmark_samples'::integer
);

select
  writer.profile,
  pg_catalog.count(*) as samples,
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
from portal_sitemap_writer_timings as writer
group by writer.profile
order by writer.profile;

select
  pg_catalog.round(index_build.elapsed_ms, 3) as temporary_build_ms,
  index_build.index_bytes as temporary_index_bytes
from portal_sitemap_index_build as index_build;

do $portal_sitemap_writer_performance_guard$
declare
  v_baseline_p95 numeric;
  v_indexed_p95 numeric;
  v_build_ms numeric;
  v_index_bytes bigint;
begin
  select pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  into v_baseline_p95
  from portal_sitemap_writer_timings as writer
  where writer.profile = 'baseline-without-sitemap-index';

  select pg_catalog.percentile_cont(0.95) within group (
      order by writer.elapsed_ms
    )
  into v_indexed_p95
  from portal_sitemap_writer_timings as writer
  where writer.profile = 'with-sitemap-index';

  select index_build.elapsed_ms,
    index_build.index_bytes
  into v_build_ms,
    v_index_bytes
  from portal_sitemap_index_build as index_build;

  if v_baseline_p95 is null
     or v_indexed_p95 is null
     or v_build_ms is null
     or v_index_bytes is null
     or (
       select count(*)
       from portal_sitemap_writer_timings
       where profile = 'baseline-without-sitemap-index'
     ) <> 20
     or (
       select count(*)
       from portal_sitemap_writer_timings
       where profile = 'with-sitemap-index'
     ) <> 20
     or v_indexed_p95 > greatest(5::numeric, v_baseline_p95 * 3)
     or v_build_ms > 5000
     or v_index_bytes > 32 * 1024 * 1024 then
    raise exception 'Portal sitemap index writer/build budget failed'
      using errcode = '54000';
  end if;
end
$portal_sitemap_writer_performance_guard$;

create temporary table portal_sitemap_manifest_payload (
  payload jsonb not null
) on commit drop;

insert into portal_sitemap_manifest_payload (payload)
select api.portal_sitemap_manifest_v1();

do $portal_sitemap_manifest_shape_guard$
declare
  v_payload jsonb;
begin
  select manifest.payload
  into strict v_payload
  from portal_sitemap_manifest_payload as manifest;

  if pg_catalog.jsonb_typeof(v_payload) <> 'object'
     or (
       select count(*)
       from pg_catalog.jsonb_object_keys(v_payload)
     ) <> 2
     or v_payload ->> 'schemaVersion' <>
       'portal.public-sitemap-manifest.v1'
     or pg_catalog.jsonb_typeof(v_payload -> 'shards') <> 'array'
     or pg_catalog.jsonb_array_length(v_payload -> 'shards') <> 64 then
    raise exception 'Portal sitemap manifest shape drifted'
      using errcode = '55000';
  end if;
end
$portal_sitemap_manifest_shape_guard$;

create temporary table portal_sitemap_manifest_rows (
  ordinal integer not null unique,
  bucket integer not null unique,
  shard_cursor text not null unique,
  max_items integer not null,
  descriptor jsonb not null,
  primary key (bucket)
) on commit drop;

insert into portal_sitemap_manifest_rows (
  ordinal,
  bucket,
  shard_cursor,
  max_items,
  descriptor
)
select
  shard.ordinality::integer - 1,
  shard.ordinality::integer - 1,
  shard.value ->> 'shardCursor',
  (shard.value ->> 'maxItems')::integer,
  shard.value
from portal_sitemap_manifest_payload as manifest
cross join lateral pg_catalog.jsonb_array_elements(
  manifest.payload -> 'shards'
) with ordinality as shard(value, ordinality);

do $portal_sitemap_manifest_rows_guard$
begin
  if (select count(*) from portal_sitemap_manifest_rows) <> 64
     or exists (
       select 1
       from portal_sitemap_manifest_rows as manifest
       where manifest.ordinal <> manifest.bucket
          or manifest.bucket not between 0 and 63
          or manifest.max_items <> 4096
          or pg_catalog.jsonb_typeof(manifest.descriptor) <> 'object'
          or (
            select count(*)
            from pg_catalog.jsonb_object_keys(manifest.descriptor)
          ) <> 2
          or manifest.descriptor ->> 'shardCursor' <>
            manifest.shard_cursor
          or manifest.descriptor ->> 'maxItems' <> '4096'
          or pg_catalog.octet_length(manifest.shard_cursor)
            not between 1 and 4096
          or manifest.shard_cursor !~ '^[A-Za-z0-9_-]+$'
     ) then
    raise exception 'Portal sitemap manifest cursor contract drifted'
      using errcode = '55000';
  end if;
end
$portal_sitemap_manifest_rows_guard$;

create temporary table portal_sitemap_expected_latest on commit drop as
select distinct on (projection.dataset_kind, projection.id)
  projection.dataset_kind,
  projection.id,
  projection.version,
  projection.modified_at,
  projection.shard_no,
  projection.contract_version
from private.portal_sitemap_rows_v1 as projection
where projection.contract_version = 1
order by projection.dataset_kind,
  projection.id,
  projection.version desc,
  projection.modified_at desc;

alter table portal_sitemap_expected_latest
  add primary key (dataset_kind, id);

create temporary table portal_sitemap_bucket_stats (
  bucket integer primary key,
  item_count bigint not null
) on commit drop;

insert into portal_sitemap_bucket_stats (bucket, item_count)
with bucket_counts as (
  select
    projection.shard_no::integer as bucket,
    count(*) as item_count
  from portal_sitemap_expected_latest as projection
  where projection.contract_version = 1
  group by 1
)
select
  bucket.bucket,
  coalesce(bucket_counts.item_count, 0)
from pg_catalog.generate_series(0, 63) as bucket(bucket)
left join bucket_counts using (bucket)
order by bucket.bucket;

select
  stats.bucket as largest_bucket,
  stats.item_count as largest_item_count,
  manifest.shard_cursor as largest_shard_cursor
from portal_sitemap_bucket_stats as stats
join portal_sitemap_manifest_rows as manifest using (bucket)
order by stats.item_count desc, stats.bucket
limit 1
\gset

select
  pg_catalog.sum(stats.item_count) as total_items,
  pg_catalog.min(stats.item_count) as minimum_shard_items,
  pg_catalog.round(pg_catalog.avg(stats.item_count), 3) as average_shard_items,
  pg_catalog.max(stats.item_count) as maximum_shard_items,
  pg_catalog.max(stats.item_count) * 1200 + 8192
    as conservative_xml_bytes,
  :largest_bucket::integer as largest_bucket
from portal_sitemap_bucket_stats as stats;

do $portal_sitemap_bucket_distribution_guard$
begin
  if (select count(*) from portal_sitemap_bucket_stats) <> 64
     or (select sum(item_count) from portal_sitemap_bucket_stats) <>
       126246
     or (select max(item_count) from portal_sitemap_bucket_stats) > 4096
     or (
       select max(item_count) * 1200 + 8192
       from portal_sitemap_bucket_stats
     ) >= 6 * 1024 * 1024
     or (select count(*) from portal_sitemap_bucket_stats where item_count = 0)
       <> 0 then
    raise exception 'Portal sitemap bucket distribution is unsafe'
      using errcode = '54000';
  end if;
end
$portal_sitemap_bucket_distribution_guard$;

create temporary table portal_sitemap_shard_responses (
  bucket integer primary key,
  shard_cursor text not null unique,
  payload jsonb not null
) on commit drop;

insert into portal_sitemap_shard_responses (
  bucket,
  shard_cursor,
  payload
)
select
  manifest.bucket,
  manifest.shard_cursor,
  api.portal_sitemap_shard_v1(manifest.shard_cursor)
from portal_sitemap_manifest_rows as manifest
order by manifest.bucket;

do $portal_sitemap_shard_response_guard$
begin
  if (select count(*) from portal_sitemap_shard_responses) <> 64
     or exists (
       select 1
       from portal_sitemap_shard_responses as response
       where pg_catalog.jsonb_typeof(response.payload) <> 'object'
          or (
            select count(*)
            from pg_catalog.jsonb_object_keys(response.payload)
          ) <> 3
          or response.payload ->> 'schemaVersion' <>
            'portal.public-sitemap-shard.v1'
          or response.payload ->> 'shardCursor' <> response.shard_cursor
          or pg_catalog.jsonb_typeof(response.payload -> 'items') <> 'array'
          or pg_catalog.jsonb_array_length(response.payload -> 'items') > 4096
          or pg_catalog.jsonb_array_length(response.payload -> 'items') <>
            (
              select stats.item_count
              from portal_sitemap_bucket_stats as stats
              where stats.bucket = response.bucket
            )
          or pg_catalog.octet_length(response.payload::text) >
            2 * 1024 * 1024
     ) then
    raise exception 'Portal sitemap shard response contract drifted'
      using errcode = '54000';
  end if;
end
$portal_sitemap_shard_response_guard$;

create temporary table portal_sitemap_union_entries (
  bucket integer not null,
  dataset_kind text not null,
  id uuid not null,
  version text not null,
  modified_at text not null
) on commit drop;

insert into portal_sitemap_union_entries (
  bucket,
  dataset_kind,
  id,
  version,
  modified_at
)
select
  response.bucket,
  item.value #>> '{key,kind}',
  (item.value #>> '{key,id}')::uuid,
  item.value #>> '{key,version}',
  item.value ->> 'modifiedAt'
from portal_sitemap_shard_responses as response
cross join lateral pg_catalog.jsonb_array_elements(
  response.payload -> 'items'
) as item(value);

do $portal_sitemap_union_guard$
begin
  if (select count(*) from portal_sitemap_union_entries) <>
       126246
     or (
       select count(*)
       from portal_sitemap_union_entries
       where dataset_kind = 'process'
     ) <> 17299
     or (
       select count(*)
       from portal_sitemap_union_entries
       where dataset_kind = 'flow'
     ) <> 108947
     or (
       select count(*)
       from (
         select union_entry.dataset_kind,
           union_entry.id,
           union_entry.version
         from portal_sitemap_union_entries as union_entry
         group by union_entry.dataset_kind,
           union_entry.id,
           union_entry.version
         having count(*) > 1
       ) as duplicate
     ) <> 0
     or exists (
       select 1
       from portal_sitemap_union_entries as union_entry
       left join portal_sitemap_expected_latest as projection
         on projection.dataset_kind = union_entry.dataset_kind
        and projection.id = union_entry.id
        and projection.version = union_entry.version
       where union_entry.dataset_kind not in ('process', 'flow')
          or union_entry.version !~ '^\d{2}\.\d{2}\.\d{3}$'
          or projection.id is null
          or union_entry.bucket <> projection.shard_no
     )
     or exists (
       select 1
       from (
         (
           select
             projection.dataset_kind,
             projection.id,
             projection.version
           from portal_sitemap_expected_latest as projection
           where projection.contract_version = 1
           except
           select
             union_entry.dataset_kind,
             union_entry.id,
             union_entry.version
           from portal_sitemap_union_entries as union_entry
         )
         union all
         (
           select
             union_entry.dataset_kind,
             union_entry.id,
             union_entry.version
           from portal_sitemap_union_entries as union_entry
           except
           select
             projection.dataset_kind,
             projection.id,
             projection.version
           from portal_sitemap_expected_latest as projection
           where projection.contract_version = 1
         )
       ) as difference
     )
     or exists (
       select 1
       from portal_sitemap_union_entries as union_entry
       join portal_sitemap_expected_latest as projection
         on projection.dataset_kind = union_entry.dataset_kind
        and projection.id = union_entry.id
        and projection.version = union_entry.version
       where union_entry.modified_at <>
         private.portal_timestamp_v1(projection.modified_at)
     ) then
    raise exception
      'Portal sitemap 64-shard union has a missing or duplicate identity'
      using errcode = '55000';
  end if;
end
$portal_sitemap_union_guard$;

select
  count(*) as union_items,
  count(distinct (dataset_kind, id, version)) as distinct_union_items,
  count(distinct bucket) as represented_buckets
from portal_sitemap_union_entries;

create temporary table portal_sitemap_timings (
  profile text not null,
  sample integer not null,
  elapsed_ms numeric not null,
  response_bytes integer not null,
  item_count integer not null,
  primary key (profile, sample)
) on commit drop;

create function pg_temp.measure_portal_sitemap_manifest(
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
  perform api.portal_sitemap_manifest_v1();
  for v_sample in 1..p_samples loop
    v_started_at := pg_catalog.clock_timestamp();
    v_payload := api.portal_sitemap_manifest_v1();
    insert into pg_temp.portal_sitemap_timings (
      profile,
      sample,
      elapsed_ms,
      response_bytes,
      item_count
    ) values (
      'manifest',
      v_sample,
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000,
      pg_catalog.octet_length(v_payload::text),
      pg_catalog.jsonb_array_length(v_payload -> 'shards')
    );
  end loop;
end
$function$;

create function pg_temp.measure_portal_sitemap_shard(
  p_shard_cursor text,
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
  perform api.portal_sitemap_shard_v1(p_shard_cursor);
  for v_sample in 1..p_samples loop
    v_started_at := pg_catalog.clock_timestamp();
    v_payload := api.portal_sitemap_shard_v1(p_shard_cursor);
    insert into pg_temp.portal_sitemap_timings (
      profile,
      sample,
      elapsed_ms,
      response_bytes,
      item_count
    ) values (
      'largest-shard',
      v_sample,
      extract(
        epoch from pg_catalog.clock_timestamp() - v_started_at
      ) * 1000,
      pg_catalog.octet_length(v_payload::text),
      pg_catalog.jsonb_array_length(v_payload -> 'items')
    );
  end loop;
end
$function$;

select pg_temp.measure_portal_sitemap_manifest(
  :'benchmark_samples'::integer
);

select pg_temp.measure_portal_sitemap_shard(
  :'largest_shard_cursor',
  :'benchmark_samples'::integer
);

select
  timing.profile,
  count(*) as samples,
  pg_catalog.round(pg_catalog.min(timing.elapsed_ms), 3) as minimum_ms,
  pg_catalog.round(pg_catalog.avg(timing.elapsed_ms), 3) as average_ms,
  pg_catalog.round(
    (
      pg_catalog.percentile_cont(0.95) within group (
        order by timing.elapsed_ms
      )
    )::numeric,
    3
  ) as p95_ms,
  pg_catalog.round(pg_catalog.max(timing.elapsed_ms), 3) as maximum_ms,
  pg_catalog.max(timing.response_bytes) as maximum_response_bytes,
  pg_catalog.min(timing.item_count) as minimum_items,
  pg_catalog.max(timing.item_count) as maximum_items
from portal_sitemap_timings as timing
group by timing.profile
order by timing.profile;

do $portal_sitemap_request_performance_guard$
declare
  v_profile record;
begin
  for v_profile in
    select
      timing.profile,
      count(*) as samples,
      pg_catalog.percentile_cont(0.95) within group (
        order by timing.elapsed_ms
      ) as p95_ms,
      pg_catalog.max(timing.elapsed_ms) as maximum_ms,
      pg_catalog.max(timing.response_bytes) as maximum_response_bytes,
      pg_catalog.min(timing.item_count) as minimum_items,
      pg_catalog.max(timing.item_count) as maximum_items
    from portal_sitemap_timings as timing
    group by timing.profile
  loop
    if v_profile.samples <> 20
       or v_profile.minimum_items <> v_profile.maximum_items
       or (
         v_profile.profile = 'manifest'
         and (
           v_profile.p95_ms > 250
           or v_profile.maximum_ms >= 2000
           or v_profile.maximum_response_bytes > 64 * 1024
           or v_profile.minimum_items <> 64
         )
       )
       or (
         v_profile.profile = 'largest-shard'
         and (
           v_profile.p95_ms > 2000
           or v_profile.maximum_ms >= 4000
           or v_profile.maximum_response_bytes > 2 * 1024 * 1024
           or v_profile.minimum_items <>
             (select max(item_count) from portal_sitemap_bucket_stats)
         )
       ) then
      raise exception 'Portal sitemap request budget failed for %',
        v_profile.profile
        using errcode = '54000';
    end if;
  end loop;

  if (select count(distinct profile) from portal_sitemap_timings) <> 2 then
    raise exception 'Portal sitemap timing profiles are incomplete'
      using errcode = '54000';
  end if;
end
$portal_sitemap_request_performance_guard$;

-- Capture the exact shard query without disabling sequential scans or forcing
-- an index. The installed exact-version covering B-tree must win naturally at
-- release cardinality and the projection must not be scanned sequentially.
set local work_mem = '8MB';
select
  pg_catalog.current_setting('enable_seqscan') = 'on'
  and pg_catalog.current_setting('enable_indexscan') = 'on'
  and pg_catalog.current_setting('enable_indexonlyscan') = 'on'
  and pg_catalog.current_setting('enable_bitmapscan') = 'on'
  and pg_catalog.current_setting('work_mem') = '8MB'
  as natural_plan_settings_ok
\gset
\if :natural_plan_settings_ok
\else
  \echo 'ERROR: restore natural planner scan settings before benchmarking'
  \quit 3
\endif

-- Exercise the exact read shape with 64 retained public versions for 2,048
-- identities in one shard (131,072 rows). This isolates history-density cost
-- without creating wide parent cards or changing the durable projection.
create temporary table portal_sitemap_history_probe (
  dataset_kind text not null,
  id uuid not null,
  version text not null,
  modified_at timestamptz not null,
  shard_no smallint not null,
  contract_version smallint not null,
  primary key (dataset_kind, id, version)
) on commit drop;

alter table portal_sitemap_history_probe enable row level security;
alter table portal_sitemap_history_probe force row level security;
create policy portal_sitemap_history_probe_select
on portal_sitemap_history_probe
for select
to portal_public_executor
using (contract_version = 1 and shard_no between 0 and 63);

insert into portal_sitemap_history_probe (
  dataset_kind,
  id,
  version,
  modified_at,
  shard_no,
  contract_version
)
select identity.dataset_kind,
  identity.id,
  pg_catalog.lpad(version.ordinal::text, 2, '0') || '.00.000',
  identity.modified_at + version.ordinal * interval '1 microsecond',
  identity.shard_no,
  1
from (
  select expected.dataset_kind,
    expected.id,
    expected.modified_at,
    expected.shard_no
  from portal_sitemap_expected_latest as expected
  where expected.shard_no = :largest_bucket::integer
  order by expected.dataset_kind, expected.id
  limit 2048
) as identity
cross join pg_catalog.generate_series(1, 64) as version(ordinal);

create index portal_sitemap_history_probe_idx
on portal_sitemap_history_probe (
  shard_no,
  contract_version,
  dataset_kind,
  id,
  version desc,
  modified_at desc
);

analyze portal_sitemap_history_probe;

select
  (select pg_catalog.count(*) from portal_sitemap_history_probe) = 131072
  and (
    select pg_catalog.count(*)
    from (
      select distinct on (history.dataset_kind, history.id)
        history.dataset_kind,
        history.id
      from portal_sitemap_history_probe as history
      where history.shard_no = :largest_bucket::integer
        and history.contract_version = 1
      order by history.dataset_kind,
        history.id,
        history.version desc,
        history.modified_at desc
      limit 4097
    ) as latest
  ) = 2048 as history_probe_cardinality_ok
\gset
\if :history_probe_cardinality_ok
\else
  \echo 'ERROR: Portal sitemap history-density fixture is incomplete'
  \quit 3
\endif

create function pg_temp.capture_portal_sitemap_plan(
  p_bucket integer
)
returns jsonb
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_plan jsonb;
begin
  if p_bucket not between 0 and 63 then
    raise exception 'invalid benchmark bucket';
  end if;

  execute pg_catalog.format($query$
    explain (analyze, buffers, settings, summary, format json)
    with latest as materialized (
      select distinct on (projection.dataset_kind, projection.id)
        projection.dataset_kind,
        projection.id,
        projection.version,
        projection.modified_at
      from private.portal_sitemap_rows_v1 as projection
      where projection.shard_no = %s
        and projection.contract_version = 1
      order by projection.dataset_kind,
        projection.id,
        projection.version desc,
        projection.modified_at desc
      limit 4097
    )
    select coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'kind', latest.dataset_kind,
        'id', latest.id::text,
        'version', latest.version,
        'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
      )
      order by latest.dataset_kind, latest.id
    ), '[]'::jsonb)
    from latest
  $query$, p_bucket)
  into v_plan;

  return v_plan;
end
$function$;

create function pg_temp.capture_portal_sitemap_history_plan(
  p_bucket integer
)
returns jsonb
language plpgsql
volatile
set search_path = ''
as $function$
declare
  v_plan jsonb;
begin
  execute pg_catalog.format($query$
    explain (analyze, buffers, settings, summary, format json)
    with latest as materialized (
      select distinct on (projection.dataset_kind, projection.id)
        projection.dataset_kind,
        projection.id,
        projection.version,
        projection.modified_at
      from pg_temp.portal_sitemap_history_probe as projection
      where projection.shard_no = %s
        and projection.contract_version = 1
      order by projection.dataset_kind,
        projection.id,
        projection.version desc,
        projection.modified_at desc
      limit 4097
    )
    select coalesce(pg_catalog.jsonb_agg(
      pg_catalog.jsonb_build_object(
        'kind', latest.dataset_kind,
        'id', latest.id::text,
        'version', latest.version,
        'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
      ) order by latest.dataset_kind, latest.id
    ), '[]'::jsonb)
    from latest
  $query$, p_bucket)
  into v_plan;

  return v_plan;
end
$function$;

create temporary table portal_sitemap_plans (
  profile text primary key,
  plan jsonb not null
) on commit drop;

grant execute on function pg_temp.capture_portal_sitemap_plan(integer)
to portal_public_executor;
grant execute on function pg_temp.capture_portal_sitemap_history_plan(integer)
to portal_public_executor;
grant select on table portal_sitemap_history_probe
to portal_public_executor;
grant insert, select on table portal_sitemap_plans
to portal_public_executor;
grant portal_public_executor to postgres;
set local role portal_public_executor;

insert into portal_sitemap_plans (profile, plan)
values
  (
    'largest-shard-natural-index',
    pg_temp.capture_portal_sitemap_plan(:largest_bucket::integer)
  ),
  (
    'history-dense-natural-index',
    pg_temp.capture_portal_sitemap_history_plan(:largest_bucket::integer)
  );

reset role;

select benchmark.profile,
  pg_catalog.round(
    (benchmark.plan #>> '{0,Execution Time}')::numeric,
    3
  ) as execution_ms,
  benchmark.plan #>> '{0,Plan,Actual Rows}' as top_actual_rows
from portal_sitemap_plans as benchmark
order by benchmark.profile;

create temporary table portal_sitemap_plan_nodes on commit drop as
with recursive plan_nodes as (
  select
    benchmark_plan.profile,
    benchmark_plan.plan #> '{0,Plan}' as node
  from portal_sitemap_plans as benchmark_plan

  union all

  select
    plan_nodes.profile,
    child_plan.value
  from plan_nodes
  cross join lateral pg_catalog.jsonb_array_elements(
    coalesce(plan_nodes.node -> 'Plans', '[]'::jsonb)
  ) as child_plan(value)
)
select *
from plan_nodes;

select
  plan_node.node ->> 'Node Type' as node_type,
  plan_node.node ->> 'Index Name' as index_name,
  plan_node.node ->> 'Relation Name' as relation_name,
  plan_node.node ->> 'Actual Rows' as actual_rows,
  plan_node.node ->> 'Actual Loops' as actual_loops,
  plan_node.node ->> 'Shared Hit Blocks' as shared_hit_blocks,
  plan_node.node ->> 'Shared Read Blocks' as shared_read_blocks,
  plan_node.node ->> 'Temp Read Blocks' as temp_read_blocks,
  plan_node.node ->> 'Temp Written Blocks' as temp_written_blocks,
  plan_node.node ->> 'Sort Method' as sort_method
from portal_sitemap_plan_nodes as plan_node
order by node_type, index_name nulls last;

do $portal_sitemap_natural_index_plan_guard$
begin
  if not exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile = 'largest-shard-natural-index'
         and plan_node.node ->> 'Index Name' =
           'portal_sitemap_rows_shard_v1_idx'
     )
     or not exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile = 'history-dense-natural-index'
         and plan_node.node ->> 'Index Name' =
           'portal_sitemap_history_probe_idx'
     )
     or exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile = 'largest-shard-natural-index'
         and plan_node.node ->> 'Relation Name' =
           'portal_sitemap_rows_v1'
         and plan_node.node ->> 'Node Type' = 'Seq Scan'
     )
     or exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile = 'history-dense-natural-index'
         and plan_node.node ->> 'Node Type' in ('Sort', 'Incremental Sort')
     )
     or exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile = 'history-dense-natural-index'
         and plan_node.node ->> 'Relation Name' =
           'portal_sitemap_history_probe'
         and plan_node.node ->> 'Node Type' = 'Seq Scan'
     )
     or exists (
       select 1
       from portal_sitemap_plan_nodes as plan_node
       where plan_node.profile in (
           'largest-shard-natural-index',
           'history-dense-natural-index'
         )
         and (
           coalesce(
             (plan_node.node ->> 'Temp Read Blocks')::bigint,
             0
           ) > 0
           or coalesce(
             (plan_node.node ->> 'Temp Written Blocks')::bigint,
             0
           ) > 0
           or coalesce(plan_node.node ->> 'Sort Method', '') ~*
             'external|disk'
         )
     ) then
    raise exception
      'Portal sitemap largest shard did not use its natural exact-version index'
      using errcode = '54000';
  end if;

  if coalesce((
       select (benchmark.plan #>> '{0,Execution Time}')::numeric
       from portal_sitemap_plans as benchmark
       where benchmark.profile = 'history-dense-natural-index'
     ), 4000) >= 4000 then
    raise exception
      'Portal sitemap history-density query exceeded four seconds'
      using errcode = '54000';
  end if;
end
$portal_sitemap_natural_index_plan_guard$;

set local role portal_public_executor;

explain (analyze, buffers, settings, summary, format text)
with latest as materialized (
  select distinct on (projection.dataset_kind, projection.id)
    projection.dataset_kind,
    projection.id,
    projection.version,
    projection.modified_at
  from private.portal_sitemap_rows_v1 as projection
  where projection.shard_no = :largest_bucket::integer
    and projection.contract_version = 1
  order by projection.dataset_kind,
    projection.id,
    projection.version desc,
    projection.modified_at desc
  limit 4097
)
select coalesce(pg_catalog.jsonb_agg(
  pg_catalog.jsonb_build_object(
    'kind', latest.dataset_kind,
    'id', latest.id::text,
    'version', latest.version,
    'modifiedAt', private.portal_timestamp_v1(latest.modified_at)
  )
  order by latest.dataset_kind, latest.id
), '[]'::jsonb)
from latest;

reset role;

rollback;
