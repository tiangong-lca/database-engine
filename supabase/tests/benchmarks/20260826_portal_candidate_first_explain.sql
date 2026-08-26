\set ON_ERROR_STOP on
\timing on

-- Required operator attestation:
--   -v benchmark_target=local
--   -v explain_output=/absolute/private/path/portal-candidate-first.log
-- This fixture writes only inside one rollback-only local transaction.  Never
-- point it at Preview, persistent Dev, or production.
\if :{?benchmark_target}
\else
  \echo 'ERROR: pass -v benchmark_target=local'
  \quit 3
\endif

\if :{?explain_output}
\else
  \echo 'ERROR: pass a private absolute path with -v explain_output=...'
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

\if :{?process_vector_rows}
\else
  \set process_vector_rows 17299
\endif

\if :{?flow_vector_rows}
\else
  \set flow_vector_rows 108947
\endif

\if :{?process_old_version_rows}
\else
  \set process_old_version_rows 100
\endif

\if :{?flow_old_version_rows}
\else
  \set flow_old_version_rows 21000
\endif

\if :{?draft_vector_rows}
\else
  \set draft_vector_rows 100
\endif

\if :{?writer_samples}
\else
  \set writer_samples 50
\endif

\if :{?benchmark_profile_name}
\else
  \set benchmark_profile_name diagnostic
\endif

select :'benchmark_target' = 'local' as benchmark_attestation_ok \gset
\if :benchmark_attestation_ok
\else
  \echo 'ERROR: this rollback-only profile is local-only'
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
  \echo 'ERROR: benchmark database socket/address is not local'
  \quit
\endif

select
  :'benchmark_profile_name' = 'release'
  and :'benchmark_samples'::integer = 20
  and :'process_rows'::integer = 17299
  and :'flow_rows'::integer = 108947
  and :'process_vector_rows'::integer = 17299
  and :'flow_vector_rows'::integer = 108947
  and :'process_old_version_rows'::integer = 100
  and :'flow_old_version_rows'::integer = 21000
  and :'draft_vector_rows'::integer = 100
  and :'writer_samples'::integer = 50
  as benchmark_release_profile
\gset

select
  :'benchmark_profile_name' in ('sparse-zero', 'sparse-199')
  and :'benchmark_samples'::integer = 20
  and :'process_rows'::integer = 17299
  and :'flow_rows'::integer = 108947
  and :'process_old_version_rows'::integer = 0
  and :'flow_old_version_rows'::integer = 0
  and :'draft_vector_rows'::integer = 0
  and :'writer_samples'::integer = 50
  and (
    (
      :'process_vector_rows'::integer = 0
      and :'flow_vector_rows'::integer = 0
    )
    or (
      :'process_vector_rows'::integer = 199
      and :'flow_vector_rows'::integer = 199
    )
  ) as benchmark_sparse_profile
\gset

select
  :'benchmark_release_profile'::boolean
  or :'benchmark_sparse_profile'::boolean
  as benchmark_semantic_plan_profile
\gset

begin;
set local search_path = public, extensions, pg_temp;
set local statement_timeout = '15min';
set local jit = off;
set local track_io_timing = on;
set local hnsw.iterative_scan = 'strict_order';
select pg_catalog.set_config(
  'application_name',
  'database-engine-531-portal-candidate-first-local',
  true
);
select pg_catalog.set_config(
  'portal.benchmark_writer_samples',
  :'writer_samples',
  true
);
select pg_catalog.set_config(
  'portal.benchmark_database_bytes_before',
  pg_catalog.pg_database_size(pg_catalog.current_database())::text,
  true
);

select
  pg_catalog.to_regclass(
    'private.portal_catalog_search_process_document_v1_pgroonga'
  ) is not null
  and pg_catalog.to_regclass(
    'private.portal_catalog_search_flow_document_v1_pgroonga'
  ) is not null
  and pg_catalog.to_regclass(
    'public.processes_embedding_ft_hnsw_idx'
  ) is not null
  and pg_catalog.to_regclass(
    'public.flows_embedding_ft_hnsw_idx'
  ) is not null
  and pg_catalog.to_regprocedure(
    'private.catalog_portal_search_v1_impl(text,text,jsonb,text,text,uuid,text,integer,text)'
  ) is not null
  and (
    select routine.prosrc ~ 'portal_catalog_search_rows_v1'
      and routine.prosrc ~ 'portal_fused_decorated'
      and routine.prosrc !~ 'public\.processes|public\.flows'
    from pg_catalog.pg_proc as routine
    where routine.oid =
      'private.portal_projection_hybrid_search_v1_impl(text,text[],extensions.vector,jsonb,integer,text)'::regprocedure
  ) as optimized_migration_installed
\gset

\if :optimized_migration_installed
\else
  \echo 'ERROR: Issue #531 migrations are not installed'
  \quit 4
\endif

create or replace function pg_temp.portal_bench_uuid(
  p_prefix text,
  p_ordinal integer
)
returns uuid
language sql
immutable
set search_path = ''
as $function$
  select (
    pg_catalog.substr(hash.value, 1, 8) || '-' ||
    pg_catalog.substr(hash.value, 9, 4) || '-' ||
    '4' || pg_catalog.substr(hash.value, 14, 3) || '-' ||
    '8' || pg_catalog.substr(hash.value, 18, 3) || '-' ||
    pg_catalog.substr(hash.value, 21, 12)
  )::uuid
  from (
    select pg_catalog.md5(p_prefix || ':' || p_ordinal::text) as value
  ) as hash
$function$;

create or replace function pg_temp.portal_bench_localized(p_text text)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    '@xml:lang', 'en',
    '#text', p_text
  )
$function$;

create or replace function pg_temp.portal_bench_vector(p_ordinal integer)
returns extensions.vector(1024)
language sql
immutable
set search_path = ''
as $function$
  select (
    '[1,' || (p_ordinal::numeric / 1000::numeric)::text || ',' ||
    pg_catalog.array_to_string(
      pg_catalog.array_fill('0'::text, array[1022]),
      ','
    ) || ']'
  )::extensions.vector(1024)
$function$;

create or replace function pg_temp.portal_bench_far_vector()
returns extensions.vector(1024)
language sql
immutable
set search_path = ''
as $function$
  select (
    '[0,1,' || pg_catalog.array_to_string(
      pg_catalog.array_fill('0'::text, array[1022]),
      ','
    ) || ']'
  )::extensions.vector(1024)
$function$;

create or replace function pg_temp.portal_bench_publication(p_version text)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'common:dataSetVersion', p_version,
    'common:licenseType', 'Free of charge for all users and uses',
    'common:referenceToOwnershipOfDataSet', pg_catalog.jsonb_build_object(
      '@refObjectId', '53100000-0000-4000-8000-000000009999',
      '@version', '01.00.000',
      'common:shortDescription', pg_temp.portal_bench_localized(
        'Synthetic benchmark provider'
      )
    )
  )
$function$;

create or replace function pg_temp.portal_bench_process_payload(p_ordinal integer)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'processDataSet', pg_catalog.jsonb_build_object(
      'processInformation', pg_catalog.jsonb_build_object(
        'dataSetInformation', pg_catalog.jsonb_build_object(
          'name', pg_catalog.jsonb_build_object(
            'baseName', pg_temp.portal_bench_localized(case
              when p_ordinal = 1 then 'portalbenchneedle process'
              when p_ordinal % 10 = 0
                then 'portalbenchcommon electricity process ' || p_ordinal::text
              else 'portalbenchnoise process ' || p_ordinal::text
            end)
          ),
          'common:generalComment', pg_temp.portal_bench_localized(
            'synthetic benchmark summary'
          ),
          'classificationInformation', pg_catalog.jsonb_build_object(
            'common:classification', pg_catalog.jsonb_build_object(
              'common:class', pg_catalog.jsonb_build_object(
                '@classId', 'PORTAL-BENCH', '#text', 'Synthetic benchmark'
              )
            )
          )
        ),
        'time', pg_catalog.jsonb_build_object('common:referenceYear', '2024'),
        'geography', pg_catalog.jsonb_build_object(
          'locationOfOperationSupplyOrProduction',
          pg_catalog.jsonb_build_object('@location', 'CN')
        ),
        'technology', pg_catalog.jsonb_build_object(
          'technologyDescriptionAndIncludedProcesses',
          pg_temp.portal_bench_localized('synthetic benchmark technology')
        )
      ),
      'modellingAndValidation', pg_catalog.jsonb_build_object(
        'LCIMethodAndAllocation', pg_catalog.jsonb_build_object(
          'typeOfDataSet', 'Unit process, single operation'
        )
      ),
      'administrativeInformation', pg_catalog.jsonb_build_object(
        'publicationAndOwnership', pg_temp.portal_bench_publication('01.00.000')
      )
    )
  )
$function$;

create or replace function pg_temp.portal_bench_flow_payload(p_ordinal integer)
returns jsonb
language sql
immutable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'flowDataSet', pg_catalog.jsonb_build_object(
      'flowInformation', pg_catalog.jsonb_build_object(
        'dataSetInformation', pg_catalog.jsonb_build_object(
          'name', pg_catalog.jsonb_build_object(
            'baseName', pg_temp.portal_bench_localized(case
              when p_ordinal = 1 then 'portalbenchneedle flow'
              when p_ordinal % 10 = 0
                then 'portalbenchcommon electricity flow ' || p_ordinal::text
              else 'portalbenchnoise flow ' || p_ordinal::text
            end)
          ),
          'common:generalComment', pg_temp.portal_bench_localized(
            'synthetic benchmark summary'
          ),
          'classificationInformation', pg_catalog.jsonb_build_object(
            'common:classification', pg_catalog.jsonb_build_object(
              'common:class', pg_catalog.jsonb_build_object(
                '@classId', 'PORTAL-BENCH', '#text', 'Synthetic benchmark'
              )
            )
          ),
          'CASNumber', '50-00-0'
        ),
        'geography', pg_catalog.jsonb_build_object(
          'locationOfSupply', pg_catalog.jsonb_build_object('@location', 'CN')
        )
      ),
      'administrativeInformation', pg_catalog.jsonb_build_object(
        'publicationAndOwnership', pg_temp.portal_bench_publication('01.00.000')
      )
    )
  )
$function$;

alter table public.processes disable trigger user;
alter table public.flows disable trigger user;

create temporary table portal_benchmark_writer_timings (
  mode text not null,
  elapsed_ms double precision not null
) on commit drop;

do $measure_portal_writer_baseline$
declare
  v_ordinal integer;
  v_samples integer := pg_catalog.current_setting(
    'portal.benchmark_writer_samples'
  )::integer;
  v_payload jsonb;
  v_started timestamptz;
begin
  for v_ordinal in 1..v_samples loop
    v_payload := pg_temp.portal_bench_process_payload(v_ordinal);
    v_started := pg_catalog.clock_timestamp();
    insert into public.processes (
      id, version, json, json_ordered, user_id, state_code,
      rule_verification, modified_at, search_text, embedding_ft, model_id
    ) values (
      pg_temp.portal_bench_uuid('writer-process-baseline', v_ordinal),
      '01.00.000', v_payload, v_payload::json,
      '53100000-0000-4000-8000-000000000001', 100, true,
      '2026-08-26 00:00:00+00', null, null, null
    );
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_insert_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_payload := pg_temp.portal_bench_flow_payload(v_ordinal);
    v_started := pg_catalog.clock_timestamp();
    insert into public.flows (
      id, version, json, json_ordered, user_id, state_code,
      rule_verification, modified_at, search_text, embedding_ft
    ) values (
      pg_temp.portal_bench_uuid('writer-flow-baseline', v_ordinal),
      '01.00.000', v_payload, v_payload::json,
      '53100000-0000-4000-8000-000000000001', 100, true,
      '2026-08-26 00:00:00+00', null, null
    );
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_insert_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  end loop;

  for v_ordinal in 1..v_samples loop
    v_started := pg_catalog.clock_timestamp();
    update public.processes
    set json = json,
        modified_at = modified_at
    where id = pg_temp.portal_bench_uuid(
        'writer-process-baseline', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_content_update_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.flows
    set json = json,
        modified_at = modified_at
    where id = pg_temp.portal_bench_uuid(
        'writer-flow-baseline', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_content_update_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.processes
    set embedding_ft = pg_temp.portal_bench_vector(v_ordinal)
    where id = pg_temp.portal_bench_uuid(
        'writer-process-baseline', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_embedding_update_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.flows
    set embedding_ft = pg_temp.portal_bench_vector(v_ordinal)
    where id = pg_temp.portal_bench_uuid(
        'writer-flow-baseline', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_embedding_update_baseline',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  end loop;

  delete from public.processes
  where id in (
    select pg_temp.portal_bench_uuid(
      'writer-process-baseline', series.ordinal
    )
    from pg_catalog.generate_series(1, v_samples) as series(ordinal)
  );
  delete from public.flows
  where id in (
    select pg_temp.portal_bench_uuid(
      'writer-flow-baseline', series.ordinal
    )
    from pg_catalog.generate_series(1, v_samples) as series(ordinal)
  );
end
$measure_portal_writer_baseline$;

alter table public.processes
  enable trigger portal_catalog_projection_content_sync_v1;
alter table public.flows
  enable trigger portal_catalog_projection_content_sync_v1;

do $measure_portal_projection_writes$
declare
  v_ordinal integer;
  v_samples integer := pg_catalog.current_setting(
    'portal.benchmark_writer_samples'
  )::integer;
  v_payload jsonb;
  v_started timestamptz;
begin
  for v_ordinal in 1..v_samples loop
    v_payload := pg_temp.portal_bench_process_payload(v_ordinal);
    v_started := pg_catalog.clock_timestamp();
    insert into public.processes (
      id, version, json, json_ordered, user_id, state_code,
      rule_verification, modified_at, search_text, embedding_ft, model_id
    ) values (
      pg_temp.portal_bench_uuid('writer-process-projection', v_ordinal),
      '01.00.000', v_payload, v_payload::json,
      '53100000-0000-4000-8000-000000000001', 100, true,
      '2026-08-26 00:00:00+00', null, null, null
    );
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_insert_projection',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_payload := pg_temp.portal_bench_flow_payload(v_ordinal);
    v_started := pg_catalog.clock_timestamp();
    insert into public.flows (
      id, version, json, json_ordered, user_id, state_code,
      rule_verification, modified_at, search_text, embedding_ft
    ) values (
      pg_temp.portal_bench_uuid('writer-flow-projection', v_ordinal),
      '01.00.000', v_payload, v_payload::json,
      '53100000-0000-4000-8000-000000000001', 100, true,
      '2026-08-26 00:00:00+00', null, null
    );
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_insert_projection',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  end loop;

  for v_ordinal in 1..v_samples loop
    v_started := pg_catalog.clock_timestamp();
    update public.processes
    set json = json,
        modified_at = modified_at
    where id = pg_temp.portal_bench_uuid(
        'writer-process-projection', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_content_update_projection',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.flows
    set json = json,
        modified_at = modified_at
    where id = pg_temp.portal_bench_uuid(
        'writer-flow-projection', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_content_update_projection',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.processes
    set embedding_ft = pg_temp.portal_bench_vector(v_ordinal)
    where id = pg_temp.portal_bench_uuid(
        'writer-process-projection', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'process_embedding_update_source_hnsw',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );

    v_started := pg_catalog.clock_timestamp();
    update public.flows
    set embedding_ft = pg_temp.portal_bench_vector(v_ordinal)
    where id = pg_temp.portal_bench_uuid(
        'writer-flow-projection', v_ordinal
      )
      and version = '01.00.000';
    insert into pg_temp.portal_benchmark_writer_timings values (
      'flow_embedding_update_source_hnsw',
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  end loop;

  delete from public.processes
  where id in (
    select pg_temp.portal_bench_uuid(
      'writer-process-projection', series.ordinal
    )
    from pg_catalog.generate_series(1, v_samples) as series(ordinal)
  );
  delete from public.flows
  where id in (
    select pg_temp.portal_bench_uuid(
      'writer-flow-projection', series.ordinal
    )
    from pg_catalog.generate_series(1, v_samples) as series(ordinal)
  );
end
$measure_portal_projection_writes$;

select mode,
  count(*) as samples,
  pg_catalog.round(
    pg_catalog.percentile_cont(0.95)
      within group (order by elapsed_ms)::numeric,
    3
  ) as p95_ms
from pg_temp.portal_benchmark_writer_timings
group by mode
order by mode;

with summary as (
  select mode,
    pg_catalog.percentile_cont(0.95)
      within group (order by elapsed_ms) as p95_ms
  from pg_temp.portal_benchmark_writer_timings
  group by mode
), pairs(projection_mode, baseline_mode) as (
  values
    ('process_insert_projection'::text, 'process_insert_baseline'::text),
    ('flow_insert_projection', 'flow_insert_baseline'),
    ('process_content_update_projection', 'process_content_update_baseline'),
    ('flow_content_update_projection', 'flow_content_update_baseline'),
    ('process_embedding_update_source_hnsw', 'process_embedding_update_baseline'),
    ('flow_embedding_update_source_hnsw', 'flow_embedding_update_baseline')
)
select pairs.projection_mode,
  pg_catalog.round(projection.p95_ms::numeric, 3) as projection_p95_ms,
  pg_catalog.round(baseline.p95_ms::numeric, 3) as baseline_p95_ms,
  pg_catalog.round(
    (projection.p95_ms - baseline.p95_ms)::numeric,
    3
  ) as delta_ms,
  pg_catalog.round(
    (projection.p95_ms / nullif(baseline.p95_ms, 0))::numeric,
    3
  ) as ratio
from pairs
join summary as projection on projection.mode = pairs.projection_mode
join summary as baseline on baseline.mode = pairs.baseline_mode
order by pairs.projection_mode;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at,
  search_text,
  embedding_ft,
  model_id
)
select
  pg_temp.portal_bench_uuid('process', series.ordinal),
  '01.00.000',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  case when series.ordinal % 5 = 0 then 200 else 100 end,
  true,
  '2026-08-26 00:00:00+00'::timestamptz
    + pg_catalog.make_interval(secs => series.ordinal::double precision / 1000),
  null,
  case when series.ordinal <= :process_vector_rows
    then case when series.ordinal = 1
      then pg_temp.portal_bench_far_vector()
      else pg_temp.portal_bench_vector(series.ordinal)
    end
    else null
  end,
  null
from pg_catalog.generate_series(1, :process_rows) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_process_payload(series.ordinal) as value
) as payload;

insert into public.flows (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  rule_verification,
  modified_at,
  search_text,
  embedding_ft
)
select
  pg_temp.portal_bench_uuid('flow', series.ordinal),
  '01.00.000',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  case when series.ordinal % 5 = 0 then 200 else 100 end,
  true,
  '2026-08-26 00:00:00+00'::timestamptz
    + pg_catalog.make_interval(secs => series.ordinal::double precision / 1000),
  null,
  case when series.ordinal <= :flow_vector_rows
    then case when series.ordinal = 1
      then pg_temp.portal_bench_far_vector()
      else pg_temp.portal_bench_vector(series.ordinal)
    end
    else null
  end
from pg_catalog.generate_series(1, :flow_rows) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_flow_payload(series.ordinal) as value
) as payload;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
select
  pg_temp.portal_bench_uuid('process', series.ordinal),
  '00.99.999',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  100,
  true,
  '2026-08-25 00:00:00+00'::timestamptz,
  null,
  pg_temp.portal_bench_vector(1),
  null
from pg_catalog.generate_series(
  1,
  least(:process_rows, :process_old_version_rows)
) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_process_payload(series.ordinal) as value
) as payload;

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft
)
select
  pg_temp.portal_bench_uuid('flow', series.ordinal),
  '00.99.999',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  100,
  true,
  '2026-08-25 00:00:00+00'::timestamptz,
  null,
  pg_temp.portal_bench_vector(1)
from pg_catalog.generate_series(
  1,
  least(:flow_rows, :flow_old_version_rows)
) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_flow_payload(series.ordinal) as value
) as payload;

insert into public.processes (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft, model_id
)
select
  pg_temp.portal_bench_uuid('process', series.ordinal),
  '99.99.999',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  0,
  true,
  '2026-08-27 00:00:00+00'::timestamptz,
  null,
  pg_temp.portal_bench_vector(1),
  null
from pg_catalog.generate_series(
  1,
  least(:process_rows, :draft_vector_rows)
) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_process_payload(series.ordinal) as value
) as payload;

insert into public.flows (
  id, version, json, json_ordered, user_id, state_code,
  rule_verification, modified_at, search_text, embedding_ft
)
select
  pg_temp.portal_bench_uuid('flow', series.ordinal),
  '99.99.999',
  payload.value,
  payload.value::json,
  '53100000-0000-4000-8000-000000000001'::uuid,
  0,
  true,
  '2026-08-27 00:00:00+00'::timestamptz,
  null,
  pg_temp.portal_bench_vector(1)
from pg_catalog.generate_series(
  1,
  least(:flow_rows, :draft_vector_rows)
) as series(ordinal)
cross join lateral (
  select pg_temp.portal_bench_flow_payload(series.ordinal) as value
) as payload;

alter table public.processes enable trigger user;
alter table public.flows enable trigger user;

analyze public.processes;
analyze public.flows;
analyze private.portal_catalog_search_rows_v1;

create temporary table portal_benchmark_fence_metrics (
  metric text primary key,
  elapsed_ms double precision not null
) on commit drop;

do $measure_portal_projection_fence$
declare
  v_started timestamptz := pg_catalog.clock_timestamp();
  v_probe bigint;
begin
  lock table public.processes, public.flows in share row exclusive mode;

  select count(*) into v_probe
  from public.processes as process
  where process.state_code in (100, 200)
    and process.modified_at is not null
    and pg_catalog.jsonb_typeof(process.json) = 'object'
    and pg_catalog.jsonb_typeof(process.json -> 'processDataSet') = 'object'
    and not exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = process.id
        and projection.version = process.version::text
    );

  select count(*) into v_probe
  from public.flows as flow
  where flow.state_code in (100, 200)
    and flow.modified_at is not null
    and pg_catalog.jsonb_typeof(flow.json) = 'object'
    and pg_catalog.jsonb_typeof(flow.json -> 'flowDataSet') = 'object'
    and not exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = flow.id
        and projection.version = flow.version::text
    );

  select count(*) into v_probe
  from private.portal_catalog_search_rows_v1 as projection
  left join public.processes as process
    on projection.dataset_kind = 'process'
   and process.id = projection.id
   and process.version::text = projection.version
  where projection.dataset_kind = 'process'
    and (
      process.id is null
      or process.state_code <> projection.state_code
      or process.modified_at <> projection.modified_at
    );

  select count(*) into v_probe
  from private.portal_catalog_search_rows_v1 as projection
  left join public.flows as flow
    on projection.dataset_kind = 'flow'
   and flow.id = projection.id
   and flow.version::text = projection.version
  where projection.dataset_kind = 'flow'
    and (
      flow.id is null
      or flow.state_code <> projection.state_code
      or flow.modified_at <> projection.modified_at
    );

  insert into pg_temp.portal_benchmark_fence_metrics values (
    'representative_fence_work',
    1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
  );
end
$measure_portal_projection_fence$;

select metric,
  pg_catalog.round(elapsed_ms::numeric, 3) as elapsed_ms
from pg_temp.portal_benchmark_fence_metrics;

select pg_catalog.jsonb_build_object(
  'profile', 'portal-candidate-first-local-v1',
  'process_rows', (select count(*) from public.processes),
  'flow_rows', (select count(*) from public.flows),
  'process_vectors', (
    select count(*) from public.processes where embedding_ft is not null
  ),
  'flow_vectors', (
    select count(*) from public.flows where embedding_ft is not null
  ),
  'projection_rows', (
    select count(*) from private.portal_catalog_search_rows_v1
  ),
  'process_latest_cards', (
    select count(distinct id)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'process'
  ),
  'flow_latest_cards', (
    select count(distinct id)
    from private.portal_catalog_search_rows_v1
    where dataset_kind = 'flow'
  ),
  'process_common_candidates', (
    select count(*)
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.version = '01.00.000'
      and projection.document
        like '%portalbenchcommon electricity%' escape E'\\'
  ),
  'flow_common_candidates', (
    select count(*)
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.version = '01.00.000'
      and projection.document
        like '%portalbenchcommon electricity%' escape E'\\'
  ),
  'process_index_bytes', pg_catalog.pg_relation_size(
    'private.portal_catalog_search_process_document_v1_pgroonga'
  ),
  'flow_index_bytes', pg_catalog.pg_relation_size(
    'private.portal_catalog_search_flow_document_v1_pgroonga'
  ),
  'reused_process_hnsw_bytes', pg_catalog.pg_relation_size(
    'public.processes_embedding_ft_hnsw_idx'
  ),
  'reused_flow_hnsw_bytes', pg_catalog.pg_relation_size(
    'public.flows_embedding_ft_hnsw_idx'
  ),
  'projection_total_bytes', pg_catalog.pg_total_relation_size(
    'private.portal_catalog_search_rows_v1'
  ),
  'projection_heap_bytes', pg_catalog.pg_relation_size(
    'private.portal_catalog_search_rows_v1'
  ),
  'database_bytes_before_fixture', pg_catalog.current_setting(
    'portal.benchmark_database_bytes_before'
  )::bigint,
  'database_bytes_with_fixture', pg_catalog.pg_database_size(
    pg_catalog.current_database()
  ),
  'database_fixture_delta_bytes', pg_catalog.pg_database_size(
    pg_catalog.current_database()
  ) - pg_catalog.current_setting(
    'portal.benchmark_database_bytes_before'
  )::bigint,
  'pgroonga_storage_note',
    'pg_relation_size does not include external Groonga files; use isolated database phase delta and hosted before/after database size'
) as redacted_benchmark_metadata;

create temporary table portal_benchmark_plans (
  label text primary key,
  plan_text text not null
) on commit drop;

create or replace function pg_temp.capture_portal_benchmark_plan(
  p_label text,
  p_query text
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_line text;
  v_plan text := '';
begin
  for v_line in execute
    'explain (analyze, buffers, settings, summary, format text) ' || p_query
  loop
    v_plan := v_plan || v_line || pg_catalog.chr(10);
  end loop;
  insert into pg_temp.portal_benchmark_plans(label, plan_text)
  values (p_label, v_plan);
end
$function$;

grant insert, select on pg_temp.portal_benchmark_plans
  to portal_public_executor, api_internal_executor;
grant execute on function pg_temp.capture_portal_benchmark_plan(text, text)
  to portal_public_executor, api_internal_executor;

\pset format unaligned
\pset tuples_only on
\o :explain_output

grant execute on function pg_temp.portal_bench_vector(integer)
  to portal_public_executor, api_internal_executor;

grant portal_public_executor to postgres;
set local role portal_public_executor;

\qecho profile=process-projection-pgroonga
explain (analyze, buffers, settings, wal, summary, format json)
select projection.id
from private.portal_catalog_search_rows_v1 as projection
where projection.dataset_kind = 'process'
  and projection.document like '%portalbenchneedle%' escape E'\\'
order by projection.id
limit 200;

select pg_temp.capture_portal_benchmark_plan(
  'process_pgroonga',
  $query$
    select projection.id
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.document like '%portalbenchneedle%' escape E'\\'
    order by projection.id
    limit 200
  $query$
);

\qecho profile=flow-projection-pgroonga
explain (analyze, buffers, settings, wal, summary, format json)
select projection.id
from private.portal_catalog_search_rows_v1 as projection
where projection.dataset_kind = 'flow'
  and projection.document like '%portalbenchneedle%' escape E'\\'
order by projection.id
limit 200;

select pg_temp.capture_portal_benchmark_plan(
  'flow_pgroonga',
  $query$
    select projection.id
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.document like '%portalbenchneedle%' escape E'\\'
    order by projection.id
    limit 200
  $query$
);

reset role;
revoke portal_public_executor from postgres;

grant api_internal_executor to postgres;
set local role api_internal_executor;

set local enable_sort = off;
set local hnsw.iterative_scan = relaxed_order;
set local hnsw.ef_search = 1000;
set local hnsw.max_scan_tuples = 200000;
set local hnsw.scan_mem_multiplier = 4;

\qecho profile=process-source-hnsw-latest-public
explain (analyze, buffers, settings, wal, summary, format json)
select process.id
from public.processes as process
where process.state_code in (100, 200)
  and process.embedding_ft is not null
  and exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'process'
      and projection.id = process.id
      and projection.version = process.version::text
      and not exists (
        select 1
        from private.portal_catalog_search_rows_v1 as newer
        where newer.dataset_kind = projection.dataset_kind
          and newer.id = projection.id
          and (
            newer.version > projection.version
            or (
              newer.version = projection.version
              and newer.modified_at > projection.modified_at
            )
            or (
              newer.version = projection.version
              and newer.modified_at = projection.modified_at
              and newer.state_code > projection.state_code
            )
          )
      )
  )
order by process.embedding_ft
  operator(extensions.<=>) pg_temp.portal_bench_vector(1)
limit 200;

select pg_temp.capture_portal_benchmark_plan(
  'process_source_hnsw',
  $query$
    select process.id
    from public.processes as process
    where process.state_code in (100, 200)
      and process.embedding_ft is not null
      and exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'process'
          and projection.id = process.id
          and projection.version = process.version::text
          and not exists (
            select 1
            from private.portal_catalog_search_rows_v1 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
      )
    order by process.embedding_ft
      operator(extensions.<=>) pg_temp.portal_bench_vector(1)
    limit 200
  $query$
);

\if :benchmark_semantic_plan_profile
\qecho profile=process-semantic-helper-candidate-path
explain (analyze, buffers, settings, wal, summary, format json)
select candidate.*
from private.portal_projection_semantic_process_v1(
  pg_temp.portal_bench_vector(1)
) as candidate;

select pg_temp.capture_portal_benchmark_plan(
  'process_semantic_candidate_path',
  $query$
    select candidate.*
    from private.portal_projection_semantic_process_v1(
      pg_temp.portal_bench_vector(1)
    ) as candidate
  $query$
);
\endif

\qecho profile=flow-source-hnsw-latest-public
explain (analyze, buffers, settings, wal, summary, format json)
select flow.id
from public.flows as flow
where flow.state_code in (100, 200)
  and flow.embedding_ft is not null
  and exists (
    select 1
    from private.portal_catalog_search_rows_v1 as projection
    where projection.dataset_kind = 'flow'
      and projection.id = flow.id
      and projection.version = flow.version::text
      and not exists (
        select 1
        from private.portal_catalog_search_rows_v1 as newer
        where newer.dataset_kind = projection.dataset_kind
          and newer.id = projection.id
          and (
            newer.version > projection.version
            or (
              newer.version = projection.version
              and newer.modified_at > projection.modified_at
            )
            or (
              newer.version = projection.version
              and newer.modified_at = projection.modified_at
              and newer.state_code > projection.state_code
            )
          )
      )
  )
order by flow.embedding_ft
  operator(extensions.<=>) pg_temp.portal_bench_vector(1)
limit 200;

select pg_temp.capture_portal_benchmark_plan(
  'flow_source_hnsw',
  $query$
    select flow.id
    from public.flows as flow
    where flow.state_code in (100, 200)
      and flow.embedding_ft is not null
      and exists (
        select 1
        from private.portal_catalog_search_rows_v1 as projection
        where projection.dataset_kind = 'flow'
          and projection.id = flow.id
          and projection.version = flow.version::text
          and not exists (
            select 1
            from private.portal_catalog_search_rows_v1 as newer
            where newer.dataset_kind = projection.dataset_kind
              and newer.id = projection.id
              and (
                newer.version > projection.version
                or (
                  newer.version = projection.version
                  and newer.modified_at > projection.modified_at
                )
                or (
                  newer.version = projection.version
                  and newer.modified_at = projection.modified_at
                  and newer.state_code > projection.state_code
                )
              )
          )
      )
    order by flow.embedding_ft
      operator(extensions.<=>) pg_temp.portal_bench_vector(1)
    limit 200
  $query$
);

\if :benchmark_semantic_plan_profile
\qecho profile=flow-semantic-helper-candidate-path
explain (analyze, buffers, settings, wal, summary, format json)
select candidate.*
from private.portal_projection_semantic_flow_v1(
  pg_temp.portal_bench_vector(1)
) as candidate;

select pg_temp.capture_portal_benchmark_plan(
  'flow_semantic_candidate_path',
  $query$
    select candidate.*
    from private.portal_projection_semantic_flow_v1(
      pg_temp.portal_bench_vector(1)
    ) as candidate
  $query$
);
\endif

set local enable_sort = on;
set local hnsw.iterative_scan = strict_order;

reset role;
revoke api_internal_executor from postgres;

\o
\pset format aligned
\pset tuples_only off

create temporary table portal_benchmark_timings (
  label text not null,
  elapsed_ms double precision not null
) on commit drop;

create temporary table portal_benchmark_failures (
  label text primary key,
  sqlstate text not null,
  message text not null,
  elapsed_ms double precision not null
) on commit drop;

insert into pg_temp.portal_benchmark_failures (
  label, sqlstate, message, elapsed_ms
)
select
  'named_profile_fixture_cardinality',
  'P0001',
  'actual row/vector/old/draft cardinality differs from the named profile',
  0
where (
    :'benchmark_release_profile'::boolean
    or :'benchmark_sparse_profile'::boolean
  )
  and (
    (select count(*) from public.processes)
      <> :'process_rows'::integer
        + :'process_old_version_rows'::integer
        + :'draft_vector_rows'::integer
    or (select count(*) from public.flows)
      <> :'flow_rows'::integer
        + :'flow_old_version_rows'::integer
        + :'draft_vector_rows'::integer
    or (select count(*) from public.processes where embedding_ft is not null)
      <> :'process_vector_rows'::integer
        + :'process_old_version_rows'::integer
        + :'draft_vector_rows'::integer
    or (select count(*) from public.flows where embedding_ft is not null)
      <> :'flow_vector_rows'::integer
        + :'flow_old_version_rows'::integer
        + :'draft_vector_rows'::integer
    or (select count(*) from public.processes where version = '00.99.999')
      <> :'process_old_version_rows'::integer
    or (select count(*) from public.flows where version = '00.99.999')
      <> :'flow_old_version_rows'::integer
    or (select count(*) from public.processes where version = '99.99.999')
      <> :'draft_vector_rows'::integer
    or (select count(*) from public.flows where version = '99.99.999')
      <> :'draft_vector_rows'::integer
  );

insert into pg_temp.portal_benchmark_failures (
  label, sqlstate, message, elapsed_ms
)
select
  'plan_index_guard',
  'P0001',
  'representative plan missed an exact PGroonga/full-source-HNSW index or used source Sort/SeqScan',
  0
where (:'process_rows'::integer >= 10000
    and :'flow_rows'::integer >= 100000)
  and (
    (select count(*) from pg_temp.portal_benchmark_plans) <> case
      when :'benchmark_semantic_plan_profile'::boolean then 6 else 4
    end
   or not coalesce((
     select plan_text ~ 'portal_catalog_search_process_document_v1_pgroonga'
     from pg_temp.portal_benchmark_plans
     where label = 'process_pgroonga'
   ), false)
   or not coalesce((
     select plan_text ~ 'portal_catalog_search_flow_document_v1_pgroonga'
     from pg_temp.portal_benchmark_plans
     where label = 'flow_pgroonga'
   ), false)
   or not coalesce((
     select plan_text ~ 'processes_embedding_ft_hnsw_idx'
       and plan_text !~ 'processes_embedding_ft_tg_hnsw_idx'
       and plan_text !~ 'Seq Scan on processes'
       and plan_text !~ '(^|\n)[[:space:]]*Sort[[:space:]]'
     from pg_temp.portal_benchmark_plans
     where label = 'process_source_hnsw'
   ), false)
   or not coalesce((
     select plan_text ~ 'flows_embedding_ft_hnsw_idx'
       and plan_text !~ 'Seq Scan on flows'
       and plan_text !~ '(^|\n)[[:space:]]*Sort[[:space:]]'
     from pg_temp.portal_benchmark_plans
     where label = 'flow_source_hnsw'
   ), false)
   or (
     :'benchmark_semantic_plan_profile'::boolean
     and (
       (select count(*)
        from pg_temp.portal_benchmark_plans
        where label in (
          'process_semantic_candidate_path',
          'flow_semantic_candidate_path'
        )) <> 2
       or exists (
         select 1
         from pg_temp.portal_benchmark_plans
          where label in (
             'process_semantic_candidate_path',
             'flow_semantic_candidate_path'
           )
           and (
             plan_text ~ 'temp (read|written)=[1-9]'
             or plan_text ~ 'Disk:'
             or plan_text ~ 'external merge'
           )
       )
     )
   )
  );

select label,
  plan_text ~ 'portal_catalog_search_process_document_v1_pgroonga'
    as process_pgroonga,
  plan_text ~ 'portal_catalog_search_flow_document_v1_pgroonga'
    as flow_pgroonga,
  plan_text ~ 'processes_embedding_ft_hnsw_idx' as process_hnsw,
  plan_text ~ 'flows_embedding_ft_hnsw_idx' as flow_hnsw,
  plan_text ~ 'temp (read|written)=[1-9]' as nonzero_temp,
  plan_text ~ 'Disk:|external merge' as disk_sort
from pg_temp.portal_benchmark_plans
order by label;

create or replace function pg_temp.record_portal_search_timing(
  p_label text,
  p_kind text,
  p_query text,
  p_filters jsonb default '{}'::jsonb,
  p_sort text default 'relevance'
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_started timestamptz := pg_catalog.clock_timestamp();
  v_result jsonb;
begin
  if exists (
    select 1 from pg_temp.portal_benchmark_failures where label = p_label
  ) then
    return;
  end if;
  begin
    if p_kind = 'process' then
      v_result := api.portal_search_processes_v1(
        p_query, p_filters, p_sort, null, 50
      );
    elsif p_kind = 'flow' then
      v_result := api.portal_search_flows_v1(
        p_query, p_filters, p_sort, null, 50
      );
    else
      raise exception 'unsupported benchmark kind';
    end if;
    if v_result is null
       or v_result ->> 'schemaVersion' <> 'portal.public-search-page.v1'
       or pg_catalog.jsonb_typeof(v_result -> 'items') <> 'array'
       or pg_catalog.jsonb_array_length(v_result -> 'items') > 50 then
      raise exception 'invalid search result';
    end if;
    insert into pg_temp.portal_benchmark_timings values (
      p_label,
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  exception
    when others then
      insert into pg_temp.portal_benchmark_failures values (
        p_label,
        sqlstate,
        sqlerrm,
        1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
      ) on conflict (label) do nothing;
  end;
end
$function$;

create or replace function pg_temp.record_portal_search_page2_timing(
  p_label text,
  p_kind text,
  p_query text,
  p_filters jsonb,
  p_sort text
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_started timestamptz;
  v_first_page jsonb;
  v_result jsonb;
  v_cursor text;
begin
  if exists (
    select 1 from pg_temp.portal_benchmark_failures where label = p_label
  ) then
    return;
  end if;
  begin
    if p_kind = 'process' then
      v_first_page := api.portal_search_processes_v1(
        p_query, p_filters, p_sort, null, 50
      );
    elsif p_kind = 'flow' then
      v_first_page := api.portal_search_flows_v1(
        p_query, p_filters, p_sort, null, 50
      );
    else
      raise exception 'unsupported benchmark kind';
    end if;
    v_cursor := v_first_page ->> 'nextCursor';
    if v_first_page ->> 'schemaVersion' <> 'portal.public-search-page.v1'
       or v_cursor is null
       or v_cursor = '' then
      raise exception 'invalid search first page';
    end if;

    v_started := pg_catalog.clock_timestamp();
    if p_kind = 'process' then
      v_result := api.portal_search_processes_v1(
        p_query, p_filters, p_sort, v_cursor, 50
      );
    else
      v_result := api.portal_search_flows_v1(
        p_query, p_filters, p_sort, v_cursor, 50
      );
    end if;
    if v_result is null
       or v_result ->> 'schemaVersion' <> 'portal.public-search-page.v1'
       or pg_catalog.jsonb_typeof(v_result -> 'items') <> 'array'
       or pg_catalog.jsonb_array_length(v_result -> 'items') > 50 then
      raise exception 'invalid search second page';
    end if;
    insert into pg_temp.portal_benchmark_timings values (
      p_label,
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  exception
    when others then
      insert into pg_temp.portal_benchmark_failures values (
        p_label,
        sqlstate,
        sqlerrm,
        case when v_started is null then 0 else
          1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
        end
      ) on conflict (label) do nothing;
  end;
end
$function$;

create or replace function pg_temp.record_portal_facet_timing(
  p_label text,
  p_kind text,
  p_query text,
  p_filters jsonb
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_started timestamptz := pg_catalog.clock_timestamp();
  v_result jsonb;
begin
  if exists (
    select 1 from pg_temp.portal_benchmark_failures where label = p_label
  ) then
    return;
  end if;
  begin
    v_result := api.portal_facets_v1(p_kind, p_query, p_filters);
    if v_result is null or v_result ->> 'schemaVersion'
       <> 'portal.public-facets.v1' then
      raise exception 'invalid facet result';
    end if;
    insert into pg_temp.portal_benchmark_timings values (
      p_label,
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  exception
    when others then
      insert into pg_temp.portal_benchmark_failures values (
        p_label,
        sqlstate,
        sqlerrm,
        1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
      ) on conflict (label) do nothing;
  end;
end
$function$;

create or replace function pg_temp.record_portal_hybrid_timing(
  p_label text,
  p_kind text,
  p_query_term text,
  p_query_vector text
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_started timestamptz := pg_catalog.clock_timestamp();
  v_result jsonb;
begin
  if exists (
    select 1 from pg_temp.portal_benchmark_failures where label = p_label
  ) then
    return;
  end if;
  begin
    v_result := api.portal_hybrid_search_v1(
      p_kind,
      array[p_query_term],
      p_query_vector,
      '{}'::jsonb,
      20
    );
    if v_result is null
       or v_result ->> 'schemaVersion'
          <> 'portal.public-hybrid-candidate-page.v1'
       or pg_catalog.jsonb_typeof(v_result -> 'items') <> 'array'
       or pg_catalog.jsonb_array_length(v_result -> 'items') > 20
       or pg_catalog.octet_length(
         pg_catalog.convert_to(v_result::text, 'UTF8')
       ) > 524288 then
      raise exception 'invalid hybrid result';
    end if;
    insert into pg_temp.portal_benchmark_timings values (
      p_label,
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  exception
    when others then
      insert into pg_temp.portal_benchmark_failures values (
        p_label,
        sqlstate,
        sqlerrm,
        1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
      ) on conflict (label) do nothing;
  end;
end
$function$;

create or replace function pg_temp.record_portal_hybrid_terms_timing(
  p_label text,
  p_kind text,
  p_query_terms text[],
  p_query_vector text,
  p_filters jsonb default '{}'::jsonb
)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_started timestamptz := pg_catalog.clock_timestamp();
  v_result jsonb;
begin
  if exists (
    select 1 from pg_temp.portal_benchmark_failures where label = p_label
  ) then
    return;
  end if;
  begin
    v_result := api.portal_hybrid_search_v1(
      p_kind,
      p_query_terms,
      p_query_vector,
      p_filters,
      20
    );
    if v_result is null
       or v_result ->> 'schemaVersion'
          <> 'portal.public-hybrid-candidate-page.v1'
       or pg_catalog.jsonb_typeof(v_result -> 'items') <> 'array'
       or pg_catalog.jsonb_array_length(v_result -> 'items') > 20
       or pg_catalog.octet_length(
         pg_catalog.convert_to(v_result::text, 'UTF8')
       ) > 524288 then
      raise exception 'invalid hybrid result';
    end if;
    insert into pg_temp.portal_benchmark_timings values (
      p_label,
      1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
    );
  exception
    when others then
      insert into pg_temp.portal_benchmark_failures values (
        p_label,
        sqlstate,
        sqlerrm,
        1000 * extract(epoch from pg_catalog.clock_timestamp() - v_started)
      ) on conflict (label) do nothing;
  end;
end
$function$;

create or replace function pg_temp.run_portal_candidate_timing(p_samples integer)
returns void
language plpgsql
set search_path = ''
as $function$
declare
  v_iteration integer;
  v_query_vector text := pg_temp.portal_bench_vector(1)::text;
  v_far_vector text := pg_temp.portal_bench_far_vector()::text;
  v_zero_vector text := '[' || pg_catalog.array_to_string(
    pg_catalog.array_fill('0'::text, array[1024]),
    ','
  ) || ']';
begin
  if p_samples not between 1 and 100 then
    raise exception 'benchmark_samples must be between 1 and 100';
  end if;
  for v_iteration in 1..p_samples loop
    perform pg_temp.record_portal_search_timing(
      'process_exact', 'process', 'portalbenchneedle'
    );
    perform pg_temp.record_portal_search_timing(
      'process_common', 'process', 'portalbenchcommon electricity'
    );
    perform pg_temp.record_portal_search_timing(
      'process_no_hit', 'process', 'portalbench-no-hit-531'
    );
    perform pg_temp.record_portal_search_timing(
      'process_identifier',
      'process',
      pg_temp.portal_bench_uuid('process', 1)::text
    );
    perform pg_temp.record_portal_search_timing(
      'process_empty', 'process', ''
    );
    perform pg_temp.record_portal_search_timing(
      'flow_exact', 'flow', 'portalbenchneedle'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_common', 'flow', 'portalbenchcommon electricity'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_no_hit', 'flow', 'portalbench-no-hit-531'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_identifier',
      'flow',
      pg_temp.portal_bench_uuid('flow', 1)::text
    );
    perform pg_temp.record_portal_search_timing(
      'flow_empty', 'flow', ''
    );
    perform pg_temp.record_portal_search_timing(
      'process_filtered_broad',
      'process',
      '',
      '{"geography":"cn"}'::jsonb,
      'relevance'
    );
    perform pg_temp.record_portal_search_timing(
      'process_filtered_selective',
      'process',
      '',
      '{"accessLevel":"metadata_only","geography":"cn","classification":"portal-bench","referenceYearFrom":2024,"referenceYearTo":2024,"processSubtype":"unit process, single operation","source":"synthetic benchmark provider"}'::jsonb,
      'relevance'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_filtered_broad',
      'flow',
      '',
      '{"geography":"cn"}'::jsonb,
      'relevance'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_filtered_selective',
      'flow',
      '',
      '{"accessLevel":"metadata_only","geography":"cn","classification":"portal-bench","source":"synthetic benchmark provider"}'::jsonb,
      'relevance'
    );
    perform pg_temp.record_portal_search_timing(
      'process_name_asc_empty', 'process', '', '{}'::jsonb, 'name_asc'
    );
    perform pg_temp.record_portal_search_timing(
      'flow_name_asc_empty', 'flow', '', '{}'::jsonb, 'name_asc'
    );
    perform pg_temp.record_portal_search_page2_timing(
      'flow_name_asc_page2', 'flow', '', '{}'::jsonb, 'name_asc'
    );
    perform pg_temp.record_portal_search_page2_timing(
      'flow_filtered_relevance_page2',
      'flow',
      '',
      '{"accessLevel":"metadata_only","geography":"cn","classification":"portal-bench","source":"synthetic benchmark provider"}'::jsonb,
      'relevance'
    );
    perform pg_temp.record_portal_hybrid_timing(
      'process_hybrid_fused',
      'process',
      'portalbenchcommon electricity',
      v_query_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'flow_hybrid_fused',
      'flow',
      'portalbenchcommon electricity',
      v_query_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'process_hybrid_semantic_only',
      'process',
      'portalbench-semantic-only-no-lexical-hit',
      v_query_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'flow_hybrid_semantic_only',
      'flow',
      'portalbench-semantic-only-no-lexical-hit',
      v_query_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'process_hybrid_lexical_only',
      'process',
      'portalbenchcommon electricity',
      v_far_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'flow_hybrid_lexical_only',
      'flow',
      'portalbenchcommon electricity',
      v_far_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'process_hybrid_zero_boundary',
      'process',
      'portalbenchcommon electricity',
      v_zero_vector
    );
    perform pg_temp.record_portal_hybrid_timing(
      'flow_hybrid_zero_boundary',
      'flow',
      'portalbenchcommon electricity',
      v_zero_vector
    );
    perform pg_temp.record_portal_hybrid_terms_timing(
      'process_hybrid_max_terms',
      'process',
      array[
        'synthetic', 'benchmark', 'summary', 'portal-bench', 'cn',
        '2024', 'provider', 'process', 'portalbenchnoise',
        'portalbenchcommon', 'electricity', 'technology'
      ],
      v_query_vector
    );
    perform pg_temp.record_portal_hybrid_terms_timing(
      'flow_hybrid_max_terms',
      'flow',
      array[
        'synthetic', 'benchmark', 'summary', 'portal-bench', 'cn',
        '50-00-0', 'provider', 'flow', 'portalbenchnoise',
        'portalbenchcommon', 'electricity', 'synthetic benchmark provider'
      ],
      v_query_vector
    );
    perform pg_temp.record_portal_facet_timing(
      'process_facets_empty', 'process', '', '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'process_facets_common',
      'process',
      'portalbenchcommon electricity',
      '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'process_facets_filtered',
      'process',
      '',
      '{"accessLevel":"metadata_only"}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'process_facets_no_hit',
      'process',
      'portalbench-no-hit-531',
      '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'flow_facets_empty', 'flow', '', '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'flow_facets_common',
      'flow',
      'portalbenchcommon electricity',
      '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'flow_facets_filtered',
      'flow',
      '',
      '{"accessLevel":"metadata_only"}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'flow_facets_no_hit',
      'flow',
      'portalbench-no-hit-531',
      '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'all_facets_empty', 'all', '', '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'all_facets_filtered',
      'all',
      '',
      '{"accessLevel":"metadata_only"}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'all_facets_common',
      'all',
      'portalbenchcommon electricity',
      '{}'::jsonb
    );
    perform pg_temp.record_portal_facet_timing(
      'all_facets_no_hit',
      'all',
      'portalbench-no-hit-531',
      '{}'::jsonb
    );
  end loop;
end
$function$;

grant select, insert on pg_temp.portal_benchmark_timings,
  pg_temp.portal_benchmark_failures
  to anon;
grant execute on function pg_temp.record_portal_search_timing(
  text, text, text, jsonb, text
)
  to anon;
grant execute on function pg_temp.record_portal_search_page2_timing(
  text, text, text, jsonb, text
) to anon;
grant execute on function pg_temp.record_portal_facet_timing(
  text, text, text, jsonb
) to anon;
grant execute on function pg_temp.record_portal_hybrid_timing(
  text, text, text, text
)
  to anon;
grant execute on function pg_temp.record_portal_hybrid_terms_timing(
  text, text, text[], text, jsonb
) to anon;
grant execute on function pg_temp.run_portal_candidate_timing(integer)
  to anon;
grant execute on function pg_temp.portal_bench_uuid(text, integer),
  pg_temp.portal_bench_vector(integer),
  pg_temp.portal_bench_far_vector()
  to anon;
set local role anon;
select pg_temp.run_portal_candidate_timing(1);
reset role;
truncate table pg_temp.portal_benchmark_timings;
set local role anon;
select pg_temp.run_portal_candidate_timing(:benchmark_samples);
reset role;

create temporary table portal_benchmark_semantic_probe (
  dataset_kind text primary key,
  payload jsonb not null
) on commit drop;
grant insert, select on pg_temp.portal_benchmark_semantic_probe to anon;
set local role anon;
insert into pg_temp.portal_benchmark_semantic_probe (dataset_kind, payload)
values
  (
    'process',
    api.portal_hybrid_search_v1(
      'process',
      array['portalbench-semantic-only-no-lexical-hit'],
      pg_temp.portal_bench_vector(1)::text,
      '{}'::jsonb,
      20
    )
  ),
  (
    'flow',
    api.portal_hybrid_search_v1(
      'flow',
      array['portalbench-semantic-only-no-lexical-hit'],
      pg_temp.portal_bench_vector(1)::text,
      '{}'::jsonb,
      20
    )
  );
reset role;

create temporary table portal_benchmark_zero_vector_probe (
  dataset_kind text primary key,
  payload jsonb not null
) on commit drop;
grant insert, select on pg_temp.portal_benchmark_zero_vector_probe to anon;
set local role anon;
insert into pg_temp.portal_benchmark_zero_vector_probe (dataset_kind, payload)
values
  (
    'process',
    api.portal_hybrid_search_v1(
      'process',
      array['portalbenchcommon electricity'],
      '[' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1024]),
        ','
      ) || ']',
      '{}'::jsonb,
      20
    )
  ),
  (
    'flow',
    api.portal_hybrid_search_v1(
      'flow',
      array['portalbenchcommon electricity'],
      '[' || pg_catalog.array_to_string(
        pg_catalog.array_fill('0'::text, array[1024]),
        ','
      ) || ']',
      '{}'::jsonb,
      20
    )
  );
reset role;

create temporary table portal_benchmark_recall (
  dataset_kind text not null,
  requested_k integer not null,
  exact_count integer not null,
  matched_count integer not null,
  ann_count integer not null,
  false_positive_count integer not null,
  recall numeric not null,
  primary key (dataset_kind, requested_k)
) on commit drop;
grant insert, select on pg_temp.portal_benchmark_recall
  to api_internal_executor;

grant api_internal_executor to postgres;
set local role api_internal_executor;

create temporary table portal_exact_process on commit drop as
with eligible as materialized (
  select process.id,
    process.version::text as version,
    process.embedding_ft operator(extensions.<=>)
      pg_temp.portal_bench_vector(1) as semantic_distance
  from public.processes as process
  where process.state_code in (100, 200)
    and process.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'process'
        and projection.id = process.id
        and projection.version = process.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
)
select eligible.*,
  pg_catalog.row_number() over (
    order by eligible.semantic_distance, eligible.id, eligible.version desc
  )::integer as exact_rank
from eligible
where eligible.semantic_distance between 0::double precision
  and 0.5::double precision
order by eligible.semantic_distance, eligible.id, eligible.version desc
limit 200;

create temporary table portal_ann_process on commit drop as
select semantic.*,
  pg_catalog.row_number() over (
    order by semantic.semantic_distance, semantic.id, semantic.version desc
  )::integer as ann_rank
from private.portal_projection_semantic_process_v1(
  pg_temp.portal_bench_vector(1)
) as semantic;

create temporary table portal_exact_flow on commit drop as
with eligible as materialized (
  select flow.id,
    flow.version::text as version,
    flow.embedding_ft operator(extensions.<=>)
      pg_temp.portal_bench_vector(1) as semantic_distance
  from public.flows as flow
  where flow.state_code in (100, 200)
    and flow.embedding_ft is not null
    and exists (
      select 1
      from private.portal_catalog_search_rows_v1 as projection
      where projection.dataset_kind = 'flow'
        and projection.id = flow.id
        and projection.version = flow.version::text
        and not exists (
          select 1
          from private.portal_catalog_search_rows_v1 as newer
          where newer.dataset_kind = projection.dataset_kind
            and newer.id = projection.id
            and (
              newer.version > projection.version
              or (
                newer.version = projection.version
                and newer.modified_at > projection.modified_at
              )
              or (
                newer.version = projection.version
                and newer.modified_at = projection.modified_at
                and newer.state_code > projection.state_code
              )
            )
        )
    )
)
select eligible.*,
  pg_catalog.row_number() over (
    order by eligible.semantic_distance, eligible.id, eligible.version desc
  )::integer as exact_rank
from eligible
where eligible.semantic_distance between 0::double precision
  and 0.5::double precision
order by eligible.semantic_distance, eligible.id, eligible.version desc
limit 200;

create temporary table portal_ann_flow on commit drop as
select semantic.*,
  pg_catalog.row_number() over (
    order by semantic.semantic_distance, semantic.id, semantic.version desc
  )::integer as ann_rank
from private.portal_projection_semantic_flow_v1(
  pg_temp.portal_bench_vector(1)
) as semantic;

insert into pg_temp.portal_benchmark_recall (
  dataset_kind,
  requested_k,
  exact_count,
  matched_count,
  ann_count,
  false_positive_count,
  recall
)
select kind.value,
  level.value,
  exact.expected_count,
  matched.value,
  ann.value,
  false_positive.value,
  case when exact.expected_count = 0 then 1::numeric
    else matched.value::numeric / exact.expected_count
  end
from (values ('process'::text), ('flow'::text)) as kind(value)
cross join (values (20), (200)) as level(value)
cross join lateral (
  select case kind.value
    when 'process' then (
      select count(*)::integer
      from pg_temp.portal_exact_process
      where exact_rank <= level.value
    )
    else (
      select count(*)::integer
      from pg_temp.portal_exact_flow
      where exact_rank <= level.value
    )
  end as expected_count
) as exact
cross join lateral (
  select case kind.value
    when 'process' then (
      select count(*)::integer
      from pg_temp.portal_exact_process as expected
      where expected.exact_rank <= level.value
        and exists (
          select 1
          from pg_temp.portal_ann_process as actual
          where actual.id = expected.id
            and actual.version = expected.version
            and actual.ann_rank <= level.value
        )
    )
    else (
      select count(*)::integer
      from pg_temp.portal_exact_flow as expected
      where expected.exact_rank <= level.value
        and exists (
          select 1
          from pg_temp.portal_ann_flow as actual
          where actual.id = expected.id
            and actual.version = expected.version
            and actual.ann_rank <= level.value
        )
    )
  end as value
) as matched
cross join lateral (
  select case kind.value
    when 'process' then (
      select count(*)::integer
      from pg_temp.portal_ann_process
      where ann_rank <= level.value
    )
    else (
      select count(*)::integer
      from pg_temp.portal_ann_flow
      where ann_rank <= level.value
    )
  end as value
) as ann
cross join lateral (
  select case kind.value
    when 'process' then (
      select count(*)::integer
      from pg_temp.portal_ann_process as actual
      where actual.ann_rank <= level.value
        and not exists (
          select 1
          from public.processes as process
          where process.id = actual.id
            and process.version::text = actual.version
            and process.state_code in (100, 200)
            and process.embedding_ft is not null
            and exists (
              select 1
              from private.portal_catalog_search_rows_v1 as projection
              where projection.dataset_kind = 'process'
                and projection.id = process.id
                and projection.version = process.version::text
                and not exists (
                  select 1
                  from private.portal_catalog_search_rows_v1 as newer
                  where newer.dataset_kind = projection.dataset_kind
                    and newer.id = projection.id
                    and (
                      newer.version > projection.version
                      or (
                        newer.version = projection.version
                        and newer.modified_at > projection.modified_at
                      )
                      or (
                        newer.version = projection.version
                        and newer.modified_at = projection.modified_at
                        and newer.state_code > projection.state_code
                      )
                    )
                )
            )
        )
    )
    else (
      select count(*)::integer
      from pg_temp.portal_ann_flow as actual
      where actual.ann_rank <= level.value
        and not exists (
          select 1
          from public.flows as flow
          where flow.id = actual.id
            and flow.version::text = actual.version
            and flow.state_code in (100, 200)
            and flow.embedding_ft is not null
            and exists (
              select 1
              from private.portal_catalog_search_rows_v1 as projection
              where projection.dataset_kind = 'flow'
                and projection.id = flow.id
                and projection.version = flow.version::text
                and not exists (
                  select 1
                  from private.portal_catalog_search_rows_v1 as newer
                  where newer.dataset_kind = projection.dataset_kind
                    and newer.id = projection.id
                    and (
                      newer.version > projection.version
                      or (
                        newer.version = projection.version
                        and newer.modified_at > projection.modified_at
                      )
                      or (
                        newer.version = projection.version
                        and newer.modified_at = projection.modified_at
                        and newer.state_code > projection.state_code
                      )
                    )
                )
            )
        )
    )
  end as value
) as false_positive;

reset role;
revoke api_internal_executor from postgres;

select dataset_kind,
  requested_k,
  exact_count,
  matched_count,
  ann_count,
  false_positive_count,
  pg_catalog.round(recall, 6) as recall
from pg_temp.portal_benchmark_recall
order by dataset_kind, requested_k;

insert into pg_temp.portal_benchmark_failures (
  label, sqlstate, message, elapsed_ms
)
select
  'semantic_latest_version_probe',
  'P0001',
  'semantic-only Hybrid violated the zero-vector or latest-visible contract',
  0
where exists (
    select 1
    from pg_temp.portal_benchmark_semantic_probe as probe
    where case
      when (
        probe.dataset_kind = 'process'
        and :'process_vector_rows'::integer = 0
      ) or (
        probe.dataset_kind = 'flow'
        and :'flow_vector_rows'::integer = 0
      ) then pg_catalog.jsonb_array_length(probe.payload -> 'items') <> 0
      else pg_catalog.jsonb_array_length(probe.payload -> 'items') = 0
        or exists (
          select 1
          from pg_catalog.jsonb_array_elements(
            probe.payload -> 'items'
          ) as item(value)
          where item.value #>> '{key,version}' = '00.99.999'
             or item.value #>> '{key,id}' = pg_temp.portal_bench_uuid(
               probe.dataset_kind,
               1
             )::text
        )
    end
);

insert into pg_temp.portal_benchmark_failures (
  label, sqlstate, message, elapsed_ms
)
select
  'zero_vector_actual_evidence',
  'P0001',
  'zero query vector emitted semantic evidence or lost lexical evidence',
  0
where exists (
  select 1
  from pg_temp.portal_benchmark_zero_vector_probe as probe
  where pg_catalog.jsonb_array_length(probe.payload -> 'items') = 0
     or exists (
       select 1
       from pg_catalog.jsonb_array_elements(
         probe.payload -> 'items'
       ) as item(value)
       where not (item.value #> '{match,reasonCodes}'
         ? 'lexical_public_projection')
          or item.value #> '{match,reasonCodes}'
            ? 'semantic_public_projection'
          or item.value #> '{match,evidence,semanticRank}'
            is distinct from 'null'::jsonb
          or item.value #> '{match,evidence,semanticDistance}'
            is distinct from 'null'::jsonb
     )
);

insert into pg_temp.portal_benchmark_failures (
  label, sqlstate, message, elapsed_ms
)
select
  'semantic_ann_recall',
  'P0001',
  'source-HNSW recall/count differs from exact latest-visible top-k',
  0
where exists (
  select 1
  from pg_temp.portal_benchmark_recall
  where recall < 0.95::numeric
     or ann_count < pg_catalog.ceil(exact_count * 0.95)::integer
     or ann_count > requested_k
     or false_positive_count <> 0
     or (
       :'benchmark_release_profile'::boolean
       and requested_k = 200
       and exact_count <> 200
     )
     or (
       :'benchmark_sparse_profile'::boolean
       and :'process_vector_rows'::integer = 0
       and dataset_kind = 'process'
       and (exact_count <> 0 or ann_count <> 0)
     )
     or (
       :'benchmark_sparse_profile'::boolean
       and :'flow_vector_rows'::integer = 0
       and dataset_kind = 'flow'
       and (exact_count <> 0 or ann_count <> 0)
     )
     or (
       :'benchmark_sparse_profile'::boolean
       and requested_k = 200
       and :'process_vector_rows'::integer = 199
       and dataset_kind = 'process'
       and (exact_count <> 198 or ann_count <> 198 or recall <> 1::numeric)
     )
     or (
       :'benchmark_sparse_profile'::boolean
       and requested_k = 200
       and :'flow_vector_rows'::integer = 199
       and dataset_kind = 'flow'
       and (exact_count <> 198 or ann_count <> 198 or recall <> 1::numeric)
     )
);

select label,
  count(*) as samples,
  pg_catalog.round(min(elapsed_ms)::numeric, 3) as min_ms,
  pg_catalog.round(
    pg_catalog.percentile_cont(0.50) within group (order by elapsed_ms)::numeric,
    3
  ) as p50_ms,
  pg_catalog.round(
    pg_catalog.percentile_cont(0.95) within group (order by elapsed_ms)::numeric,
    3
  ) as p95_ms,
  pg_catalog.round(max(elapsed_ms)::numeric, 3) as max_ms
from portal_benchmark_timings
group by label
order by label;

select label,
  sqlstate,
  message,
  pg_catalog.round(elapsed_ms::numeric, 3) as elapsed_ms
from portal_benchmark_failures
order by label;

with expected(label) as (
  values
    ('process_exact'), ('process_common'), ('process_no_hit'),
    ('process_identifier'), ('process_empty'),
    ('flow_exact'), ('flow_common'), ('flow_no_hit'),
    ('flow_identifier'), ('flow_empty'),
    ('process_filtered_broad'), ('process_filtered_selective'),
    ('flow_filtered_broad'), ('flow_filtered_selective'),
    ('process_name_asc_empty'), ('flow_name_asc_empty'),
    ('flow_name_asc_page2'), ('flow_filtered_relevance_page2'),
    ('process_hybrid_fused'), ('flow_hybrid_fused'),
    ('process_hybrid_semantic_only'), ('flow_hybrid_semantic_only'),
    ('process_hybrid_lexical_only'), ('flow_hybrid_lexical_only'),
    ('process_hybrid_zero_boundary'), ('flow_hybrid_zero_boundary'),
    ('process_hybrid_max_terms'), ('flow_hybrid_max_terms'),
    ('process_facets_empty'), ('process_facets_common'),
    ('process_facets_filtered'), ('process_facets_no_hit'),
    ('flow_facets_empty'), ('flow_facets_common'),
    ('flow_facets_filtered'), ('flow_facets_no_hit'),
    ('all_facets_empty'), ('all_facets_filtered'),
    ('all_facets_common'), ('all_facets_no_hit')
), summary as (
  select label,
    count(*) as samples,
    pg_catalog.percentile_cont(0.95)
      within group (order by elapsed_ms) as p95_ms,
    max(elapsed_ms) as max_ms
  from portal_benchmark_timings
  group by label
), expected_writer(mode) as (
  values
    ('process_insert_baseline'),
    ('flow_insert_baseline'),
    ('process_content_update_baseline'),
    ('flow_content_update_baseline'),
    ('process_embedding_update_baseline'),
    ('flow_embedding_update_baseline'),
    ('process_insert_projection'),
    ('flow_insert_projection'),
    ('process_content_update_projection'),
    ('flow_content_update_projection'),
    ('process_embedding_update_source_hnsw'),
    ('flow_embedding_update_source_hnsw')
), writer_summary as (
  select mode,
    count(*) as samples,
    pg_catalog.percentile_cont(0.95)
      within group (order by elapsed_ms) as p95_ms
  from pg_temp.portal_benchmark_writer_timings
  group by mode
), writer_pairs(projection_mode, baseline_mode) as (
  values
    ('process_insert_projection'::text, 'process_insert_baseline'::text),
    ('flow_insert_projection', 'flow_insert_baseline'),
    ('process_content_update_projection', 'process_content_update_baseline'),
    ('flow_content_update_projection', 'flow_content_update_baseline'),
    ('process_embedding_update_source_hnsw', 'process_embedding_update_baseline'),
    ('flow_embedding_update_source_hnsw', 'flow_embedding_update_baseline')
)
select not exists (select 1 from portal_benchmark_failures)
  and coalesce((
    select elapsed_ms <= 5000
    from pg_temp.portal_benchmark_fence_metrics
    where metric = 'representative_fence_work'
  ), false)
  and not exists (
    select 1
    from expected
    left join summary using (label)
    where summary.label is null
       or summary.samples <> :'benchmark_samples'::integer
       or summary.p95_ms > case
         when summary.label like '%_hybrid_%' then 6000
         else 2000
       end
       or (
         summary.label like '%_hybrid_%'
         and summary.max_ms >= 8000
       )
  )
  and not exists (
    select 1
    from summary
    left join expected using (label)
    where expected.label is null
  )
  and not exists (
    select 1
    from expected_writer
    left join writer_summary using (mode)
    where writer_summary.mode is null
       or writer_summary.samples <> :'writer_samples'::integer
       or (expected_writer.mode not like '%_baseline'
           and writer_summary.p95_ms > 25)
  )
  and not exists (
    select 1
    from writer_pairs
    join writer_summary as projection
      on projection.mode = writer_pairs.projection_mode
    join writer_summary as baseline
      on baseline.mode = writer_pairs.baseline_mode
    where projection.p95_ms - baseline.p95_ms > 5
       or projection.p95_ms / nullif(baseline.p95_ms, 0) > 10
  ) as benchmark_pass
\gset

rollback;

-- ANALYZE statistics are not transactional.  Restore the real local table
-- statistics after the rollback removes every synthetic row.
analyze public.processes;
analyze public.flows;
analyze private.portal_catalog_search_rows_v1;

\if :benchmark_pass
  \if :benchmark_release_profile
    \echo 'SQL_STATUS=RELEASE_PASS'
  \elif :benchmark_sparse_profile
    \echo 'SQL_STATUS=SPARSE_PASS'
  \else
    \echo 'SQL_STATUS=DIAGNOSTIC_PASS'
  \endif
\else
  \echo 'ERROR: Portal projection benchmark failed its recall/plan/fence/writer or Search/Facet 2s and Hybrid 6s budgets'
  \echo 'SQL_STATUS=FAIL'
\endif
