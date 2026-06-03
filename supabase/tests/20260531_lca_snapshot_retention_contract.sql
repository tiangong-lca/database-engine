begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_empty_search_path(p_signature text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_proc p
    cross join lateral unnest(coalesce(p.proconfig, array[]::text[])) as config(setting)
    where p.oid = p_signature::regprocedure
      and config.setting in ('search_path=', 'search_path=""')
  );
$$;

select plan(28);

select ok(
  to_regprocedure('util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)') is not null,
  'snapshot retention preview function exists'
);

select ok(
  to_regprocedure('util.list_lca_snapshot_gc_candidates(interval,interval,timestamp with time zone,integer,integer,bigint)') is not null,
  'snapshot GC candidate function exists'
);

select ok(
  pg_temp.has_empty_search_path('util.preview_lca_snapshot_retention(interval,interval,timestamp with time zone)'),
  'snapshot retention preview function pins an empty search_path'
);

select ok(
  pg_temp.has_empty_search_path('util.list_lca_snapshot_gc_candidates(interval,interval,timestamp with time zone,integer,integer,bigint)'),
  'snapshot GC candidate function pins an empty search_path'
);

select ok(
  to_regclass('public.lca_snapshot_gc_runs') is not null,
  'snapshot GC run audit table exists'
);

select ok(
  to_regclass('public.lca_snapshot_gc_run_items') is not null,
  'snapshot GC run item audit table exists'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.lca_snapshot_gc_runs'::regclass),
  'snapshot GC run audit table has RLS enabled'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.lca_snapshot_gc_run_items'::regclass),
  'snapshot GC run item audit table has RLS enabled'
);

select ok(
  has_table_privilege('service_role', 'public.lca_snapshot_gc_runs', 'INSERT'),
  'service_role can insert snapshot GC run audit rows'
);

select ok(
  has_table_privilege('service_role', 'public.lca_snapshot_gc_run_items', 'INSERT'),
  'service_role can insert snapshot GC run item audit rows'
);

insert into storage.buckets (id, name, public)
values ('lca_results', 'lca_results', false)
on conflict (id) do nothing;

insert into public.lca_network_snapshots (
  id,
  process_filter,
  source_hash,
  status,
  created_at,
  updated_at
) values
  (
    '91530000-0000-4000-8000-000000000001',
    '{}'::jsonb,
    'active-hash',
    'ready',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91530000-0000-4000-8000-000000000002',
    jsonb_build_object(
      'artifact_lifecycle',
      jsonb_build_object('expires_at_utc', '2026-02-15T00:00:00Z')
    ),
    'ttl-future-hash',
    'ready',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91530000-0000-4000-8000-000000000003',
    jsonb_build_object(
      'artifact_lifecycle',
      jsonb_build_object('expires_at_utc', '2025-12-15T00:00:00Z')
    ),
    'ttl-expired-hash',
    'ready',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91530000-0000-4000-8000-000000000004',
    '{}'::jsonb,
    'default-old-hash',
    'ready',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91530000-0000-4000-8000-000000000005',
    '{}'::jsonb,
    'default-recent-hash',
    'ready',
    '2026-01-20 00:00:00+00',
    '2026-01-20 00:00:00+00'
  );

insert into public.lca_active_snapshots (
  scope,
  snapshot_id,
  source_hash,
  activated_at
) values (
  'pgtap_snapshot_gc',
  '91530000-0000-4000-8000-000000000001',
  'active-hash',
  '2025-12-02 00:00:00+00'
);

insert into public.worker_jobs (
  id,
  job_kind,
  worker_runtime,
  worker_queue,
  subject_type,
  subject_id,
  requester_type,
  requested_by,
  status,
  payload_schema_version,
  payload_json,
  result_schema_version,
  result_json,
  created_at,
  updated_at,
  finished_at
) values (
  '91530000-0000-4000-8000-000000000101',
  'lca.solve_one',
  'calculator',
  'solver',
  'lca_network_snapshot',
  '91530000-0000-4000-8000-000000000004',
  'user',
  '91530000-0000-4000-8000-000000000201',
  'completed',
  'lca.solve_one.request.v1',
  '{"snapshotId":"91530000-0000-4000-8000-000000000004"}'::jsonb,
  'lca.solve.result.v1',
  '{"snapshot":{"id":"91530000-0000-4000-8000-000000000004"}}'::jsonb,
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00'
);

insert into public.lca_results (
  id,
  job_id,
  worker_job_id,
  snapshot_id,
  payload,
  diagnostics,
  artifact_url,
  artifact_byte_size,
  created_at
) values (
  '91530000-0000-4000-8000-000000000102',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000004',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/result.json',
  15,
  '2025-12-01 00:00:00+00'
);

insert into public.lca_result_cache (
  id,
  scope,
  snapshot_id,
  request_key,
  request_payload,
  status,
  job_id,
  worker_job_id,
  result_id,
  created_at,
  updated_at
) values (
  '91530000-0000-4000-8000-000000000103',
  'prod',
  '91530000-0000-4000-8000-000000000004',
  'pgtap-request-key',
  '{}'::jsonb,
  'ready',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000102',
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00'
);

insert into public.lca_latest_all_unit_results (
  id,
  snapshot_id,
  job_id,
  worker_job_id,
  result_id,
  query_artifact_url,
  query_artifact_sha256,
  query_artifact_byte_size,
  query_artifact_format,
  status,
  computed_at,
  created_at,
  updated_at
) values (
  '91530000-0000-4000-8000-000000000104',
  '91530000-0000-4000-8000-000000000004',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000101',
  '91530000-0000-4000-8000-000000000102',
  'storage://lca_results/query.json',
  'sha',
  16,
  'json:v1',
  'ready',
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00'
);

insert into public.lca_factorization_registry (
  id,
  scope,
  snapshot_id,
  backend,
  numeric_options_hash,
  status,
  diagnostics,
  created_at,
  updated_at
) values (
  '91530000-0000-4000-8000-000000000105',
  'prod',
  '91530000-0000-4000-8000-000000000004',
  'umfpack',
  'numeric-options-hash',
  'ready',
  '{}'::jsonb,
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00'
);

insert into public.lca_snapshot_artifacts (
  id,
  snapshot_id,
  artifact_url,
  artifact_sha256,
  artifact_byte_size,
  artifact_format,
  process_count,
  flow_count,
  impact_count,
  a_nnz,
  b_nnz,
  c_nnz,
  coverage,
  status,
  created_at,
  updated_at
) values (
  '91530000-0000-4000-8000-000000000106',
  '91530000-0000-4000-8000-000000000004',
  'storage://lca_results/snapshots/91530000-0000-4000-8000-000000000004/matrix.json',
  'sha',
  20,
  'snapshot:v1',
  1,
  1,
  1,
  1,
  1,
  1,
  '{}'::jsonb,
  'ready',
  '2025-12-01 00:00:00+00',
  '2025-12-01 00:00:00+00'
);

insert into storage.objects (
  bucket_id,
  name,
  metadata,
  created_at,
  updated_at
) values
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000001/matrix.json',
    '{"size": 10}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000002/matrix.json',
    '{"size": 11}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000003/matrix.json',
    '{"size": 12}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000003/coverage.json',
    '{"size": 13}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000004/matrix.json',
    '{"size": 14}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000005/matrix.json',
    '{"size": 15}'::jsonb,
    '2026-01-20 00:00:00+00',
    '2026-01-20 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000006/matrix.json',
    '{"size": 16}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000007/matrix.json',
    '{"size": 17}'::jsonb,
    '2026-01-25 00:00:00+00',
    '2026-01-25 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/91530000-0000-4000-8000-000000000008/matrix.json',
    '{"size": 18}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    'lca_results',
    'lca-results/snapshots/not-a-uuid/matrix.json',
    '{"size": 19}'::jsonb,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  );

create temporary table pg_temp.snapshot_gc_preview as
select *
from util.preview_lca_snapshot_retention(
  interval '30 days',
  interval '30 days',
  '2026-02-01 00:00:00+00'::timestamp with time zone
);

create temporary table pg_temp.snapshot_gc_candidates as
select *
from util.list_lca_snapshot_gc_candidates(
  interval '30 days',
  interval '30 days',
  '2026-02-01 00:00:00+00'::timestamp with time zone,
  100,
  100,
  10000
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'protected_active_snapshot'
  ),
  1,
  'active snapshot is always protected'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'protected_ttl_future'
  ),
  1,
  'TTL future snapshot is protected'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'eligible_ttl_expired_snapshot'
  ),
  1,
  'TTL expired snapshot is eligible'
);

select is(
  (
    select count(*)::integer
    from pg_temp.snapshot_gc_candidates
    where snapshot_id = '91530000-0000-4000-8000-000000000003'
      and reason = 'eligible_ttl_expired_snapshot'
  ),
  2,
  'TTL expired snapshot returns object-level candidates'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'eligible_default_30d_snapshot'
  ),
  1,
  'no-TTL snapshot older than default window is eligible'
);

select ok(
  (
    select downstream_job_count = 1
      and downstream_result_count = 1
      and downstream_cache_count = 1
      and downstream_latest_count = 1
      and downstream_factorization_count = 1
      and downstream_artifact_count = 1
    from pg_temp.snapshot_gc_candidates
    where snapshot_id = '91530000-0000-4000-8000-000000000004'
    limit 1
  ),
  'downstream references are counted but do not block eligibility'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where retention_area = 'lca_snapshot_storage_directories'
      and reason = 'protected_inside_retention_window'
  ),
  1,
  'no-TTL snapshot inside default window is protected'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'eligible_orphan_storage_directory'
  ),
  2,
  'orphan storage directories older than orphan window are eligible'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where retention_area = 'lca_snapshot_storage_orphan_directories'
      and reason = 'protected_inside_retention_window'
  ),
  1,
  'orphan storage directory inside orphan window is protected'
);

select is(
  (
    select snapshot_count::integer
    from pg_temp.snapshot_gc_preview
    where reason = 'protected_unparsed_storage_path'
  ),
  1,
  'malformed storage path is report-only protected'
);

select is(
  (
    select count(*)::integer
    from pg_temp.snapshot_gc_candidates
    where snapshot_directory = 'not-a-uuid'
  ),
  0,
  'malformed storage path is not returned as a GC candidate'
);

select is(
  (select count(*)::integer from pg_temp.snapshot_gc_candidates),
  5,
  'candidate function returns the expected eligible object rows'
);

select ok(
  (
    select bool_and(delete_db_snapshot)
    from pg_temp.snapshot_gc_candidates
    where candidate_type = 'snapshot_directory'
  ),
  'snapshot directory candidates request DB snapshot deletion after storage deletion'
);

select ok(
  not (
    select bool_or(delete_db_snapshot)
    from pg_temp.snapshot_gc_candidates
    where candidate_type = 'orphan_storage_directory'
  ),
  'orphan storage directory candidates do not request DB deletion'
);

select is(
  (
    select count(distinct snapshot_id)::integer
    from util.list_lca_snapshot_gc_candidates(
      interval '30 days',
      interval '30 days',
      '2026-02-01 00:00:00+00'::timestamp with time zone,
      1,
      100,
      10000
    )
    where candidate_type = 'snapshot_directory'
  ),
  1,
  'max_snapshots cap limits eligible snapshot directories'
);

select is(
  (
    select count(distinct snapshot_directory)::integer
    from util.list_lca_snapshot_gc_candidates(
      interval '30 days',
      interval '30 days',
      '2026-02-01 00:00:00+00'::timestamp with time zone,
      100,
      1,
      10000
    )
    where candidate_type = 'orphan_storage_directory'
  ),
  1,
  'max_orphan_dirs cap limits eligible orphan directories'
);

select is(
  (
    select count(*)::integer
    from util.list_lca_snapshot_gc_candidates(
      interval '30 days',
      interval '30 days',
      '2026-02-01 00:00:00+00'::timestamp with time zone,
      100,
      100,
      1
    )
  ),
  0,
  'max_bytes cap can prevent candidate selection'
);

create temporary table pg_temp.snapshot_gc_error_check (
  raised boolean not null
);

do $$
begin
  perform util.list_lca_snapshot_gc_candidates(
    interval '12 hours',
    interval '30 days',
    '2026-02-01 00:00:00+00'::timestamp with time zone,
    100,
    100,
    10000
  );
  insert into pg_temp.snapshot_gc_error_check values (false);
exception when invalid_parameter_value then
  insert into pg_temp.snapshot_gc_error_check values (true);
end
$$;

select is(
  (select raised from pg_temp.snapshot_gc_error_check),
  true,
  'candidate function rejects snapshot retention windows shorter than one day'
);

select * from finish();

rollback;
