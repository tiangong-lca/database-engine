begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, util, auth;

select plan(35);

create temporary table p0_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on p0_results to service_role;

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

-- Heartbeat coalescing keeps lease renewal independent from business updates.
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, requester_type, requested_by,
  status, visibility, leased_by, lease_token, lease_expires_at,
  payload_schema_version, payload_json, diagnostics, created_at, updated_at
) values (
  '54900000-0000-4000-8000-000000000001', 'lca.build_snapshot', 'calculator',
  'solver', 'user', '54900000-0000-4000-8000-000000000101', 'running',
  'user', 'p0-worker', '54900000-0000-4000-8000-000000000201', now() + interval '10 minutes',
  'lca.build_snapshot.request.v2', '{}', '{}', '2026-08-01', '2026-08-01'
);

insert into p0_results values (
  'lease_only',
  private.worker_heartbeat_job(
    '54900000-0000-4000-8000-000000000001',
    '54900000-0000-4000-8000-000000000201',
    null, null, null, 300
  )
);

select is(
  (select value ->> 'ok' from p0_results where label = 'lease_only'),
  'true',
  'lease-only heartbeat succeeds'
);
select is(
  (select value ->> 'eventEmitted' from p0_results where label = 'lease_only'),
  'false',
  'lease-only heartbeat reports that no event was emitted'
);
select is(
  (select count(*) from private.worker_job_events where job_id = '54900000-0000-4000-8000-000000000001'),
  0::bigint,
  'lease-only heartbeat does not append an event'
);
select is(
  (select updated_at from private.worker_jobs where id = '54900000-0000-4000-8000-000000000001'),
  '2026-08-01 00:00:00+00'::timestamptz,
  'lease-only heartbeat does not advance business updated_at'
);
select ok(
  (select heartbeat_at is not null and lease_expires_at > now()
   from private.worker_jobs where id = '54900000-0000-4000-8000-000000000001'),
  'lease-only heartbeat still renews heartbeat and lease timestamps'
);

select private.worker_heartbeat_job(
  '54900000-0000-4000-8000-000000000001',
  '54900000-0000-4000-8000-000000000201',
  'prepare', 0.01, '{"sample":1}', 300
);
select is(
  (select count(*) from private.worker_job_events
   where job_id = '54900000-0000-4000-8000-000000000001' and event_type = 'heartbeat'),
  1::bigint,
  'phase change and first progress append one heartbeat event'
);
select is(
  (select diagnostics from private.worker_jobs where id = '54900000-0000-4000-8000-000000000001'),
  '{"sample":1}'::jsonb,
  'heartbeat diagnostics remain on the current job row'
);
select ok(
  not exists (
    select 1 from private.worker_job_events
    where job_id = '54900000-0000-4000-8000-000000000001'
      and details ? 'diagnostics'
  ),
  'heartbeat events do not copy diagnostic payloads'
);

select private.worker_heartbeat_job(
  '54900000-0000-4000-8000-000000000001',
  '54900000-0000-4000-8000-000000000201',
  null, 0.04, null, 300
);
select is(
  (select count(*) from private.worker_job_events
   where job_id = '54900000-0000-4000-8000-000000000001' and event_type = 'heartbeat'),
  1::bigint,
  'progress inside one five-percent bucket does not append an event'
);

select private.worker_heartbeat_job(
  '54900000-0000-4000-8000-000000000001',
  '54900000-0000-4000-8000-000000000201',
  null, 0.05, null, 300
);
select is(
  (select count(*) from private.worker_job_events
   where job_id = '54900000-0000-4000-8000-000000000001' and event_type = 'heartbeat'),
  2::bigint,
  'crossing a five-percent bucket appends one event'
);

select private.worker_heartbeat_job(
  '54900000-0000-4000-8000-000000000001',
  '54900000-0000-4000-8000-000000000201',
  null, 0.02, null, 300
);
select is(
  (select count(*) from private.worker_job_events
   where job_id = '54900000-0000-4000-8000-000000000001' and event_type = 'heartbeat'),
  2::bigint,
  'progress regression updates current state without appending a heartbeat event'
);

-- Exact latest deterministic failures suppress admission without side effects.
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, queue_key, requester_type, requested_by,
  request_hash, status, visibility, payload_schema_version, payload_json,
  error_code, error_message, retryable, created_at, updated_at, finished_at
) values (
  '54900000-0000-4000-8000-000000000002', 'lca.solve_all_unit', 'calculator',
  'solver', 'snapshot-a', 'user', '54900000-0000-4000-8000-000000000101',
  'deterministic-request-a', 'failed', 'user', 'lca.solve_all_unit.request.v1', '{}',
  'INVALID_FROZEN_INPUT', 'fixture deterministic failure', false,
  '2026-08-01', '2026-08-01', '2026-08-01'
);

insert into p0_results values (
  'negative_admission',
  private.worker_enqueue_job(
    p_job_kind => 'lca.solve_all_unit',
    p_payload_json => '{}',
    p_payload_schema_version => 'lca.solve_all_unit.request.v1',
    p_requested_by => '54900000-0000-4000-8000-000000000101',
    p_requester_type => 'user',
    p_idempotency_key => 'negative-admission-new',
    p_request_hash => 'deterministic-request-a',
    p_concurrency_key => 'negative-admission-new',
    p_queue_key => 'snapshot-a'
  )
);
select is(
  (select value ->> 'code' from p0_results where label = 'negative_admission'),
  'WORKER_REQUEST_NON_RETRYABLE_FAILURE',
  'exact latest deterministic failure is rejected by canonical admission'
);
select is(
  (select value #>> '{details,workerJob,id}' from p0_results where label = 'negative_admission'),
  '54900000-0000-4000-8000-000000000002',
  'negative admission returns the exact failed worker job identity'
);
select is(
  (select value ->> 'reused' from p0_results where label = 'negative_admission'),
  'true',
  'negative admission explicitly reports terminal job reuse'
);
select is(
  (select value ->> 'reuseReason' from p0_results where label = 'negative_admission'),
  'terminal_non_retryable_failure',
  'negative admission reports the stable terminal reuse reason'
);
select is(
  (select count(*) from private.worker_jobs where request_hash = 'deterministic-request-a'),
  1::bigint,
  'negative admission does not insert another worker job'
);
select is(
  (select count(*) from private.worker_job_events where job_id = '54900000-0000-4000-8000-000000000002'),
  0::bigint,
  'negative admission does not append an event'
);

select is(
  (
    private.worker_enqueue_job(
      p_job_kind => 'lca.solve_all_unit',
      p_payload_json => '{}',
      p_payload_schema_version => 'lca.solve_all_unit.request.v1',
      p_requested_by => '54900000-0000-4000-8000-000000000102',
      p_requester_type => 'user',
      p_idempotency_key => 'negative-admission-other-actor',
      p_request_hash => 'deterministic-request-a',
      p_concurrency_key => 'negative-admission-other-actor',
      p_queue_key => 'snapshot-a'
    ) ->> 'ok'
  ),
  'true',
  'negative admission never crosses requester identity'
);
select is(
  (
    private.worker_enqueue_job(
      p_job_kind => 'lca.solve_all_unit',
      p_payload_json => '{}',
      p_payload_schema_version => 'lca.solve_all_unit.request.v1',
      p_requested_by => '54900000-0000-4000-8000-000000000101',
      p_requester_type => 'user',
      p_idempotency_key => 'negative-admission-other-hash',
      p_request_hash => 'deterministic-request-b',
      p_concurrency_key => 'negative-admission-other-hash',
      p_queue_key => 'snapshot-a'
    ) ->> 'ok'
  ),
  'true',
  'negative admission never crosses request hash identity'
);

insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, queue_key, requester_type, requested_by,
  request_hash, status, visibility, payload_schema_version, payload_json,
  created_at, updated_at, finished_at
) values (
  '54900000-0000-4000-8000-000000000003', 'lca.solve_all_unit', 'calculator',
  'solver', 'snapshot-a', 'user', '54900000-0000-4000-8000-000000000101',
  'deterministic-request-a', 'completed', 'user', 'lca.solve_all_unit.request.v1', '{}',
  '2026-08-02', '2026-08-02', '2026-08-02'
);
insert into p0_results values (
  'latest_success_allows',
  private.worker_enqueue_job(
    p_job_kind => 'lca.solve_all_unit',
    p_payload_json => '{}',
    p_payload_schema_version => 'lca.solve_all_unit.request.v1',
    p_requested_by => '54900000-0000-4000-8000-000000000101',
    p_requester_type => 'user',
    p_idempotency_key => 'latest-success-new',
    p_request_hash => 'deterministic-request-a',
    p_concurrency_key => 'latest-success-new',
    p_queue_key => 'snapshot-a'
  )
);
select is(
  (select value ->> 'ok' from p0_results where label = 'latest_success_allows'),
  'true',
  'a later successful exact request prevents stale negative-cache suppression'
);

-- Terminal scheduled maintenance is reused under its logical key.
insert into p0_results values (
  'maintenance_first',
  private.worker_enqueue_job(
    p_job_kind => 'tidas.package_artifact_gc',
    p_payload_json => '{"execute":true}',
    p_payload_schema_version => 'tidas.package_artifact_gc.request.v1',
    p_requester_type => 'system',
    p_idempotency_key => 'tidas.package_artifact_gc:execute:2026-08-29',
    p_request_hash => 'maintenance-2026-08-29',
    p_queue_key => 'package-gc'
  )
);
update private.worker_jobs
set status = 'completed', finished_at = now(), updated_at = now()
where id = (select (value #>> '{data,id}')::uuid from p0_results where label = 'maintenance_first');
insert into p0_results values (
  'maintenance_second',
  private.worker_enqueue_job(
    p_job_kind => 'tidas.package_artifact_gc',
    p_payload_json => '{"execute":true}',
    p_payload_schema_version => 'tidas.package_artifact_gc.request.v1',
    p_requester_type => 'system',
    p_idempotency_key => 'tidas.package_artifact_gc:execute:2026-08-29',
    p_request_hash => 'maintenance-2026-08-29',
    p_queue_key => 'package-gc'
  )
);
select is(
  (select value ->> 'reused' from p0_results where label = 'maintenance_second'),
  'true',
  'terminal maintenance replay is reused'
);
select is(
  (select value #>> '{data,id}' from p0_results where label = 'maintenance_second'),
  (select value #>> '{data,id}' from p0_results where label = 'maintenance_first'),
  'terminal maintenance replay returns the original job'
);
select is(
  (select count(*) from private.worker_jobs
   where idempotency_key = 'tidas.package_artifact_gc:execute:2026-08-29'),
  1::bigint,
  'terminal maintenance replay creates no duplicate job'
);

-- Snapshot and result-cache facades preserve non-retryable failure state.
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, queue_key, requester_type, requested_by,
  request_hash, status, visibility, payload_schema_version, payload_json,
  error_code, error_message, retryable, created_at, updated_at, finished_at
) values (
  '54900000-0000-4000-8000-000000000004', 'lca.build_snapshot', 'calculator',
  'solver', 'full_library', 'user', '54900000-0000-4000-8000-000000000101',
  'snapshot-failure-key', 'failed', 'user', 'lca.build_snapshot.request.v2',
  '{"job_id":"54900000-0000-4000-8000-000000000301","snapshot_id":"54900000-0000-4000-8000-000000000401"}',
  'INVALID_SNAPSHOT_INPUT', 'fixture snapshot failure', false,
  '2026-08-03', '2026-08-03', '2026-08-03'
);
insert into p0_results values (
  'snapshot_failure',
  api.svc_lca_snapshot_build_enqueue(
    'full_library', '{}', '54900000-0000-4000-8000-000000000101',
    'snapshot-failure-key', '54900000-0000-4000-8000-000000000302',
    '54900000-0000-4000-8000-000000000402', '{}',
    'lca.build_snapshot.request.v2'
  )
);
select is(
  (select value ->> 'code' from p0_results where label = 'snapshot_failure'),
  'WORKER_REQUEST_NON_RETRYABLE_FAILURE',
  'snapshot facade surfaces the deterministic failure'
);
select is(
  (select count(*) from private.lca_network_snapshots where id = '54900000-0000-4000-8000-000000000402'),
  0::bigint,
  'snapshot failure replay does not create a replacement draft snapshot'
);

insert into private.lca_network_snapshots (
  id, scope, process_filter, status, created_by, created_at, updated_at
) values (
  '54900000-0000-4000-8000-000000000501', 'full_library', '{}', 'ready',
  '54900000-0000-4000-8000-000000000101', '2026-08-01', '2026-08-01'
);
insert into private.worker_jobs (
  id, job_kind, worker_runtime, worker_queue, queue_key, requester_type, requested_by,
  request_hash, status, visibility, payload_schema_version, payload_json,
  error_code, error_message, retryable, created_at, updated_at, finished_at
) values (
  '54900000-0000-4000-8000-000000000005', 'lca.solve_all_unit', 'calculator',
  'solver', '54900000-0000-4000-8000-000000000501', 'user',
  '54900000-0000-4000-8000-000000000101', 'cache-failure-key', 'failed', 'user',
  'lca.solve_all_unit.request.v1', '{"job_id":"54900000-0000-4000-8000-000000000601"}',
  'INVALID_SOLVE_INPUT', 'fixture solve failure', false,
  '2026-08-04', '2026-08-04', '2026-08-04'
);
insert into private.lca_result_cache (
  scope, snapshot_id, request_key, request_payload, status, job_id, worker_job_id,
  error_code, error_message, created_at, updated_at, last_accessed_at
) values (
  'full_library', '54900000-0000-4000-8000-000000000501', 'cache-failure-key', '{}',
  'failed', '54900000-0000-4000-8000-000000000601',
  '54900000-0000-4000-8000-000000000005', 'INVALID_SOLVE_INPUT',
  'fixture solve failure', '2026-08-04', '2026-08-04', '2026-08-04'
);
insert into p0_results values (
  'cache_failure',
  api.svc_lca_cached_job_enqueue(
    'full_library', '54900000-0000-4000-8000-000000000501', 'cache-failure-key',
    '{}', 'lca.solve_all_unit', '54900000-0000-4000-8000-000000000602', '{}',
    'lca.solve_all_unit.request.v1', '54900000-0000-4000-8000-000000000101',
    'cache-failure-new', '54900000-0000-4000-8000-000000000501'
  )
);
select is(
  (select value ->> 'code' from p0_results where label = 'cache_failure'),
  'WORKER_REQUEST_NON_RETRYABLE_FAILURE',
  'result-cache facade surfaces the cached deterministic failure'
);
select is(
  (select value ->> 'mode' from p0_results where label = 'cache_failure'),
  'failed_cache_hit',
  'result-cache facade projects a stable failed-cache-hit mode'
);
select is(
  (select value ->> 'retryable' from p0_results where label = 'cache_failure'),
  'false',
  'result-cache facade exposes the non-retryable disposition'
);
select is(
  (select status from private.lca_result_cache where request_key = 'cache-failure-key'),
  'failed',
  'result-cache facade does not reset deterministic failure to pending'
);
select is(
  (select worker_job_id from private.lca_result_cache where request_key = 'cache-failure-key'),
  '54900000-0000-4000-8000-000000000005'::uuid,
  'result-cache facade retains the failed worker job identity'
);
select is(
  (select count(*) from private.worker_jobs where request_hash = 'cache-failure-key'),
  1::bigint,
  'result-cache failure replay creates no replacement worker job'
);

-- Package retention remains observable but cannot mutate without object proof.
select ok(
  to_regclass('private.lca_package_artifacts_gc_candidate_idx') is not null
    and to_regclass('private.lca_package_request_cache_gc_candidate_idx') is not null
    and to_regclass('private.lca_package_export_items_gc_candidate_idx') is not null,
  'package GC candidate lookups have supporting indexes'
);
select is(
  (
    select count(*)
    from pg_constraint as constraint_row
    join pg_class as relation on relation.oid = constraint_row.conrelid
    join pg_namespace as namespace on namespace.oid = relation.relnamespace
    where namespace.nspname = 'private'
      and relation.relname in (
        'lca_package_artifacts', 'lca_package_export_items', 'lca_package_request_cache'
      )
      and constraint_row.conname like '%worker_job_id_fkey'
      and constraint_row.confdeltype = 'r'
      and constraint_row.convalidated
  ),
  3::bigint,
  'all package worker-job foreign keys use validated ON DELETE RESTRICT'
);
select is(
  (select count(*) from util.apply_lca_package_retention(
    interval '30 days', interval '30 days', '2026-08-29', 100, true
  )),
  3::bigint,
  'package retention preview remains available in dry-run mode'
);
select throws_ok(
  $$select * from util.apply_lca_package_retention(
    interval '30 days', interval '30 days', '2026-08-29', 100, false
  )$$,
  '0A000',
  'database-only package retention apply is disabled',
  'database-only mutating package retention fails closed'
);

reset role;
select * from finish();
rollback;
