begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, api, private, auth;

select plan(14);

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

create temporary table issue_448_ids (
  label text primary key,
  job_id uuid not null,
  lease_token uuid
) on commit drop;
grant all on issue_448_ids to service_role;

create temporary table issue_448_results (
  label text primary key,
  value jsonb not null
) on commit drop;
grant all on issue_448_results to service_role;

insert into issue_448_results values (
  'export',
  api.svc_tidas_package_export_enqueue(
    '44800000-0000-4000-8000-000000000001',
    'current_user',
    '[]'::jsonb,
    'issue448-export',
    '{"scope":"current_user","roots":[]}'::jsonb,
    '44810000-0000-4000-8000-000000000001',
    'issue448:export'
  )
);

select is(
  (select value ->> 'mode' from issue_448_results where label = 'export'),
  'queued',
  'package fixture is queued through the capability facade'
);

insert into private.lca_package_artifacts (
  id, job_id, worker_job_id, artifact_kind, status, artifact_url,
  artifact_sha256, artifact_byte_size, artifact_format, content_type, metadata
) values (
  '44820000-0000-4000-8000-000000000001',
  '44810000-0000-4000-8000-000000000001',
  (select (value ->> 'worker_job_id')::uuid from issue_448_results where label = 'export'),
  'export_zip', 'ready', 's3://packages/issue448.zip', repeat('a', 64), 448,
  'tidas-package-zip:v1', 'application/zip', '{"producer":"worker"}'::jsonb
);

select is(
  api.svc_tidas_package_read(
    '44800000-0000-4000-8000-000000000001',
    '44810000-0000-4000-8000-000000000001'
  ) #>> '{data,artifacts,0,id}',
  '44820000-0000-4000-8000-000000000001',
  'owner can read worker-produced artifacts without requested_by metadata by logical job id'
);

select is(
  api.svc_tidas_package_read(
    '44800000-0000-4000-8000-000000000001',
    (select (value ->> 'worker_job_id')::uuid from issue_448_results where label = 'export')
  ) #>> '{data,artifacts,0,metadata,producer}',
  'worker',
  'owner can read the same artifact by canonical worker job id'
);

select is(
  api.svc_tidas_package_read(
    '44800000-0000-4000-8000-000000000002',
    '44810000-0000-4000-8000-000000000001'
  ) -> 'data',
  'null'::jsonb,
  'another user cannot read a known logical job id'
);

select is(
  api.svc_tidas_package_read(
    '44800000-0000-4000-8000-000000000002',
    (select (value ->> 'worker_job_id')::uuid from issue_448_results where label = 'export')
  ) -> 'data',
  'null'::jsonb,
  'another user cannot read a known canonical worker job id'
);

insert into issue_448_results values (
  'import_prepare',
  api.svc_tidas_package_import_prepare(
    '44800000-0000-4000-8000-000000000001',
    '44810000-0000-4000-8000-000000000002',
    '44820000-0000-4000-8000-000000000002',
    's3://packages/issue448-source.zip',
    'application/zip',
    'issue448-source.zip',
    'issue448:import-prepare'
  )
);

select is(
  (
    select metadata ->> 'requested_by'
    from private.lca_package_artifacts
    where id = '44820000-0000-4000-8000-000000000002'
  ),
  '44800000-0000-4000-8000-000000000001',
  'import prepare keeps its internal admission metadata contract'
);

insert into issue_448_ids (label, job_id)
select 'explicit_retry', (value #>> '{data,id}')::uuid
from (
  select private.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{"fixture":"explicit-retry"}'::jsonb,
    p_requested_by => '44800000-0000-4000-8000-000000000001',
    p_idempotency_key => 'issue448:explicit-retry',
    p_max_attempts => 2
  ) as value
) as enqueue;

update private.worker_jobs
set diagnostics = '{"admission":"preserved"}'::jsonb
where id = (select job_id from issue_448_ids where label = 'explicit_retry');

update issue_448_ids
set lease_token = (claimed ->> 'leaseToken')::uuid
from jsonb_array_elements(
  private.worker_claim_jobs('review_submit_gate', 'issue448-worker-a', 1, 300) -> 'data'
) as claimed
where label = 'explicit_retry'
  and job_id = (claimed ->> 'id')::uuid;

select is(
  (
    select diagnostics
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'explicit_retry')
  ),
  '{"admission":"preserved"}'::jsonb,
  'the first claim preserves admission diagnostics'
);

select private.worker_heartbeat_job(
  p_job_id => (select job_id from issue_448_ids where label = 'explicit_retry'),
  p_lease_token => (select lease_token from issue_448_ids where label = 'explicit_retry'),
  p_diagnostics => '{"failedField":"stale"}'::jsonb
);

select private.worker_record_job_result(
  p_job_id => (select job_id from issue_448_ids where label = 'explicit_retry'),
  p_lease_token => (select lease_token from issue_448_ids where label = 'explicit_retry'),
  p_status => 'failed',
  p_diagnostics => '{"attempt":"failed"}'::jsonb,
  p_error_code => 'ISSUE448_FIXTURE_FAILURE'
);

select is(
  (
    select diagnostics
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'explicit_retry')
  ),
  '{"attempt":"failed"}'::jsonb,
  'terminal result diagnostics replace heartbeat diagnostics for the attempt'
);

select ok(
  not exists (
    select 1
    from private.worker_job_events
    where job_id = (select job_id from issue_448_ids where label = 'explicit_retry')
      and event_type = 'heartbeat'
      and details ? 'diagnostics'
  ),
  'heartbeat events omit attempt diagnostics while terminal state still replaces the job-row diagnostics'
);

select private.worker_retry_job(
  p_job_id => (select job_id from issue_448_ids where label = 'explicit_retry'),
  p_reason => 'issue448 retry fixture'
);

select is(
  (
    select diagnostics
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'explicit_retry')
  ),
  '{}'::jsonb,
  'explicit retry clears diagnostics before the next attempt'
);

update issue_448_ids
set lease_token = (claimed ->> 'leaseToken')::uuid
from jsonb_array_elements(
  private.worker_claim_jobs('review_submit_gate', 'issue448-worker-b', 1, 300) -> 'data'
) as claimed
where label = 'explicit_retry'
  and job_id = (claimed ->> 'id')::uuid;

select private.worker_heartbeat_job(
  p_job_id => (select job_id from issue_448_ids where label = 'explicit_retry'),
  p_lease_token => (select lease_token from issue_448_ids where label = 'explicit_retry'),
  p_diagnostics => '{"newHeartbeat":true}'::jsonb
);

select private.worker_record_job_result(
  p_job_id => (select job_id from issue_448_ids where label = 'explicit_retry'),
  p_lease_token => (select lease_token from issue_448_ids where label = 'explicit_retry'),
  p_status => 'completed',
  p_diagnostics => '{"attempt":"completed"}'::jsonb
);

select is(
  (
    select diagnostics
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'explicit_retry')
  ),
  '{"attempt":"completed"}'::jsonb,
  'successful retry exposes only the current terminal diagnostics'
);

insert into issue_448_ids (label, job_id)
select 'expired_lease', (value #>> '{data,id}')::uuid
from (
  select private.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{"fixture":"expired-lease"}'::jsonb,
    p_requested_by => '44800000-0000-4000-8000-000000000001',
    p_idempotency_key => 'issue448:expired-lease',
    p_max_attempts => 2
  ) as value
) as enqueue;

update issue_448_ids
set lease_token = (claimed ->> 'leaseToken')::uuid
from jsonb_array_elements(
  private.worker_claim_jobs('review_submit_gate', 'issue448-worker-c', 1, 300) -> 'data'
) as claimed
where label = 'expired_lease'
  and job_id = (claimed ->> 'id')::uuid;

select private.worker_heartbeat_job(
  p_job_id => (select job_id from issue_448_ids where label = 'expired_lease'),
  p_lease_token => (select lease_token from issue_448_ids where label = 'expired_lease'),
  p_diagnostics => '{"expiredAttempt":"stale"}'::jsonb
);

update private.worker_jobs
set lease_expires_at = now() - interval '1 second'
where id = (select job_id from issue_448_ids where label = 'expired_lease');

update issue_448_ids
set lease_token = (claimed ->> 'leaseToken')::uuid
from jsonb_array_elements(
  private.worker_claim_jobs('review_submit_gate', 'issue448-worker-d', 1, 300) -> 'data'
) as claimed
where label = 'expired_lease'
  and job_id = (claimed ->> 'id')::uuid;

select is(
  (
    select diagnostics
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'expired_lease')
  ),
  '{}'::jsonb,
  'an expired lease starts a new attempt with empty diagnostics'
);

select is(
  (
    select attempt_count::text
    from private.worker_jobs
    where id = (select job_id from issue_448_ids where label = 'expired_lease')
  ),
  '2',
  'expired lease reclaim still increments the attempt counter'
);

select is(
  (
    select count(*)::text
    from pg_proc as routine
    join pg_namespace as namespace on namespace.oid = routine.pronamespace
    where namespace.nspname = 'public'
      and routine.proname like 'worker_%'
  ),
  '0',
  'the fix does not recreate public worker routines'
);

select * from finish();

rollback;
