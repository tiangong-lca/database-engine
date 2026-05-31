begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(40);

select ok(to_regclass('public.worker_job_kinds') is not null, 'worker job kind registry exists');
select ok(to_regclass('public.worker_jobs') is not null, 'worker_jobs table exists');
select ok(to_regclass('public.worker_job_events') is not null, 'worker job events table exists');
select ok(to_regclass('public.worker_job_artifacts') is not null, 'worker job artifacts table exists');

select ok(
  (select relrowsecurity from pg_class where oid = 'public.worker_jobs'::regclass),
  'worker_jobs has RLS enabled'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.worker_job_events'::regclass),
  'worker_job_events has RLS enabled'
);

select ok(
  (select relrowsecurity from pg_class where oid = 'public.worker_job_artifacts'::regclass),
  'worker_job_artifacts has RLS enabled'
);

select ok(
  not has_table_privilege('authenticated', 'public.worker_jobs', 'INSERT'),
  'authenticated cannot directly insert worker_jobs'
);

select ok(
  not has_table_privilege('authenticated', 'public.worker_jobs', 'SELECT'),
  'authenticated cannot directly select worker_jobs'
);

select cmp_ok(
  (select count(*) from public.worker_job_kinds),
  '>=',
  12::bigint,
  'initial calculator worker job kinds are registered'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '96000000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.worker_enqueue_job(text,jsonb,text,text,uuid,text,uuid,text,uuid,text,text,text,integer,text,timestamp with time zone,text,integer,timestamp with time zone,jsonb,uuid,uuid)',
    'EXECUTE'
  ),
  'authenticated users cannot call worker_enqueue_job directly'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.worker_claim_jobs(text,text,integer,integer)',
    'EXECUTE'
  ),
  'authenticated users cannot call worker_claim_jobs directly'
);

select is(
  (
    select count(*)::text
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'public'
      and p.proname like 'worker_%'
      and has_function_privilege('authenticated', p.oid, 'EXECUTE')
  ),
  '0',
  'authenticated users cannot execute any public worker_* RPC'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.worker_enqueue_job(
    p_job_kind => 'not.a.real.kind',
    p_requested_by => '96000000-0000-4000-8000-000000000001'
  )->>'code',
  'WORKER_JOB_KIND_UNSUPPORTED',
  'enqueue rejects unknown worker job kinds'
);

select is(
  public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate'
  )->>'code',
  'WORKER_JOB_REQUESTED_BY_REQUIRED',
  'user-requested worker jobs require requestedBy'
);

create temporary table worker_job_test_ids (
  label text primary key,
  job_id uuid not null,
  lease_token uuid
) on commit drop;

grant all on worker_job_test_ids to public;

insert into worker_job_test_ids (label, job_id)
select
  'gate_primary',
  (result->'data'->>'id')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{"datasetRevision":{"table":"processes","id":"96000000-0000-4000-8000-000000000101","version":"01.00.000"}}'::jsonb,
    p_requested_by => '96000000-0000-4000-8000-000000000001',
    p_idempotency_key => 'review-submit:primary',
    p_concurrency_key => 'processes:96000000-0000-4000-8000-000000000101:01.00.000:review-submit',
    p_subject_type => 'processes',
    p_subject_id => '96000000-0000-4000-8000-000000000101',
    p_subject_version => '01.00.000'
  ) as result
) as enqueue;

select is(
  (
    select status
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'gate_primary')
  ),
  'queued',
  'enqueue creates a queued worker job'
);

select is(
  public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{"datasetRevision":{"table":"processes","id":"96000000-0000-4000-8000-000000000101","version":"01.00.000"}}'::jsonb,
    p_requested_by => '96000000-0000-4000-8000-000000000001',
    p_idempotency_key => 'review-submit:primary',
    p_concurrency_key => 'processes:96000000-0000-4000-8000-000000000101:01.00.000:review-submit',
    p_subject_type => 'processes',
    p_subject_id => '96000000-0000-4000-8000-000000000101',
    p_subject_version => '01.00.000'
  )->'data'->>'id',
  (select job_id::text from worker_job_test_ids where label = 'gate_primary'),
  'enqueue reuses active jobs by idempotency key'
);

select is(
  public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{}'::jsonb,
    p_requested_by => '96000000-0000-4000-8000-000000000001',
    p_idempotency_key => 'review-submit:conflicting',
    p_concurrency_key => 'processes:96000000-0000-4000-8000-000000000101:01.00.000:review-submit'
  )->>'code',
  'WORKER_JOB_CONCURRENCY_CONFLICT',
  'concurrencyKey prevents conflicting active jobs'
);

insert into worker_job_test_ids (label, job_id)
select
  'operator_gc',
  (result->'data'->>'id')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'lca.snapshot_gc',
    p_payload_json => '{"execute":false,"environment":"test"}'::jsonb,
    p_requester_type => 'system',
    p_idempotency_key => 'snapshot-gc:dry-run:test',
    p_concurrency_key => 'snapshot-gc:test:dry-run'
  ) as result
) as enqueue;

select is(
  jsonb_array_length(
    public.worker_list_jobs(
      p_requested_by => '96000000-0000-4000-8000-000000000001',
      p_visibility => 'user'
    )->'data'
  )::text,
  '1',
  'user-visible list excludes operator/system jobs'
);

insert into worker_job_test_ids (label, job_id, lease_token)
select
  'claimed_gate',
  (claimed->>'id')::uuid,
  (claimed->>'leaseToken')::uuid
from jsonb_array_elements(
  public.worker_claim_jobs(
    p_worker_queue => 'review_submit_gate',
    p_worker_id => 'worker-a',
    p_limit => 1,
    p_lease_seconds => 300
  )->'data'
) as claimed;

select is(
  (select job_id::text from worker_job_test_ids where label = 'claimed_gate'),
  (select job_id::text from worker_job_test_ids where label = 'gate_primary'),
  'claim returns the queued gate job'
);

select is(
  (
    select status || ':' || attempt_count::text
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'gate_primary')
  ),
  'running:1',
  'claim marks job running and increments attempt count'
);

select is(
  jsonb_array_length(public.worker_claim_jobs('review_submit_gate', 'worker-b', 1, 300)->'data')::text,
  '0',
  'running job with active lease is not claimed twice'
);

select is(
  public.worker_heartbeat_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_lease_token => '00000000-0000-0000-0000-000000000000',
    p_phase => 'checking',
    p_progress => 0.4
  )->>'code',
  'WORKER_JOB_LEASE_TOKEN_MISMATCH',
  'heartbeat rejects the wrong lease token'
);

select is(
  public.worker_heartbeat_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_lease_token => (select lease_token from worker_job_test_ids where label = 'claimed_gate'),
    p_phase => 'checking',
    p_progress => 0.4,
    p_diagnostics => '{"rows":10}'::jsonb
  )->'data'->>'phase',
  'checking',
  'heartbeat updates phase with the correct lease token'
);

select is(
  (
    select progress::text
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'gate_primary')
  ),
  '0.4',
  'heartbeat updates progress'
);

select is(
  public.worker_record_job_result(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_lease_token => '00000000-0000-0000-0000-000000000000',
    p_status => 'completed'
  )->>'code',
  'WORKER_JOB_LEASE_TOKEN_MISMATCH',
  'record result rejects the wrong lease token'
);

select is(
  public.worker_record_job_result(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_lease_token => (select lease_token from worker_job_test_ids where label = 'claimed_gate'),
    p_status => 'blocked'
  )->>'code',
  'WORKER_JOB_BLOCKER_DETAILS_REQUIRED',
  'blocked worker jobs require blocker details'
);

select is(
  public.worker_record_job_result(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_lease_token => (select lease_token from worker_job_test_ids where label = 'claimed_gate'),
    p_status => 'completed',
    p_result_json => '{"revisionChecksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'::jsonb
  )->'data'->>'status',
  'completed',
  'record result marks a job completed'
);

select is(
  public.worker_cancel_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_reason => 'should not cancel terminal'
  )->>'code',
  'WORKER_JOB_TERMINAL',
  'completed jobs cannot be cancelled'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_job_events
    where job_id = (select job_id from worker_job_test_ids where label = 'gate_primary')
  ),
  '>=',
  4::bigint,
  'enqueue, claim, heartbeat, and result events are recorded'
);

insert into worker_job_test_ids (label, job_id)
select
  'reclaim_gate',
  (result->'data'->>'id')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{}'::jsonb,
    p_requested_by => '96000000-0000-4000-8000-000000000001',
    p_idempotency_key => 'review-submit:reclaim',
    p_max_attempts => 2
  ) as result
) as enqueue;

update worker_job_test_ids
set lease_token = (claimed->>'leaseToken')::uuid
from jsonb_array_elements(
  public.worker_claim_jobs('review_submit_gate', 'worker-a', 1, 300)->'data'
) as claimed
where label = 'reclaim_gate'
  and job_id = (claimed->>'id')::uuid;

select is(
  (
    select attempt_count::text
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'reclaim_gate')
  ),
  '1',
  'first claim increments reclaim fixture attempt count'
);

update public.worker_jobs
set lease_expires_at = now() - interval '1 second'
where id = (select job_id from worker_job_test_ids where label = 'reclaim_gate');

update worker_job_test_ids
set lease_token = (claimed->>'leaseToken')::uuid
from jsonb_array_elements(
  public.worker_claim_jobs('review_submit_gate', 'worker-b', 1, 300)->'data'
) as claimed
where label = 'reclaim_gate'
  and job_id = (claimed->>'id')::uuid;

select is(
  (
    select leased_by || ':' || attempt_count::text
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'reclaim_gate')
  ),
  'worker-b:2',
  'expired running job can be reclaimed before max attempts'
);

select is(
  public.worker_record_job_result(
    p_job_id => (select job_id from worker_job_test_ids where label = 'reclaim_gate'),
    p_lease_token => '00000000-0000-0000-0000-000000000000',
    p_status => 'completed'
  )->>'code',
  'WORKER_JOB_LEASE_TOKEN_MISMATCH',
  'old or unknown lease token cannot record after reclaim'
);

update public.worker_jobs
set lease_expires_at = now() - interval '1 second'
where id = (select job_id from worker_job_test_ids where label = 'reclaim_gate');

select is(
  jsonb_array_length(public.worker_claim_jobs('review_submit_gate', 'worker-c', 1, 300)->'data')::text,
  '0',
  'expired running job at max attempts is not reclaimed again'
);

select is(
  (
    select status || ':' || error_code
    from public.worker_jobs
    where id = (select job_id from worker_job_test_ids where label = 'reclaim_gate')
  ),
  'failed:lease_expired_max_attempts',
  'expired running job at max attempts is marked failed'
);

insert into worker_job_test_ids (label, job_id, lease_token)
select
  'blocked_gate',
  (claimed->>'id')::uuid,
  (claimed->>'leaseToken')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'review_submit.gate',
    p_payload_json => '{}'::jsonb,
    p_requested_by => '96000000-0000-4000-8000-000000000001',
    p_idempotency_key => 'review-submit:blocked'
  )
) as enqueue(result),
jsonb_array_elements(
  public.worker_claim_jobs('review_submit_gate', 'worker-d', 1, 300)->'data'
) as claimed;

select is(
  public.worker_record_job_result(
    p_job_id => (select job_id from worker_job_test_ids where label = 'blocked_gate'),
    p_lease_token => (select lease_token from worker_job_test_ids where label = 'blocked_gate'),
    p_status => 'blocked',
    p_blocker_codes => array['singular_risk_medium_or_high'],
    p_resolution_scope => 'user',
    p_retryable => true,
    p_result_json => '{"summary":{"blockerCount":1}}'::jsonb
  )->'data'->>'status',
  'blocked',
  'record result marks business blockers as blocked'
);

select is(
  public.worker_retry_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'blocked_gate'),
    p_reason => 'retry after user fix'
  )->'data'->>'status',
  'queued',
  'retry moves blocked jobs back to queued'
);

select is(
  public.worker_cancel_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'blocked_gate'),
    p_cancelled_by => '96000000-0000-4000-8000-000000000001',
    p_reason => 'user cancelled'
  )->'data'->>'status',
  'cancelled',
  'cancel marks a queued job cancelled'
);

select is(
  public.worker_read_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_include_internal => false
  )->'data' ? 'payload',
  false,
  'public projection omits raw payload'
);

select is(
  public.worker_read_job(
    p_job_id => (select job_id from worker_job_test_ids where label = 'gate_primary'),
    p_include_internal => true
  )->'data' ? 'payload',
  true,
  'internal projection can include raw payload'
);

select * from finish();
rollback;
