begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.disable_trigger_if_exists(p_table regclass, p_trigger name)
returns void
language plpgsql
as $$
begin
  if exists (
    select 1
    from pg_trigger
    where tgrelid = p_table
      and tgname = p_trigger
      and not tgisinternal
  ) then
    execute format('alter table %s disable trigger %I', p_table, p_trigger);
  end if;
end;
$$;

select plan(21);

select set_config('request.jwt.claim.role', 'authenticated', true);

insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at,
  is_sso_user,
  is_anonymous
)
values
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'review-submit-job-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000001","email":"review-submit-job-owner@example.com","display_name":"Review Submit Job Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '15000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'review-submit-job-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"15000000-0000-0000-0000-000000000002","email":"review-submit-job-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into public.users (id, raw_user_meta_data)
values
  (
    '15000000-0000-0000-0000-000000000001',
    '{"email":"review-submit-job-owner@example.com","display_name":"Review Submit Job Owner"}'::jsonb
  ),
  (
    '15000000-0000-0000-0000-000000000002',
    '{"email":"review-submit-job-outsider@example.com"}'::jsonb
  );

insert into public.teams (id, json, rank, is_public)
values
  ('25000000-0000-0000-0000-000000000001', '{"title":"Review Submit Job Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
values
  ('15000000-0000-0000-0000-000000000001', '25000000-0000-0000-0000-000000000001', 'owner');

alter table public.processes disable trigger "processes_json_sync_trigger";
alter table public.processes disable trigger "process_extract_md_trigger_insert";
alter table public.processes disable trigger "process_extract_md_trigger_update";
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_insert');
select pg_temp.disable_trigger_if_exists('public.processes'::regclass, 'process_extract_text_trigger_update');

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  team_id,
  model_id,
  rule_verification
)
values
  (
    '35000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Passed"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Passed"}]}}}}}'::json,
    '15000000-0000-0000-0000-000000000001',
    0,
    '25000000-0000-0000-0000-000000000001',
    '45000000-0000-0000-0000-000000000001',
    true
  ),
  (
    '35000000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Stale"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Stale"}]}}}}}'::json,
    '15000000-0000-0000-0000-000000000001',
    0,
    '25000000-0000-0000-0000-000000000001',
    '45000000-0000-0000-0000-000000000002',
    true
  ),
  (
    '35000000-0000-0000-0000-000000000003',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Blocked"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Review Submit Job Blocked"}]}}}}}'::json,
    '15000000-0000-0000-0000-000000000001',
    0,
    '25000000-0000-0000-0000-000000000001',
    '45000000-0000-0000-0000-000000000003',
    true
  );

create temporary table review_submit_job_ids (
  label text primary key,
  job_id uuid not null,
  gate_worker_job_id uuid not null
) on commit drop;

grant all on review_submit_job_ids to public;

select is(
  public.cmd_dataset_review_submit_job_enqueue(
    p_table => 'processes',
    p_id => '35000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_revision_checksum => repeat('a', 64)
  )->>'code',
  'AUTH_REQUIRED',
  'review-submit job enqueue requires authentication'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);

insert into review_submit_job_ids (label, job_id, gate_worker_job_id)
select
  'passed_process',
  (result->'data'->>'reviewSubmitJobId')::uuid,
  (result->'data'->>'gateWorkerJobId')::uuid
from (
  select public.cmd_dataset_review_submit_job_enqueue(
    p_table => 'processes',
    p_id => '35000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_revision_checksum => repeat('a', 64),
    p_audit => '{"command":"review_submit_job_enqueue"}'::jsonb
  ) as result
) as enqueue;

select is(
  (
    public.cmd_dataset_review_submit_job_read(
      (select job_id from review_submit_job_ids where label = 'passed_process')
    )->'data'->>'status'
  ) || ':' || (
    public.cmd_dataset_review_submit_job_read(
      (select job_id from review_submit_job_ids where label = 'passed_process')
    )->'data' ? 'gateWorkerJobId'
  )::text,
  'waiting_gate:true',
  'enqueue persists a job waiting for the worker gate result'
);

reset role;
select ok(
  to_regclass('public.dataset_review_submit_jobs') is null,
  'legacy review-submit job table is retired; active coordinator state lives in dataset_review_submit_requests'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);

select is(
  (
    public.cmd_dataset_review_submit_job_enqueue(
      p_table => 'processes',
      p_id => '35000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64)
    )->'data'->>'reviewSubmitJobId'
  ),
  (select job_id::text from review_submit_job_ids where label = 'passed_process'),
  'enqueue is idempotent for the same active dataset revision'
);

select is(
  (
    public.cmd_dataset_review_submit_job_read_latest(
      p_table => 'processes',
      p_id => '35000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64)
    )->'data'->>'reviewSubmitJobId'
  ),
  (select job_id::text from review_submit_job_ids where label = 'passed_process'),
  'owner can read the latest review-submit job for a revision'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000002', true);
select set_config('request.jwt.claim.role', 'authenticated', true);

select is(
  public.cmd_dataset_review_submit_job_read(
    (select job_id from review_submit_job_ids where label = 'passed_process')
  )->>'code',
  'DATASET_OWNER_REQUIRED',
  'non-requester cannot read another user review-submit job'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.cmd_dataset_review_submit_job_claim(integer,integer)',
    'EXECUTE'
  ),
  'authenticated users cannot execute review-submit job claim'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  public.cmd_dataset_review_submit_job_claim(1)->'data'->0->>'reviewSubmitJobId',
  (select job_id::text from review_submit_job_ids where label = 'passed_process'),
  'service role can claim a waiting review-submit job'
);

select is(
  (
    select status || ':' || attempt_count::text
    from public.dataset_review_submit_requests
    where id = (select job_id from review_submit_job_ids where label = 'passed_process')
  ),
  'submitting:1',
  'claim marks the job submitting and increments attempt count'
);

select is(
  public.cmd_dataset_review_submit_job_record_result(
    p_job_id => (select job_id from review_submit_job_ids where label = 'passed_process'),
    p_status => 'waiting_gate',
    p_audit => '{"command":"review_submit_job_record_waiting"}'::jsonb
  )->'data'->>'status',
  'waiting_gate',
  'service role can return a claimed job to waiting_gate'
);

select is(
  public.cmd_dataset_review_submit_job_read(
    (select job_id from review_submit_job_ids where label = 'passed_process')
  )->'data'->>'status',
  'waiting_gate',
  'service role can read review-submit job state'
);

select public.worker_claim_jobs('review_submit_gate', 'review-submit-job-test-worker', 1, 300);

select public.worker_record_job_result(
  p_job_id => (select gate_worker_job_id from review_submit_job_ids where label = 'passed_process'),
  p_lease_token => (
    select lease_token
    from public.worker_jobs
    where id = (select gate_worker_job_id from review_submit_job_ids where label = 'passed_process')
  ),
  p_status => 'completed',
  p_result_json => jsonb_build_object(
    'status', 'passed',
    'datasetRevision', jsonb_build_object(
      'table', 'processes',
      'id', '35000000-0000-0000-0000-000000000001',
      'version', '01.00.000',
      'revisionChecksum', repeat('a', 64)
    ),
    'calculatorReport', '{"summary":{"blockerCount":0}}'::jsonb,
    'blockingReasons', '[]'::jsonb
  ),
  p_result_schema_version => 'review_submit_gate_report.v1'
);

select is(
  public.cmd_review_submit_from_job(
    p_job_id => (select job_id from review_submit_job_ids where label = 'passed_process'),
    p_audit => '{"command":"review_submit_from_job"}'::jsonb
  )->'data'->>'status',
  'submitted',
  'service role submits review using the job requester as actor'
);

select is(
  (
    select status
    from public.dataset_review_submit_requests
    where id = (select job_id from review_submit_job_ids where label = 'passed_process')
  ),
  'submitted',
  'successful from-job submit marks the job submitted'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '35000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '20',
  'successful from-job submit moves the process under review'
);

select is(
  (
    select count(*)::text
    from public.command_audit_log
    where command = 'cmd_review_submit_from_job'
      and actor_user_id = '15000000-0000-0000-0000-000000000001'
      and target_id = '35000000-0000-0000-0000-000000000001'
  ),
  '1',
  'from-job submit records an audit row for the requester'
);

select is(
  public.cmd_review_submit_from_job(
    p_job_id => (select job_id from review_submit_job_ids where label = 'passed_process')
  )->'data'->>'status',
  'submitted',
  'from-job submit is idempotent for an already submitted job'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

insert into review_submit_job_ids (label, job_id, gate_worker_job_id)
select
  'stale_process',
  (result->'data'->>'reviewSubmitJobId')::uuid,
  (result->'data'->>'gateWorkerJobId')::uuid
from (
  select public.cmd_dataset_review_submit_job_enqueue(
    p_table => 'processes',
    p_id => '35000000-0000-0000-0000-000000000002',
    p_version => '01.00.000',
    p_revision_checksum => repeat('b', 64)
  ) as result
) as enqueue;

reset role;
update public.processes
set modified_at = now() + interval '1 second'
where id = '35000000-0000-0000-0000-000000000002'
  and version = '01.00.000';

set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select public.worker_claim_jobs('review_submit_gate', 'review-submit-job-test-worker', 1, 300);

select public.worker_record_job_result(
  p_job_id => (select gate_worker_job_id from review_submit_job_ids where label = 'stale_process'),
  p_lease_token => (
    select lease_token
    from public.worker_jobs
    where id = (select gate_worker_job_id from review_submit_job_ids where label = 'stale_process')
  ),
  p_status => 'completed',
  p_result_json => jsonb_build_object(
    'status', 'passed',
    'datasetRevision', jsonb_build_object(
      'table', 'processes',
      'id', '35000000-0000-0000-0000-000000000002',
      'version', '01.00.000',
      'revisionChecksum', repeat('b', 64)
    ),
    'calculatorReport', '{"summary":{"blockerCount":0}}'::jsonb,
    'blockingReasons', '[]'::jsonb
  ),
  p_result_schema_version => 'review_submit_gate_report.v1'
);

select is(
  public.cmd_review_submit_from_job(
    p_job_id => (select job_id from review_submit_job_ids where label = 'stale_process')
  )->>'code',
  'REVIEW_SUBMIT_JOB_STALE',
  'from-job submit rejects a dataset modified after job creation'
);

select is(
  (
    select status
    from public.dataset_review_submit_requests
    where id = (select job_id from review_submit_job_ids where label = 'stale_process')
  ),
  'stale',
  'stale from-job submit marks the job stale'
);

reset role;
set local role authenticated;
select set_config('request.jwt.claim.sub', '15000000-0000-0000-0000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

insert into review_submit_job_ids (label, job_id, gate_worker_job_id)
select
  'blocked_process',
  (result->'data'->>'reviewSubmitJobId')::uuid,
  (result->'data'->>'gateWorkerJobId')::uuid
from (
  select public.cmd_dataset_review_submit_job_enqueue(
    p_table => 'processes',
    p_id => '35000000-0000-0000-0000-000000000003',
    p_version => '01.00.000',
    p_revision_checksum => repeat('c', 64)
  ) as result
) as enqueue;

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select public.worker_claim_jobs('review_submit_gate', 'review-submit-job-test-worker', 1, 300);

select public.worker_record_job_result(
  p_job_id => (select gate_worker_job_id from review_submit_job_ids where label = 'blocked_process'),
  p_lease_token => (
    select lease_token
    from public.worker_jobs
    where id = (select gate_worker_job_id from review_submit_job_ids where label = 'blocked_process')
  ),
  p_status => 'blocked',
  p_result_json => jsonb_build_object(
    'status', 'blocked',
    'datasetRevision', jsonb_build_object(
      'table', 'processes',
      'id', '35000000-0000-0000-0000-000000000003',
      'version', '01.00.000',
      'revisionChecksum', repeat('c', 64)
    ),
    'calculatorReport', '{"summary":{"blockerCount":1}}'::jsonb,
    'blockingReasons', '[{"code":"singular_risk_medium_or_high","message":"matrix risk"}]'::jsonb
  ),
  p_result_schema_version => 'review_submit_gate_report.v1',
  p_blocker_codes => array['singular_risk_medium_or_high'],
  p_resolution_scope => 'user',
  p_retryable => true
);

select is(
  public.cmd_review_submit_from_job(
    p_job_id => (select job_id from review_submit_job_ids where label = 'blocked_process')
  )->>'code',
  'REVIEW_SUBMIT_GATE_BLOCKED',
  'from-job submit propagates a blocked gate result'
);

select is(
  (
    select status
    from public.dataset_review_submit_requests
    where id = (select job_id from review_submit_job_ids where label = 'blocked_process')
  ),
  'blocked',
  'blocked gate result marks the job blocked'
);

select is(
  (
    select count(*)::text
    from public.reviews
    where data_id = '35000000-0000-0000-0000-000000000003'
      and data_version = '01.00.000'
  ),
  '0',
  'blocked from-job submit does not create a review row'
);

select * from finish();
rollback;
