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

select plan(20);

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
    '13000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'gate-owner@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"13000000-0000-0000-0000-000000000001","email":"gate-owner@example.com","display_name":"Gate Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '13000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'gate-outsider@example.com',
    'test-password-hash',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"sub":"13000000-0000-0000-0000-000000000002","email":"gate-outsider@example.com"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data)
values
  (
    '13000000-0000-0000-0000-000000000001',
    '{"email":"gate-owner@example.com","display_name":"Gate Owner"}'::jsonb
  ),
  (
    '13000000-0000-0000-0000-000000000002',
    '{"email":"gate-outsider@example.com"}'::jsonb
  );

insert into private.teams (id, json, rank, is_public)
values
  ('23000000-0000-0000-0000-000000000001', '{"title":"Gate Team"}'::jsonb, 1, false);

insert into private.roles (user_id, team_id, role)
values
  ('13000000-0000-0000-0000-000000000001', '23000000-0000-0000-0000-000000000001', 'owner');

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
    '33000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Gate Passed Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Gate Passed Process"}]}}}}}'::json,
    '13000000-0000-0000-0000-000000000001',
    0,
    '23000000-0000-0000-0000-000000000001',
    '43000000-0000-0000-0000-000000000001',
    true
  ),
  (
    '33000000-0000-0000-0000-000000000002',
    '01.00.000',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Gate Blocked Process"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Gate Blocked Process"}]}}}}}'::json,
    '13000000-0000-0000-0000-000000000001',
    0,
    '23000000-0000-0000-0000-000000000001',
    '43000000-0000-0000-0000-000000000002',
    true
  );

create temporary table review_submit_gate_ids (
  label text primary key,
  gate_run_id uuid not null
) on commit drop;

grant all on review_submit_gate_ids to public;

select is(
  api.cmd_dataset_review_submit_gate(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_revision_checksum => repeat('a', 64)
  )->>'code',
  'AUTH_REQUIRED',
  'review-submit gate requires authentication'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

insert into review_submit_gate_ids (label, gate_run_id)
select
  'passed_process',
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64),
      p_action => 'ensure',
      p_audit => '{"command":"dataset_review_submit_gate"}'::jsonb
    )->'data'->>'gateRunId'
  )::uuid;

select is(
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64),
      p_action => 'read',
      p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process')
    )->'data'->>'status'
  ),
  'queued',
  'ensure creates a queued review-submit gate run'
);

select ok(
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64),
      p_action => 'read',
      p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process')
    )->'data'
  ) ? 'workerJobId',
  'standalone ensure links the retained gate run to a worker_jobs record'
);

select is(
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('a', 64),
      p_action => 'ensure'
    )->'data'->>'gateRunId'
  ),
  (select gate_run_id::text from review_submit_gate_ids where label = 'passed_process'),
  'ensure reuses the current gate run for the same revision checksum and policy'
);

select is(
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000001',
      p_version => '01.00.000',
      p_revision_checksum => repeat('b', 64),
      p_action => 'read',
      p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process')
    )->'data'->>'status'
  ),
  'stale',
  'read reports stale when the caller presents a different revision checksum'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000002', true);

select is(
  api.cmd_dataset_review_submit_gate(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_revision_checksum => repeat('a', 64)
  )->>'code',
  'DATASET_OWNER_REQUIRED',
  'non-owners cannot run another user dataset review-submit gate'
);

reset role;

set local role service_role;

select is(
  private.cmd_dataset_review_submit_gate_record_result(
    p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
    p_status => 'passed',
    p_calculator_report => '{"reportId":"passed-report","generatedAt":"2026-05-25T00:00:00Z"}'::jsonb,
    p_audit => '{"command":"dataset_review_submit_gate_record_result"}'::jsonb
  )->'data'->>'status',
  'passed',
  'service role can persist a passed worker gate result'
);

select is(
  (
    select worker_job.status
    from private.dataset_review_submit_gate_runs as gate_run
    join private.worker_jobs as worker_job
      on worker_job.id = gate_run.worker_job_id
    where gate_run.id = (select gate_run_id from review_submit_gate_ids where label = 'passed_process')
  ),
  'completed',
  'legacy record_result synchronizes linked worker_jobs status for passed gates'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    'processes',
    '33000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{}'::jsonb
  )->>'ok',
  'true',
  'process review submission no longer requires Gate metadata'
);

select is(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit(text,uuid,text,jsonb)'::pg_catalog.regprocedure
    ),
    'cmd_dataset_assert_review_submit_gate_passed'
  ),
  0,
  'the stable submit command no longer contains a Gate assertion'
);

select is(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit_v2(text,uuid,text,jsonb,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'cmd_dataset_assert_review_submit_gate_passed'
  ),
  0,
  'the compatibility wrapper no longer evaluates legacy Gate context'
);

select is(
  api.cmd_review_submit_v2(
    'processes',
    '33000000-0000-0000-0000-000000000001',
    '01.00.000',
    jsonb_build_object(
      'reviewSubmitGateRunId',
      (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
      'revisionChecksum', repeat('b', 64),
      'policyProfile', 'wrong_policy.v1'
    ),
    '{}'::jsonb
  )->>'code',
  'DATA_UNDER_REVIEW',
  'legacy Gate context cannot override ordinary review lifecycle state'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '20',
  'Gate-free process review submission marks the dataset under review'
);

reset role;

select ok(
  exists (
    select 1
    from private.command_audit_log
    where command = 'cmd_review_submit'
      and target_id = '33000000-0000-0000-0000-000000000001'
      and not payload ? 'review_submit_gate_run_id'
      and not payload ? 'review_submit_revision_checksum'
  ),
  'cmd_review_submit audit payload contains no Gate authority metadata'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

insert into review_submit_gate_ids (label, gate_run_id)
select
  'blocked_process',
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000002',
      p_version => '01.00.000',
      p_revision_checksum => repeat('c', 64),
      p_action => 'ensure'
    )->'data'->>'gateRunId'
  )::uuid;

reset role;

set local role service_role;

select is(
  private.cmd_dataset_review_submit_gate_record_result(
    p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'blocked_process'),
    p_status => 'blocked',
    p_calculator_report => '{"reportId":"blocked-report","generatedAt":"2026-05-25T00:00:00Z"}'::jsonb,
    p_blocking_reasons => '[{"code":"provider_unresolved","message":"provider is missing"}]'::jsonb
  )->'data'->>'status',
  'blocked',
  'service role can persist a blocked worker gate result'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

select is(
  api.cmd_review_submit(
    'processes',
    '33000000-0000-0000-0000-000000000002',
    '01.00.000',
    '{}'::jsonb
  )->>'ok',
  'true',
  'a historical blocked Gate result does not block review submission'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33000000-0000-0000-0000-000000000002'
      and version = '01.00.000'
  ),
  '20',
  'the formerly blocked Process enters review normally'
);

select is(
  (
    select count(*)::text
    from private.reviews
    where data_id = '33000000-0000-0000-0000-000000000002'
      and data_version = '01.00.000'
  ),
  '1',
  'historical blocked Gate state does not suppress review creation'
);

select is(
  (
    api.cmd_dataset_review_submit_gate(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000002',
      p_version => '01.00.000',
      p_revision_checksum => repeat('c', 64),
      p_action => 'rerun',
      p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'blocked_process')
    )->'data'->>'status'
  ),
  'queued',
  'rerun creates a fresh queued gate run after a blocked result'
);

reset role;

select is(
  (
    select count(*)::text
    from private.dataset_review_submit_gate_runs
    where status in ('queued', 'running')
      and worker_job_id is null
  ),
  '0',
  'standalone gate API does not leave queued gate runs without worker_jobs linkage'
);

select * from finish();
rollback;
