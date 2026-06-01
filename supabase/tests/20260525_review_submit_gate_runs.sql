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

select plan(17);

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

insert into public.users (id, raw_user_meta_data)
values
  (
    '13000000-0000-0000-0000-000000000001',
    '{"email":"gate-owner@example.com","display_name":"Gate Owner"}'::jsonb
  ),
  (
    '13000000-0000-0000-0000-000000000002',
    '{"email":"gate-outsider@example.com"}'::jsonb
  );

insert into public.teams (id, json, rank, is_public)
values
  ('23000000-0000-0000-0000-000000000001', '{"title":"Gate Team"}'::jsonb, 1, false);

insert into public.roles (user_id, team_id, role)
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
  public.cmd_dataset_review_submit_gate(
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
    public.cmd_dataset_review_submit_gate(
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
    public.cmd_dataset_review_submit_gate(
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

select is(
  (
    public.cmd_dataset_review_submit_gate(
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
    public.cmd_dataset_review_submit_gate(
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
  public.cmd_dataset_review_submit_gate(
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
  public.cmd_dataset_review_submit_gate_record_result(
    p_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
    p_status => 'passed',
    p_calculator_report => '{"reportId":"passed-report","generatedAt":"2026-05-25T00:00:00Z"}'::jsonb,
    p_audit => '{"command":"dataset_review_submit_gate_record_result"}'::jsonb
  )->'data'->>'status',
  'passed',
  'service role can persist a passed worker gate result'
);

reset role;

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

select is(
  public.cmd_review_submit(
    'processes',
    '33000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{}'::jsonb
  )->>'code',
  'REVIEW_SUBMIT_GATE_REQUIRED',
  'process review submission without gate metadata is rejected'
);

select is(
  public.cmd_review_submit(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_audit => '{}'::jsonb,
    p_review_submit_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
    p_review_submit_revision_checksum => repeat('b', 64),
    p_review_submit_policy_profile => 'review_submit_fast.v1',
    p_review_submit_report_schema_version => 'review_submit_gate_report.v1'
  )->>'code',
  'REVIEW_SUBMIT_GATE_STALE',
  'stale gate run cannot authorize review submission'
);

select is(
  public.cmd_review_submit(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_audit => '{}'::jsonb,
    p_review_submit_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
    p_review_submit_revision_checksum => repeat('a', 64),
    p_review_submit_policy_profile => 'wrong_policy.v1',
    p_review_submit_report_schema_version => 'review_submit_gate_report.v1'
  )->>'code',
  'REVIEW_SUBMIT_GATE_POLICY_MISMATCH',
  'wrong policy profile cannot authorize review submission'
);

select is(
  public.cmd_review_submit(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000001',
    p_version => '01.00.000',
    p_audit => '{"command":"review_submit"}'::jsonb,
    p_review_submit_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'passed_process'),
    p_review_submit_revision_checksum => repeat('a', 64),
    p_review_submit_policy_profile => 'review_submit_fast.v1',
    p_review_submit_report_schema_version => 'review_submit_gate_report.v1'
  )->>'ok',
  'true',
  'passed gate run authorizes process review submission'
);

select is(
  (
    select state_code::text
    from public.processes
    where id = '33000000-0000-0000-0000-000000000001'
      and version = '01.00.000'
  ),
  '20',
  'authorized process review submission marks the dataset under review'
);

reset role;

select ok(
  exists (
    select 1
    from public.command_audit_log
    where command = 'cmd_review_submit'
      and target_id = '33000000-0000-0000-0000-000000000001'
      and payload->>'review_submit_gate_run_id' = (
        select gate_run_id::text
        from review_submit_gate_ids
        where label = 'passed_process'
      )
  ),
  'cmd_review_submit audit payload records gate assertion metadata'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '13000000-0000-0000-0000-000000000001', true);

insert into review_submit_gate_ids (label, gate_run_id)
select
  'blocked_process',
  (
    public.cmd_dataset_review_submit_gate(
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
  public.cmd_dataset_review_submit_gate_record_result(
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
  public.cmd_review_submit(
    p_table => 'processes',
    p_id => '33000000-0000-0000-0000-000000000002',
    p_version => '01.00.000',
    p_audit => '{}'::jsonb,
    p_review_submit_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'blocked_process'),
    p_review_submit_revision_checksum => repeat('c', 64),
    p_review_submit_policy_profile => 'review_submit_fast.v1',
    p_review_submit_report_schema_version => 'review_submit_gate_report.v1'
  )->>'code',
  'REVIEW_SUBMIT_GATE_BLOCKED',
  'blocked gate run cannot authorize review submission'
);

select is(
  (
    public.cmd_review_submit(
      p_table => 'processes',
      p_id => '33000000-0000-0000-0000-000000000002',
      p_version => '01.00.000',
      p_audit => '{}'::jsonb,
      p_review_submit_gate_run_id => (select gate_run_id from review_submit_gate_ids where label = 'blocked_process'),
      p_review_submit_revision_checksum => repeat('c', 64),
      p_review_submit_policy_profile => 'review_submit_fast.v1',
      p_review_submit_report_schema_version => 'review_submit_gate_report.v1'
    )->'details'->'blockingReasons'->0->>'code'
  ),
  'provider_unresolved',
  'blocked review submission returns persisted worker blocking reasons'
);

select is(
  (
    select count(*)::text
    from public.reviews
    where data_id = '33000000-0000-0000-0000-000000000002'
      and data_version = '01.00.000'
  ),
  '0',
  'blocked gate run does not create a review row'
);

select is(
  (
    public.cmd_dataset_review_submit_gate(
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

select * from finish();
rollback;
