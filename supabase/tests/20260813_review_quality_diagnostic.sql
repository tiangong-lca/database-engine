begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(29);

select ok(
  pg_catalog.to_regprocedure(
    'api.cmd_review_submit(text,uuid,text,jsonb)'
  ) is not null,
  'the stable four-argument review-submit command exists'
);

select ok(
  pg_catalog.to_regprocedure(
    'api.cmd_review_submit(text,uuid,text,jsonb,uuid,text,text,text)'
  ) is null,
  'the Gate-shaped cmd_review_submit overload is removed'
);

select ok(
  pg_catalog.has_function_privilege(
    'authenticated',
    'api.cmd_review_submit(text,uuid,text,jsonb)',
    'EXECUTE'
  ),
  'authenticated callers can execute the stable submit command'
);

select is(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit(text,uuid,text,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'cmd_dataset_assert_review_submit_gate_passed'
  ),
  0,
  'the stable submit command has no numerical Gate assertion'
);

select is(
  pg_catalog.strpos(
    pg_catalog.pg_get_functiondef(
      'api.cmd_review_submit(text,uuid,text,jsonb)'
        ::pg_catalog.regprocedure
    ),
    'cmd_review_assert_lifecycle_closure'
  ),
  0,
  'the stable submit command has no upstream lifecycle-completeness Gate'
);

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
) values
  (
    '00000000-0000-0000-0000-000000000000',
    '1d000000-0000-0000-0000-000000000001',
    'authenticated',
    'authenticated',
    'quality-owner@example.com',
    'test',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"quality-owner@example.com","display_name":"Quality Owner"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '1d000000-0000-0000-0000-000000000002',
    'authenticated',
    'authenticated',
    'quality-member@example.com',
    'test',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"quality-member@example.com","display_name":"Quality Member"}'::jsonb,
    now(),
    now(),
    false,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '1d000000-0000-0000-0000-000000000003',
    'authenticated',
    'authenticated',
    'quality-admin@example.com',
    'test',
    now(),
    '{"provider":"email","providers":["email"]}'::jsonb,
    '{"email":"quality-admin@example.com","display_name":"Quality Admin"}'::jsonb,
    now(),
    now(),
    false,
    false
  );

insert into private.users (id, raw_user_meta_data)
values
  (
    '1d000000-0000-0000-0000-000000000001',
    '{"email":"quality-owner@example.com","display_name":"Quality Owner"}'
  ),
  (
    '1d000000-0000-0000-0000-000000000002',
    '{"email":"quality-member@example.com","display_name":"Quality Member"}'
  ),
  (
    '1d000000-0000-0000-0000-000000000003',
    '{"email":"quality-admin@example.com","display_name":"Quality Admin"}'
  );

insert into private.teams (id, json, rank, is_public)
values
  (
    '2d000000-0000-0000-0000-000000000001',
    '{"name":"Quality Review Team"}',
    1,
    false
  ),
  (
    '00000000-0000-0000-0000-000000000000',
    '{"name":"System Team"}',
    0,
    false
  )
on conflict (id) do nothing;

insert into private.roles (user_id, team_id, role)
values
  (
    '1d000000-0000-0000-0000-000000000001',
    '2d000000-0000-0000-0000-000000000001',
    'owner'
  ),
  (
    '1d000000-0000-0000-0000-000000000002',
    '00000000-0000-0000-0000-000000000000',
    'review-member'
  ),
  (
    '1d000000-0000-0000-0000-000000000003',
    '00000000-0000-0000-0000-000000000000',
    'review-admin'
  );

alter table public.processes disable trigger "processes_json_sync_trigger";
alter table public.processes disable trigger "process_extract_md_trigger_insert";
alter table public.processes disable trigger "process_extract_md_trigger_update";

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
) values (
  '3d000000-0000-0000-0000-000000000001',
  '01.00.000',
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Quality Diagnostic Process"}]}}}}}'::jsonb,
  '{"processDataSet":{"processInformation":{"dataSetInformation":{"name":{"baseName":[{"@xml:lang":"en","#text":"Quality Diagnostic Process"}]}}}}}'::json,
  '1d000000-0000-0000-0000-000000000001',
  0,
  '2d000000-0000-0000-0000-000000000001',
  '4d000000-0000-0000-0000-000000000001',
  true
);

set local role authenticated;
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config('request.jwt.claim.sub', '', true);

select is(
  api.cmd_review_submit(
    'processes',
    '3d000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{}'::jsonb
  )->>'code',
  'AUTH_REQUIRED',
  'review submission still requires an authenticated actor'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000001',
  true
);

select is(
  api.cmd_review_submit(
    'processes',
    '3d000000-0000-0000-0000-000000000001',
    '01.00.000',
    '{"source":"quality-diagnostic-contract-test"}'::jsonb
  )->>'ok',
  'true',
  'a Process can be submitted without Gate metadata or a Worker result'
);

reset role;

select is(
  (
    select process.state_code
    from public.processes as process
    where process.id = '3d000000-0000-0000-0000-000000000001'
      and process.version = '01.00.000'
  ),
  20,
  'successful submission moves the Process into review'
);

select ok(
  exists (
    select 1
    from private.command_audit_log as audit
    where audit.command = 'cmd_review_submit'
      and audit.target_id = '3d000000-0000-0000-0000-000000000001'
      and not audit.payload ? 'review_submit_gate_run_id'
      and not audit.payload ? 'review_submit_revision_checksum'
  ),
  'submit audit facts do not persist legacy Gate authority metadata'
);

set local role authenticated;
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000002',
  true
);

select is(
  api.cmd_review_quality_diagnostic_start()->>'code',
  'REVIEW_ADMIN_REQUIRED',
  'Review Members cannot start the diagnostic'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000003',
  true
);

select ok(
  api.qry_review_quality_diagnostic()->'data' = 'null'::jsonb,
  'submitting review data does not automatically start a diagnostic'
);

reset role;

create temporary table review_quality_run (
  label text primary key,
  result jsonb not null
) on commit drop;

create temporary table review_quality_claim (
  result jsonb not null
) on commit drop;

grant all on review_quality_run, review_quality_claim to public;

set local role authenticated;
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000003',
  true
);

insert into review_quality_run (label, result)
values ('first', api.cmd_review_quality_diagnostic_start());

select is(
  (select result->>'ok' from review_quality_run where label = 'first'),
  'true',
  'Review Admin can manually start the diagnostic'
);

select is(
  (select result #>> '{data,status}' from review_quality_run where label = 'first'),
  'queued',
  'a new diagnostic starts asynchronously in queued state'
);

reset role;

select is(
  (
    select concat_ws('|', job.job_kind, job.worker_queue, job.visibility)
    from private.worker_jobs as job
    where job.id = (
      select (result #>> '{data,runId}')::uuid
      from review_quality_run
      where label = 'first'
    )
  ),
  'review.quality_diagnostic|review_quality|operator',
  'the diagnostic uses its own operator-only worker contract'
);

set local role authenticated;
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000003',
  true
);

insert into review_quality_run (label, result)
values ('reused', api.cmd_review_quality_diagnostic_start());

select is(
  (select result->>'reused' from review_quality_run where label = 'reused'),
  'true',
  'a repeated click reuses the active diagnostic run'
);

select is(
  (select result #>> '{data,runId}' from review_quality_run where label = 'reused'),
  (select result #>> '{data,runId}' from review_quality_run where label = 'first'),
  'active-run reuse returns the same run identifier'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000002',
  true
);

select is(
  api.qry_review_quality_diagnostic()->>'code',
  'REVIEW_ADMIN_REQUIRED',
  'Review Members cannot read the diagnostic report'
);

select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000003',
  true
);

select is(
  api.qry_review_quality_diagnostic()->'data'->>'status',
  'queued',
  'Review Admin can read the latest diagnostic state'
);

select ok(
  not pg_catalog.has_table_privilege(
    'authenticated',
    'private.worker_jobs',
    'SELECT'
  ),
  'diagnostic access does not grant direct worker_jobs reads'
);

reset role;
set local role service_role;
select pg_catalog.set_config('request.jwt.claim.role', 'service_role', true);
select pg_catalog.set_config('request.jwt.claim.sub', '', true);

insert into review_quality_claim (result)
select private.worker_claim_jobs(
  'review_quality',
  'review-quality-contract-test',
  1,
  3600
);

select is(
  (
    select result #>> '{data,0,id}'
    from review_quality_claim
  ),
  (
    select result #>> '{data,runId}'
    from review_quality_run
    where label = 'first'
  ),
  'the Worker can claim the dedicated review-quality queue'
);

select is(
  private.worker_record_job_result(
    p_job_id => (
      select (result #>> '{data,runId}')::uuid
      from review_quality_run
      where label = 'first'
    ),
    p_lease_token => (
      select (result #>> '{data,0,leaseToken}')::uuid
      from review_quality_claim
    ),
    p_status => 'completed',
    p_result_json => '{
      "schemaVersion":"review.quality_diagnostic.report.v1",
      "outcome":"findings",
      "summary":{"evaluatedRecords":3,"findingCount":2},
      "findings":[
        {"code":"missing_upstream","message":"An upstream dataset is unavailable"},
        {"code":"numerically_not_evaluable","message":"The pending matrix cannot be evaluated"}
      ]
    }'::jsonb,
    p_result_schema_version => 'review.quality_diagnostic.report.v1'
  )->>'ok',
  'true',
  'the Worker can record informational findings as a completed diagnostic'
);

reset role;
set local role authenticated;
select pg_catalog.set_config('request.jwt.claim.role', 'authenticated', true);
select pg_catalog.set_config(
  'request.jwt.claim.sub',
  '1d000000-0000-0000-0000-000000000003',
  true
);

select is(
  api.qry_review_quality_diagnostic()->'data'->>'status',
  'completed',
  'Review Admin sees the completed diagnostic state'
);

select is(
  api.qry_review_quality_diagnostic()->'data'->>'outcome',
  'findings',
  'findings are exposed as an informational outcome, not blocked state'
);

select is(
  api.qry_review_quality_diagnostic()
    #>> '{data,report,summary,findingCount}',
  '2',
  'Review Admin can read the structured findings summary'
);

insert into review_quality_run (label, result)
values ('second', api.cmd_review_quality_diagnostic_start());

select is(
  (select result->>'reused' from review_quality_run where label = 'second'),
  'false',
  'a manual click after completion creates a fresh diagnostic run'
);

select isnt(
  (select result #>> '{data,runId}' from review_quality_run where label = 'second'),
  (select result #>> '{data,runId}' from review_quality_run where label = 'first'),
  'the fresh run has a new worker-job identity without a Batch entity'
);

reset role;

select throws_like(
  $$
    update private.worker_jobs
    set status = 'blocked',
        blocker_codes = array['quality_finding'],
        resolution_scope = 'operator'
    where id = (
      select (result #>> '{data,runId}')::uuid
      from review_quality_run
      where label = 'second'
    )
  $$,
  '%worker_jobs_review_quality_diagnostic_semantics_check%',
  'diagnostic runs cannot acquire Gate-style blocked semantics'
);

select is(
  (
    select count(*)::integer
    from private.reviews
    where data_id = '3d000000-0000-0000-0000-000000000001'
      and data_version = '01.00.000'
  ),
  1,
  'diagnostic execution does not mutate or duplicate review workflow state'
);

select is(
  (
    select concat_ws(
      '|',
      kind.user_visible::text,
      coalesce(kind.task_center_category, ''),
      coalesce(kind.task_center_surface, ''),
      coalesce(kind.presenter_key, '')
    )
    from private.worker_job_kinds as kind
    where kind.job_kind = 'review.quality_diagnostic'
  ),
  'false|||',
  'the optional diagnostic is not injected into the normal task center'
);

select * from finish();
rollback;
