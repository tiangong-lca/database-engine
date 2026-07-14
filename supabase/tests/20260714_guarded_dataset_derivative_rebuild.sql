begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(108);

select ok(
  to_regclass('util.dataset_derivative_rebuild_requests') is not null,
  'durable derivative rebuild request table exists'
);
select ok(
  to_regclass('util.dataset_derivative_rebuild_proposals') is not null,
  'private derivative proposal table exists'
);
select ok(
  to_regclass('util.dataset_derivative_rebuild_permits') is not null,
  'private transaction permit table exists'
);
select ok(
  to_regprocedure(
    'public.cmd_dataset_derivative_rebuild_snapshot(text,uuid,text)'
  ) is not null,
  'action-scoped snapshot RPC exists'
);
select ok(
  to_regprocedure(
    'public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)'
  ) is not null,
  'guarded admission RPC exists'
);
select ok(
  to_regprocedure(
    'public.cmd_dataset_derivative_rebuild_read(uuid)'
  ) is not null,
  'owner status read RPC exists'
);
select ok(
  to_regprocedure(
    'util.process_dataset_derivative_rebuilds(integer)'
  ) is not null,
  'private derivative coordinator exists'
);

select ok(
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_derivative_rebuild_snapshot(text,uuid,text)',
    'execute'
  ),
  'authenticated can call snapshot RPC'
);
select ok(
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)',
    'execute'
  ),
  'authenticated can call guarded admission RPC'
);
select ok(
  has_function_privilege(
    'authenticated',
    'public.cmd_dataset_derivative_rebuild_read(uuid)',
    'execute'
  ),
  'authenticated can call owner status read RPC'
);
select ok(
  not has_function_privilege(
    'anon',
    'public.cmd_dataset_derivative_rebuild_snapshot(text,uuid,text)',
    'execute'
  ),
  'anon cannot call snapshot RPC'
);
select ok(
  not has_function_privilege(
    'anon',
    'public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)',
    'execute'
  ),
  'anon cannot call guarded admission RPC'
);
select ok(
  not has_function_privilege(
    'anon',
    'public.cmd_dataset_derivative_rebuild_read(uuid)',
    'execute'
  ),
  'anon cannot call owner status read RPC'
);
select ok(
  not has_function_privilege(
    'service_role',
    'public.cmd_dataset_derivative_rebuild_plan_guarded(jsonb)',
    'execute'
  ),
  'service_role cannot call owner guarded admission RPC'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'util.process_dataset_derivative_rebuilds(integer)',
    'execute'
  ),
  'authenticated cannot call the private coordinator'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'util.queue_embeddings()',
    'execute'
  ),
  'authenticated cannot invoke the raw embedding queue trigger function'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'public.processes_derivative_rebuild_embedding_input(public.processes)',
    'execute'
  ),
  'authenticated cannot read private staged Markdown through the worker helper'
);
select ok(
  not has_table_privilege(
    'authenticated',
    'util.dataset_derivative_rebuild_requests',
    'select'
  ),
  'authenticated cannot read private request rows directly'
);
select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as namespace
      on namespace.oid = proc.pronamespace
    where namespace.nspname = 'public'
      and proc.proname in (
        'cmd_dataset_derivative_rebuild_snapshot',
        'cmd_dataset_derivative_rebuild_plan_guarded',
        'cmd_dataset_derivative_rebuild_read'
      )
      and proc.prosecdef
  ),
  3,
  'all three public RPCs are security definer'
);
select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as namespace
      on namespace.oid = proc.pronamespace
    where namespace.nspname = 'public'
      and proc.proname in (
        'cmd_dataset_derivative_rebuild_snapshot',
        'cmd_dataset_derivative_rebuild_plan_guarded',
        'cmd_dataset_derivative_rebuild_read'
      )
      and exists (
        select 1
        from unnest(coalesce(proc.proconfig, array[]::text[])) as config(value)
        where config.value in ('search_path=', 'search_path=""')
      )
  ),
  3,
  'all three public RPCs pin an empty search_path'
);
select is(
  (
    select count(*)::integer
    from cron.job
    where jobname = 'process-dataset-derivative-rebuilds'
  ),
  1,
  'derivative coordinator cron exists exactly once'
);
select is(
  (
    select schedule
    from cron.job
    where jobname = 'process-dataset-derivative-rebuilds'
  ),
  '* * * * *',
  'derivative coordinator runs once per minute'
);
select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid = 'public.processes'::regclass
      and tgname like 'process_derivative_rebuild_%'
      and not tgisinternal
  ),
  4,
  'processes has primary freeze plus Markdown/vector staging triggers'
);
select ok(
  exists (
    select 1
    from pg_trigger
    where tgrelid = 'pgmq.q_embedding_jobs'::regclass
      and tgname = 'dataset_derivative_rebuild_embedding_visibility_fence'
      and not tgisinternal
  ),
  'request-tagged embedding jobs have a visibility fence trigger'
);
select ok(
  exists (
    select 1
    from pg_indexes
    where schemaname = 'util'
      and indexname = 'dataset_derivative_rebuild_active_target_uidx'
  ),
  'active target fence is unique'
);
select ok(
  exists (
    select 1
    from pg_indexes
    where schemaname = 'public'
      and indexname = 'command_audit_log_derivative_rebuild_plan_uidx'
  ),
  'plan summary audit replay is unique'
);
select ok(
  exists (
    select 1
    from pg_indexes
    where schemaname = 'public'
      and indexname = 'command_audit_log_derivative_rebuild_action_uidx'
  ),
  'action audit replay is unique'
);

alter table public.processes
  disable trigger process_extract_md_trigger_insert;
alter table public.processes
  disable trigger processes_json_sync_trigger;
alter table public.processes
  disable trigger zz_processes_extracted_text_sync_trigger;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  extracted_text,
  extracted_md,
  modified_at
) values
  (
    '11111111-1111-4111-8111-111111111111',
    '00.00.001',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"11111111-1111-4111-8111-111111111111","name":{"baseName":[{"@xml:lang":"en","#text":"fixture one"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"11111111-1111-4111-8111-111111111111","name":{"baseName":[{"@xml:lang":"en","#text":"fixture one"}]}}}}}'::json,
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    0,
    'fixture extracted text one',
    'same markdown',
    '2026-07-14 00:00:00+00'
  ),
  (
    '22222222-2222-4222-8222-222222222222',
    '00.00.001',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"22222222-2222-4222-8222-222222222222","name":{"baseName":[{"@xml:lang":"en","#text":"fixture two"}]}}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"22222222-2222-4222-8222-222222222222","name":{"baseName":[{"@xml:lang":"en","#text":"fixture two"}]}}}}}'::json,
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    0,
    'fixture extracted text two',
    'failure markdown',
    '2026-07-14 00:00:00+00'
  ),
  (
    '33333333-3333-4333-8333-333333333333',
    '00.00.001',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"33333333-3333-4333-8333-333333333333"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"33333333-3333-4333-8333-333333333333"}}}}'::json,
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
    0,
    'foreign extracted text',
    'foreign markdown',
    '2026-07-14 00:00:00+00'
  ),
  (
    '44444444-4444-4444-8444-444444444444',
    '00.00.001',
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"44444444-4444-4444-8444-444444444444"}}}}'::jsonb,
    '{"processDataSet":{"processInformation":{"dataSetInformation":{"UUID":"44444444-4444-4444-8444-444444444444"}}}}'::json,
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    100,
    'public extracted text',
    'public markdown',
    '2026-07-14 00:00:00+00'
  );

alter table public.processes
  enable trigger process_extract_md_trigger_insert;
alter table public.processes
  enable trigger processes_json_sync_trigger;
alter table public.processes
  enable trigger zz_processes_extracted_text_sync_trigger;

delete from pgmq.q_embedding_jobs;
delete from util.pending_embedding_jobs;
delete from net.http_request_queue;
delete from net._http_response where id < 0;
delete from public.command_audit_log
where command = 'cmd_dataset_derivative_rebuild_plan_guarded';

select set_config(
  'request.jwt.claim.sub',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  true
);
set local role authenticated;

create temporary table derivative_snapshot_one as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'processes',
  '11111111-1111-4111-8111-111111111111',
  '00.00.001'
) as result;

reset role;

select ok(
  (select (result->>'ok')::boolean from derivative_snapshot_one),
  'owner can snapshot one exact draft process'
);
select ok(
  (
    select result->>'table' = 'processes'
      and result->>'id' = '11111111-1111-4111-8111-111111111111'
      and result->>'state_code' = '0'
      and result->>'snapshot_sha256' ~ '^[a-f0-9]{64}$'
    from derivative_snapshot_one
  ),
  'snapshot binds identity, state, and opaque full fingerprint'
);

set local role authenticated;
create temporary table derivative_foreign_snapshot as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'processes',
  '33333333-3333-4333-8333-333333333333',
  '00.00.001'
) as result;
create temporary table derivative_public_snapshot as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'processes',
  '44444444-4444-4444-8444-444444444444',
  '00.00.001'
) as result;
reset role;

select is(
  (select result->>'code' from derivative_foreign_snapshot),
  'DERIVATIVE_SNAPSHOT_NOT_AVAILABLE',
  'foreign-owner snapshot is uniformly unavailable'
);
select is(
  (select result->>'code' from derivative_public_snapshot),
  'DERIVATIVE_SNAPSHOT_NOT_AVAILABLE',
  'public state-100 snapshot is uniformly unavailable'
);

create temporary table derivative_plan_one as
select jsonb_build_object(
  'schema_version', 'dataset-derivative-rebuild-plan.v1',
  'plan_sha256', repeat('a', 64),
  'operation_id', 'pgtap-derivative-success',
  'target_visibility', 'owner_draft',
  'actions', jsonb_build_array(jsonb_build_object(
    'action_id', 'rebuild-one',
    'action', 'rebuild_derivatives',
    'table', 'processes',
    'id', '11111111-1111-4111-8111-111111111111',
    'version', '00.00.001',
    'expected_state_code', 0,
    'expected_snapshot_sha256',
      (select result->>'snapshot_sha256' from derivative_snapshot_one),
    'components', jsonb_build_array('extracted_md', 'embedding_ft'),
    'reason_code', 'pgtap_rebuild'
  ))
) as plan;
grant select on derivative_plan_one to authenticated;

set local role authenticated;
create temporary table derivative_invalid_result as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan || jsonb_build_object('unexpected', true) from derivative_plan_one)
) as result;
reset role;

select is(
  (select result->>'code' from derivative_invalid_result),
  'DERIVATIVE_PLAN_INVALID_REQUEST',
  'unexpected plan keys fail closed'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  0,
  'invalid plan creates no request'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  0,
  'invalid plan creates no audit'
);

select net.http_post(
  url => 'http://127.0.0.1:1/functions/v1/embedding_ft',
  body => jsonb_build_array(jsonb_build_object(
    'jobId', 7001,
    'id', '11111111-1111-4111-8111-111111111111',
    'version', '00.00.001',
    'schema', 'public',
    'table', 'processes',
    'contentFunction', 'processes_embedding_ft_input',
    'embeddingColumn', 'embedding_ft',
    'edgeFunction', 'embedding_ft'
  )),
  timeout_milliseconds => 300000
);
select net.http_post(
  url => 'http://127.0.0.1:1/functions/v1/embedding_ft',
  body => jsonb_build_array(jsonb_build_object(
    'jobId', 7000,
    'id', '33333333-3333-4333-8333-333333333333',
    'version', '00.00.001',
    'schema', 'public',
    'table', 'processes',
    'contentFunction', 'processes_embedding_ft_input',
    'embeddingColumn', 'embedding_ft',
    'edgeFunction', 'embedding_ft'
  )),
  timeout_milliseconds => 300000
);
create temporary table derivative_mixed_http as
select net.http_post(
  url => 'http://127.0.0.1:1/functions/v1/embedding_ft',
  body => jsonb_build_array(
    jsonb_build_object(
      'jobId', 7002,
      'id', '11111111-1111-4111-8111-111111111111',
      'version', '00.00.001',
      'schema', 'public',
      'table', 'processes',
      'contentFunction', 'processes_embedding_ft_input',
      'embeddingColumn', 'embedding_ft',
      'edgeFunction', 'embedding_ft'
    ),
    jsonb_build_object(
      'jobId', 7003,
      'id', '33333333-3333-4333-8333-333333333333',
      'version', '00.00.001',
      'schema', 'public',
      'table', 'processes',
      'contentFunction', 'processes_embedding_ft_input',
      'embeddingColumn', 'embedding_ft',
      'edgeFunction', 'embedding_ft'
    )
  ),
  timeout_milliseconds => 300000
) as request_id;
select is(
  (
    select count(*)::integer
    from net.http_request_queue
    where url like '%/functions/v1/embedding_ft'
  ),
  3,
  'fixture has target, foreign, and mixed already-claimed embedding HTTP requests'
);

set local role authenticated;
create temporary table derivative_submit_one as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan from derivative_plan_one)
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from derivative_submit_one),
  'valid exact owner-draft plan is admitted'
);
select is(
  (select result->>'status' from derivative_submit_one),
  'queued',
  'admission reports queued rather than completed'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  1,
  'valid admission creates one durable request'
);
select is(
  (
    select util.dataset_derivative_rebuild_primary_matches(
      request,
      null::public.processes
    )
    from util.dataset_derivative_rebuild_requests as request
  ),
  false,
  'primary safety predicate fails closed for a null composite row'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  2,
  'valid admission creates one action and one plan audit'
);
select is(
  (select count(*)::integer from pgmq.q_embedding_jobs),
  0,
  'admission does not start embedding before the drain and Markdown phases'
);
select is(
  (
    select count(*)::integer
    from net.http_request_queue
    where url like '%/functions/v1/embedding_ft'
      and util.dataset_derivative_rebuild_http_body_matches(
        body,
        '11111111-1111-4111-8111-111111111111',
        '00.00.001'
      )
  ),
  0,
  'admission quarantines already-claimed embedding HTTP work for the target'
);
select is(
  (
    select count(*)::integer
    from net.http_request_queue
    where url like '%/functions/v1/embedding_ft'
      and util.dataset_derivative_rebuild_http_body_matches(
        body,
        '33333333-3333-4333-8333-333333333333',
        '00.00.001'
      )
  ),
  1,
  'target quarantine preserves the standalone unrelated embedding HTTP request'
);
select is(
  (
    select count(*)::integer
    from net.http_request_queue
    where id = (select request_id from derivative_mixed_http)
  ),
  0,
  'mixed target and foreign HTTP batch is canceled as one retryable worker batch'
);
select is(
  (
    select quarantined_http_requests
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  2,
  'admission records both target-bearing quarantined embedding HTTP requests'
);

set local role authenticated;
create temporary table derivative_replay_one as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan from derivative_plan_one)
) as result;
reset role;

select ok(
  (select (result->>'idempotent_replay')::boolean from derivative_replay_one),
  'identical admission is reported as an idempotent replay'
);
select is(
  (select result->>'request_id' from derivative_replay_one),
  (select result->>'request_id' from derivative_submit_one),
  'identical replay preserves the original request id'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  1,
  'identical replay creates no duplicate request'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  2,
  'identical replay creates no duplicate audit'
);

select throws_ok(
  $$update public.processes
      set extracted_text = 'must be fenced'
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'$$,
  '55006',
  'Process primary row is fenced by an active derivative rebuild',
  'active request rejects primary updates'
);
select throws_ok(
  $$delete from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'$$,
  '55006',
  'Process primary row is fenced by an active derivative rebuild',
  'active request rejects target deletion'
);

select lives_ok(
  $$update public.processes
      set extracted_md = 'same markdown'
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'$$,
  'late external same-Markdown write is staged without failing its caller'
);
select is(
  (
    select extracted_md
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'same markdown',
  'staged external Markdown does not directly mutate the target'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_proposals
    where proposal_kind = 'markdown'
      and status = 'captured'
  ),
  1,
  'same-Markdown write still creates one proposal'
);

update util.dataset_derivative_rebuild_proposals
set status = 'discarded', discarded_at = clock_timestamp()
where proposal_kind = 'markdown';

update util.dataset_derivative_rebuild_requests
set
  status = 'markdown_pending',
  phase = 'markdown_dispatched',
  markdown_request_id = -9714001,
  markdown_dispatched_at = clock_timestamp() - interval '1 second',
  markdown_deadline_at = clock_timestamp() + interval '10 minutes',
  updated_at = clock_timestamp()
where id = (
  select (result->>'request_id')::uuid
  from derivative_submit_one
);

update public.processes
set extracted_md = 'new staged markdown'
where id = '11111111-1111-4111-8111-111111111111'
  and version = '00.00.001';

insert into net._http_response (
  id,
  status_code,
  content_type,
  headers,
  content,
  timed_out,
  error_msg,
  created
) values (
  -9714001,
  200,
  'application/json',
  '{}'::jsonb,
  '{"success":true,"results":[{"index":0,"id":"11111111-1111-4111-8111-111111111111","version":"00.00.001","type":"UPDATE","table":"processes","status":"success","markdownLength":13}]}',
  false,
  null,
  clock_timestamp()
);

select is(
  util.process_dataset_derivative_rebuilds(5),
  1,
  'coordinator advances exact Markdown response and proposal'
);
select is(
  (
    select status
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  'embedding_pending',
  'Markdown commit advances to embedding pending'
);
select is(
  (select count(*)::integer from pgmq.q_embedding_jobs),
  1,
  'same-Markdown rebuild explicitly enqueues exactly one embedding job'
);
select is(
  (
    select count(*)::integer
    from pgmq.q_embedding_jobs
    where message->>'requestId' = (
      select result->>'request_id'
      from derivative_submit_one
    )
  ),
  1,
  'embedding job is tagged with the durable request id'
);
update pgmq.q_embedding_jobs
set vt = clock_timestamp() + interval '300 seconds'
where message->>'requestId' = (
  select result->>'request_id'
  from derivative_submit_one
);
select ok(
  (
    select vt >= clock_timestamp() + interval '419 seconds'
    from pgmq.q_embedding_jobs
    where message->>'requestId' = (
      select result->>'request_id'
      from derivative_submit_one
    )
  ),
  'request-tagged embedding claim remains invisible past the Edge max runtime'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_proposals
    where proposal_kind = 'markdown'
      and status = 'accepted'
  ),
  1,
  'coordinator accepts exactly one staged Markdown without exposing it yet'
);
select is(
  (
    select extracted_md
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'same markdown',
  'accepted Markdown remains private until the vector is ready'
);
select is(
  (
    select message->>'contentFunction'
    from pgmq.q_embedding_jobs
    where message->>'requestId' = (
      select result->>'request_id'
      from derivative_submit_one
    )
  ),
  'processes_derivative_rebuild_embedding_input',
  'request-specific embedding reads the accepted staged Markdown'
);

create temporary table derivative_pending_bridge as
with inserted as (
  insert into util.pending_embedding_jobs (
    schema_name,
    table_name,
    record_id,
    record_version,
    content_function,
    embedding_column,
    edge_function,
    message,
    status,
    queue_msg_id,
    created_at,
    updated_at,
    enqueued_at
  )
  select
    'public',
    'processes',
    '11111111-1111-4111-8111-111111111111',
    '00.00.001',
    'processes_embedding_ft_input',
    'embedding_ft',
    'embedding_ft',
    job.message,
    'enqueued',
    job.msg_id,
    clock_timestamp() - interval '10 seconds',
    clock_timestamp() - interval '5 seconds',
    clock_timestamp() - interval '5 seconds'
  from pgmq.q_embedding_jobs as job
  where job.message->>'requestId' = (
    select result->>'request_id'
    from derivative_submit_one
  )
  returning id, queue_msg_id, enqueued_at
)
select * from inserted;

update util.dataset_derivative_rebuild_requests
set
  embedding_pending_job_id = (
    select id from derivative_pending_bridge
  ),
  embedding_queue_msg_id = null
where id = (
  select (result->>'request_id')::uuid
  from derivative_submit_one
);

select lives_ok(
  $$update public.processes
      set
        embedding_ft = (
          '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
        )::extensions.vector,
        embedding_ft_at = clock_timestamp() + interval '1 second'
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'$$,
  'external embedding write is staged while the request fence is active'
);
select ok(
  (
    select embedding_ft is null
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'staged embedding does not directly mutate the target'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_proposals
    where proposal_kind = 'embedding'
      and status = 'captured'
  ),
  1,
  'external embedding write creates one request proposal'
);

update util.dataset_derivative_rebuild_proposals
set embedding_ft_at = '-infinity'::timestamp with time zone
where proposal_kind = 'embedding'
  and status = 'captured';

select throws_ok(
  format(
    'select util.commit_dataset_derivative_rebuild_proposal(%L, %s, %s)',
    (select result->>'request_id' from derivative_submit_one),
    (
      select markdown_proposal_id
      from util.dataset_derivative_rebuild_requests
      where id = (select (result->>'request_id')::uuid from derivative_submit_one)
    ),
    (
      select min(id)
      from util.dataset_derivative_rebuild_proposals
      where proposal_kind = 'embedding'
        and status = 'captured'
    )
  ),
  '22023',
  'Embedding proposal is not newer than the frozen derivative baseline',
  'stale embedding timestamp is rejected before derivative publication'
);
select ok(
  (
    select extracted_md = 'same markdown'
      and embedding_ft is null
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'rejected completion preserves the previously visible derivative pair'
);

update util.dataset_derivative_rebuild_proposals
set embedding_ft_at = clock_timestamp() + interval '1 second'
where proposal_kind = 'embedding'
  and status = 'captured';

delete from pgmq.q_embedding_jobs
where msg_id = (
  select queue_msg_id
  from derivative_pending_bridge
);

select is(
  util.process_dataset_derivative_rebuilds(5),
  1,
  'coordinator commits the ACKed request-specific embedding proposal'
);
select is(
  (
    select status
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  'completed',
  'request becomes completed only after staged vector commit'
);
select ok(
  (
    select status = 'completed'
      and terminal_at is not null
      and drained_at is not null
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  'completed request records terminal and drained proof'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_terminal'
      and payload->>'request_id' = (
        select result->>'request_id'
        from derivative_submit_one
      )
      and payload->>'status' = 'completed'
  ),
  1,
  'completed request appends one immutable terminal audit'
);
select ok(
  (
    select extracted_md is not null
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'completed target has non-null Markdown'
);
select is(
  (
    select extracted_md
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'new staged markdown',
  'completion atomically exposes the accepted Markdown with its vector'
);
select ok(
  (
    select embedding_ft is not null
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'completed target has non-null embedding'
);
select ok(
  (
    select embedding_ft_at > '2026-07-14 00:00:00+00'
    from public.processes
    where id = '11111111-1111-4111-8111-111111111111'
      and version = '00.00.001'
  ),
  'completed target has a fresh embedding timestamp'
);
select ok(
  (
    select util.dataset_derivative_rebuild_primary_matches(request, process)
    from util.dataset_derivative_rebuild_requests as request
    join public.processes as process
      on process.id = request.target_id
     and btrim(process.version::text) = request.target_version
    where request.id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  'completed rebuild leaves the full frozen primary fingerprint unchanged'
);
select is(
  (
    select embedding_queued_at
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_one
    )
  ),
  (select enqueued_at from derivative_pending_bridge),
  'deferred handoff preserves the actual enqueue time for proposal correlation'
);

set local role authenticated;
create temporary table derivative_read_one as
select public.cmd_dataset_derivative_rebuild_read(
  (select (result->>'request_id')::uuid from derivative_submit_one)
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from derivative_read_one),
  'owner can independently read durable request proof'
);
select is(
  (select result->>'status' from derivative_read_one),
  'completed',
  'owner read reports completed'
);
select is(
  (select result->>'fence_active' from derivative_read_one),
  'false',
  'completed owner read proves the target fence is released'
);

set local role authenticated;
select set_config(
  'request.jwt.claim.sub',
  'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
  true
);
create temporary table derivative_foreign_read as
select public.cmd_dataset_derivative_rebuild_read(
  (select (result->>'request_id')::uuid from derivative_submit_one)
) as result;
reset role;

select is(
  (select result->>'code' from derivative_foreign_read),
  'DERIVATIVE_REQUEST_NOT_AVAILABLE',
  'foreign actor cannot read another request'
);

select set_config(
  'request.jwt.claim.sub',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  true
);
set local role authenticated;
create temporary table derivative_completed_replay as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan from derivative_plan_one)
) as result;
reset role;

select is(
  (select result->>'status' from derivative_completed_replay),
  'queued',
  'completed admission replay still reports admission rather than verify'
);
select ok(
  (
    select (result->>'idempotent_replay')::boolean
      and result->>'request_id' = (
        select result->>'request_id' from derivative_submit_one
      )
    from derivative_completed_replay
  ),
  'completed replay preserves original proof ids'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  1,
  'completed replay creates no request'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  2,
  'completed replay creates no audit'
);

set local role authenticated;
create temporary table derivative_snapshot_two as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'processes',
  '22222222-2222-4222-8222-222222222222',
  '00.00.001'
) as result;
reset role;

create temporary table derivative_plan_two as
select jsonb_build_object(
  'schema_version', 'dataset-derivative-rebuild-plan.v1',
  'plan_sha256', repeat('b', 64),
  'operation_id', 'pgtap-derivative-failure',
  'target_visibility', 'owner_draft',
  'actions', jsonb_build_array(jsonb_build_object(
    'action_id', 'rebuild-two',
    'action', 'rebuild_derivatives',
    'table', 'processes',
    'id', '22222222-2222-4222-8222-222222222222',
    'version', '00.00.001',
    'expected_state_code', 0,
    'expected_snapshot_sha256',
      (select result->>'snapshot_sha256' from derivative_snapshot_two),
    'components', jsonb_build_array('extracted_md', 'embedding_ft'),
    'reason_code', 'pgtap_failure'
  ))
) as plan;
grant select on derivative_plan_two to authenticated;

set local role authenticated;
create temporary table derivative_submit_two as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan from derivative_plan_two)
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from derivative_submit_two),
  'second exact owner-draft plan is admitted'
);

update util.dataset_derivative_rebuild_requests
set
  status = 'markdown_pending',
  phase = 'markdown_dispatched',
  markdown_request_id = -9714002,
  markdown_dispatched_at = clock_timestamp() - interval '1 second',
  markdown_deadline_at = clock_timestamp() + interval '10 minutes',
  updated_at = clock_timestamp()
where id = (
  select (result->>'request_id')::uuid
  from derivative_submit_two
);

insert into net._http_response (
  id,
  status_code,
  content_type,
  headers,
  content,
  timed_out,
  error_msg,
  created
) values (
  -9714002,
  400,
  'application/json',
  '{}'::jsonb,
  '{"error":"invalid request"}',
  false,
  null,
  clock_timestamp()
);

select net.http_post(
  url => 'http://127.0.0.1:1/functions/v1/embedding_ft',
  body => jsonb_build_array(jsonb_build_object(
    'jobId', 7002,
    'id', '22222222-2222-4222-8222-222222222222',
    'version', '00.00.001',
    'schema', 'public',
    'table', 'processes',
    'contentFunction', 'processes_embedding_ft_input',
    'embeddingColumn', 'embedding_ft',
    'edgeFunction', 'embedding_ft'
  )),
  timeout_milliseconds => 300000
);

select is(
  util.process_dataset_derivative_rebuilds(5),
  1,
  'coordinator records non-2xx Markdown response'
);
select is(
  (
    select status
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_two
    )
  ),
  'dispatching',
  'failed external response remains nonterminal while draining'
);
select is(
  (
    select phase
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_two
    )
  ),
  'failure_draining',
  'failure drain phase is explicit'
);
select is(
  (
    select count(*)::integer
    from net.http_request_queue
    where url like '%/functions/v1/embedding_ft'
      and util.dataset_derivative_rebuild_http_body_matches(
        body,
        '22222222-2222-4222-8222-222222222222',
        '00.00.001'
      )
  ),
  0,
  'failure drain quarantines already-claimed embedding HTTP before timing starts'
);
select ok(
  (
    select failure_release_not_before
      >= clock_timestamp() + interval '419 seconds'
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_two
    )
  ),
  'failure release remains fenced for the full in-flight Edge window'
);
select throws_ok(
  $$update public.processes
      set extracted_text = 'still fenced'
    where id = '22222222-2222-4222-8222-222222222222'
      and version = '00.00.001'$$,
  '55006',
  'Process primary row is fenced by an active derivative rebuild',
  'primary remains frozen throughout failure drain'
);

alter table public.processes
  disable trigger process_derivative_rebuild_primary_update_fence;
select lives_ok(
  $$update public.processes
      set extracted_text = 'administrative drift during failure drain'
    where id = '22222222-2222-4222-8222-222222222222'
      and version = '00.00.001'$$,
  'test fixture simulates privileged primary drift during failure drain'
);
alter table public.processes
  enable trigger process_derivative_rebuild_primary_update_fence;

update util.dataset_derivative_rebuild_requests
set failure_release_not_before = clock_timestamp() - interval '1 second'
where id = (
  select (result->>'request_id')::uuid
  from derivative_submit_two
);

select is(
  util.process_dataset_derivative_rebuilds(5),
  1,
  'coordinator releases a fully drained failed request'
);
select is(
  (
    select status
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_two
    )
  ),
  'failed',
  'drained request reaches failed terminal state'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_terminal'
      and payload->>'request_id' = (
        select result->>'request_id'
        from derivative_submit_two
      )
      and payload->>'status' = 'failed'
  ),
  1,
  'failed request appends one immutable terminal audit'
);
select lives_ok(
  $$update public.processes
      set extracted_text = 'editable after drain'
    where id = '22222222-2222-4222-8222-222222222222'
      and version = '00.00.001'$$,
  'primary becomes editable only after failed request drain completes'
);

-- Five future-dated waiters must not monopolize the default coordinator
-- limit.  Touching no-progress rows rotates the sixth runnable request into
-- the next bounded batch.
alter table public.processes
  disable trigger process_extract_md_trigger_insert;
alter table public.processes
  disable trigger processes_json_sync_trigger;
alter table public.processes
  disable trigger zz_processes_extracted_text_sync_trigger;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  extracted_text,
  extracted_md,
  modified_at
)
select
  ('50000000-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
  '00.00.001',
  '{"processDataSet":{}}'::jsonb,
  '{"processDataSet":{}}'::json,
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid,
  0,
  'fairness fixture ' || i::text,
  'fairness markdown ' || i::text,
  '2026-07-14 00:00:00+00'::timestamp with time zone
from generate_series(1, 6) as series(i);

alter table public.processes
  enable trigger process_extract_md_trigger_insert;
alter table public.processes
  enable trigger processes_json_sync_trigger;
alter table public.processes
  enable trigger zz_processes_extracted_text_sync_trigger;

insert into util.dataset_derivative_rebuild_requests (
  id,
  actor_user_id,
  plan_sha256,
  operation_id,
  action_id,
  target_id,
  target_version,
  expected_snapshot_sha256,
  expected_modified_at,
  expected_json_sha256,
  expected_json_ordered_sha256,
  expected_extracted_text_sha256,
  plan_request_sha256,
  action_request_sha256,
  reason_code,
  status,
  phase,
  admitted_at,
  drain_not_before,
  failure_release_not_before,
  action_audit_id,
  summary_audit_id,
  updated_at
)
select
  ('70000000-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid,
  lpad(i::text, 64, '0'),
  'fairness-' || i::text,
  'fairness-' || i::text,
  process.id,
  btrim(process.version::text),
  snapshot.value->>'snapshot_sha256',
  (snapshot.value->>'modified_at')::timestamp with time zone,
  snapshot.value->>'json_sha256',
  snapshot.value->>'json_ordered_sha256',
  snapshot.value->>'extracted_text_sha256',
  lpad((i + 10)::text, 64, '0'),
  lpad((i + 20)::text, 64, '0'),
  'fairness',
  case when i <= 5 then 'dispatching' else 'queued' end,
  case when i <= 5 then 'failure_draining' else 'admitted' end,
  clock_timestamp() - interval '2 hours',
  clock_timestamp() + interval '420 seconds',
  case
    when i <= 5 then clock_timestamp() + interval '420 seconds'
    else null
  end,
  1,
  1,
  case
    when i <= 5 then clock_timestamp() - interval '2 hours'
    else clock_timestamp() - interval '1 hour'
  end
from generate_series(1, 6) as series(i)
join public.processes as process
  on process.id = (
    '50000000-0000-4000-8000-' || lpad(i::text, 12, '0')
  )::uuid
cross join lateral (
  select util.dataset_derivative_rebuild_snapshot(process) as value
) as snapshot;

select is(
  util.process_dataset_derivative_rebuilds(5),
  5,
  'bounded coordinator first rotates five non-progressing waiters'
);
select is(
  util.process_dataset_derivative_rebuilds(5),
  5,
  'bounded coordinator next batch includes the previously starved request'
);
select is(
  (
    select phase
    from util.dataset_derivative_rebuild_requests
    where target_id = '50000000-0000-4000-8000-000000000006'
  ),
  'quarantining',
  'fair rotation advances the runnable request despite five waiters'
);

delete from util.dataset_derivative_rebuild_requests
where target_id::text like '50000000-0000-4000-8000-%';
delete from public.processes
where id::text like '50000000-0000-4000-8000-%';

-- A request-scoped exception must enter failure drain without rolling back a
-- healthy request selected in the same coordinator batch.
alter table public.processes
  disable trigger process_extract_md_trigger_insert;
alter table public.processes
  disable trigger processes_json_sync_trigger;
alter table public.processes
  disable trigger zz_processes_extracted_text_sync_trigger;

insert into public.processes (
  id,
  version,
  json,
  json_ordered,
  user_id,
  state_code,
  extracted_text,
  extracted_md,
  modified_at
) values
  (
    '60000000-0000-4000-8000-000000000001',
    '00.00.001',
    '{"processDataSet":{}}'::jsonb,
    '{"processDataSet":{}}'::json,
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    0,
    'poison fixture',
    'poison markdown',
    '2026-07-14 00:00:00+00'
  ),
  (
    '60000000-0000-4000-8000-000000000002',
    '00.00.001',
    '{"processDataSet":{}}'::jsonb,
    '{"processDataSet":{}}'::json,
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
    0,
    'healthy fixture',
    'healthy markdown',
    '2026-07-14 00:00:00+00'
  );

alter table public.processes
  enable trigger process_extract_md_trigger_insert;
alter table public.processes
  enable trigger processes_json_sync_trigger;
alter table public.processes
  enable trigger zz_processes_extracted_text_sync_trigger;

insert into util.dataset_derivative_rebuild_requests (
  id,
  actor_user_id,
  plan_sha256,
  operation_id,
  action_id,
  target_id,
  target_version,
  expected_snapshot_sha256,
  expected_modified_at,
  expected_json_sha256,
  expected_json_ordered_sha256,
  expected_extracted_text_sha256,
  plan_request_sha256,
  action_request_sha256,
  reason_code,
  status,
  phase,
  admitted_at,
  drain_not_before,
  embedding_queue_msg_id,
  embedding_queued_at,
  embedding_deadline_at,
  action_audit_id,
  summary_audit_id,
  updated_at
)
select
  ('80000000-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'::uuid,
  lpad((30 + i)::text, 64, '0'),
  'error-isolation-' || i::text,
  'error-isolation-' || i::text,
  process.id,
  btrim(process.version::text),
  snapshot.value->>'snapshot_sha256',
  (snapshot.value->>'modified_at')::timestamp with time zone,
  snapshot.value->>'json_sha256',
  snapshot.value->>'json_ordered_sha256',
  snapshot.value->>'extracted_text_sha256',
  lpad((40 + i)::text, 64, '0'),
  lpad((50 + i)::text, 64, '0'),
  'error-isolation',
  case when i = 1 then 'embedding_pending' else 'queued' end,
  case when i = 1 then 'embedding_queued' else 'admitted' end,
  clock_timestamp() - interval '2 hours',
  clock_timestamp() - interval '1 second',
  case when i = 1 then -8001 else null end,
  case when i = 1 then clock_timestamp() - interval '1 second' else null end,
  case when i = 1 then clock_timestamp() + interval '1 day' else null end,
  1,
  1,
  clock_timestamp() - (3 - i) * interval '1 hour'
from generate_series(1, 2) as series(i)
join public.processes as process
  on process.id = (
    '60000000-0000-4000-8000-' || lpad(i::text, 12, '0')
  )::uuid
cross join lateral (
  select util.dataset_derivative_rebuild_snapshot(process) as value
) as snapshot;

insert into util.dataset_derivative_rebuild_proposals (
  request_id,
  proposal_kind,
  embedding_ft,
  embedding_ft_sha256,
  embedding_ft_at,
  source_extracted_md_sha256
)
select
  request.id,
  'embedding',
  vector.value,
  util.dataset_derivative_rebuild_sha256(vector.value::text),
  clock_timestamp() + interval '1 second',
  repeat('a', 64)
from util.dataset_derivative_rebuild_requests as request
cross join lateral (
  select (
    '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
  )::extensions.vector as value
) as vector
where request.target_id = '60000000-0000-4000-8000-000000000001';

select is(
  util.process_dataset_derivative_rebuilds(2),
  2,
  'request-scoped coordinator exception does not abort the healthy peer'
);
select ok(
  (
    select status = 'dispatching' and phase = 'failure_draining'
    from util.dataset_derivative_rebuild_requests
    where target_id = '60000000-0000-4000-8000-000000000001'
  )
  and (
    select status = 'dispatching' and phase = 'quarantining'
    from util.dataset_derivative_rebuild_requests
    where target_id = '60000000-0000-4000-8000-000000000002'
  ),
  'poison request drains while the healthy request advances in the same batch'
);

delete from util.dataset_derivative_rebuild_requests
where target_id::text like '60000000-0000-4000-8000-%';
delete from public.processes
where id::text like '60000000-0000-4000-8000-%';

select lives_ok(
  $$update public.processes
      set extracted_text = 'fixture extracted text two'
    where id = '22222222-2222-4222-8222-222222222222'
      and version = '00.00.001'$$,
  'failed target can be restored to the same primary snapshot for retry'
);

set local role authenticated;
create temporary table derivative_retry_snapshot_two as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'processes',
  '22222222-2222-4222-8222-222222222222',
  '00.00.001'
) as result;
reset role;

create temporary table derivative_retry_plan_two as
select jsonb_build_object(
  'schema_version', 'dataset-derivative-rebuild-plan.v1',
  'plan_sha256', repeat('c', 64),
  'operation_id', 'pgtap-derivative-failure-retry',
  'target_visibility', 'owner_draft',
  'actions', jsonb_build_array(jsonb_build_object(
    'action_id', 'rebuild-two',
    'action', 'rebuild_derivatives',
    'table', 'processes',
    'id', '22222222-2222-4222-8222-222222222222',
    'version', '00.00.001',
    'expected_state_code', 0,
    'expected_snapshot_sha256',
      (select result->>'snapshot_sha256' from derivative_retry_snapshot_two),
    'components', jsonb_build_array('extracted_md', 'embedding_ft'),
    'reason_code', 'pgtap_failure'
  ))
) as plan;
grant select on derivative_retry_plan_two to authenticated;

set local role authenticated;
create temporary table derivative_retry_submit_two as
select public.cmd_dataset_derivative_rebuild_plan_guarded(
  (select plan from derivative_retry_plan_two)
) as result;
reset role;

select ok(
  (select (result->>'ok')::boolean from derivative_retry_submit_two),
  'a fresh immutable plan can retry a terminal failed action'
);
select isnt(
  (select result->>'request_id' from derivative_retry_submit_two),
  (select result->>'request_id' from derivative_submit_two),
  'fresh failed-action retry receives a new durable request id'
);
select is(
  (
    select action_request_sha256
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_retry_submit_two
    )
  ),
  (
    select action_request_sha256
    from util.dataset_derivative_rebuild_requests
    where id = (
      select (result->>'request_id')::uuid
      from derivative_submit_two
    )
  ),
  'retry may reuse the exact action hash under a new approved plan'
);

set local role authenticated;
create temporary table derivative_read_two as
select public.cmd_dataset_derivative_rebuild_read(
  (select (result->>'request_id')::uuid from derivative_submit_two)
) as result;
reset role;

select is(
  (select result->>'status' from derivative_read_two),
  'failed',
  'owner read reports failed terminal state'
);
select is(
  (select result->>'fence_active' from derivative_read_two),
  'false',
  'failed owner read proves the drain released the target fence'
);

select * from finish();
rollback;
