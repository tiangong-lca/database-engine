begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(40);

select ok(
  to_regprocedure(
    'util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)'
  ) is not null,
  'private bounded batch admission exists'
);
select ok(
  to_regprocedure(
    'util.read_dataset_derivative_rebuild_batch(uuid,uuid)'
  ) is not null,
  'private exact 23+27 aggregate proof exists'
);
select ok(
  to_regprocedure(
    'util.dataset_derivative_rebuild_snapshot(public.flows)'
  ) is not null,
  'flow snapshot overload exists'
);
select ok(
  to_regprocedure(
    'util.dataset_derivative_rebuild_primary_matches(util.dataset_derivative_rebuild_requests,public.flows)'
  ) is not null,
  'flow primary-match overload exists'
);
select ok(
  to_regprocedure(
    'public.flows_derivative_rebuild_embedding_input(public.flows)'
  ) is not null,
  'private flow accepted-Markdown embedding input exists'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)',
    'execute'
  ),
  'authenticated cannot call internal batch admission'
);
select ok(
  not has_function_privilege(
    'service_role',
    'util.admit_dataset_derivative_rebuild_batch(uuid,uuid,text,text,text,jsonb)',
    'execute'
  ),
  'service role cannot bypass the protected alias executor'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'util.read_dataset_derivative_rebuild_batch(uuid,uuid)',
    'execute'
  ),
  'authenticated cannot call internal aggregate proof directly'
);
select ok(
  not has_function_privilege(
    'service_role',
    'util.read_dataset_derivative_rebuild_batch(uuid,uuid)',
    'execute'
  ),
  'service role cannot call internal aggregate proof directly'
);
select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as namespace
      on namespace.oid = proc.pronamespace
    where namespace.nspname = 'util'
      and proc.proname in (
        'admit_dataset_derivative_rebuild_batch',
        'read_dataset_derivative_rebuild_batch'
      )
      and proc.prosecdef
      and exists (
        select 1
        from unnest(coalesce(proc.proconfig, array[]::text[])) as config(value)
        where config.value in ('search_path=', 'search_path=""')
      )
  ),
  2,
  'both internal batch functions are security definer with empty search path'
);
select is(
  (
    select count(*)::integer
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname like 'flow_derivative_rebuild_%'
      and not tgisinternal
  ),
  4,
  'flows has update/delete fences and Markdown/vector staging parity'
);
select matches(
  pg_get_constraintdef(
    (
      select oid
      from pg_constraint
      where conrelid = 'util.dataset_derivative_rebuild_requests'::regclass
        and conname = 'dataset_derivative_rebuild_request_table_check'
    )
  ),
  '.*flows.*processes.*',
  'request target constraint permits only flows and processes'
);

alter table public.flows
  disable trigger flow_dataset_extraction_trigger_insert;
alter table public.flows
  disable trigger flows_json_sync_trigger;
alter table public.flows
  disable trigger zz_flows_extracted_text_sync_trigger;
alter table public.processes
  disable trigger process_extract_md_trigger_insert;
alter table public.processes
  disable trigger processes_json_sync_trigger;
alter table public.processes
  disable trigger zz_processes_extracted_text_sync_trigger;

insert into public.flows (
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
  (
    '81000000-0000-4000-8000-' || lpad(series.value::text, 12, '0')
  )::uuid,
  '00.00.001',
  jsonb_build_object('kind', 'flow', 'n', series.value),
  jsonb_build_object('kind', 'flow', 'n', series.value)::json,
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  0,
  'flow extracted text ' || series.value::text,
  'flow old markdown ' || series.value::text,
  '2026-07-15 00:00:00+00'
from generate_series(1, 24) as series(value);

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
  (
    '82000000-0000-4000-8000-' || lpad(series.value::text, 12, '0')
  )::uuid,
  '00.00.001',
  jsonb_build_object('kind', 'process', 'n', series.value),
  jsonb_build_object('kind', 'process', 'n', series.value)::json,
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  0,
  'process extracted text ' || series.value::text,
  'process old markdown ' || series.value::text,
  '2026-07-15 00:00:00+00'
from generate_series(1, 28) as series(value);

alter table public.flows
  enable trigger flow_dataset_extraction_trigger_insert;
alter table public.flows
  enable trigger flows_json_sync_trigger;
alter table public.flows
  enable trigger zz_flows_extracted_text_sync_trigger;
alter table public.processes
  enable trigger process_extract_md_trigger_insert;
alter table public.processes
  enable trigger processes_json_sync_trigger;
alter table public.processes
  enable trigger zz_processes_extracted_text_sync_trigger;

delete from pgmq.q_embedding_jobs;
delete from util.pending_embedding_jobs;
delete from net.http_request_queue;
delete from vault.secrets
where name in ('project_secret_key', 'project_url');
select vault.create_secret(
  'test-derivative-service-key',
  'project_secret_key',
  'pgTAP guarded derivative service auth'
);
select vault.create_secret(
  'http://127.0.0.1:1',
  'project_url',
  'pgTAP guarded derivative unreachable endpoint'
);
delete from public.command_audit_log
where command in (
  'cmd_dataset_derivative_rebuild_plan_guarded',
  'cmd_dataset_derivative_rebuild_terminal'
);

select set_config(
  'request.jwt.claim.sub',
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  true
);
set local role authenticated;
create temporary table derivative_flow_snapshot as
select public.cmd_dataset_derivative_rebuild_snapshot(
  'flows',
  '81000000-0000-4000-8000-000000000001',
  '00.00.001'
) as result;
reset role;

select ok(
  (
    select (result->>'ok')::boolean
      and result->>'table' = 'flows'
      and result->>'state_code' = '0'
      and result->>'snapshot_sha256' ~ '^[a-f0-9]{64}$'
    from derivative_flow_snapshot
  ),
  'authenticated owner can snapshot one private draft flow'
);

create temporary table derivative_exact_targets as
select jsonb_agg(target.value order by target.value->>'table', target.value->>'id')
  as targets
from (
  select jsonb_build_object(
    'table', 'flows',
    'id', flow.id,
    'version', btrim(flow.version::text),
    'expected_json_ordered_sha256',
      util.dataset_derivative_rebuild_sha256(flow.json_ordered::jsonb::text),
    'baseline_snapshot_sha256', repeat('a', 64)
  ) as value
  from public.flows as flow
  where flow.id::text like '81000000-0000-4000-8000-%'
    and right(flow.id::text, 12)::bigint between 1 and 23
  union all
  select jsonb_build_object(
    'table', 'processes',
    'id', process.id,
    'version', btrim(process.version::text),
    'expected_json_ordered_sha256',
      util.dataset_derivative_rebuild_sha256(
        process.json_ordered::jsonb::text
      ),
    'baseline_snapshot_sha256', repeat('b', 64)
  ) as value
  from public.processes as process
  where process.id::text like '82000000-0000-4000-8000-%'
    and right(process.id::text, 12)::bigint between 1 and 27
) as target;

create temporary table derivative_single_flow_target as
select jsonb_build_array(jsonb_build_object(
  'table', 'flows',
  'id', flow.id,
  'version', btrim(flow.version::text),
  'expected_json_ordered_sha256',
    util.dataset_derivative_rebuild_sha256(flow.json_ordered::jsonb::text),
  'baseline_snapshot_sha256', repeat('c', 64)
)) as targets
from public.flows as flow
where flow.id = '81000000-0000-4000-8000-000000000024';

create temporary table derivative_51_targets as
select jsonb_agg(jsonb_build_object(
  'table', 'flows',
  'id', (
    '85000000-0000-4000-8000-' || lpad(series.value::text, 12, '0')
  )::uuid,
  'version', '00.00.001',
  'expected_json_ordered_sha256', repeat('d', 64),
  'baseline_snapshot_sha256', repeat('e', 64)
)) as targets
from generate_series(1, 51) as series(value);

select throws_ok(
  $$select util.admit_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000051',
      repeat('1', 64),
      'pgtap-too-many',
      'pgtap_batch',
      (select targets from derivative_51_targets)
    )$$,
  '22023',
  'Invalid bounded derivative rebuild batch request',
  'batch admission rejects more than 50 targets before reading rows'
);

select throws_ok(
  $$select util.admit_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000002',
      repeat('2', 64),
      'pgtap-duplicate-target',
      'pgtap_batch',
      (
        select jsonb_build_array(targets->0, targets->0)
        from derivative_single_flow_target
      )
    )$$,
  '22023',
  'Derivative rebuild batch targets must be unique',
  'batch admission rejects duplicate canonical targets'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  0,
  'bounded and duplicate rejections create no child request'
);

create temporary table derivative_wrong_desired as
select jsonb_set(
  targets,
  '{49,expected_json_ordered_sha256}',
  to_jsonb(repeat('0', 64))
) as targets
from derivative_exact_targets;

select throws_ok(
  $$select util.admit_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000003',
      repeat('3', 64),
      'pgtap-full-validation',
      'pgtap_batch',
      (select targets from derivative_wrong_desired)
    )$$,
  '40001',
  'Derivative rebuild batch desired primary hash drifted',
  'one late desired-hash mismatch fails the whole validation pass'
);
select is(
  (select count(*)::integer from util.dataset_derivative_rebuild_requests),
  0,
  'desired-hash mismatch creates no partial child requests'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  0,
  'desired-hash mismatch creates no partial audits'
);

create temporary table derivative_single_flow_admission as
select util.admit_dataset_derivative_rebuild_batch(
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  '90000000-0000-4000-8000-000000000004',
  repeat('4', 64),
  'pgtap-single-flow',
  'pgtap_batch',
  (select targets from derivative_single_flow_target)
) as result;

select ok(
  (
    select (result->>'ok')::boolean
      and (result->>'target_count')::integer = 1
      and (result->>'flow_count')::integer = 1
      and (result->>'process_count')::integer = 0
      and jsonb_array_length(result->'child_request_ids') = 1
    from derivative_single_flow_admission
  ),
  'generic batch admission returns exact flow/process counts and child ids'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_requests
    where batch_id = '90000000-0000-4000-8000-000000000004'
      and target_table = 'flows'
      and source_baseline_snapshot_sha256 = repeat('c', 64)
  ),
  1,
  'single-flow child binds the supplied frozen baseline and live post-write snapshot'
);
select is(
  (
    select result->>'code'
    from util.read_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000004'
    ) as result
  ),
  'DERIVATIVE_BATCH_TARGET_SET_MISMATCH',
  'aggregate proof fails closed for a non-50/non-23+27 target set'
);

select throws_ok(
  $$update public.flows
      set extracted_text = 'must be fenced'
    where id = '81000000-0000-4000-8000-000000000024'
      and version = '00.00.001'$$,
  '55006',
  'Flow primary row is fenced by an active derivative rebuild',
  'active flow child rejects primary updates'
);
select lives_ok(
  $$update public.flows
      set extracted_md = 'staged flow markdown'
    where id = '81000000-0000-4000-8000-000000000024'
      and version = '00.00.001'$$,
  'active flow child stages external Markdown writes'
);
select is(
  (
    select extracted_md
    from public.flows
    where id = '81000000-0000-4000-8000-000000000024'
      and version = '00.00.001'
  ),
  'flow old markdown 24',
  'staged flow Markdown is not visible before causal acceptance'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_proposals as proposal
    join util.dataset_derivative_rebuild_requests as request
      on request.id = proposal.request_id
    where request.batch_id = '90000000-0000-4000-8000-000000000004'
      and proposal.proposal_kind = 'markdown'
      and proposal.status = 'captured'
  ),
  1,
  'flow staging captures one request-scoped Markdown proposal'
);

delete from util.dataset_derivative_rebuild_proposals
where request_id = (
  select id
  from util.dataset_derivative_rebuild_requests
  where batch_id = '90000000-0000-4000-8000-000000000004'
);
update util.dataset_derivative_rebuild_requests
set drain_not_before = clock_timestamp() - interval '1 second'
where batch_id = '90000000-0000-4000-8000-000000000004';

select is(
  util.process_dataset_derivative_rebuilds(1),
  1,
  'generic coordinator advances queued flow child to quarantine phase'
);
select is(
  util.process_dataset_derivative_rebuilds(1),
  1,
  'generic coordinator dispatches the flow Markdown webhook once'
);
select is(
  (
    select count(*)::integer
    from util.dataset_derivative_rebuild_requests as request
    where request.batch_id = '90000000-0000-4000-8000-000000000004'
      and request.target_table = 'flows'
      and request.status = 'markdown_pending'
      and request.phase = 'markdown_dispatched'
      and request.markdown_request_id is not null
      and util.dataset_derivative_rebuild_http_body_matches(
        pg_catalog.convert_to(
          jsonb_build_object(
            'schema', 'public',
            'table', 'flows',
            'record', jsonb_build_object(
              'id', request.target_id,
              'version', request.target_version
            )
          )::text,
          'UTF8'
        ),
        request.target_table,
        request.target_id,
        request.target_version
      )
  ),
  1,
  'flow coordinator records one dispatch with exact table/id/version identity'
);

delete from util.dataset_derivative_rebuild_requests
where batch_id = '90000000-0000-4000-8000-000000000004';
delete from public.command_audit_log
where payload->>'batch_id' = '90000000-0000-4000-8000-000000000004';
delete from net.http_request_queue;

create temporary table derivative_exact_admission as
select util.admit_dataset_derivative_rebuild_batch(
  'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
  '90000000-0000-4000-8000-000000000050',
  repeat('5', 64),
  'pgtap-exact-50',
  'pgtap_batch',
  (select targets from derivative_exact_targets)
) as result;

select ok(
  (
    select (result->>'ok')::boolean
      and (result->>'target_count')::integer = 50
      and (result->>'flow_count')::integer = 23
      and (result->>'process_count')::integer = 27
    from derivative_exact_admission
  ),
  'exact protected batch admits 23 flows plus 27 processes'
);
select is(
  (
    select jsonb_array_length(result->'child_request_ids')
    from derivative_exact_admission
  ),
  50,
  'batch result returns all 50 child request ids'
);
select ok(
  (
    select count(*) = 50
      and count(*) filter (where target_table = 'flows') = 23
      and count(*) filter (where target_table = 'processes') = 27
      and count(distinct batch_ordinal) = 50
      and min(batch_ordinal) = 1
      and max(batch_ordinal) = 50
      and bool_and(batch_target_count = 50)
    from util.dataset_derivative_rebuild_requests
    where batch_id = '90000000-0000-4000-8000-000000000050'
  ),
  'persisted batch has exact target distribution, ordinals, and size binding'
);
select is(
  (
    select count(*)::integer
    from public.command_audit_log
    where payload->>'batch_id' = '90000000-0000-4000-8000-000000000050'
      and command = 'cmd_dataset_derivative_rebuild_plan_guarded'
  ),
  51,
  'batch admission creates one summary plus 50 action audits atomically'
);
select ok(
  (
    select bool_and(
      source_baseline_snapshot_sha256 ~ '^[a-f0-9]{64}$'
      and expected_snapshot_sha256 ~ '^[a-f0-9]{64}$'
      and expected_json_sha256 = expected_json_ordered_sha256
    )
    from util.dataset_derivative_rebuild_requests
    where batch_id = '90000000-0000-4000-8000-000000000050'
  ),
  'every child binds frozen baseline, actual post-write snapshot, and desired JSON'
);
select ok(
  (
    select (result->>'ok')::boolean
      and result->>'status' = 'pending'
      and (result->>'target_count')::integer = 50
      and (result->>'flow_count')::integer = 23
      and (result->>'process_count')::integer = 27
      and result->>'proof_level' = 'status_only'
      and (result->>'proof_deferred')::boolean
      and not (result->>'causal_terminal_proof')::boolean
      and result->'invalid_proof_count' = 'null'::jsonb
      and result->'completed_invalid_proof_count' = 'null'::jsonb
      and jsonb_array_length(result->'targets') = 50
      and (
        select bool_and(target.value ?& array[
          'ordinal',
          'request_id',
          'table',
          'id',
          'version',
          'status',
          'phase',
          'error',
          'causal_terminal_proof'
        ])
        from jsonb_array_elements(result->'targets') as target(value)
      )
      and not exists (
        select 1
        from jsonb_array_elements(result->'targets') as target(value)
        cross join lateral jsonb_object_keys(target.value) as target_key(key)
        where target_key.key <> all (array[
          'ordinal',
          'request_id',
          'table',
          'id',
          'version',
          'status',
          'phase',
          'error',
          'causal_terminal_proof'
        ])
      )
    from util.read_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000050'
    ) as result
  ),
  'exact aggregate read reports pending without claiming terminal proof'
);

select throws_ok(
  $$update public.flows
      set extracted_text = 'batch flow fenced'
    where id = '81000000-0000-4000-8000-000000000001'
      and version = '00.00.001'$$,
  '55006',
  'Flow primary row is fenced by an active derivative rebuild',
  'exact batch fences one representative flow primary'
);
select throws_ok(
  $$update public.processes
      set extracted_text = 'batch process fenced'
    where id = '82000000-0000-4000-8000-000000000001'
      and version = '00.00.001'$$,
  '55006',
  'Process primary row is fenced by an active derivative rebuild',
  'exact batch fences one representative process primary'
);

do $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_markdown_id bigint;
  v_embedding_id bigint;
  v_markdown text;
  v_markdown_sha256 text;
  v_embedding extensions.vector;
  v_markdown_dispatched_at timestamp with time zone;
  v_markdown_received_at timestamp with time zone;
  v_embedding_queued_at timestamp with time zone;
  v_embedding_at timestamp with time zone;
  v_snapshot jsonb;
begin
  v_embedding := (
    '[' || array_to_string(array_fill('0'::text, array[1024]), ',') || ']'
  )::extensions.vector;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.batch_id = '90000000-0000-4000-8000-000000000050'
    order by request.batch_ordinal
  loop
    v_markdown := 'accepted batch markdown ' || v_request.batch_ordinal::text;
    v_markdown_sha256 := util.dataset_derivative_rebuild_sha256(v_markdown);
    v_markdown_dispatched_at := clock_timestamp() - interval '3 seconds';
    v_markdown_received_at := clock_timestamp() - interval '2 seconds';
    v_embedding_queued_at := clock_timestamp() - interval '1 second';
    v_embedding_at := clock_timestamp() + interval '1 second';

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      extracted_md,
      extracted_md_sha256,
      status
    ) values (
      v_request.id,
      'markdown',
      v_markdown,
      v_markdown_sha256,
      'accepted'
    ) returning id into v_markdown_id;

    insert into util.dataset_derivative_rebuild_proposals (
      request_id,
      proposal_kind,
      embedding_ft,
      embedding_ft_sha256,
      embedding_ft_at,
      source_extracted_md_sha256,
      status
    ) values (
      v_request.id,
      'embedding',
      v_embedding,
      util.dataset_derivative_rebuild_sha256(v_embedding::text),
      v_embedding_at,
      v_markdown_sha256,
      'captured'
    ) returning id into v_embedding_id;

    update util.dataset_derivative_rebuild_requests
    set
      markdown_proposal_id = v_markdown_id,
      accepted_extracted_md_sha256 = v_markdown_sha256,
      markdown_request_id = -9000000 - v_request.batch_ordinal,
      markdown_dispatched_at = v_markdown_dispatched_at,
      markdown_response_status = 200,
      markdown_response_received_at = v_markdown_received_at,
      embedding_queue_msg_id = -8000000 - v_request.batch_ordinal,
      embedding_queued_at = v_embedding_queued_at
    where id = v_request.id;

    perform util.commit_dataset_derivative_rebuild_proposal(
      v_request.id,
      v_markdown_id,
      v_embedding_id
    );

    v_snapshot := util.dataset_derivative_rebuild_snapshot(
      v_request.target_table,
      v_request.target_id,
      v_request.target_version
    );
    update util.dataset_derivative_rebuild_requests
    set
      status = 'completed',
      phase = 'completed',
      completed_snapshot_sha256 = v_snapshot->>'snapshot_sha256',
      completed_at = clock_timestamp(),
      terminal_at = clock_timestamp(),
      drained_at = clock_timestamp()
    where id = v_request.id;
    perform util.record_dataset_derivative_rebuild_terminal(v_request.id);
  end loop;
end
$$;

select ok(
  (
    select (result->>'ok')::boolean
      and result->>'status' = 'completed'
      and (result->>'causal_terminal_proof')::boolean
      and (result->>'invalid_proof_count')::integer = 0
    from util.read_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000050'
    ) as result
  ),
  'aggregate proof completes only after all 50 causal terminal proofs match live state'
);
select ok(
  (
    select bool_and((target.value->>'causal_terminal_proof')::boolean)
    from util.read_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000050'
    ) as result
    cross join lateral jsonb_array_elements(result->'targets') as target(value)
  ),
  'all 50 aggregate target proofs expose successful causal evidence'
);

update util.dataset_derivative_rebuild_requests
set completed_snapshot_sha256 = repeat('0', 64)
where batch_id = '90000000-0000-4000-8000-000000000050'
  and batch_ordinal = 1;

select ok(
  (
    select not (result->>'ok')::boolean
      and result->>'status' = 'failed'
      and result->>'code' = 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH'
      and not (result->>'causal_terminal_proof')::boolean
    from util.read_dataset_derivative_rebuild_batch(
      'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
      '90000000-0000-4000-8000-000000000050'
    ) as result
  ),
  'aggregate proof fails closed when one completed snapshot digest is corrupted'
);

select * from finish();
rollback;
