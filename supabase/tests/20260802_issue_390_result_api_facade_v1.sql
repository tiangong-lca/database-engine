begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(78);

select has_function('api', 'lca_read_job_projection_v1', array['uuid', 'uuid', 'uuid', 'boolean']);
select has_function('api', 'lca_read_result_projection_v1', array['uuid', 'uuid', 'text', 'boolean']);
select has_function('api', 'lca_read_latest_single_solve_result_v1', array['uuid', 'uuid', 'integer']);
select has_function('api', 'lca_read_result_cache_v1', array['text', 'uuid', 'text']);
select has_function('api', 'cmd_lca_touch_result_cache_v1', array['uuid']);
select has_function('api', 'cmd_lca_admit_result_cache_v1', array['text', 'uuid', 'text', 'jsonb', 'uuid', 'uuid', 'boolean']);
select has_function('api', 'cmd_lca_reconcile_result_cache_v1', array['uuid', 'uuid']);
select has_function('api', 'lca_read_latest_all_unit_result_v1', array['uuid']);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
  ),
  8,
  'the v1 surface has exactly eight overloads before property filtering'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and proc.prolang = (select oid from pg_language where lanname = 'sql')
      and not proc.prosecdef
      and proc.proowner = 'postgres'::regrole
      and proc.proconfig = array['search_path=""']::text[]
  ),
  8,
  'all eight v1 facade routines are SQL SECURITY INVOKER with an empty search_path'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and has_function_privilege('service_role', proc.oid, 'EXECUTE')
  ),
  8,
  'service_role can execute all eight facade routines'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and has_function_privilege('api_internal_executor', proc.oid, 'EXECUTE')
  ),
  0,
  'api_internal_executor cannot execute any read/projection facade'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1'
      )
      and has_function_privilege('api_internal_executor', proc.oid, 'EXECUTE')
  ),
  0,
  'api_internal_executor has no ambiguous execute grant on write commands'
);

select ok(
  (
    select count(*) = 8
      and bool_and(not acl.is_grantable)
      and bool_and(role_name.rolname = 'service_role')
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    cross join lateral aclexplode(proc.proacl) as acl
    join pg_roles as role_name on role_name.oid = acl.grantee
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and acl.privilege_type = 'EXECUTE'
      and acl.grantee <> proc.proowner
  ),
  'direct non-owner ACLs are exactly service_role x8 without grant option'
);

select ok(
  has_schema_privilege('api_internal_executor', 'api', 'USAGE'),
  'api_internal_executor retains inherited api schema usage but no Issue #390 function execute'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and has_function_privilege('authenticated', proc.oid, 'EXECUTE')
  ),
  0,
  'authenticated cannot execute any Issue #390 facade'
);

select is(
  (
    select count(*)::integer
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and has_function_privilege('anon', proc.oid, 'EXECUTE')
  ),
  0,
  'anon cannot execute any Issue #390 facade'
);

select ok(
  not exists (
    select 1
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    cross join lateral aclexplode(coalesce(proc.proacl, acldefault('f', proc.proowner))) as acl
    where proc_schema.nspname = 'api'
      and proc.proname in (
        'lca_read_job_projection_v1',
        'lca_read_result_projection_v1',
        'lca_read_latest_single_solve_result_v1',
        'lca_read_result_cache_v1',
        'cmd_lca_touch_result_cache_v1',
        'cmd_lca_admit_result_cache_v1',
        'cmd_lca_reconcile_result_cache_v1',
        'lca_read_latest_all_unit_result_v1'
      )
      and acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  ),
  'PUBLIC has no execute grant on any Issue #390 facade'
);

select is(
  (
    select count(*)::integer
    from pg_class as relation
    join pg_namespace as relation_schema on relation_schema.oid = relation.relnamespace
    where relation_schema.nspname = 'public'
      and relation.relname in (
        'lca_results',
        'lca_result_cache',
        'lca_latest_all_unit_results',
        'lca_factorization_registry'
      )
      and relation.relkind = 'r'
  ),
  4,
  'facade Expand does not move or replace any physical target'
);

select ok(
  has_table_privilege('authenticated', 'public.lca_results', 'SELECT'),
  'facade Expand preserves the historical authenticated lca_results SELECT contract'
);

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

insert into public.lca_network_snapshots (
  id, scope, process_filter, provider_matching_rule, source_hash, status
) values (
  '98220000-0000-4000-8000-000000000010',
  'full_library',
  '{}'::jsonb,
  'split_by_evidence_hybrid',
  'issue-390-api-facade-v1',
  'ready'
);

create temporary table issue_390_jobs (
  label text primary key,
  job_id uuid not null
) on commit drop;
grant all on issue_390_jobs to public;

insert into issue_390_jobs (label, job_id)
select label, (enqueued->'data'->>'id')::uuid
from (
  values
    ('completed_with_result', '98220000-0000-4000-8000-000000000101'::uuid),
    ('completed_without_result', '98220000-0000-4000-8000-000000000102'::uuid),
    ('failed', '98220000-0000-4000-8000-000000000103'::uuid),
    ('completed_reconcile_result', '98220000-0000-4000-8000-000000000104'::uuid),
    ('completed_wrong_legacy', '98220000-0000-4000-8000-000000000105'::uuid),
    ('foreign_owner', '98220000-0000-4000-8000-000000000106'::uuid)
) as fixture(label, legacy_job_id)
cross join lateral (
  select public.worker_enqueue_job(
    p_job_kind => 'lca.solve_one',
    p_payload_json => jsonb_build_object(
      'type', 'solve_one',
      'job_id', fixture.legacy_job_id,
      'snapshot_id', '98220000-0000-4000-8000-000000000010'
    ),
    p_payload_schema_version => 'lca.solve_one.request.v1',
    p_subject_type => 'lca_job',
    p_subject_id => fixture.legacy_job_id,
    p_subject_version => '98220000-0000-4000-8000-000000000010',
    p_requested_by => '98220000-0000-4000-8000-000000000001',
    p_idempotency_key => 'issue-390:' || fixture.label,
    p_request_hash => 'issue-390-api-facade-v1',
    p_queue_key => '98220000-0000-4000-8000-000000000010',
    p_visibility => 'user'
  ) as enqueued
) as enqueue;

reset role;
update private.worker_jobs as worker_job
set status = case
      when fixture.label = 'failed' then 'failed'
      else 'completed'
    end,
    result_schema_version = 'lca.solve.result.v1',
    result_json = case
      when fixture.label = 'failed' then null
      else '{"ok":true}'::jsonb
    end,
    error_code = case when fixture.label = 'failed' then 'fixture_failed' else null end,
    error_message = case when fixture.label = 'failed' then 'fixture failed' else null end,
    requested_by = case
      when fixture.label = 'foreign_owner'
        then '98220000-0000-4000-8000-000000000099'::uuid
      else worker_job.requested_by
    end,
    finished_at = now(),
    updated_at = now()
from issue_390_jobs as fixture
where worker_job.id = fixture.job_id;
set local role service_role;

insert into public.lca_results (
  id, job_id, worker_job_id, snapshot_id, payload, diagnostics,
  artifact_url, artifact_sha256, artifact_byte_size, artifact_format
) values (
  '98220000-0000-4000-8000-000000000201',
  '98220000-0000-4000-8000-000000000101',
  (select job_id from issue_390_jobs where label = 'completed_with_result'),
  '98220000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/issue-390/result.h5',
  repeat('a', 64),
  512,
  'hdf5:v1'
), (
  '98220000-0000-4000-8000-000000000203',
  '98220000-0000-4000-8000-000000000104',
  (select job_id from issue_390_jobs where label = 'completed_reconcile_result'),
  '98220000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/issue-390/reconcile-result.h5',
  repeat('c', 64),
  768,
  'hdf5:v1'
), (
  '98220000-0000-4000-8000-000000000204',
  '98220000-0000-4000-8000-000000000105',
  (select job_id from issue_390_jobs where label = 'completed_wrong_legacy'),
  '98220000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/issue-390/wrong-legacy-result.h5',
  repeat('d', 64),
  896,
  'hdf5:v1'
);

insert into public.lca_result_cache (
  id, scope, snapshot_id, request_key, request_payload, status,
  job_id, worker_job_id, result_id, error_code, error_message, hit_count
) values
  (
    '98220000-0000-4000-8000-000000000301', 'prod',
    '98220000-0000-4000-8000-000000000010', 'ready',
    '{"demand_mode":"single","demand":{"process_index":1,"amount":2.5}}'::jsonb,
    'ready', '98220000-0000-4000-8000-000000000101',
    (select job_id from issue_390_jobs where label = 'completed_with_result'),
    '98220000-0000-4000-8000-000000000201', null, null, 3
  ),
  (
    '98220000-0000-4000-8000-000000000302', 'prod',
    '98220000-0000-4000-8000-000000000010', 'reconcile-ready',
    '{}'::jsonb, 'pending', '98220000-0000-4000-8000-000000000104',
    (select job_id from issue_390_jobs where label = 'completed_reconcile_result'),
    null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000303', 'prod',
    '98220000-0000-4000-8000-000000000010', 'reconcile-pending',
    '{}'::jsonb, 'pending', '98220000-0000-4000-8000-000000000102',
    (select job_id from issue_390_jobs where label = 'completed_without_result'),
    null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000304', 'prod',
    '98220000-0000-4000-8000-000000000010', 'reconcile-failed',
    '{}'::jsonb, 'running', '98220000-0000-4000-8000-000000000103',
    (select job_id from issue_390_jobs where label = 'failed'),
    null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000305', 'prod',
    '98220000-0000-4000-8000-000000000010', 'broken-ready',
    '{}'::jsonb, 'ready', null, null, null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000306', 'prod',
    '98220000-0000-4000-8000-000000000010', 'broken-pending',
    '{}'::jsonb, 'pending', null, null, null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000307', 'prod',
    '98220000-0000-4000-8000-000000000010', 'active-worker-only',
    '{}'::jsonb, 'running', null,
    (select job_id from issue_390_jobs where label = 'completed_without_result'),
    null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000308', 'prod',
    '98220000-0000-4000-8000-000000000010', 'missing-job',
    '{}'::jsonb, 'pending', '98220000-0000-4000-8000-000000000106',
    (select job_id from issue_390_jobs where label = 'foreign_owner'),
    null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000309', 'prod',
    '98220000-0000-4000-8000-000000000010', 'no-job-identities',
    '{}'::jsonb, 'pending', null, null, null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000310', 'prod',
    '98220000-0000-4000-8000-000000000010', 'broken-ready-replace',
    '{}'::jsonb, 'ready', null, null, null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000311', 'prod',
    '98220000-0000-4000-8000-000000000010', 'malformed-projection',
    '{}'::jsonb, 'pending', '98220000-0000-4000-8000-000000000198',
    null, null, null, null, 0
  ),
  (
    '98220000-0000-4000-8000-000000000312', 'prod',
    '98220000-0000-4000-8000-000000000010', 'mismatched-job-identities',
    '{}'::jsonb, 'pending', '98220000-0000-4000-8000-000000000105',
    (select job_id from issue_390_jobs where label = 'completed_without_result'),
    null, null, null, 0
  );

create temporary table issue_390_malformed_cache_before on commit drop as
select id, to_jsonb(cache_row) as row_state
from public.lca_result_cache as cache_row
where id = '98220000-0000-4000-8000-000000000311';

create temporary table issue_390_untouched_cache_rows on commit drop as
select id, to_jsonb(cache_row) as row_state
from public.lca_result_cache as cache_row
where id in (
  '98220000-0000-4000-8000-000000000302',
  '98220000-0000-4000-8000-000000000308',
  '98220000-0000-4000-8000-000000000309'
);

insert into public.lca_latest_all_unit_results (
  id, snapshot_id, job_id, worker_job_id, result_id,
  query_artifact_url, query_artifact_sha256, query_artifact_byte_size,
  query_artifact_format, status
) values (
  '98220000-0000-4000-8000-000000000401',
  '98220000-0000-4000-8000-000000000010',
  '98220000-0000-4000-8000-000000000101',
  (select job_id from issue_390_jobs where label = 'completed_with_result'),
  '98220000-0000-4000-8000-000000000201',
  'storage://lca_results/issue-390/query.jsonl',
  repeat('b', 64),
  1024,
  'jsonl:v1',
  'ready'
);

select is(
  api.lca_read_job_projection_v1(
    '98220000-0000-4000-8000-000000000001',
    (select job_id from issue_390_jobs where label = 'completed_with_result'),
    null,
    false
  ),
  public.lca_read_job_projection(
    '98220000-0000-4000-8000-000000000001',
    (select job_id from issue_390_jobs where label = 'completed_with_result'),
    null,
    false
  ),
  'job projection v1 preserves the canonical projection DTO exactly'
);

select is(
  api.lca_read_result_projection_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000201',
    'hdf5:v1',
    false
  ),
  public.lca_read_result_projection(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000201',
    'hdf5:v1',
    false
  ),
  'result projection v1 preserves the canonical projection DTO exactly'
);

select is(
  api.lca_read_latest_single_solve_result_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000010',
    1
  ),
  public.lca_read_latest_single_solve_result(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000010',
    1
  ),
  'single-solve projection v1 preserves the canonical projection DTO exactly'
);

select is(
  api.lca_read_job_projection_v1(
    '98220000-0000-4000-8000-000000000099',
    (select job_id from issue_390_jobs where label = 'completed_with_result'),
    null,
    false
  ),
  public.lca_read_job_projection(
    '98220000-0000-4000-8000-000000000099',
    (select job_id from issue_390_jobs where label = 'completed_with_result'),
    null,
    false
  ),
  'foreign job projection remains exactly data-null and leaks no result metadata'
);

select is(
  api.lca_read_result_projection_v1(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000201',
    'hdf5:v1',
    false
  ),
  public.lca_read_result_projection(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000201',
    'hdf5:v1',
    false
  ),
  'foreign result projection remains exactly data-null'
);

select is(
  api.lca_read_latest_single_solve_result_v1(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000010',
    1
  ),
  public.lca_read_latest_single_solve_result(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000010',
    1
  ),
  'foreign single-solve projection remains exactly data-null'
);

select is(
  api.lca_read_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'ready'
  )->'data'->>'resultId',
  '98220000-0000-4000-8000-000000000201',
  'cache read returns the canonical ready result'
);

select is(
  api.cmd_lca_touch_result_cache_v1(
    '98220000-0000-4000-8000-000000000301'
  )->'data'->>'hitCount',
  '4',
  'cache touch atomically returns the incremented hit count'
);

select is(
  (select hit_count::text from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000301'),
  '4',
  'cache touch persists exactly one increment'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'new-admission',
    '{"request":"new"}'::jsonb,
    '98220000-0000-4000-8000-000000000501', null, false
  )->>'outcome',
  'accepted',
  'new cache admission accepts the requested pending binding'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'new-admission',
    '{"request":"new"}'::jsonb,
    '98220000-0000-4000-8000-000000000501', null, false
  )->>'outcome',
  'accepted',
  'same-binding replay is accepted idempotently without claiming a physical insert'
);

select is(
  (select hit_count::text from public.lca_result_cache where request_key = 'new-admission'),
  '2',
  'same-binding replay increments cache access exactly once'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'new-admission',
    '{"request":"different"}'::jsonb,
    '98220000-0000-4000-8000-000000000502', null, false
  )->>'outcome',
  'reused',
  'a different request cannot replace an active pending canonical binding'
);

select is(
  (select job_id::text from public.lca_result_cache where request_key = 'new-admission'),
  '98220000-0000-4000-8000-000000000501',
  'active conflict preserves the canonical job binding'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'ready',
    '{"request":"different"}'::jsonb,
    '98220000-0000-4000-8000-000000000502', null, false
  )->>'outcome',
  'reused',
  'ready cache rows are reused rather than reset to pending'
);

select is(
  (select result_id::text from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000301'),
  '98220000-0000-4000-8000-000000000201',
  'ready conflict preserves the canonical result binding'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'active-worker-only',
    '{"request":"all-unit-must-not-replace-active"}'::jsonb,
    '98220000-0000-4000-8000-000000000509', null, true
  )->>'outcome',
  'reused',
  'replace-ready never replaces a valid active binding'
);

do $fixture$
begin
  perform api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'active-worker-only',
    '{"request":"must-not-rebind"}'::jsonb,
    '98220000-0000-4000-8000-000000000506', null, false
  );
end
$fixture$;

select is(
  (
    select concat_ws(':', worker_job_id::text, hit_count::text)
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000307'
  ),
  (select job_id::text || ':2' from issue_390_jobs where label = 'completed_without_result'),
  'both active admission branches preserve the worker binding and increment once each'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'ready',
    '{"request":"all-unit-replace"}'::jsonb,
    '98220000-0000-4000-8000-000000000507', null, true
  )->>'outcome',
  'accepted',
  'all-unit admission can explicitly replace a valid ready binding'
);

select is(
  (
    select concat_ws(':', status, job_id::text, coalesce(result_id::text, 'null'))
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000301'
  ),
  'pending:98220000-0000-4000-8000-000000000507:null',
  'replace-ready atomically binds the new all-unit job and clears the old result'
);

update public.lca_result_cache
set status = 'failed', error_code = 'old', error_message = 'old failure'
where request_key = 'new-admission';

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'new-admission',
    '{"request":"retry"}'::jsonb,
    '98220000-0000-4000-8000-000000000503', null, false
  )->>'outcome',
  'accepted',
  'failed cache rows accept a retry binding'
);

select is(
  (
    select concat_ws(':', status, job_id::text, coalesce(error_code, 'null'))
    from public.lca_result_cache
    where request_key = 'new-admission'
  ),
  'pending:98220000-0000-4000-8000-000000000503:null',
  'failed retry atomically resets state and error while binding the new job'
);

select is(
  (select hit_count::text from public.lca_result_cache where request_key = 'new-admission'),
  '4',
  'retry admission increments hit_count exactly once and must be mutually exclusive with touch'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'broken-ready',
    '{"request":"repair-ready"}'::jsonb,
    '98220000-0000-4000-8000-000000000504', null, false
  )->>'outcome',
  'accepted',
  'ready rows without a result are recoverable and accept a new binding'
);

select is(
  (
    select concat_ws(':', status, job_id::text, hit_count::text)
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000305'
  ),
  'pending:98220000-0000-4000-8000-000000000504:1',
  'broken ready recovery with replace-ready false binds once and touches once'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'broken-ready-replace',
    '{"request":"repair-ready-replace"}'::jsonb,
    '98220000-0000-4000-8000-000000000510', null, true
  )->>'outcome',
  'accepted',
  'broken ready recovery is accepted with replace-ready true'
);

select is(
  (
    select concat_ws(':', status, job_id::text, hit_count::text)
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000310'
  ),
  'pending:98220000-0000-4000-8000-000000000510:1',
  'broken ready recovery with replace-ready true binds once and touches once'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'broken-pending',
    '{"request":"repair-pending"}'::jsonb,
    '98220000-0000-4000-8000-000000000505', null, false
  )->>'outcome',
  'accepted',
  'pending rows without either job identity are recoverable'
);

update public.lca_result_cache
set status = 'stale'
where request_key = 'broken-pending';

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'broken-pending',
    '{"request":"repair-stale"}'::jsonb,
    '98220000-0000-4000-8000-000000000508', null, false
  )->>'outcome',
  'accepted',
  'stale cache rows accept a replacement binding'
);

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'active-worker-only',
    '{"request":"must-not-rebind"}'::jsonb,
    '98220000-0000-4000-8000-000000000506', null, false
  )->>'outcome',
  'reused',
  'an active row with either canonical job identity is preserved'
);

select is(
  api.lca_read_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'missing-cache'
  ),
  '{"ok": true, "data": null}'::jsonb,
  'missing cache lookup has an exact JSON null-data contract'
);

select is(
  api.cmd_lca_touch_result_cache_v1('98220000-0000-4000-8000-000000000399'),
  '{"ok": true, "data": null}'::jsonb,
  'missing touch target has an exact JSON null-data contract'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000399'
  ),
  '{"ok": true, "code": "cache_not_found", "data": null}'::jsonb,
  'reconcile returns an exact cache_not_found JSON contract after concurrent deletion'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000308'
  )->>'code',
  'job_not_found',
  'reconcile distinguishes a missing job projection from a missing cache row'
);

select is(
  (select status from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000308'),
  'pending',
  'job_not_found preserves pending state for fail-closed Edge handling'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000302'
  )->>'code',
  'job_not_found',
  'foreign reconcile returns job_not_found rather than exposing result existence'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000099',
    '98220000-0000-4000-8000-000000000302'
  )->>'data',
  null,
  'foreign reconcile returns null data and leaks no cache or result identifiers'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000309'
  )->>'ok',
  'false',
  'reconcile does not success-map an invalid no-identity projection'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000309'
  )->>'code',
  'INVALID_LCA_JOB_LOOKUP',
  'reconcile propagates the exact underlying projection failure code'
);

select ok(
  (
    select bool_and(to_jsonb(cache_row) = frozen.row_state)
    from issue_390_untouched_cache_rows as frozen
    join public.lca_result_cache as cache_row using (id)
  ),
  'job_not_found, foreign ownership, and projection errors preserve the complete cache rows and timestamps'
);

select is(
  api.lca_read_latest_all_unit_result_v1(
    '98220000-0000-4000-8000-000000000010'
  ),
  (
    select jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'snapshotId', snapshot_id,
        'resultId', result_id,
        'computedAt', computed_at,
        'queryArtifactUrl', query_artifact_url,
        'queryArtifactFormat', query_artifact_format
      )
    )
    from public.lca_latest_all_unit_results
    where id = '98220000-0000-4000-8000-000000000401'
  ),
  'latest all-unit read returns exactly the five reviewed projection fields'
);

select is(
  api.lca_read_latest_all_unit_result_v1(
    '98220000-0000-4000-8000-000000000099'
  ),
  '{"ok": true, "data": null}'::jsonb,
  'missing latest all-unit result returns the exact null-data contract'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000302'
  )->>'code',
  'reconciled',
  'completed jobs with a visible result reconcile in one capability'
);

select is(
  (
    select concat_ws(':', status, result_id::text, hit_count::text)
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000302'
  ),
  'ready:98220000-0000-4000-8000-000000000203:1',
  'owned reconcile binds the result and touches exactly once after no-op foreign probes'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000303'
  )->>'code',
  'result_pending',
  'completed jobs without a visible result return result_pending without a ninth query'
);

select is(
  (select status from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000303'),
  'pending',
  'result_pending preserves the pending cache state'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000312'
  )->>'code',
  'result_pending',
  'reconcile resolves only worker_job_id when cache job identities disagree'
);

select is(
  (select result_id::text from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000312'),
  null,
  'mismatched legacy identity cannot bind a result owned by another worker'
);

insert into public.lca_results (
  id, job_id, worker_job_id, snapshot_id, payload, diagnostics,
  artifact_url, artifact_sha256, artifact_byte_size, artifact_format
) values (
  '98220000-0000-4000-8000-000000000202',
  '98220000-0000-4000-8000-000000000102',
  (select job_id from issue_390_jobs where label = 'completed_without_result'),
  '98220000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/issue-390/result-late.h5',
  repeat('c', 64),
  256,
  'hdf5:v1'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000303'
  )->>'code',
  'reconciled',
  'result_pending converges on retry after the result becomes visible'
);

select is(
  (
    select concat_ws(':', status, result_id::text)
    from public.lca_result_cache
    where id = '98220000-0000-4000-8000-000000000303'
  ),
  'ready:98220000-0000-4000-8000-000000000202',
  'retry convergence binds the late result and marks the cache ready'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000304'
  )->>'code',
  'reconciled',
  'failed worker projections reconcile without direct worker_jobs access'
);

select is(
  (select status from public.lca_result_cache where id = '98220000-0000-4000-8000-000000000304'),
  'failed',
  'failed reconcile marks cache failed'
);

select set_config('search_path', 'extensions, pg_temp, public', true);
select is(
  api.lca_read_result_cache_v1(
    'prod', '98220000-0000-4000-8000-000000000010', 'ready'
  )->'data'->>'cacheId',
  '98220000-0000-4000-8000-000000000301',
  'caller search_path poisoning cannot redirect qualified facade dependencies'
);

reset role;
set local search_path = extensions, public, auth;
create or replace function public.lca_read_job_projection(
  p_requested_by uuid,
  p_worker_job_id uuid default null,
  p_legacy_job_id uuid default null,
  p_include_internal boolean default false
) returns jsonb
language sql
security definer
set search_path = ''
as $malformed$
  select case
    when p_requested_by = '98220000-0000-4000-8000-000000000002'::uuid
      then '{"ok":true}'::jsonb
    when p_requested_by = '98220000-0000-4000-8000-000000000003'::uuid
      then '{"ok":false,"code":"BROKEN","status":"oops","message":"broken"}'::jsonb
    when p_requested_by = '98220000-0000-4000-8000-000000000004'::uuid
      then '{"ok":true,"data":{"job":{"status":"completed"},"result":{"resultId":"not-a-uuid"}}}'::jsonb
    else '{"ok":"true","data":{"job":{"status":"completed"}}}'::jsonb
  end
$malformed$;
set local role service_role;

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000001',
    '98220000-0000-4000-8000-000000000311'
  ),
  '{"ok":false,"code":"INVALID_LCA_JOB_PROJECTION","status":500,"data":null}'::jsonb,
  'non-boolean projection success markers fail closed with the exact stable contract'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000002',
    '98220000-0000-4000-8000-000000000311'
  ),
  '{"ok":false,"code":"INVALID_LCA_JOB_PROJECTION","status":500,"data":null}'::jsonb,
  'boolean success without an explicit data key also fails closed'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000003',
    '98220000-0000-4000-8000-000000000311'
  ),
  '{"ok":false,"code":"INVALID_LCA_JOB_PROJECTION","status":500,"data":null}'::jsonb,
  'malformed projection failures cannot raise through an unsafe status cast'
);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98220000-0000-4000-8000-000000000004',
    '98220000-0000-4000-8000-000000000311'
  ),
  '{"ok":false,"code":"INVALID_LCA_JOB_PROJECTION","status":500,"data":null}'::jsonb,
  'malformed result identifiers fail closed before UUID conversion'
);

select is(
  (select to_jsonb(cache_row) from public.lca_result_cache as cache_row where id = '98220000-0000-4000-8000-000000000311'),
  (select row_state from issue_390_malformed_cache_before),
  'malformed projection leaves status, result, hit count, and timestamps unchanged'
);

select * from finish();
rollback;
