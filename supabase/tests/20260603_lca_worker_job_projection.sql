begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(19);

select has_function(
  'public',
  'lca_read_job_projection',
  array['uuid', 'uuid', 'uuid', 'boolean'],
  'lca_read_job_projection service function exists'
);

select has_function(
  'public',
  'lca_read_result_projection',
  array['uuid', 'uuid', 'text', 'boolean'],
  'lca_read_result_projection service function exists'
);

select has_function(
  'public',
  'lca_read_latest_single_solve_result',
  array['uuid', 'uuid', 'integer'],
  'lca_read_latest_single_solve_result service function exists'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '98110000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.lca_read_result_projection(uuid,uuid,text,boolean)',
    'EXECUTE'
  ),
  'authenticated users cannot execute internal LCA result projection'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

insert into public.lca_network_snapshots (
  id,
  scope,
  process_filter,
  provider_matching_rule,
  source_hash,
  status,
  created_at,
  updated_at
) values (
  '98110000-0000-4000-8000-000000000010',
  'full_library',
  '{}'::jsonb,
  'split_by_evidence_hybrid',
  'pgtap-lca-worker-job-projection',
  'ready',
  now(),
  now()
);

create temporary table lca_projection_test_ids (
  label text primary key,
  job_id uuid not null
) on commit drop;

grant all on lca_projection_test_ids to public;

insert into lca_projection_test_ids (label, job_id)
select
  'solve_one_worker',
  (result->'data'->>'id')::uuid
from (
  select public.worker_enqueue_job(
    p_job_kind => 'lca.solve_one',
    p_payload_json => jsonb_build_object(
      'type', 'solve_one',
      'job_id', '98110000-0000-4000-8000-000000000011',
      'snapshot_id', '98110000-0000-4000-8000-000000000010',
      'rhs', jsonb_build_array(0, 2.5, 0),
      'solve', jsonb_build_object('return_x', true, 'return_g', true, 'return_h', true),
      'print_level', 0
    ),
    p_payload_schema_version => 'lca.solve_one.request.v1',
    p_subject_type => 'lca_job',
    p_subject_id => '98110000-0000-4000-8000-000000000011',
    p_subject_version => '98110000-0000-4000-8000-000000000010',
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_idempotency_key => 'pgtap:lca-worker-job-projection:solve-one',
    p_request_hash => 'pgtap-lca-worker-job-projection',
    p_queue_key => '98110000-0000-4000-8000-000000000010',
    p_visibility => 'user'
  ) as result
) as enqueue;

select public.worker_record_job_result(
  p_job_id => (select job_id from lca_projection_test_ids where label = 'solve_one_worker'),
  p_lease_token => null::uuid,
  p_status => 'completed',
  p_result_json => jsonb_build_object('ok', true),
  p_result_schema_version => 'lca.solve.result.v1',
  p_result_ref => jsonb_build_object(
    'domainSource', 'lca_results',
    'resultId', '98110000-0000-4000-8000-000000000012',
    'legacyJobId', '98110000-0000-4000-8000-000000000011'
  )
);

insert into public.lca_results (
  id,
  job_id,
  worker_job_id,
  snapshot_id,
  payload,
  diagnostics,
  artifact_url,
  artifact_sha256,
  artifact_byte_size,
  artifact_format
) values (
  '98110000-0000-4000-8000-000000000012',
  '98110000-0000-4000-8000-000000000011',
  (select job_id from lca_projection_test_ids where label = 'solve_one_worker'),
  '98110000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/pgtap/solve-one.h5',
  repeat('a', 64),
  512,
  'hdf5:v1'
);

insert into public.lca_result_cache (
  id,
  scope,
  snapshot_id,
  request_key,
  request_payload,
  status,
  job_id,
  worker_job_id,
  result_id,
  hit_count,
  created_at,
  updated_at
) values (
  '98110000-0000-4000-8000-000000000013',
  'prod',
  '98110000-0000-4000-8000-000000000010',
  'pgtap-lca-worker-job-projection-cache',
  jsonb_build_object(
    'version', 'lca_solve_request.v1',
    'scope', 'prod',
    'snapshot_id', '98110000-0000-4000-8000-000000000010',
    'demand_mode', 'single',
    'demand', jsonb_build_object('process_index', 1, 'amount', 2.5)
  ),
  'ready',
  '98110000-0000-4000-8000-000000000011',
  (select job_id from lca_projection_test_ids where label = 'solve_one_worker'),
  '98110000-0000-4000-8000-000000000012',
  1,
  now() - interval '1 minute',
  now()
);

select is(
  public.lca_read_job_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_legacy_job_id => '98110000-0000-4000-8000-000000000011',
    p_include_internal => false
  )->'data'->'job'->>'workerJobId',
  (select job_id::text from lca_projection_test_ids where label = 'solve_one_worker'),
  'lca_read_job_projection resolves a legacy LCA job id through worker_jobs'
);

select is(
  public.lca_read_job_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_worker_job_id => (select job_id from lca_projection_test_ids where label = 'solve_one_worker'),
    p_include_internal => false
  )->'data'->'result'->>'resultId',
  '98110000-0000-4000-8000-000000000012',
  'lca_read_job_projection returns the latest result by worker job id'
);

select is(
  public.lca_read_result_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_result_id => '98110000-0000-4000-8000-000000000012',
    p_required_artifact_format => 'hdf5:v1'
  )->'data'->'job'->>'jobKind',
  'lca.solve_one',
  'lca_read_result_projection authorizes result reads through worker_jobs'
);

select is(
  public.lca_read_result_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000002',
    p_result_id => '98110000-0000-4000-8000-000000000012',
    p_required_artifact_format => 'hdf5:v1'
  )->>'data',
  null,
  'lca_read_result_projection hides results from non-owner users'
);

select is(
  public.lca_read_result_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_result_id => '98110000-0000-4000-8000-000000000012',
    p_required_artifact_format => 'contribution-path:v1'
  )->>'code',
  'UNSUPPORTED_LCA_RESULT_ARTIFACT_FORMAT',
  'lca_read_result_projection validates required artifact format'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => 1
  )->'data'->'result'->>'resultId',
  '98110000-0000-4000-8000-000000000012',
  'latest single solve projection returns the cached result without lca_jobs'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => 1
  )->'data'->>'amount',
  '2.5',
  'latest single solve projection returns the demand amount'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => 2
  )->>'data',
  null,
  'latest single solve projection returns null when process index does not match'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000002',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => 1
  )->>'data',
  null,
  'latest single solve projection hides non-owner cache entries'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => -1
  )->>'code',
  'INVALID_LCA_SOLVE_LOOKUP',
  'latest single solve projection validates process index'
);

select set_config('request.jwt.claim.role', '', true);
select set_config('request.jwt.claims', '', true);
select set_config('request.headers', '', true);

select is(
  public.lca_read_job_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_legacy_job_id => '98110000-0000-4000-8000-000000000011',
    p_include_internal => false
  )->'data'->'job'->>'workerJobId',
  (select job_id::text from lca_projection_test_ids where label = 'solve_one_worker'),
  'service_role execute grant is sufficient for lca_read_job_projection without request GUCs'
);

select is(
  public.lca_read_result_projection(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_result_id => '98110000-0000-4000-8000-000000000012',
    p_required_artifact_format => 'hdf5:v1'
  )->'data'->'job'->>'jobKind',
  'lca.solve_one',
  'service_role execute grant is sufficient for lca_read_result_projection without request GUCs'
);

select is(
  public.lca_read_latest_single_solve_result(
    p_requested_by => '98110000-0000-4000-8000-000000000001',
    p_snapshot_id => '98110000-0000-4000-8000-000000000010',
    p_process_index => 1
  )->'data'->'result'->>'resultId',
  '98110000-0000-4000-8000-000000000012',
  'service_role execute grant is sufficient for latest single solve projection without request GUCs'
);

select ok(
  not exists (
    select 1
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'public'
      and proc.proname in (
        'lca_read_job_projection',
        'lca_read_result_projection',
        'lca_read_latest_single_solve_result'
      )
      and position('lca_jobs' in proc.prosrc) > 0
  ),
  'LCA projection functions do not read public.lca_jobs'
);

select ok(
  not exists (
    select 1
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_jobs'
      and blocker_type in ('foreign_key', 'dependent_view', 'policy')
      and is_drop_restrict_blocker
  ),
  'lca_jobs still has no DB DROP RESTRICT blockers after adding LCA projections'
);

select * from finish();
rollback;
