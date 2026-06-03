begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(13);

select has_function(
  'public',
  'lca_enqueue_job',
  array['text', 'jsonb'],
  'legacy lca enqueue function still exists for explicit disabled error'
);

select has_function(
  'public',
  'lca_package_enqueue_job',
  array['jsonb'],
  'legacy package enqueue function still exists for explicit disabled error'
);

select has_view(
  'public',
  'worker_legacy_lifecycle_audit',
  'legacy lifecycle audit view exists'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '96000000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.lca_enqueue_job(text,jsonb)',
    'EXECUTE'
  ),
  'authenticated cannot execute legacy lca enqueue'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'public.lca_package_enqueue_job(jsonb)',
    'EXECUTE'
  ),
  'authenticated cannot execute legacy package enqueue'
);

select hasnt_table(
  'public',
  'lca_jobs',
  'legacy lca job table is physically retired after worker_jobs cutover'
);

select hasnt_table(
  'public',
  'lca_package_jobs',
  'legacy package job table is physically retired after worker_jobs cutover'
);

select ok(
  not has_table_privilege('authenticated', 'public.dataset_review_submit_gate_runs', 'SELECT'),
  'authenticated cannot directly read gate run history'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select ok(
  has_function_privilege(
    'service_role',
    'public.lca_enqueue_job(text,jsonb)',
    'EXECUTE'
  ),
  'service_role receives explicit disabled error instead of permission denied for lca enqueue'
);

select throws_like(
  $$select public.lca_enqueue_job('lca_jobs', '{}'::jsonb)$$,
  '%disabled after worker_jobs cutover%',
  'legacy lca enqueue fails with a cutover message'
);

select throws_like(
  $$select public.lca_package_enqueue_job('{}'::jsonb)$$,
  '%disabled after worker_jobs cutover%',
  'legacy package enqueue fails with a cutover message'
);

select lives_ok(
  $$select count(*) from public.worker_legacy_lifecycle_audit$$,
  'service_role can read the legacy lifecycle audit view'
);

select is(
  public.worker_enqueue_job(
    p_job_kind => 'lca.snapshot_gc',
    p_payload_json => '{"execute":false,"environment":"test"}'::jsonb,
    p_requester_type => 'system',
    p_idempotency_key => 'legacy-cleanup-test:snapshot-gc',
    p_concurrency_key => 'legacy-cleanup-test:snapshot-gc'
  )->>'ok',
  'true',
  'worker_enqueue_job remains the supported delivery path'
);

select * from finish();
rollback;
