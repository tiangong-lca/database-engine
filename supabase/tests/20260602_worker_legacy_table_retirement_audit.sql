begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

select has_view(
  'public',
  'worker_legacy_table_retirement_blockers',
  'legacy table retirement blocker audit view exists'
);

select has_table(
  'public',
  'dataset_review_submit_requests',
  'review-submit request coordinator replacement table exists'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '97000000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_table_privilege(
    'authenticated',
    'public.worker_legacy_table_retirement_blockers',
    'SELECT'
  ),
  'authenticated users cannot read legacy table retirement blockers'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select ok(
  has_table_privilege(
    'service_role',
    'public.worker_legacy_table_retirement_blockers',
    'SELECT'
  ),
  'service_role can read legacy table retirement blockers'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_jobs'
      and blocker_type = 'foreign_key'
      and is_drop_restrict_blocker
  ),
  '=',
  0::bigint,
  'audit reports no lca_jobs foreign-key DROP RESTRICT blockers'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_package_jobs'
      and blocker_type = 'foreign_key'
      and is_drop_restrict_blocker
  ),
  '=',
  0::bigint,
  'audit reports no lca_package_jobs foreign-key DROP RESTRICT blockers'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.dataset_review_submit_jobs'
      and blocker_type = 'function_signature'
      and blocker_identity like 'public.cmd_dataset_review_submit_job_payload(%'
      and is_drop_restrict_blocker
  ),
  '=',
  0::bigint,
  'audit reports no dataset_review_submit_jobs function-signature DROP RESTRICT blocker'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where blocker_type = 'dependent_view'
      and is_drop_restrict_blocker
      and legacy_table in (
        'public.lca_jobs',
        'public.lca_package_jobs',
        'public.dataset_review_submit_jobs'
      )
  ),
  '=',
  0::bigint,
  'audit reports no dependent-view DROP RESTRICT blockers for legacy job tables'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where blocker_type = 'policy'
      and is_drop_restrict_blocker
      and legacy_table in (
        'public.lca_jobs',
        'public.lca_package_jobs',
        'public.dataset_review_submit_jobs'
      )
  ),
  '=',
  0::bigint,
  'audit reports no RLS policy DROP RESTRICT blockers for legacy job tables'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where is_drop_restrict_blocker
      and legacy_table in (
        'public.lca_jobs',
        'public.lca_package_jobs',
        'public.dataset_review_submit_jobs'
      )
  ),
  '=',
  0::bigint,
  'audit reports no DROP RESTRICT blockers for the three legacy worker job tables'
);

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.dataset_review_submit_jobs'
      and blocker_type = 'function_source_reference'
  ),
  '=',
  0::bigint,
  'audit reports no runtime function body references to dataset_review_submit_jobs after coordinator cutover'
);

select ok(
  exists (
    select 1
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_package_jobs'
      and blocker_type = 'function_source_reference'
      and not is_drop_restrict_blocker
  ),
  'audit reports package function source references as runtime blockers'
);

select ok(
  not exists (
    select 1
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_jobs'
      and details ? 'dependentColumns'
  ),
  'foreign-key dependent column blocker details are gone after DB FK cutover'
);

select is(
  (
    select count(*)::text
    from public.worker_legacy_table_retirement_blockers
    where legacy_table not in (
      'public.lca_jobs',
      'public.lca_package_jobs',
      'public.dataset_review_submit_jobs'
    )
  ),
  '0',
  'audit is scoped to the three legacy worker job retirement tables'
);

reset role;

select lives_ok(
  $$drop table public.lca_jobs, public.lca_package_jobs, public.dataset_review_submit_jobs restrict$$,
  'legacy job tables can be dropped with RESTRICT inside the test transaction after DB blockers are removed'
);

select * from finish();
rollback;
