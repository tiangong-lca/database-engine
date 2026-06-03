begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(21);

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

select cmp_ok(
  (
    select count(*)
    from public.worker_legacy_table_retirement_blockers
    where legacy_table = 'public.lca_package_jobs'
      and blocker_type = 'function_source_reference'
  ),
  '=',
  0::bigint,
  'audit reports no runtime function body references to lca_package_jobs after retention preview cutover'
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

select hasnt_table(
  'public',
  'lca_jobs',
  'legacy lca job table has been physically retired'
);

select hasnt_table(
  'public',
  'lca_package_jobs',
  'legacy package job table has been physically retired'
);

select hasnt_table(
  'public',
  'dataset_review_submit_jobs',
  'legacy review-submit coordinator job table has been physically retired'
);

select has_table(
  'archive',
  'worker_legacy_job_table_rows',
  'legacy job rows are archived before table retirement'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '97000000-0000-4000-8000-000000000001', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_table_privilege(
    'authenticated',
    (
      select c.oid
      from pg_class as c
      join pg_namespace as n on n.oid = c.relnamespace
      where n.nspname = 'archive'
        and c.relname = 'worker_legacy_job_table_rows'
    ),
    'SELECT'
  ),
  'authenticated users cannot read legacy job archives'
);

select ok(
  has_table_privilege(
    'service_role',
    (
      select c.oid
      from pg_class as c
      join pg_namespace as n on n.oid = c.relnamespace
      where n.nspname = 'archive'
        and c.relname = 'worker_legacy_job_table_rows'
    ),
    'SELECT'
  ),
  'service_role can read legacy job archives'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select lives_ok(
  $$select count(*) from util.preview_lca_package_retention(interval '30 days', interval '7 days', now())$$,
  'package retention preview still runs after the legacy package job table is absent'
);

select * from finish();
rollback;
