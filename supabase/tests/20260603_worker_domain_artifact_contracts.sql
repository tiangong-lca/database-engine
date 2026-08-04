begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create or replace function pg_temp.has_empty_search_path(p_signature text)
returns boolean
language sql
stable
as $$
  select exists (
    select 1
    from pg_proc p
    cross join lateral unnest(coalesce(p.proconfig, array[]::text[])) as config(setting)
    where p.oid = p_signature::regprocedure
      and config.setting in ('search_path=', 'search_path=""')
  );
$$;

select plan(40);

select ok(
  to_regprocedure('util.apply_lca_package_retention(interval,interval,timestamp with time zone,integer,boolean)') is not null,
  'package retention apply function exists'
);

select has_view(
  'public',
  'worker_domain_traceability_cutoffs',
  'worker domain traceability cutoff contract view exists'
);

select has_view(
  'public',
  'worker_domain_traceability_violations',
  'worker domain traceability violation audit view exists'
);

select ok(
  has_function_privilege(
    'service_role',
    'util.apply_lca_package_retention(interval,interval,timestamp with time zone,integer,boolean)',
    'EXECUTE'
  ),
  'service_role can execute package retention apply helper'
);

select ok(
  has_table_privilege('service_role', 'public.worker_domain_traceability_cutoffs', 'SELECT'),
  'service_role can read worker domain traceability cutoffs'
);

select ok(
  has_table_privilege('service_role', 'public.worker_domain_traceability_violations', 'SELECT'),
  'service_role can read worker domain traceability violations'
);

select ok(
  not has_function_privilege(
    'authenticated',
    'util.apply_lca_package_retention(interval,interval,timestamp with time zone,integer,boolean)',
    'EXECUTE'
  ),
  'authenticated users cannot execute package retention apply helper'
);

set local role authenticated;
select set_config('request.jwt.claim.sub', '98200000-0000-4000-8000-000000000100', true);
select set_config('request.jwt.claim.role', 'authenticated', true);
select set_config('request.jwt.claims', '{"role":"authenticated"}', true);

select ok(
  not has_table_privilege('authenticated', 'public.worker_domain_traceability_cutoffs', 'SELECT'),
  'authenticated users cannot read worker domain traceability cutoffs'
);

select ok(
  not has_table_privilege('authenticated', 'public.worker_domain_traceability_violations', 'SELECT'),
  'authenticated users cannot read worker domain traceability violations'
);

reset role;
set local role service_role;
select set_config('request.jwt.claim.sub', '', true);
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select ok(
  obj_description('public.lca_package_artifacts'::regclass, 'pg_class') like '%domain artifact%'
  and obj_description('public.lca_package_artifacts'::regclass, 'pg_class') like '%not a task lifecycle/job table%',
  'package artifact table comment distinguishes domain artifacts from job lifecycle'
);

select ok(
  obj_description('archive.worker_legacy_job_table_rows'::regclass, 'pg_class') like '%Manual rollback archive%'
  and obj_description('archive.worker_legacy_job_table_rows'::regclass, 'pg_class') like '%without automatic TTL deletion%',
  'legacy archive retention policy is documented as manual/signoff based'
);

select is(
  (select count(*)::integer from public.worker_domain_traceability_cutoffs),
  10,
  'traceability cutoff contract covers all worker-produced domain tables and documented exceptions'
);

select is(
  (
    select traceability_required
    from public.worker_domain_traceability_cutoffs
    where domain_source = 'lca_network_snapshots'
  ),
  false,
  'network snapshots are documented as a traceability exception'
);

insert into public.worker_jobs (
  id,
  job_kind,
  worker_runtime,
  worker_queue,
  status,
  payload_schema_version,
  payload_json,
  diagnostics,
  requester_type,
  requested_by,
  created_at,
  updated_at,
  finished_at
) values
  (
    '98200000-0000-4000-8000-000000000001',
    'tidas.export_package',
    'calculator',
    'package',
    'completed',
    'tidas.export_package.request.v1',
    '{}'::jsonb,
    '{}'::jsonb,
    'user',
    '98200000-0000-4000-8000-000000000100',
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000002',
    'tidas.export_package',
    'calculator',
    'package',
    'completed',
    'tidas.export_package.request.v1',
    '{}'::jsonb,
    '{}'::jsonb,
    'user',
    '98200000-0000-4000-8000-000000000100',
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000003',
    'lca.solve_one',
    'calculator',
    'solver',
    'completed',
    'lca.solve_one.request.v1',
    '{}'::jsonb,
    '{}'::jsonb,
    'user',
    '98200000-0000-4000-8000-000000000100',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000004',
    'review_submit.submit',
    'calculator',
    'review_submit',
    'waiting',
    'review_submit.submit.request.v1',
    '{}'::jsonb,
    '{}'::jsonb,
    'user',
    '98200000-0000-4000-8000-000000000100',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00',
    null
  ),
  (
    '98200000-0000-4000-8000-000000000005',
    'review_submit.gate',
    'calculator',
    'review_submit_gate',
    'completed',
    'review_submit.gate.request.v1',
    '{}'::jsonb,
    '{}'::jsonb,
    'user',
    '98200000-0000-4000-8000-000000000100',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  );

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
  '98200000-0000-4000-8000-000000000010',
  'full_library',
  '{}'::jsonb,
  'split_by_evidence_hybrid',
  'pgtap-worker-domain-artifact-contracts',
  'ready',
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
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
  artifact_format,
  created_at
) values
  (
    '98200000-0000-4000-8000-000000000101',
    '98200000-0000-4000-8000-000000000201',
    '98200000-0000-4000-8000-000000000003',
    '98200000-0000-4000-8000-000000000010',
    '{}'::jsonb,
    '{}'::jsonb,
    's3://lca-results/linked.json',
    repeat('a', 64),
    10,
    'lca-result:v1',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000102',
    '98200000-0000-4000-8000-000000000202',
    null,
    '98200000-0000-4000-8000-000000000010',
    '{}'::jsonb,
    '{}'::jsonb,
    's3://lca-results/unlinked.json',
    repeat('b', 64),
    10,
    'lca-result:v1',
    '2026-06-03 14:00:00+00'
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
  last_accessed_at,
  created_at,
  updated_at
) values (
  '98200000-0000-4000-8000-000000000103',
  'prod',
  '98200000-0000-4000-8000-000000000010',
  'pgtap-worker-domain-artifact-contracts-cache',
  '{}'::jsonb,
  'ready',
  '98200000-0000-4000-8000-000000000203',
  null,
  '98200000-0000-4000-8000-000000000101',
  0,
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
);

insert into public.lca_latest_all_unit_results (
  id,
  snapshot_id,
  job_id,
  worker_job_id,
  result_id,
  query_artifact_url,
  query_artifact_sha256,
  query_artifact_byte_size,
  query_artifact_format,
  status,
  computed_at,
  created_at,
  updated_at
) values (
  '98200000-0000-4000-8000-000000000104',
  '98200000-0000-4000-8000-000000000010',
  '98200000-0000-4000-8000-000000000204',
  null,
  '98200000-0000-4000-8000-000000000101',
  's3://lca-results/latest-query.json',
  repeat('c', 64),
  11,
  'lca-query:v1',
  'ready',
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
);

insert into public.lca_factorization_registry (
  id,
  scope,
  snapshot_id,
  backend,
  numeric_options_hash,
  status,
  prepared_job_id,
  prepared_worker_job_id,
  diagnostics,
  prepared_at,
  created_at,
  updated_at
) values (
  '98200000-0000-4000-8000-000000000105',
  'prod',
  '98200000-0000-4000-8000-000000000010',
  'umfpack',
  'pgtap-worker-domain-artifact-contracts-factorization',
  'ready',
  '98200000-0000-4000-8000-000000000205',
  null,
  '{}'::jsonb,
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
);

insert into public.lca_package_artifacts (
  id,
  job_id,
  worker_job_id,
  artifact_kind,
  status,
  artifact_url,
  artifact_sha256,
  artifact_byte_size,
  artifact_format,
  content_type,
  metadata,
  expires_at,
  is_pinned,
  created_at,
  updated_at
) values
  (
    '98200000-0000-4000-8000-000000000301',
    '98200000-0000-4000-8000-000000000211',
    null,
    'export_zip',
    'ready',
    's3://packages/unlinked.zip',
    repeat('d', 64),
    20,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2026-06-10 00:00:00+00',
    false,
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000302',
    '98200000-0000-4000-8000-000000000212',
    '98200000-0000-4000-8000-000000000001',
    'export_zip',
    'ready',
    's3://packages/linked.zip',
    repeat('e', 64),
    20,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2026-06-10 00:00:00+00',
    false,
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000303',
    '98200000-0000-4000-8000-000000000213',
    null,
    'export_zip',
    'ready',
    's3://packages/historical-unlinked.zip',
    repeat('f', 64),
    20,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2026-06-10 00:00:00+00',
    false,
    '2026-06-03 13:00:00+00',
    '2026-06-03 13:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000304',
    '98200000-0000-4000-8000-000000000214',
    '98200000-0000-4000-8000-000000000001',
    'export_zip',
    'ready',
    's3://packages/eligible-gc.zip',
    repeat('1', 64),
    30,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2026-05-01 00:00:00+00',
    false,
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000305',
    '98200000-0000-4000-8000-000000000215',
    '98200000-0000-4000-8000-000000000001',
    'export_zip',
    'ready',
    's3://packages/pinned.zip',
    repeat('2', 64),
    40,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2026-05-01 00:00:00+00',
    true,
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  );

insert into public.lca_package_export_items (
  id,
  job_id,
  worker_job_id,
  table_name,
  dataset_id,
  version,
  is_seed,
  refs_done,
  created_at,
  updated_at
) values
  (
    '98200000-0000-4000-8000-000000000401',
    '98200000-0000-4000-8000-000000000221',
    null,
    'processes',
    '98200000-0000-4000-8000-000000000501',
    '01.00.000',
    false,
    true,
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000402',
    '98200000-0000-4000-8000-000000000222',
    '98200000-0000-4000-8000-000000000002',
    'processes',
    '98200000-0000-4000-8000-000000000502',
    '01.00.000',
    false,
    true,
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  );

insert into public.lca_package_request_cache (
  id,
  requested_by,
  operation,
  request_key,
  request_payload,
  status,
  job_id,
  worker_job_id,
  export_artifact_id,
  hit_count,
  last_accessed_at,
  created_at,
  updated_at
) values
  (
    '98200000-0000-4000-8000-000000000403',
    '98200000-0000-4000-8000-000000000100',
    'export_package',
    'pgtap-worker-domain-artifact-contracts-unlinked-cache',
    '{}'::jsonb,
    'ready',
    '98200000-0000-4000-8000-000000000223',
    null,
    null,
    0,
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00',
    '2026-06-03 14:00:00+00'
  ),
  (
    '98200000-0000-4000-8000-000000000404',
    '98200000-0000-4000-8000-000000000100',
    'export_package',
    'pgtap-worker-domain-artifact-contracts-stale-cache',
    '{}'::jsonb,
    'ready',
    '98200000-0000-4000-8000-000000000224',
    '98200000-0000-4000-8000-000000000001',
    '98200000-0000-4000-8000-000000000304',
    0,
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00',
    '2026-05-01 00:00:00+00'
  );

insert into public.dataset_review_submit_requests (
  id,
  dataset_table,
  dataset_id,
  dataset_version,
  revision_checksum,
  policy_profile,
  report_schema_version,
  status,
  requested_by,
  submit_worker_job_id,
  gate_worker_job_id,
  created_at,
  modified_at
) values (
  '98200000-0000-4000-8000-000000000601',
  'processes',
  '98200000-0000-4000-8000-000000000602',
  '01.00.000',
  repeat('3', 64),
  'review_submit_fast.v1',
  'review_submit_gate_report.v1',
  'waiting_gate',
  '98200000-0000-4000-8000-000000000100',
  '98200000-0000-4000-8000-000000000004',
  null,
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
);

insert into public.dataset_review_submit_gate_runs (
  id,
  dataset_table,
  dataset_id,
  dataset_version,
  revision_checksum,
  policy_profile,
  report_schema_version,
  status,
  requested_by,
  worker_job_id,
  blocking_reasons,
  created_at,
  modified_at
) values (
  '98200000-0000-4000-8000-000000000603',
  'processes',
  '98200000-0000-4000-8000-000000000604',
  '01.00.000',
  repeat('4', 64),
  'review_submit_fast.v1',
  'review_submit_gate_report.v1',
  'queued',
  '98200000-0000-4000-8000-000000000100',
  null,
  '[]'::jsonb,
  '2026-06-03 14:00:00+00',
  '2026-06-03 14:00:00+00'
);

select is(
  (
    select count(*)::integer
    from public.worker_domain_traceability_violations
    where domain_id in (
      '98200000-0000-4000-8000-000000000102',
      '98200000-0000-4000-8000-000000000103',
      '98200000-0000-4000-8000-000000000104',
      '98200000-0000-4000-8000-000000000105',
      '98200000-0000-4000-8000-000000000301',
      '98200000-0000-4000-8000-000000000401',
      '98200000-0000-4000-8000-000000000403',
      '98200000-0000-4000-8000-000000000601',
      '98200000-0000-4000-8000-000000000603'
    )
  ),
  9,
  'post-cutover unlinked domain rows are reported as traceability violations'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_results'
      and domain_id = '98200000-0000-4000-8000-000000000102'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked LCA result rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_result_cache'
      and domain_id = '98200000-0000-4000-8000-000000000103'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked LCA result-cache rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_latest_all_unit_results'
      and domain_id = '98200000-0000-4000-8000-000000000104'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked latest all-unit result rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_factorization_registry'
      and domain_id = '98200000-0000-4000-8000-000000000105'
      and violation_code = 'missing_prepared_worker_job_id'
  ),
  'audit reports unlinked prepared factorization rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_package_artifacts'
      and domain_id = '98200000-0000-4000-8000-000000000301'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked package artifact rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_package_export_items'
      and domain_id = '98200000-0000-4000-8000-000000000401'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked package export item rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'lca_package_request_cache'
      and domain_id = '98200000-0000-4000-8000-000000000403'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked package request-cache rows'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'dataset_review_submit_requests'
      and domain_id = '98200000-0000-4000-8000-000000000601'
      and violation_code = 'missing_gate_worker_job_id'
  ),
  'audit reports review-submit coordinator rows missing the gate worker job link'
);

select ok(
  exists (
    select 1 from public.worker_domain_traceability_violations
    where domain_source = 'dataset_review_submit_gate_runs'
      and domain_id = '98200000-0000-4000-8000-000000000603'
      and violation_code = 'missing_worker_job_id'
  ),
  'audit reports unlinked review-submit gate run rows'
);

select is(
  (
    select count(*)::integer
    from public.worker_domain_traceability_violations
    where domain_id = '98200000-0000-4000-8000-000000000303'
  ),
  0,
  'historical pre-cutover unlinked rows are ignored by traceability audit'
);

select is(
  (
    select count(*)::integer
    from public.worker_domain_traceability_violations
    where domain_id = '98200000-0000-4000-8000-000000000302'
  ),
  0,
  'linked post-cutover rows are not reported as traceability violations'
);

create temporary table pg_temp.package_retention_apply_preview as
select *
from util.apply_lca_package_retention(
  interval '30 days',
  interval '7 days',
  '2026-06-03 14:00:00+00'::timestamp with time zone,
  1000,
  true
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_preview
    where retention_area = 'lca_package_artifacts'
  ),
  1,
  'retention dry-run reports one eligible package artifact metadata row'
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_preview
    where retention_area = 'lca_package_request_cache'
  ),
  1,
  'retention dry-run reports one eligible stale package request-cache row'
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_preview
    where retention_area = 'lca_package_export_items'
  ),
  1,
  'retention dry-run reports one eligible package export item row'
);

select is(
  (
    select status
    from public.lca_package_artifacts
    where id = '98200000-0000-4000-8000-000000000304'
  ),
  'ready',
  'retention dry-run does not mutate package artifact metadata'
);

select is(
  (
    select count(*)::integer
    from public.lca_package_request_cache
    where id = '98200000-0000-4000-8000-000000000404'
  ),
  1,
  'retention dry-run does not delete package request-cache rows'
);

select is(
  (
    select count(*)::integer
    from public.lca_package_export_items
    where id = '98200000-0000-4000-8000-000000000402'
  ),
  1,
  'retention dry-run does not delete package export item rows'
);

create temporary table pg_temp.package_retention_apply_result as
select *
from util.apply_lca_package_retention(
  interval '30 days',
  interval '7 days',
  '2026-06-03 14:00:00+00'::timestamp with time zone,
  1000,
  false
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_result
    where retention_area = 'lca_package_artifacts'
  ),
  1,
  'retention apply marks one eligible package artifact metadata row'
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_result
    where retention_area = 'lca_package_request_cache'
  ),
  1,
  'retention apply deletes one stale package request-cache row'
);

select is(
  (
    select affected_count::integer
    from pg_temp.package_retention_apply_result
    where retention_area = 'lca_package_export_items'
  ),
  1,
  'retention apply deletes one package export item row'
);

select ok(
  (
    select status = 'deleted'
      and metadata->>'retentionAction' = 'package_metadata_retention_gc'
    from public.lca_package_artifacts
    where id = '98200000-0000-4000-8000-000000000304'
  ),
  'retention apply marks eligible artifacts deleted and records GC metadata'
);

select is(
  (
    select count(*)::integer
    from public.lca_package_request_cache
    where id = '98200000-0000-4000-8000-000000000404'
  ),
  0,
  'retention apply deletes eligible stale request-cache metadata'
);

select is(
  (
    select count(*)::integer
    from public.lca_package_export_items
    where id = '98200000-0000-4000-8000-000000000402'
  ),
  0,
  'retention apply deletes eligible export item metadata'
);

select is(
  (
    select status
    from public.lca_package_artifacts
    where id = '98200000-0000-4000-8000-000000000305'
  ),
  'ready',
  'retention apply preserves pinned package artifacts'
);

select ok(
  pg_temp.has_empty_search_path('util.apply_lca_package_retention(interval,interval,timestamp with time zone,integer,boolean)'),
  'package retention apply function pins an empty search_path'
);

create temporary table pg_temp.package_retention_apply_error_check (
  case_name text primary key,
  raised boolean not null
);

do $$
begin
  perform util.apply_lca_package_retention(
    interval '30 days',
    interval '7 days',
    '2026-06-03 14:00:00+00'::timestamp with time zone,
    0,
    true
  );
  insert into pg_temp.package_retention_apply_error_check values ('max-rows', false);
exception when invalid_parameter_value then
  insert into pg_temp.package_retention_apply_error_check values ('max-rows', true);
end
$$;

select is(
  (select raised from pg_temp.package_retention_apply_error_check where case_name = 'max-rows'),
  true,
  'retention apply rejects non-positive max row limits'
);

select * from finish();

rollback;
