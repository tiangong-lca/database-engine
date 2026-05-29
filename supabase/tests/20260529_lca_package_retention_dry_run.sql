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

select plan(16);

select ok(
  to_regprocedure('util.preview_lca_package_retention(interval,interval,timestamp with time zone)') is not null,
  'package retention dry-run function exists'
);

select ok(
  has_function_privilege(
    'service_role',
    'util.preview_lca_package_retention(interval,interval,timestamp with time zone)',
    'EXECUTE'
  ),
  'service_role can execute package retention dry-run function'
);

insert into public.lca_package_jobs (
  id,
  job_type,
  status,
  payload,
  diagnostics,
  requested_by,
  created_at,
  updated_at,
  finished_at
) values
  (
    '91460000-0000-4000-8000-000000000001',
    'export_package',
    'ready',
    '{}'::jsonb,
    '{}'::jsonb,
    '91460000-0000-4000-8000-000000000100',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000002',
    'export_package',
    'queued',
    '{}'::jsonb,
    '{}'::jsonb,
    '91460000-0000-4000-8000-000000000100',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    null
  ),
  (
    '91460000-0000-4000-8000-000000000003',
    'export_package',
    'ready',
    '{}'::jsonb,
    '{}'::jsonb,
    '91460000-0000-4000-8000-000000000100',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000004',
    'export_package',
    'ready',
    '{}'::jsonb,
    '{}'::jsonb,
    '91460000-0000-4000-8000-000000000100',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000005',
    'export_package',
    'ready',
    '{}'::jsonb,
    '{}'::jsonb,
    '91460000-0000-4000-8000-000000000100',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  );

insert into public.lca_package_artifacts (
  id,
  job_id,
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
    '91460000-0000-4000-8000-000000000103',
    '91460000-0000-4000-8000-000000000003',
    'export_zip',
    'ready',
    'storage://package/missing-expiry.zip',
    null,
    10,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    null,
    false,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000104',
    '91460000-0000-4000-8000-000000000004',
    'export_zip',
    'ready',
    'storage://package/expired.zip',
    null,
    20,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2025-12-15 00:00:00+00',
    false,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000105',
    '91460000-0000-4000-8000-000000000005',
    'export_zip',
    'ready',
    'storage://package/recent-cache.zip',
    null,
    30,
    'tidas-package-zip:v1',
    'application/zip',
    '{}'::jsonb,
    '2025-12-15 00:00:00+00',
    false,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  );

insert into public.lca_package_export_items (
  id,
  job_id,
  table_name,
  dataset_id,
  version,
  is_seed,
  refs_done,
  created_at,
  updated_at
) values
  (
    '91460000-0000-4000-8000-000000000201',
    '91460000-0000-4000-8000-000000000001',
    'processes',
    '91460000-0000-4000-8000-000000000301',
    '000000001',
    false,
    true,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000202',
    '91460000-0000-4000-8000-000000000002',
    'processes',
    '91460000-0000-4000-8000-000000000302',
    '000000001',
    false,
    false,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000203',
    '91460000-0000-4000-8000-000000000003',
    'processes',
    '91460000-0000-4000-8000-000000000303',
    '000000001',
    false,
    true,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000204',
    '91460000-0000-4000-8000-000000000004',
    'processes',
    '91460000-0000-4000-8000-000000000304',
    '000000001',
    false,
    true,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  );

insert into public.lca_package_request_cache (
  id,
  requested_by,
  operation,
  request_key,
  request_payload,
  status,
  job_id,
  export_artifact_id,
  report_artifact_id,
  hit_count,
  last_accessed_at,
  created_at,
  updated_at
) values
  (
    '91460000-0000-4000-8000-000000000401',
    '91460000-0000-4000-8000-000000000100',
    'export_package',
    'old-ready-cache',
    '{}'::jsonb,
    'ready',
    null,
    null,
    null,
    2,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000402',
    '91460000-0000-4000-8000-000000000100',
    'export_package',
    'recent-ready-cache',
    '{}'::jsonb,
    'ready',
    null,
    null,
    null,
    3,
    '2026-01-30 00:00:00+00',
    '2026-01-30 00:00:00+00',
    '2026-01-30 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000403',
    '91460000-0000-4000-8000-000000000100',
    'export_package',
    'old-pending-cache',
    '{}'::jsonb,
    'pending',
    null,
    null,
    null,
    0,
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00',
    '2025-12-01 00:00:00+00'
  ),
  (
    '91460000-0000-4000-8000-000000000404',
    '91460000-0000-4000-8000-000000000100',
    'export_package',
    'recent-artifact-cache',
    '{}'::jsonb,
    'ready',
    '91460000-0000-4000-8000-000000000005',
    '91460000-0000-4000-8000-000000000105',
    null,
    1,
    '2026-01-30 00:00:00+00',
    '2026-01-30 00:00:00+00',
    '2026-01-30 00:00:00+00'
  );

create temporary table pg_temp.package_retention_preview as
select *
from util.preview_lca_package_retention(
  interval '30 days',
  interval '7 days',
  '2026-02-01 00:00:00+00'::timestamp with time zone
);

select is(
  (select count(distinct retention_area)::integer from pg_temp.package_retention_preview),
  4,
  'dry-run reports all package retention areas'
);

select ok(
  (
    select row_count >= 2
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_jobs'
      and is_eligible
      and reason = 'eligible_terminal_job_older_than_window'
  ),
  'old terminal jobs without protected artifacts are eligible'
);

select ok(
  (
    select row_count >= 1
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_jobs'
      and not is_eligible
      and reason = 'protected_artifact_not_expired'
  ),
  'jobs with attached missing-expiry artifacts are protected'
);

select ok(
  (
    select row_count >= 1
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_jobs'
      and not is_eligible
      and reason = 'protected_recent_request_cache_reference'
  ),
  'jobs with recently referenced artifacts are protected'
);

select ok(
  (
    select row_count >= 1
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_artifacts'
      and not is_eligible
      and reason = 'protected_missing_expires_at'
  ),
  'artifacts without expires_at are protected'
);

select ok(
  (
    select row_count >= 1
      and total_artifact_bytes >= 20
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_artifacts'
      and is_eligible
      and reason = 'eligible_expired_unpinned_artifact'
  ),
  'expired unpinned artifacts are eligible and report bytes'
);

select ok(
  (
    select row_count >= 1
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_artifacts'
      and not is_eligible
      and reason = 'protected_recent_request_cache_reference'
  ),
  'expired artifacts with recent request-cache references are protected'
);

select ok(
  (
    select row_count >= 1
      and total_hit_count >= 3
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_request_cache'
      and not is_eligible
      and reason = 'protected_recent_request_cache_access'
  ),
  'recent request-cache rows are protected and report hit counts'
);

select ok(
  (
    select row_count >= 1
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_request_cache'
      and is_eligible
      and reason = 'eligible_stale_request_cache'
  ),
  'old terminal request-cache rows are eligible'
);

select ok(
  (
    select row_count >= 2
    from pg_temp.package_retention_preview
    where retention_area = 'lca_package_export_items'
      and is_eligible
      and reason = 'eligible_parent_job_cascade'
  ),
  'export items follow eligible parent job lifecycle'
);

create temporary table pg_temp.package_retention_error_check (
  case_name text primary key,
  raised boolean not null
);

do $$
begin
  perform util.preview_lca_package_retention(
    interval '12 hours',
    interval '7 days',
    '2026-02-01 00:00:00+00'::timestamp with time zone
  );
  insert into pg_temp.package_retention_error_check values ('job-window', false);
exception when invalid_parameter_value then
  insert into pg_temp.package_retention_error_check values ('job-window', true);
end
$$;

do $$
begin
  perform util.preview_lca_package_retention(
    interval '30 days',
    interval '12 hours',
    '2026-02-01 00:00:00+00'::timestamp with time zone
  );
  insert into pg_temp.package_retention_error_check values ('cache-window', false);
exception when invalid_parameter_value then
  insert into pg_temp.package_retention_error_check values ('cache-window', true);
end
$$;

select is(
  (select raised from pg_temp.package_retention_error_check where case_name = 'job-window'),
  true,
  'dry-run rejects package job retention windows shorter than one day'
);

select is(
  (select raised from pg_temp.package_retention_error_check where case_name = 'cache-window'),
  true,
  'dry-run rejects request-cache retention windows shorter than one day'
);

select ok(
  pg_temp.has_empty_search_path('util.preview_lca_package_retention(interval,interval,timestamp with time zone)'),
  'package retention dry-run function pins an empty search_path'
);

select ok(
  lower(pg_get_functiondef('util.preview_lca_package_retention(interval,interval,timestamp with time zone)'::regprocedure)) not like '%delete from public.lca_package%'
  and lower(pg_get_functiondef('util.preview_lca_package_retention(interval,interval,timestamp with time zone)'::regprocedure)) not like '%update public.lca_package%'
  and lower(pg_get_functiondef('util.preview_lca_package_retention(interval,interval,timestamp with time zone)'::regprocedure)) not like '%insert into public.lca_package%',
  'dry-run function does not contain destructive package DML'
);

select * from finish();

rollback;
