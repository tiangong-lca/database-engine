begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(14);

select has_column(
  'public',
  'lca_results',
  'expires_at',
  'lca_results exposes a result retention deadline'
);

select col_type_is(
  'public',
  'lca_results',
  'expires_at',
  'timestamp with time zone',
  'lca_results.expires_at is timestamptz'
);

select col_not_null(
  'public',
  'lca_results',
  'expires_at',
  'lca_results.expires_at is required'
);

select col_has_default(
  'public',
  'lca_results',
  'expires_at',
  'lca_results.expires_at has a default retention deadline'
);

select has_column(
  'public',
  'lca_results',
  'is_pinned',
  'lca_results exposes a result GC pin flag'
);

select col_type_is(
  'public',
  'lca_results',
  'is_pinned',
  'boolean',
  'lca_results.is_pinned is boolean'
);

select col_not_null(
  'public',
  'lca_results',
  'is_pinned',
  'lca_results.is_pinned is required'
);

select col_has_default(
  'public',
  'lca_results',
  'is_pinned',
  'lca_results.is_pinned has a default value'
);

select ok(
  to_regclass('public.lca_results_expires_at_idx') is not null,
  'lca_results has an expires_at retention index'
);

select ok(
  exists (
    select 1
    from pg_index i
    join pg_class c on c.oid = i.indexrelid
    where c.oid = 'public.lca_results_expires_at_idx'::regclass
      and pg_get_indexdef(i.indexrelid) like '%expires_at%'
      and pg_get_indexdef(i.indexrelid) like '%created_at%'
      and pg_get_expr(i.indpred, i.indrelid) like '%is_pinned%'
      and pg_get_expr(i.indpred, i.indrelid) like '%false%'
  ),
  'lca_results expires_at index covers unpinned retention candidates'
);

select ok(
  to_regclass('public.lca_results_created_desc_idx') is not null,
  'lca_results has a created_at desc index for recent result lookups'
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
  '91620000-0000-4000-8000-000000000001',
  'full_library',
  '{}'::jsonb,
  'split_by_evidence_hybrid',
  'pgtap-lca-results-retention',
  'ready',
  now(),
  now()
);

insert into public.lca_jobs (
  id,
  job_type,
  snapshot_id,
  status,
  payload,
  diagnostics,
  requested_by,
  request_key,
  created_at,
  updated_at
) values (
  '91620000-0000-4000-8000-000000000002',
  'solve_one',
  '91620000-0000-4000-8000-000000000001',
  'completed',
  '{}'::jsonb,
  '{}'::jsonb,
  '91620000-0000-4000-8000-000000000101',
  'pgtap-lca-results-retention',
  now(),
  now()
);

insert into public.lca_results (
  id,
  job_id,
  snapshot_id,
  payload,
  diagnostics,
  artifact_url,
  artifact_byte_size,
  artifact_format
) values (
  '91620000-0000-4000-8000-000000000003',
  '91620000-0000-4000-8000-000000000002',
  '91620000-0000-4000-8000-000000000001',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/pgtap/result.h5',
  128,
  'hdf5:v1'
);

select is(
  (
    select is_pinned::text
    from public.lca_results
    where id = '91620000-0000-4000-8000-000000000003'
  ),
  'false',
  'new lca_results rows default to unpinned'
);

select ok(
  (
    select expires_at > created_at
    from public.lca_results
    where id = '91620000-0000-4000-8000-000000000003'
  ),
  'new lca_results rows default to a future retention deadline'
);

select ok(
  (
    select expires_at <= created_at + interval '31 days'
    from public.lca_results
    where id = '91620000-0000-4000-8000-000000000003'
  ),
  'default lca_results retention deadline is approximately 30 days'
);

select * from finish();
rollback;
