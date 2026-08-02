begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(16);

select has_function(
  'api', 'cmd_lca_reconcile_result_cache_v1', array['uuid', 'uuid']
);

select ok(
  (
    select proc.prolang = (select oid from pg_language where lanname = 'sql')
      and not proc.prosecdef
      and proc.proowner = 'postgres'::regrole
      and proc.proconfig = array['search_path=""']::text[]
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    where proc_schema.nspname = 'api'
      and proc.proname = 'cmd_lca_reconcile_result_cache_v1'
      and pg_get_function_identity_arguments(proc.oid) =
        'p_requested_by uuid, p_cache_id uuid'
  ),
  'reconcile keeps the exact SQL SECURITY INVOKER empty-search-path signature'
);

select ok(
  (
    select count(*) = 1
      and bool_and(role_name.rolname = 'service_role')
      and bool_and(not acl.is_grantable)
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    cross join lateral aclexplode(proc.proacl) as acl
    join pg_roles as role_name on role_name.oid = acl.grantee
    where proc_schema.nspname = 'api'
      and proc.proname = 'cmd_lca_reconcile_result_cache_v1'
      and acl.privilege_type = 'EXECUTE'
      and acl.grantee <> proc.proowner
  ),
  'reconcile direct non-owner ACL is exactly service_role without grant option'
);

select ok(
  has_function_privilege(
    'service_role',
    'api.cmd_lca_reconcile_result_cache_v1(uuid,uuid)',
    'EXECUTE'
  ),
  'service_role can execute reconcile'
);
select ok(
  not exists (
    select 1
    from pg_proc as proc
    join pg_namespace as proc_schema on proc_schema.oid = proc.pronamespace
    cross join lateral aclexplode(proc.proacl) as acl
    where proc_schema.nspname = 'api'
      and proc.proname = 'cmd_lca_reconcile_result_cache_v1'
      and acl.grantee = 0
      and acl.privilege_type = 'EXECUTE'
  ),
  'PUBLIC cannot execute reconcile'
);
select ok(
  not has_function_privilege(
    'anon', 'api.cmd_lca_reconcile_result_cache_v1(uuid,uuid)', 'EXECUTE'
  ),
  'anon cannot execute reconcile'
);
select ok(
  not has_function_privilege(
    'authenticated',
    'api.cmd_lca_reconcile_result_cache_v1(uuid,uuid)',
    'EXECUTE'
  ),
  'authenticated cannot execute reconcile'
);
select ok(
  not has_function_privilege(
    'api_internal_executor',
    'api.cmd_lca_reconcile_result_cache_v1(uuid,uuid)',
    'EXECUTE'
  ),
  'api_internal_executor cannot execute reconcile'
);

insert into public.lca_network_snapshots (
  id, scope, process_filter, provider_matching_rule, source_hash, status
) values (
  '98395000-0000-4000-8000-000000000010',
  'full_library',
  '{}'::jsonb,
  'split_by_evidence_hybrid',
  'issue-395-reconcile-cancelled',
  'ready'
);

create temporary table issue_395_statuses (
  worker_status text primary key,
  ordinal integer not null,
  worker_job_id uuid not null,
  legacy_job_id uuid not null,
  cache_id uuid not null,
  expected_cache_status text not null,
  expected_code text not null
) on commit drop;
grant all on issue_395_statuses to public;

insert into issue_395_statuses values
  ('queued',    1, '98395000-0000-4000-8000-000000000101', '98395000-0000-4000-8000-000000000201', '98395000-0000-4000-8000-000000000301', 'pending', 'reconciled'),
  ('running',   2, '98395000-0000-4000-8000-000000000102', '98395000-0000-4000-8000-000000000202', '98395000-0000-4000-8000-000000000302', 'pending', 'reconciled'),
  ('waiting',   3, '98395000-0000-4000-8000-000000000103', '98395000-0000-4000-8000-000000000203', '98395000-0000-4000-8000-000000000303', 'pending', 'reconciled'),
  ('completed', 4, '98395000-0000-4000-8000-000000000104', '98395000-0000-4000-8000-000000000204', '98395000-0000-4000-8000-000000000304', 'pending', 'result_pending'),
  ('blocked',   5, '98395000-0000-4000-8000-000000000105', '98395000-0000-4000-8000-000000000205', '98395000-0000-4000-8000-000000000305', 'pending', 'reconciled'),
  ('stale',     6, '98395000-0000-4000-8000-000000000106', '98395000-0000-4000-8000-000000000206', '98395000-0000-4000-8000-000000000306', 'failed',  'reconciled'),
  ('failed',    7, '98395000-0000-4000-8000-000000000107', '98395000-0000-4000-8000-000000000207', '98395000-0000-4000-8000-000000000307', 'failed',  'reconciled'),
  ('cancelled', 8, '98395000-0000-4000-8000-000000000108', '98395000-0000-4000-8000-000000000208', '98395000-0000-4000-8000-000000000308', 'failed',  'reconciled');

insert into private.worker_jobs (
  id, job_kind, worker_queue, subject_type, subject_id, subject_version,
  requester_type, requested_by, status, payload_schema_version, payload_json,
  result_schema_version, result_json, finished_at, cancelled_at,
  blocker_codes, resolution_scope
)
select worker_job_id, 'lca.solve_one', 'solver', 'lca_job', legacy_job_id,
       '98395000-0000-4000-8000-000000000010', 'user',
       '98395000-0000-4000-8000-000000000001', worker_status,
       'lca.solve_one.request.v1',
       jsonb_build_object(
         'type', 'solve_one',
         'job_id', legacy_job_id,
         'snapshot_id', '98395000-0000-4000-8000-000000000010'
       ),
       case when worker_status = 'completed' then 'lca.solve.result.v1' end,
       case when worker_status = 'completed' then '{"ok":true}'::jsonb end,
       case when worker_status in ('completed', 'failed', 'cancelled') then now() end,
       case when worker_status = 'cancelled' then now() end,
       case when worker_status = 'blocked' then array['fixture_blocked']::text[]
         else '{}'::text[] end,
       case when worker_status = 'blocked' then 'user' end
from issue_395_statuses;

insert into public.lca_results (
  id, job_id, worker_job_id, snapshot_id, payload, diagnostics,
  artifact_url, artifact_sha256, artifact_byte_size, artifact_format
) values (
  '98395000-0000-4000-8000-000000000408',
  '98395000-0000-4000-8000-000000000208',
  '98395000-0000-4000-8000-000000000108',
  '98395000-0000-4000-8000-000000000010',
  '{}'::jsonb,
  '{}'::jsonb,
  'storage://lca_results/issue-395/cancelled-preserved.h5',
  repeat('8', 64),
  395,
  'hdf5:v1'
);

insert into public.lca_result_cache (
  id, scope, snapshot_id, request_key, request_payload, status,
  job_id, worker_job_id, result_id, error_code, error_message, hit_count
)
select cache_id, 'prod', '98395000-0000-4000-8000-000000000010',
       'issue-395-' || worker_status, '{}'::jsonb, 'pending',
       legacy_job_id, worker_job_id,
       case when worker_status = 'cancelled'
         then '98395000-0000-4000-8000-000000000408'::uuid end,
       case when worker_status = 'cancelled' then 'old-code' end,
       case when worker_status = 'cancelled' then 'old-message' end,
       0
from issue_395_statuses;

create temporary table issue_395_cancelled_before as
select to_jsonb(cache_row) as row_state
from public.lca_result_cache as cache_row
where id = '98395000-0000-4000-8000-000000000308';
grant all on issue_395_cancelled_before to public;

set local role service_role;
select set_config('request.jwt.claim.role', 'service_role', true);
select set_config('request.jwt.claims', '{"role":"service_role"}', true);

select is(
  api.cmd_lca_reconcile_result_cache_v1(
    '98395000-0000-4000-8000-000000000099',
    '98395000-0000-4000-8000-000000000308'
  ),
  '{"ok":true,"code":"job_not_found","data":null}'::jsonb,
  'foreign cancelled reconcile returns the exact non-disclosing contract'
);

select is(
  (
    select to_jsonb(cache_row)
    from public.lca_result_cache as cache_row
    where id = '98395000-0000-4000-8000-000000000308'
  ),
  (select row_state from issue_395_cancelled_before),
  'foreign cancelled reconcile has no status, identity, count, error, or timestamp side effect'
);

create temporary table issue_395_responses (
  worker_status text primary key,
  response jsonb not null
) on commit drop;
grant all on issue_395_responses to public;

insert into issue_395_responses
select fixture.worker_status,
       api.cmd_lca_reconcile_result_cache_v1(
         '98395000-0000-4000-8000-000000000001', fixture.cache_id
       )
from issue_395_statuses as fixture
order by fixture.ordinal;

select is(
  (
    select string_agg(
      worker_status || ':' || (responses.response->>'code') || ':' ||
      (responses.response #>> '{data,workerStatus}') || ':' ||
      (responses.response #>> '{data,cache,status}'),
      ',' order by fixture.ordinal
    )
    from issue_395_responses as responses
    join issue_395_statuses as fixture using (worker_status)
  ),
  'queued:reconciled:queued:pending,running:reconciled:running:pending,waiting:reconciled:waiting:pending,completed:result_pending:completed:pending,blocked:reconciled:blocked:pending,stale:reconciled:stale:failed,failed:reconciled:failed:failed,cancelled:reconciled:cancelled:failed',
  'all eight frozen Worker states retain their prior mapping and cancelled maps to failed'
);

select ok(
  (
    select bool_and(
      cache_row.job_id = fixture.legacy_job_id
      and cache_row.worker_job_id = fixture.worker_job_id
      and cache_row.result_id is not distinct from
        case when fixture.worker_status = 'cancelled'
          then '98395000-0000-4000-8000-000000000408'::uuid end
    )
    from issue_395_statuses as fixture
    join public.lca_result_cache as cache_row on cache_row.id = fixture.cache_id
  ),
  'reconcile preserves legacy job, Worker job, and result identity for every status'
);

select ok(
  (
    select bool_and(
      cache_row.status = fixture.expected_cache_status
      and cache_row.hit_count = 1
    )
    from issue_395_statuses as fixture
    join public.lca_result_cache as cache_row on cache_row.id = fixture.cache_id
  ),
  'each successful reconcile persists its expected status and exactly one hit'
);

reset role;
insert into private.worker_jobs (
  id, job_kind, worker_queue, subject_type, subject_id, subject_version,
  requester_type, requested_by, status, payload_schema_version, payload_json
) values (
  '98395000-0000-4000-8000-000000000109', 'lca.solve_one', 'solver',
  'lca_job', '98395000-0000-4000-8000-000000000209',
  '98395000-0000-4000-8000-000000000010', 'user',
  '98395000-0000-4000-8000-000000000001', 'queued',
  'lca.solve_one.request.v1',
  '{"type":"solve_one","job_id":"98395000-0000-4000-8000-000000000209","snapshot_id":"98395000-0000-4000-8000-000000000010"}'::jsonb
);
set local role service_role;

select is(
  api.cmd_lca_admit_result_cache_v1(
    'prod',
    '98395000-0000-4000-8000-000000000010',
    'issue-395-cancelled',
    '{"retry":true}'::jsonb,
    '98395000-0000-4000-8000-000000000209',
    '98395000-0000-4000-8000-000000000109',
    false
  )->>'outcome',
  'accepted',
  'a cancelled job first reconciles to failed and then admits a retry'
);

select is(
  (
    select concat_ws(
      ':', status, job_id::text, worker_job_id::text,
      coalesce(result_id::text, 'null'), coalesce(error_code, 'null'),
      coalesce(error_message, 'null'), hit_count::text
    )
    from public.lca_result_cache
    where id = '98395000-0000-4000-8000-000000000308'
  ),
  'pending:98395000-0000-4000-8000-000000000209:98395000-0000-4000-8000-000000000109:null:null:null:2',
  'retry admission atomically rebinds, clears result/error state, and adds exactly one hit'
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
  ),
  8,
  'the forward replacement adds no ninth query or overload'
);

select * from finish();
rollback;
