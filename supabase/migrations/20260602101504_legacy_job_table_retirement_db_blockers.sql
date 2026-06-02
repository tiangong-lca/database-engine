-- Remove database-level DROP RESTRICT blockers for legacy worker job tables.
--
-- This does not drop public.lca_jobs, public.lca_package_jobs, or
-- public.dataset_review_submit_jobs. Runtime cutover is handled by Edge and
-- worker follow-up work. This migration only makes retained result/cache/
-- artifact/coordinator data independent from those legacy task tables at the
-- database dependency layer.

alter table if exists public.lca_factorization_registry
  drop constraint if exists lca_factorization_registry_prepared_job_fk;

alter table if exists public.lca_latest_all_unit_results
  drop constraint if exists lca_latest_all_unit_results_job_fk;

alter table if exists public.lca_result_cache
  drop constraint if exists lca_result_cache_job_fk;

alter table if exists public.lca_results
  drop constraint if exists lca_results_job_fk;

alter table if exists public.lca_package_artifacts
  drop constraint if exists lca_package_artifacts_job_fk;

alter table if exists public.lca_package_export_items
  drop constraint if exists lca_package_export_items_job_fk;

alter table if exists public.lca_package_request_cache
  drop constraint if exists lca_package_request_cache_job_fk;

comment on column public.lca_results.job_id
  is 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';
comment on column public.lca_result_cache.job_id
  is 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';
comment on column public.lca_latest_all_unit_results.job_id
  is 'Historical legacy LCA job identifier retained for compatibility. New canonical task identity is worker_job_id.';
comment on column public.lca_factorization_registry.prepared_job_id
  is 'Historical legacy LCA preparation job identifier retained for compatibility. New canonical task identity is prepared_worker_job_id.';
comment on column public.lca_package_artifacts.job_id
  is 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';
comment on column public.lca_package_export_items.job_id
  is 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';
comment on column public.lca_package_request_cache.job_id
  is 'Historical legacy package job identifier retained for compatibility. New canonical task identity is worker_job_id.';

drop policy if exists lca_results_select_own on public.lca_results;
create policy lca_results_select_own
on public.lca_results
for select
to authenticated
using (
  exists (
    select 1
    from public.worker_jobs as worker_job
    where worker_job.id = lca_results.worker_job_id
      and worker_job.requested_by = (select auth.uid())
  )
);

drop policy if exists lca_package_artifacts_select_own on public.lca_package_artifacts;
create policy lca_package_artifacts_select_own
on public.lca_package_artifacts
for select
to authenticated
using (
  exists (
    select 1
    from public.worker_jobs as worker_job
    where worker_job.id = lca_package_artifacts.worker_job_id
      and worker_job.requested_by = (select auth.uid())
  )
);

drop function if exists public.cmd_dataset_review_submit_job_payload(public.dataset_review_submit_jobs);

create or replace function public.cmd_dataset_review_submit_job_payload(
  p_job anyelement
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  with job as (
    select to_jsonb(p_job) as row_json
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', row_json->>'status',
      'reviewSubmitJobId', row_json->'id',
      'submitWorkerJobId', row_json->'submit_worker_job_id',
      'gateRunId', row_json->'gate_run_id',
      'gateWorkerJobId', row_json->'gate_worker_job_id',
      'datasetRevision', jsonb_build_object(
        'table', row_json->>'dataset_table',
        'id', row_json->'dataset_id',
        'version', row_json->>'dataset_version',
        'revisionChecksum', row_json->>'revision_checksum'
      ),
      'policy', jsonb_build_object(
        'profile', row_json->>'policy_profile',
        'reportSchemaVersion', row_json->>'report_schema_version'
      ),
      'requestedBy', row_json->'requested_by',
      'attemptCount', row_json->'attempt_count',
      'error',
        case
          when row_json->>'last_error_code' is null
            and row_json->>'last_error_message' is null
            and row_json->'last_error_details' is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'code', row_json->>'last_error_code',
              'message', row_json->>'last_error_message',
              'details', row_json->'last_error_details'
            )
          )
        end,
      'result', row_json->'result',
      'submitWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'submit_worker_job_id', '')::uuid
        ),
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = nullif(row_json->>'gate_run_id', '')::uuid
        ),
      'gateWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'gate_worker_job_id', '')::uuid
        ),
      'createdAt', row_json->'created_at',
      'modifiedAt', row_json->'modified_at',
      'completedAt', row_json->'completed_at'
    )
  )
  from job
$$;

drop view if exists public.worker_job_domain_refs;

create view public.worker_job_domain_refs
with (security_invoker = true)
as
select
  worker_job_id,
  'lca_results'::text as domain_source,
  id as domain_id,
  'lca_result_artifact'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  created_at as updated_at
from public.lca_results
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_result_cache'::text as domain_source,
  id as domain_id,
  'lca_result_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_result_cache
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_latest_all_unit_results'::text as domain_source,
  id as domain_id,
  'lca_latest_all_unit_result'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_latest_all_unit_results
where worker_job_id is not null
union all
select
  prepared_worker_job_id as worker_job_id,
  'lca_factorization_registry'::text as domain_source,
  id as domain_id,
  'lca_factorization_artifact'::text as domain_role,
  prepared_job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_factorization_registry
where prepared_worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_artifacts'::text as domain_source,
  id as domain_id,
  'package_artifact'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_artifacts
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_export_items'::text as domain_source,
  id as domain_id,
  'package_export_item'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  updated_at
from public.lca_package_export_items
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_request_cache'::text as domain_source,
  id as domain_id,
  'package_request_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_request_cache
where worker_job_id is not null
union all
select
  worker_job_id,
  'dataset_review_submit_gate_runs'::text as domain_source,
  id as domain_id,
  'review_submit_gate_report'::text as domain_role,
  null::uuid as legacy_job_id,
  status,
  created_at,
  modified_at as updated_at
from public.dataset_review_submit_gate_runs
where worker_job_id is not null;

revoke all on public.worker_job_domain_refs from public, anon, authenticated;
grant select on public.worker_job_domain_refs to service_role;

comment on view public.worker_job_domain_refs
  is 'Service-role projection from canonical worker_jobs to retained non-legacy-table domain artifact/cache/history rows. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';

drop view if exists public.worker_legacy_lifecycle_audit;

create view public.worker_legacy_lifecycle_audit
with (security_invoker = true)
as
select
  'worker_jobs'::text as legacy_source,
  job_kind::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running', 'waiting', 'stale', 'blocked'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(updated_at) as latest_updated_at
from public.worker_jobs
where worker_runtime = 'calculator'
  and job_kind in (
    'lca.solve_one',
    'lca.solve_batch',
    'lca.solve_all_unit',
    'lca.build_snapshot',
    'lca.contribution_path',
    'lca.factorization_prepare',
    'lca.snapshot_gc',
    'lca.result_gc',
    'tidas.package_artifact_gc',
    'tidas.export_package',
    'tidas.import_package',
    'review_submit.submit',
    'review_submit.gate'
  )
group by job_kind, status
union all
select
  'dataset_review_submit_gate_runs'::text as legacy_source,
  'review_submit.gate'::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(modified_at) as latest_updated_at
from public.dataset_review_submit_gate_runs
group by status;

revoke all on public.worker_legacy_lifecycle_audit from public, anon, authenticated;
grant select on public.worker_legacy_lifecycle_audit to service_role;

comment on view public.worker_legacy_lifecycle_audit
  is 'Service-role lifecycle audit for canonical worker_jobs and retained gate reports. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';

create or replace view public.worker_legacy_table_retirement_blockers
with (security_invoker = true)
as
with legacy_targets as (
  select
    target_namespace.nspname as legacy_schema,
    target_class.relname as legacy_table,
    target_class.oid as table_oid,
    target_class.reltype as row_type_oid
  from (
    values
      ('public'::name, 'lca_jobs'::name),
      ('public'::name, 'lca_package_jobs'::name),
      ('public'::name, 'dataset_review_submit_jobs'::name)
  ) as targets(schema_name, table_name)
  join pg_namespace as target_namespace
    on target_namespace.nspname = targets.schema_name
  join pg_class as target_class
    on target_class.relnamespace = target_namespace.oid
   and target_class.relname = targets.table_name
   and target_class.relkind in ('r', 'p')
),
foreign_key_blockers as (
  select
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'foreign_key'::text as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    constraint_record.conname::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'constraintName', constraint_record.conname,
      'dependentTable', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'dependentColumns',
        (
          select jsonb_agg(dependent_attribute.attname order by dependent_attribute.attnum)
          from unnest(constraint_record.conkey) as constraint_column(attnum)
          join pg_attribute as dependent_attribute
            on dependent_attribute.attrelid = constraint_record.conrelid
           and dependent_attribute.attnum = constraint_column.attnum
        ),
      'referencedColumns',
        (
          select jsonb_agg(referenced_attribute.attname order by referenced_attribute.attnum)
          from unnest(constraint_record.confkey) as referenced_column(attnum)
          join pg_attribute as referenced_attribute
            on referenced_attribute.attrelid = constraint_record.confrelid
           and referenced_attribute.attnum = referenced_column.attnum
        ),
      'onDelete', constraint_record.confdeltype
    ) as details
  from legacy_targets
  join pg_constraint as constraint_record
    on constraint_record.confrelid = legacy_targets.table_oid
   and constraint_record.contype = 'f'
  join pg_class as dependent_class
    on dependent_class.oid = constraint_record.conrelid
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where constraint_record.conrelid <> legacy_targets.table_oid
),
view_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    case dependent_class.relkind
      when 'm' then 'dependent_materialized_view'
      else 'dependent_view'
    end as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    concat(dependent_namespace.nspname, '.', dependent_class.relname)::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'dependentView', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'relkind', dependent_class.relkind
    ) as details
  from legacy_targets
  join pg_depend as dependency
    on dependency.refobjid = legacy_targets.table_oid
  join pg_rewrite as rewrite_rule
    on rewrite_rule.oid = dependency.objid
  join pg_class as dependent_class
    on dependent_class.oid = rewrite_rule.ev_class
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where dependent_class.oid <> legacy_targets.table_oid
    and dependent_class.relkind in ('v', 'm')
),
policy_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'policy'::text as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    concat(dependent_namespace.nspname, '.', dependent_class.relname, '.', policy_record.polname)::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'policyName', policy_record.polname,
      'dependentTable', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'command', policy_record.polcmd
    ) as details
  from legacy_targets
  join pg_depend as dependency
    on dependency.refobjid = legacy_targets.table_oid
  join pg_policy as policy_record
    on policy_record.oid = dependency.objid
  join pg_class as dependent_class
    on dependent_class.oid = policy_record.polrelid
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where dependent_class.oid <> legacy_targets.table_oid
),
function_signature_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_signature'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(pg_get_function_arguments(function_record.oid)) like '%' || lower(legacy_table) || '%'
    or lower(pg_get_function_result(function_record.oid)) like '%' || lower(legacy_table) || '%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
),
function_source_references as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_source_reference'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    false as is_drop_restrict_blocker,
    jsonb_build_object(
      'reason', 'Function source text references the legacy table name; this may not block DROP TABLE RESTRICT, but it is a runtime migration blocker.',
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(pg_get_functiondef(function_record.oid)) like '%' || lower(legacy_table) || '%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
  where function_namespace.nspname not in ('pg_catalog', 'information_schema')
)
select *
from foreign_key_blockers
union all
select *
from view_blockers
union all
select *
from policy_blockers
union all
select *
from function_signature_blockers
union all
select *
from function_source_references;

revoke all on public.worker_legacy_table_retirement_blockers from public, anon, authenticated;
grant select on public.worker_legacy_table_retirement_blockers to service_role;

comment on view public.worker_legacy_table_retirement_blockers
  is 'Service-role audit view for DROP TABLE RESTRICT blockers, including FK, view, policy, function signature, and runtime function-source references that must be resolved before retiring legacy worker job tables.';

revoke all on function public.cmd_dataset_review_submit_job_payload(anyelement) from public, anon, authenticated;
grant execute on function public.cmd_dataset_review_submit_job_payload(anyelement) to service_role;
