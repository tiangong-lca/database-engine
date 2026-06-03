-- Clarify the post-cutover contract for worker-produced domain artifact/state
-- rows. worker_jobs is the canonical task lifecycle table; the tables below
-- are retained domain artifacts, caches, reports, and coordinator state.

create schema if not exists util;

comment on table public.lca_package_artifacts
  is 'Worker-produced TIDAS package artifact metadata. This is a domain artifact table, not a task lifecycle/job table; canonical execution state lives in public.worker_jobs.';
comment on column public.lca_package_artifacts.job_id
  is 'Retained legacy lca_package_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_package_artifacts.worker_job_id
  is 'Canonical worker_jobs task that produced or owns this package artifact metadata row.';
comment on column public.lca_package_artifacts.status
  is 'Domain artifact metadata status. deleted means the metadata row has been GC-marked after object cleanup or operator-approved retention handling.';

comment on table public.lca_package_export_items
  is 'Worker-produced package export item state used to resume or inspect TIDAS package export/import domain work. This is not a task lifecycle/job table.';
comment on column public.lca_package_export_items.job_id
  is 'Retained legacy lca_package_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_package_export_items.worker_job_id
  is 'Canonical worker_jobs task that discovered, exported, or imported this package item row.';

comment on table public.lca_package_request_cache
  is 'Request-level cache for package import/export commands. This is domain cache state backed by worker_jobs, not a task lifecycle/job table.';
comment on column public.lca_package_request_cache.job_id
  is 'Retained legacy lca_package_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_package_request_cache.worker_job_id
  is 'Canonical worker_jobs task currently backing this package request cache row.';

comment on table public.lca_results
  is 'Worker-produced LCA result artifact metadata. This is domain result state, not a task lifecycle/job table.';
comment on column public.lca_results.job_id
  is 'Retained legacy lca_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_results.worker_job_id
  is 'Canonical worker_jobs task that produced this LCA result artifact.';

comment on table public.lca_result_cache
  is 'Request-level cache for LCA result lookups. This is domain cache state backed by worker_jobs, not a task lifecycle/job table.';
comment on column public.lca_result_cache.job_id
  is 'Retained legacy lca_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_result_cache.worker_job_id
  is 'Canonical worker_jobs task currently backing this LCA result cache row.';

comment on table public.lca_latest_all_unit_results
  is 'Latest all-unit LCA result pointer and query artifact metadata. This is domain projection state, not a task lifecycle/job table.';
comment on column public.lca_latest_all_unit_results.job_id
  is 'Retained legacy lca_jobs identifier for historical compatibility only. New lifecycle traceability uses worker_job_id.';
comment on column public.lca_latest_all_unit_results.worker_job_id
  is 'Canonical worker_jobs task that produced the latest all-unit result artifact.';

comment on table public.lca_factorization_registry
  is 'Domain registry for prepared LCA factorization artifacts and leases. Canonical preparation task lifecycle lives in public.worker_jobs.';
comment on column public.lca_factorization_registry.prepared_job_id
  is 'Retained legacy lca_jobs identifier for historical compatibility only. New lifecycle traceability uses prepared_worker_job_id.';
comment on column public.lca_factorization_registry.prepared_worker_job_id
  is 'Canonical worker_jobs task that prepared this factorization artifact.';

comment on table public.lca_network_snapshots
  is 'LCA network snapshot domain cache/state. Snapshot rows are not task lifecycle rows; current direct worker traceability is through downstream worker-linked result/cache/latest/factorization records and source_hash.';

comment on table public.dataset_review_submit_requests
  is 'Review-submit coordinator domain state that replaces dataset_review_submit_jobs. The root submit and child gate lifecycles are canonicalized in public.worker_jobs.';
comment on column public.dataset_review_submit_requests.submit_worker_job_id
  is 'Canonical root review_submit.submit worker_jobs task for this review-submit coordinator row.';
comment on column public.dataset_review_submit_requests.gate_worker_job_id
  is 'Canonical child review_submit.gate worker_jobs task when a numeric-stability gate has been enqueued for this coordinator row.';

comment on table public.dataset_review_submit_gate_runs
  is 'Review-submit gate report/history table. This is retained domain report state; canonical gate execution lifecycle is public.worker_jobs.';
comment on column public.dataset_review_submit_gate_runs.worker_job_id
  is 'Canonical review_submit.gate worker_jobs execution that produced or owns this retained gate report row.';

comment on table archive.worker_legacy_job_table_rows
  is 'Manual rollback archive for retired legacy job table rows. Retained without automatic TTL deletion until explicit operator signoff because it is the last DB-local restore source for public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs.';
comment on column archive.worker_legacy_job_table_rows.archived_at
  is 'Timestamp when the legacy row payload was archived before physical DROP TABLE. Retention is manual/signoff based, not time-based automatic deletion.';
comment on column archive.worker_legacy_job_table_rows.row_payload
  is 'Full to_jsonb(row) payload captured before public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs were dropped.';

drop view if exists public.worker_domain_traceability_violations;
drop view if exists public.worker_domain_traceability_cutoffs;

create view public.worker_domain_traceability_cutoffs
with (security_invoker = true)
as
select *
from (
  values
    (
      'lca_results'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new LCA result artifacts must be linked to the worker_jobs task that produced them'::text
    ),
    (
      'lca_result_cache'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new LCA result-cache rows must be linked to their backing worker_jobs task'::text
    ),
    (
      'lca_latest_all_unit_results'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new latest all-unit projections must be linked to the worker_jobs task that produced them'::text
    ),
    (
      'lca_factorization_registry'::text,
      'prepared_worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'prepared factorization rows must be linked to the worker_jobs task that prepared them'::text
    ),
    (
      'lca_package_artifacts'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new package artifact metadata must be linked to the worker_jobs task that produced it'::text
    ),
    (
      'lca_package_export_items'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new package export/import item state must be linked to the worker_jobs task that produced it'::text
    ),
    (
      'lca_package_request_cache'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new package request-cache rows must be linked to their backing worker_jobs task'::text
    ),
    (
      'dataset_review_submit_requests'::text,
      'submit_worker_job_id,gate_worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new review-submit coordinator rows must be linked to root submit and gate worker_jobs tasks once they enter the worker flow'::text
    ),
    (
      'dataset_review_submit_gate_runs'::text,
      'worker_job_id'::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      true,
      'new review-submit gate reports must be linked to their gate worker_jobs task'::text
    ),
    (
      'lca_network_snapshots'::text,
      null::text,
      timestamp with time zone '2026-06-03 13:35:00+00',
      false,
      'snapshot rows are domain cache identity and are currently traced through downstream worker-linked result/cache/latest/factorization rows'::text
    )
) as cutoffs(domain_source, required_worker_column, cutover_at, traceability_required, contract_note);

revoke all on public.worker_domain_traceability_cutoffs from public, anon, authenticated;
grant select on public.worker_domain_traceability_cutoffs to service_role;

comment on view public.worker_domain_traceability_cutoffs
  is 'Service-role audit contract listing when new worker-produced domain rows must carry a worker_jobs reference. Historical pre-cutover rows are allowed to remain nullable.';

create view public.worker_domain_traceability_violations
with (security_invoker = true)
as
select
  'lca_results'::text as domain_source,
  results.id as domain_id,
  'lca_result_artifact'::text as domain_role,
  results.created_at,
  results.created_at as updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text as violation_code,
  jsonb_build_object('legacyJobId', results.job_id, 'snapshotId', results.snapshot_id) as details
from public.lca_results as results
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_results'
where cutoffs.traceability_required
  and results.created_at >= cutoffs.cutover_at
  and results.worker_job_id is null
union all
select
  'lca_result_cache'::text,
  cache.id,
  'lca_result_cache'::text,
  cache.created_at,
  cache.updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('legacyJobId', cache.job_id, 'snapshotId', cache.snapshot_id, 'status', cache.status)
from public.lca_result_cache as cache
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_result_cache'
where cutoffs.traceability_required
  and cache.created_at >= cutoffs.cutover_at
  and cache.worker_job_id is null
union all
select
  'lca_latest_all_unit_results'::text,
  latest.id,
  'lca_latest_all_unit_result'::text,
  latest.created_at,
  latest.updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('legacyJobId', latest.job_id, 'snapshotId', latest.snapshot_id, 'status', latest.status)
from public.lca_latest_all_unit_results as latest
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_latest_all_unit_results'
where cutoffs.traceability_required
  and latest.created_at >= cutoffs.cutover_at
  and latest.worker_job_id is null
union all
select
  'lca_factorization_registry'::text,
  registry.id,
  'lca_factorization_artifact'::text,
  registry.created_at,
  registry.updated_at,
  cutoffs.cutover_at,
  'missing_prepared_worker_job_id'::text,
  jsonb_build_object('legacyPreparedJobId', registry.prepared_job_id, 'snapshotId', registry.snapshot_id, 'status', registry.status)
from public.lca_factorization_registry as registry
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_factorization_registry'
where cutoffs.traceability_required
  and coalesce(registry.prepared_at, registry.created_at) >= cutoffs.cutover_at
  and registry.status in ('ready', 'failed', 'stale')
  and registry.prepared_worker_job_id is null
union all
select
  'lca_package_artifacts'::text,
  artifacts.id,
  'package_artifact'::text,
  artifacts.created_at,
  artifacts.updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('legacyJobId', artifacts.job_id, 'artifactKind', artifacts.artifact_kind, 'status', artifacts.status)
from public.lca_package_artifacts as artifacts
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_package_artifacts'
where cutoffs.traceability_required
  and artifacts.created_at >= cutoffs.cutover_at
  and artifacts.worker_job_id is null
union all
select
  'lca_package_export_items'::text,
  export_items.id,
  'package_export_item'::text,
  export_items.created_at,
  export_items.updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('legacyJobId', export_items.job_id, 'tableName', export_items.table_name, 'datasetId', export_items.dataset_id)
from public.lca_package_export_items as export_items
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_package_export_items'
where cutoffs.traceability_required
  and export_items.created_at >= cutoffs.cutover_at
  and export_items.worker_job_id is null
union all
select
  'lca_package_request_cache'::text,
  request_cache.id,
  'package_request_cache'::text,
  request_cache.created_at,
  request_cache.updated_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('legacyJobId', request_cache.job_id, 'operation', request_cache.operation, 'status', request_cache.status)
from public.lca_package_request_cache as request_cache
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'lca_package_request_cache'
where cutoffs.traceability_required
  and request_cache.created_at >= cutoffs.cutover_at
  and request_cache.worker_job_id is null
union all
select
  'dataset_review_submit_requests'::text,
  requests.id,
  'review_submit_coordinator'::text,
  requests.created_at,
  requests.modified_at,
  cutoffs.cutover_at,
  missing.violation_code,
  jsonb_build_object(
    'status', requests.status,
    'gateRunId', requests.gate_run_id,
    'submitWorkerJobId', requests.submit_worker_job_id,
    'gateWorkerJobId', requests.gate_worker_job_id
  )
from public.dataset_review_submit_requests as requests
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'dataset_review_submit_requests'
cross join lateral (
  values
    ('missing_submit_worker_job_id'::text, requests.submit_worker_job_id, true),
    (
      'missing_gate_worker_job_id'::text,
      requests.gate_worker_job_id,
      requests.gate_run_id is not null
        or requests.status in ('waiting_gate', 'submitting', 'submitted', 'blocked')
    )
) as missing(violation_code, worker_job_id, is_required)
where cutoffs.traceability_required
  and requests.created_at >= cutoffs.cutover_at
  and requests.status <> 'cancelled'
  and missing.is_required
  and missing.worker_job_id is null
union all
select
  'dataset_review_submit_gate_runs'::text,
  gate_runs.id,
  'review_submit_gate_report'::text,
  gate_runs.created_at,
  gate_runs.modified_at,
  cutoffs.cutover_at,
  'missing_worker_job_id'::text,
  jsonb_build_object('status', gate_runs.status, 'requestedBy', gate_runs.requested_by)
from public.dataset_review_submit_gate_runs as gate_runs
join public.worker_domain_traceability_cutoffs as cutoffs
  on cutoffs.domain_source = 'dataset_review_submit_gate_runs'
where cutoffs.traceability_required
  and gate_runs.created_at >= cutoffs.cutover_at
  and gate_runs.worker_job_id is null;

revoke all on public.worker_domain_traceability_violations from public, anon, authenticated;
grant select on public.worker_domain_traceability_violations to service_role;

comment on view public.worker_domain_traceability_violations
  is 'Service-role audit view for post-cutover worker-produced domain rows that are not traceable to worker_jobs. Historical pre-cutover nullable rows are intentionally ignored.';

create or replace function util.apply_lca_package_retention(
  p_job_retention_window interval default interval '30 days',
  p_request_cache_retention_window interval default interval '30 days',
  p_as_of timestamp with time zone default pg_catalog.now(),
  p_max_rows integer default 1000,
  p_dry_run boolean default true
) returns table (
  retention_area text,
  retention_action text,
  dry_run boolean,
  affected_count bigint
)
language plpgsql
volatile
set search_path to ''
as $$
declare
  v_count bigint;
begin
  if p_as_of is null then
    raise exception using
      errcode = '22023',
      message = 'package retention apply as_of timestamp must not be null';
  end if;

  if p_job_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'package job retention window must be at least 1 day';
  end if;

  if p_request_cache_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'package request-cache retention window must be at least 1 day';
  end if;

  if coalesce(p_max_rows, 0) <= 0 then
    raise exception using
      errcode = '22023',
      message = 'package retention max rows must be greater than zero';
  end if;

  if p_dry_run then
    return query
    with artifact_candidates as (
      select artifacts.id
      from public.lca_package_artifacts as artifacts
      join public.worker_jobs as jobs
        on jobs.id = artifacts.worker_job_id
      where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
        and jobs.status not in ('queued', 'running', 'waiting')
        and artifacts.status not in ('deleted', 'pending')
        and not artifacts.is_pinned
        and artifacts.expires_at is not null
        and artifacts.expires_at <= p_as_of
        and not exists (
          select 1
          from public.lca_package_request_cache as request_cache
          where (
              request_cache.export_artifact_id = artifacts.id
              or request_cache.report_artifact_id = artifacts.id
            )
            and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
        )
      order by artifacts.expires_at, artifacts.created_at, artifacts.id
      limit p_max_rows
    ),
    request_cache_candidates as (
      select request_cache.id
      from public.lca_package_request_cache as request_cache
      left join public.worker_jobs as jobs
        on jobs.id = request_cache.worker_job_id
      where request_cache.status not in ('pending', 'running')
        and request_cache.last_accessed_at < p_as_of - p_request_cache_retention_window
        and coalesce(jobs.status not in ('queued', 'running', 'waiting'), true)
      order by request_cache.last_accessed_at, request_cache.created_at, request_cache.id
      limit p_max_rows
    ),
    export_item_candidates as (
      select export_items.id
      from public.lca_package_export_items as export_items
      join public.worker_jobs as jobs
        on jobs.id = export_items.worker_job_id
      where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
        and jobs.status not in ('queued', 'running', 'waiting')
        and coalesce(jobs.finished_at, jobs.updated_at, jobs.created_at) < p_as_of - p_job_retention_window
        and not exists (
          select 1
          from public.lca_package_artifacts as artifacts
          where artifacts.worker_job_id = export_items.worker_job_id
            and artifacts.status <> 'deleted'
        )
        and not exists (
          select 1
          from public.lca_package_request_cache as request_cache
          where request_cache.worker_job_id = export_items.worker_job_id
            and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
        )
      order by export_items.created_at, export_items.id
      limit p_max_rows
    )
    select
      'lca_package_artifacts'::text,
      'mark_expired_unpinned_artifacts_deleted'::text,
      true,
      count(*)::bigint
    from artifact_candidates
    union all
    select
      'lca_package_request_cache'::text,
      'delete_stale_request_cache_rows'::text,
      true,
      count(*)::bigint
    from request_cache_candidates
    union all
    select
      'lca_package_export_items'::text,
      'delete_export_items_after_artifact_gc'::text,
      true,
      count(*)::bigint
    from export_item_candidates;

    return;
  end if;

  with artifact_candidates as (
    select artifacts.id
    from public.lca_package_artifacts as artifacts
    join public.worker_jobs as jobs
      on jobs.id = artifacts.worker_job_id
    where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
      and jobs.status not in ('queued', 'running', 'waiting')
      and artifacts.status not in ('deleted', 'pending')
      and not artifacts.is_pinned
      and artifacts.expires_at is not null
      and artifacts.expires_at <= p_as_of
      and not exists (
        select 1
        from public.lca_package_request_cache as request_cache
        where (
            request_cache.export_artifact_id = artifacts.id
            or request_cache.report_artifact_id = artifacts.id
          )
          and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      )
    order by artifacts.expires_at, artifacts.created_at, artifacts.id
    limit p_max_rows
  )
  update public.lca_package_artifacts as artifacts
     set status = 'deleted',
         metadata = artifacts.metadata || jsonb_build_object(
           'retentionDeletedAt', p_as_of,
           'retentionAction', 'package_metadata_retention_gc'
         ),
         updated_at = p_as_of
  from artifact_candidates
  where artifacts.id = artifact_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_artifacts'::text,
    'mark_expired_unpinned_artifacts_deleted'::text,
    false,
    v_count;

  with request_cache_candidates as (
    select request_cache.id
    from public.lca_package_request_cache as request_cache
    left join public.worker_jobs as jobs
      on jobs.id = request_cache.worker_job_id
    where request_cache.status not in ('pending', 'running')
      and request_cache.last_accessed_at < p_as_of - p_request_cache_retention_window
      and coalesce(jobs.status not in ('queued', 'running', 'waiting'), true)
    order by request_cache.last_accessed_at, request_cache.created_at, request_cache.id
    limit p_max_rows
  )
  delete from public.lca_package_request_cache as request_cache
  using request_cache_candidates
  where request_cache.id = request_cache_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_request_cache'::text,
    'delete_stale_request_cache_rows'::text,
    false,
    v_count;

  with export_item_candidates as (
    select export_items.id
    from public.lca_package_export_items as export_items
    join public.worker_jobs as jobs
      on jobs.id = export_items.worker_job_id
    where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
      and jobs.status not in ('queued', 'running', 'waiting')
      and coalesce(jobs.finished_at, jobs.updated_at, jobs.created_at) < p_as_of - p_job_retention_window
      and not exists (
        select 1
        from public.lca_package_artifacts as artifacts
        where artifacts.worker_job_id = export_items.worker_job_id
          and artifacts.status <> 'deleted'
      )
      and not exists (
        select 1
        from public.lca_package_request_cache as request_cache
        where request_cache.worker_job_id = export_items.worker_job_id
          and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      )
    order by export_items.created_at, export_items.id
    limit p_max_rows
  )
  delete from public.lca_package_export_items as export_items
  using export_item_candidates
  where export_items.id = export_item_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_export_items'::text,
    'delete_export_items_after_artifact_gc'::text,
    false,
    v_count;
end;
$$;

alter function util.apply_lca_package_retention(interval, interval, timestamp with time zone, integer, boolean) owner to postgres;
revoke all on function util.apply_lca_package_retention(interval, interval, timestamp with time zone, integer, boolean) from public;
grant usage on schema util to service_role;
grant execute on function util.apply_lca_package_retention(interval, interval, timestamp with time zone, integer, boolean) to service_role;

comment on function util.apply_lca_package_retention(interval, interval, timestamp with time zone, integer, boolean)
  is 'Operator/service-role helper for package domain retention. Dry-run counts candidates by default; apply mode marks expired unpinned package artifacts deleted and removes stale package request-cache/export-item metadata rows. Storage object deletion remains a worker/operator responsibility before or around metadata GC.';

update public.dataset_review_submit_gate_runs
   set status = 'stale',
       modified_at = now(),
       completed_at = coalesce(completed_at, now()),
       calculator_report = coalesce(calculator_report, '{}'::jsonb) || jsonb_build_object(
         'staleReason', 'historical_unlinked_worker_job_after_cutover',
         'staleMarkedAt', now()
       )
 where worker_job_id is null
   and status in ('queued', 'running')
   and created_at < timestamp with time zone '2026-06-03 13:35:00+00';
