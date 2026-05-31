-- Disable legacy pgmq delivery after the calculator worker_jobs cutover.
--
-- The legacy domain/history tables remain because LCA result rows, package
-- artifacts, caches, and review-submit gate reports still reference them.
-- New queue delivery must go through public.worker_enqueue_job instead.

create or replace function public.lca_enqueue_job(
  p_queue_name text,
  p_message jsonb
) returns bigint
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'queueName', p_queue_name,
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use public.worker_enqueue_job with an lca.* job_kind.';
end;
$$;

create or replace function public.lca_package_enqueue_job(
  p_message jsonb
) returns bigint
language plpgsql
security definer
set search_path = public, pg_temp
as $$
begin
  raise exception
    using
      errcode = 'P0001',
      message = 'legacy lca package pgmq enqueue is disabled after worker_jobs cutover',
      detail = jsonb_build_object(
        'messageKeys', coalesce(
          (
            select jsonb_agg(key order by key)
            from jsonb_object_keys(coalesce(p_message, '{}'::jsonb)) as keys(key)
          ),
          '[]'::jsonb
        )
      )::text,
      hint = 'Use public.worker_enqueue_job with a tidas.* job_kind.';
end;
$$;

revoke all on function public.lca_enqueue_job(text, jsonb) from public, anon, authenticated;
revoke all on function public.lca_package_enqueue_job(jsonb) from public, anon, authenticated;
grant execute on function public.lca_enqueue_job(text, jsonb) to service_role;
grant execute on function public.lca_package_enqueue_job(jsonb) to service_role;

comment on function public.lca_enqueue_job(text, jsonb)
  is 'Disabled legacy LCA pgmq delivery entrypoint. Use public.worker_enqueue_job for new calculator worker jobs.';
comment on function public.lca_package_enqueue_job(jsonb)
  is 'Disabled legacy TIDAS package pgmq delivery entrypoint. Use public.worker_enqueue_job for new calculator worker jobs.';

revoke all on public.lca_jobs from public, anon, authenticated;
grant select on public.lca_jobs to authenticated;
grant all on public.lca_jobs to service_role;

revoke all on public.lca_package_jobs from public, anon, authenticated;
grant select on public.lca_package_jobs to authenticated;
grant all on public.lca_package_jobs to service_role;

revoke all on public.dataset_review_submit_gate_runs from public, anon, authenticated;
grant all on public.dataset_review_submit_gate_runs to service_role;

comment on table public.lca_jobs
  is 'Legacy LCA job domain/history table retained for result/cache/artifact links. New queue lifecycle is public.worker_jobs.';
comment on table public.lca_package_jobs
  is 'Legacy TIDAS package job domain/history table retained for package artifact/cache links. New queue lifecycle is public.worker_jobs.';
comment on table public.dataset_review_submit_gate_runs
  is 'Review-submit gate report/history table retained for compatibility. New calculator execution lifecycle is public.worker_jobs.';

drop view if exists public.worker_legacy_lifecycle_audit;

create view public.worker_legacy_lifecycle_audit
with (security_invoker = true)
as
select
  'lca_jobs'::text as legacy_source,
  job_type::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running', 'ready'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(updated_at) as latest_updated_at
from public.lca_jobs
group by job_type, status
union all
select
  'lca_package_jobs'::text as legacy_source,
  job_type::text as task_family,
  status::text as legacy_status,
  count(*)::bigint as row_count,
  count(*) filter (where status in ('queued', 'running', 'ready'))::bigint as active_count,
  min(created_at) as oldest_created_at,
  max(created_at) as newest_created_at,
  max(updated_at) as latest_updated_at
from public.lca_package_jobs
group by job_type, status
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
  is 'Service-role audit view for retained legacy lifecycle/domain history after worker_jobs cutover.';

do $$
declare
  v_queue_name text;
  v_queue_table text;
  v_queue_count bigint;
begin
  foreach v_queue_name in array array['lca_jobs', 'lca_package_jobs'] loop
    v_queue_table := 'q_' || v_queue_name;

    if to_regclass(format('pgmq.%I', v_queue_table)) is null then
      continue;
    end if;

    execute format('select count(*) from pgmq.%I', v_queue_table)
      into v_queue_count;

    if v_queue_count = 0 then
      if to_regprocedure('pgmq.drop_queue(text)') is not null then
        perform pgmq.drop_queue(v_queue_name);
      elsif to_regprocedure('pgmq.drop_queue(text, boolean)') is not null then
        perform pgmq.drop_queue(v_queue_name, true);
      end if;
    else
      raise notice 'legacy pgmq queue %.% has % rows; leaving it in place for explicit operator drain',
        'pgmq',
        v_queue_table,
        v_queue_count;
    end if;
  end loop;
end;
$$;
