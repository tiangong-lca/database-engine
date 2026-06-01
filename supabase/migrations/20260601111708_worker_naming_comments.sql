-- Clarify the external worker repository rename without changing published
-- database contract names such as worker_runtime='calculator' or
-- calculator_report/calculatorReport payload fields.

comment on function public.lca_enqueue_job(text, jsonb)
  is 'Disabled legacy LCA pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';

comment on function public.lca_package_enqueue_job(jsonb)
  is 'Disabled legacy TIDAS package pgmq delivery entrypoint. Use public.worker_enqueue_job for new worker jobs.';

comment on table public.dataset_review_submit_gate_runs
  is 'Review-submit gate report/history table retained for compatibility. New worker execution lifecycle is public.worker_jobs.';

comment on column public.worker_job_kinds.worker_runtime
  is 'Compatibility runtime discriminator; calculator is the existing compute-runtime key, not the repository identity.';

comment on column public.worker_jobs.worker_runtime
  is 'Compatibility runtime discriminator; calculator is the existing compute-runtime key, not the repository identity.';

comment on function util.process_dataset_review_submit_jobs(integer, integer, integer)
  is 'Invokes the Edge review-submit coordinator that advances persisted dataset_review_submit_jobs after worker gate results are available.';

comment on table public.lca_snapshot_gc_runs
  is 'Audit header for worker-driven lca-results/snapshots object-aware garbage collection runs.';

comment on table public.lca_snapshot_gc_run_items
  is 'Per-object audit items for worker-driven lca-results/snapshots object-aware garbage collection runs.';

comment on function util.list_lca_snapshot_gc_candidates(
  interval,
  interval,
  timestamp with time zone,
  integer,
  integer,
  bigint
) is
  'Object-level candidate contract for worker-driven lca-results/snapshots GC; applies snapshot/orphan/byte caps but never deletes storage objects or database rows.';

update public.worker_job_kinds
set description = 'Prepare or refresh solver factorization artifacts',
    updated_at = now()
where job_kind = 'lca.factorization_prepare'
  and description = 'Prepare or refresh calculator factorization artifacts';
