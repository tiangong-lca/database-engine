-- Clarify that dataset_review_submit_jobs remains a legacy retirement target
-- even though it still carries active review-submit coordinator responsibilities
-- during the worker_jobs cutover.

comment on table public.dataset_review_submit_jobs
  is 'Legacy review-submit coordinator/request table retained during the worker_jobs cutover. It remains a retirement target under workspace#242, but it cannot be dropped until submit-review idempotency, retry/coordinator recovery, final submit status, and UI/status recovery have moved to worker_jobs or a replacement domain coordinator table.';

comment on column public.dataset_review_submit_jobs.submit_worker_job_id
  is 'Canonical root review_submit.submit worker_jobs task for this legacy review-submit coordinator row during cutover.';

comment on function util.process_dataset_review_submit_jobs(integer, integer, integer)
  is 'Invokes the Edge review-submit coordinator that advances retained dataset_review_submit_jobs rows after worker gate results are available. This wrapper is part of the legacy coordinator cutover and must be retired or retargeted before dataset_review_submit_jobs is dropped.';

comment on view public.worker_legacy_table_retirement_blockers
  is 'Service-role audit view for legacy worker job DROP TABLE RESTRICT blockers. Targets include public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs; dataset_review_submit_jobs additionally requires coordinator/runtime migration before destructive retirement.';
