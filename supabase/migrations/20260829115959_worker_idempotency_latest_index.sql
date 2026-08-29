create index concurrently if not exists worker_jobs_idempotency_latest_idx
  on private.worker_jobs (
    worker_runtime,
    job_kind,
    coalesce(requested_by, '00000000-0000-0000-0000-000000000000'::uuid),
    idempotency_key,
    created_at desc,
    id desc
  )
  where idempotency_key is not null;
