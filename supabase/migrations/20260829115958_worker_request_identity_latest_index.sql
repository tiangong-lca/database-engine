create index concurrently if not exists worker_jobs_request_identity_latest_idx
  on private.worker_jobs (
    worker_runtime,
    job_kind,
    requester_type,
    coalesce(requested_by, '00000000-0000-0000-0000-000000000000'::uuid),
    coalesce(team_id, '00000000-0000-0000-0000-000000000000'::uuid),
    coalesce(queue_key, ''),
    payload_schema_version,
    request_hash,
    created_at desc,
    id desc
  )
  where request_hash is not null;
