begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(7);

select ok(
  to_regclass('public.dataset_review_submit_gate_runs_supersedes_idx') is not null,
  'dataset_review_submit_gate_runs has a supersedes gate run support index'
);

select ok(
  to_regclass('public.lca_network_snapshots_lcia_idx') is not null,
  'lca_network_snapshots has an LCIA method support index'
);

select ok(
  to_regclass('public.lca_package_request_cache_export_artifact_idx') is not null,
  'lca_package_request_cache has an export artifact support index'
);

select ok(
  to_regclass('public.lca_package_request_cache_report_artifact_idx') is not null,
  'lca_package_request_cache has a report artifact support index'
);

select ok(
  to_regclass('public.lca_result_cache_snapshot_idx') is not null,
  'lca_result_cache has a snapshot support index'
);

select ok(
  to_regclass('public.notifications_sender_user_id_idx') is not null,
  'notifications has a sender user support index'
);

select ok(
  to_regclass('public.worker_jobs_job_kind_idx') is not null,
  'worker_jobs has a job kind support index'
);

select * from finish();
rollback;
