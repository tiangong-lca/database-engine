create index concurrently if not exists dataset_review_submit_gate_runs_supersedes_idx
  on public.dataset_review_submit_gate_runs (supersedes_gate_run_id)
  where supersedes_gate_run_id is not null;
