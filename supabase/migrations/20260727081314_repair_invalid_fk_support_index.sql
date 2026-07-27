-- A failed concurrent build can leave the relation present but unusable.
-- REINDEX CONCURRENTLY repairs both that production state and an already-valid
-- preview/local index without blocking ordinary reads and writes for the build.
reindex index concurrently public.dataset_review_submit_gate_runs_supersedes_idx;
