-- Nullable foreign-key columns use partial indexes because null rows never
-- participate in referential checks. Non-nullable keys use full indexes.
create index if not exists lcia_document_validation_evidence_source_worker_job_idx
  on public.lcia_document_validation_evidence (source_worker_job_id)
  where source_worker_job_id is not null;

create index if not exists lcia_result_packages_latest_all_unit_result_idx
  on public.lcia_result_packages (latest_all_unit_result_id)
  where latest_all_unit_result_id is not null;

create index if not exists lcia_result_packages_result_idx
  on public.lcia_result_packages (result_id);

create index if not exists lcia_result_packages_snapshot_idx
  on public.lcia_result_packages (snapshot_id);

create index if not exists lcia_scope_closure_checks_report_artifact_idx
  on public.lcia_scope_closure_checks (report_artifact_id)
  where report_artifact_id is not null;

create index if not exists lcia_scope_closure_checks_reused_from_check_idx
  on public.lcia_scope_closure_checks (reused_from_check_id)
  where reused_from_check_id is not null;

create index if not exists lcia_scope_closure_scan_executions_completed_check_idx
  on public.lcia_scope_closure_scan_executions (completed_check_id)
  where completed_check_id is not null;

create index if not exists lcia_scope_closure_scan_executions_data_snapshot_token_idx
  on public.lcia_scope_closure_scan_executions (data_snapshot_token);

create index if not exists lcia_scope_closure_scan_executions_leased_by_job_idx
  on public.lcia_scope_closure_scan_executions (leased_by_job_id)
  where leased_by_job_id is not null;

create index if not exists dataset_derivative_rebuild_permits_proposal_idx
  on util.dataset_derivative_rebuild_permits (proposal_id);

create index if not exists dataset_flow_identity_mutation_permits_scope_ordinal_idx
  on util.dataset_flow_identity_mutation_permits (scope_id, ordinal);

create index if not exists dataset_flow_identity_process_ledger_audit_idx
  on util.dataset_flow_identity_process_ledger (audit_id)
  where audit_id is not null;

create index if not exists dataset_flow_identity_scopes_final_invocation_idx
  on util.dataset_flow_identity_scopes (final_wrapper_invocation_id)
  where final_wrapper_invocation_id is not null;

create index if not exists dataset_flow_identity_scopes_receipt_idx
  on util.dataset_flow_identity_scopes (receipt_id);

-- The unique constraint already provides the same ordered key prefix.
drop index if exists public.lca_release_artifacts_run_idx;
