CREATE OR REPLACE FUNCTION "util"."record_dataset_derivative_rebuild_terminal"("p_request_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  select
    'cmd_dataset_derivative_rebuild_terminal',
    request.actor_user_id,
    request.target_table,
    request.target_id,
    request.target_version,
    jsonb_build_object(
      'record_type', 'terminal',
      'schema_version', 'dataset-derivative-rebuild-status.v1',
      'request_id', request.id,
      'batch_id', request.batch_id,
      'batch_ordinal', request.batch_ordinal,
      'batch_target_count', request.batch_target_count,
      'source_baseline_snapshot_sha256',
        request.source_baseline_snapshot_sha256,
      'plan_sha256', request.plan_sha256,
      'operation_id', request.operation_id,
      'action_id', request.action_id,
      'status', request.status,
      'phase', request.phase,
      'plan_request_sha256', request.plan_request_sha256,
      'action_request_sha256', request.action_request_sha256,
      'completed_snapshot_sha256', request.completed_snapshot_sha256,
      'terminal_at', request.terminal_at,
      'drained_at', request.drained_at,
      'error', request.last_error,
      'quarantine', jsonb_build_object(
        'http_requests', request.quarantined_http_requests,
        'embedding_jobs', request.quarantined_embedding_jobs,
        'pending_jobs', request.quarantined_pending_jobs
      )
    )
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.status in ('completed', 'stale', 'failed')
  on conflict do nothing;
end;
$$;

ALTER FUNCTION "util"."record_dataset_derivative_rebuild_terminal"("p_request_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."record_dataset_derivative_rebuild_terminal"("p_request_id" "uuid") FROM PUBLIC;
