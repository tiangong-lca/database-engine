CREATE OR REPLACE FUNCTION "public"."cmd_dataset_derivative_rebuild_read"("p_request_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_request util.dataset_derivative_rebuild_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_request_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_REQUEST_INVALID',
      'status', 400,
      'message', 'request id is required'
    );
  end if;

  select request.*
  into v_request
  from util.dataset_derivative_rebuild_requests as request
  where request.id = p_request_id
    and request.actor_user_id = v_actor;

  if v_request.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_REQUEST_NOT_AVAILABLE',
      'status', 404,
      'message', 'Derivative rebuild request is not available'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_derivative_rebuild_read',
    'schema_version', 'dataset-derivative-rebuild-status.v1',
    'request_id', v_request.id::text,
    'plan_sha256', v_request.plan_sha256,
    'operation_id', v_request.operation_id,
    'action_id', v_request.action_id,
    'table', v_request.target_table,
    'id', v_request.target_id::text,
    'version', v_request.target_version,
    'status', v_request.status,
    'phase', v_request.phase,
    'fence_active',
      v_request.status not in ('completed', 'stale', 'failed'),
    'plan_request_sha256', v_request.plan_request_sha256,
    'action_request_sha256', v_request.action_request_sha256,
    'database_audit_id', v_request.action_audit_id::text,
    'summary_audit_id', v_request.summary_audit_id::text,
    'completed_snapshot_sha256', v_request.completed_snapshot_sha256,
    'completed_at', v_request.completed_at,
    'terminal_at', v_request.terminal_at,
    'drained_at', v_request.drained_at,
    'error', v_request.last_error,
    'expected', jsonb_build_object(
      'snapshot_sha256', v_request.expected_snapshot_sha256,
      'modified_at', v_request.expected_modified_at,
      'json_sha256', v_request.expected_json_sha256,
      'json_ordered_sha256', v_request.expected_json_ordered_sha256,
      'before_extracted_md_sha256',
        v_request.before_extracted_md_sha256,
      'before_embedding_ft_sha256',
        v_request.before_embedding_ft_sha256,
      'before_embedding_ft_at', v_request.before_embedding_ft_at
    ),
    'quarantine', jsonb_build_object(
      'drain_not_before', v_request.drain_not_before,
      'failure_release_not_before',
        v_request.failure_release_not_before,
      'http_requests', v_request.quarantined_http_requests,
      'embedding_jobs', v_request.quarantined_embedding_jobs,
      'pending_jobs', v_request.quarantined_pending_jobs
    ),
    'markdown', jsonb_build_object(
      'request_id', case
        when v_request.markdown_request_id is null then null
        else v_request.markdown_request_id::text
      end,
      'dispatched_at', v_request.markdown_dispatched_at,
      'response_status', v_request.markdown_response_status,
      'response_received_at', v_request.markdown_response_received_at,
      'proposal_id', case
        when v_request.markdown_proposal_id is null then null
        else v_request.markdown_proposal_id::text
      end,
      'accepted_sha256', v_request.accepted_extracted_md_sha256
    ),
    'embedding', jsonb_build_object(
      'pending_job_id', case
        when v_request.embedding_pending_job_id is null then null
        else v_request.embedding_pending_job_id::text
      end,
      'queue_msg_id', case
        when v_request.embedding_queue_msg_id is null then null
        else v_request.embedding_queue_msg_id::text
      end,
      'queued_at', v_request.embedding_queued_at,
      'proposal_id', case
        when v_request.embedding_proposal_id is null then null
        else v_request.embedding_proposal_id::text
      end
    )
  );
end;
$$;

ALTER FUNCTION "public"."cmd_dataset_derivative_rebuild_read"("p_request_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_derivative_rebuild_read"("p_request_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_derivative_rebuild_read"("p_request_id" "uuid") TO "authenticated";
