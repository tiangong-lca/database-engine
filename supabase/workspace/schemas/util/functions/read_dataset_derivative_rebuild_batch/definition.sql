CREATE OR REPLACE FUNCTION "util"."read_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_snapshot jsonb;
  v_targets jsonb := '[]'::jsonb;
  v_target_count integer := 0;
  v_flow_count integer := 0;
  v_process_count integer := 0;
  v_completed_count integer := 0;
  v_nonterminal_count integer := 0;
  v_failed_count integer := 0;
  v_invalid_proof_count integer := 0;
  v_completed_invalid_proof_count integer := 0;
  v_distinct_ordinal_count integer := 0;
  v_distinct_target_count integer := 0;
  v_distinct_plan_count integer := 0;
  v_distinct_summary_count integer := 0;
  v_min_ordinal integer;
  v_max_ordinal integer;
  v_batch_sizes_ok boolean := false;
  v_primary_ok boolean;
  v_terminal_snapshot_ok boolean;
  v_proposals_ok boolean;
  v_terminal_audit_ok boolean;
  v_derivative_fresh boolean;
  v_lifecycle_ok boolean;
  v_http_residue integer;
  v_embedding_residue integer;
  v_pending_residue integer;
  v_failure_residue integer;
  v_fence_residue integer;
  v_target_ok boolean;
  v_batch_integrity_ok boolean;
  v_status text;
  v_code text;
begin
  if p_actor_user_id is null or p_batch_id is null then
    return jsonb_build_object(
      'ok', false,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_READ_INVALID_REQUEST',
      'causal_terminal_proof', false
    );
  end if;

  select
    count(*)::integer,
    count(*) filter (where target_table = 'flows')::integer,
    count(*) filter (where target_table = 'processes')::integer,
    count(*) filter (where status = 'completed')::integer,
    count(*) filter (
      where status not in ('completed', 'stale', 'failed')
    )::integer,
    count(*) filter (where status in ('stale', 'failed'))::integer,
    count(distinct batch_ordinal)::integer,
    count(distinct (
      target_table || ':' || target_id::text || '@' || target_version
    ))::integer,
    count(distinct plan_request_sha256)::integer,
    count(distinct summary_audit_id)::integer,
    min(batch_ordinal)::integer,
    max(batch_ordinal)::integer,
    coalesce(bool_and(batch_target_count = 50), false)
  into
    v_target_count,
    v_flow_count,
    v_process_count,
    v_completed_count,
    v_nonterminal_count,
    v_failed_count,
    v_distinct_ordinal_count,
    v_distinct_target_count,
    v_distinct_plan_count,
    v_distinct_summary_count,
    v_min_ordinal,
    v_max_ordinal,
    v_batch_sizes_ok
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and request.batch_id = p_batch_id;

  v_batch_integrity_ok := v_target_count = 50
    and v_flow_count = 23
    and v_process_count = 27
    and v_distinct_ordinal_count = 50
    and v_distinct_target_count = 50
    and v_distinct_plan_count = 1
    and v_distinct_summary_count = 1
    and v_min_ordinal = 1
    and v_max_ordinal = 50
    and v_batch_sizes_ok;

  if not v_batch_integrity_ok then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', p_batch_id,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_TARGET_SET_MISMATCH',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count
    );
  end if;

  -- Polling must stay cheap while any child is still moving.  A causal proof
  -- is meaningful only after all 50 children are terminal; doing the full
  -- per-target snapshot/queue/proposal/audit closure on every pending poll
  -- would multiply hundreds of reads without improving the decision.
  if v_nonterminal_count > 0 or v_failed_count > 0 then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'ordinal', request.batch_ordinal,
          'request_id', request.id,
          'table', request.target_table,
          'id', request.target_id,
          'version', request.target_version,
          'status', request.status,
          'phase', request.phase,
          'error', request.last_error,
          'causal_terminal_proof', false
        ) order by request.batch_ordinal
      ),
      '[]'::jsonb
    )
    into v_targets
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and request.batch_id = p_batch_id;

    return jsonb_build_object(
      'ok', v_failed_count = 0,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
      'batch_id', p_batch_id,
      'status', case
        when v_failed_count > 0 then 'failed'
        else 'pending'
      end,
      'code', case
        when v_failed_count > 0 then 'DERIVATIVE_BATCH_CHILD_FAILED'
        else 'DERIVATIVE_BATCH_PENDING'
      end,
      'proof_level', 'status_only',
      'proof_deferred', v_failed_count = 0,
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count,
      'invalid_proof_count', null,
      'completed_invalid_proof_count', null,
      'targets', v_targets
    );
  end if;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and request.batch_id = p_batch_id
    order by request.batch_ordinal
  loop
    begin
      v_snapshot := util.dataset_derivative_rebuild_snapshot(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
    exception
      when others then
        v_snapshot := null;
    end;

    v_primary_ok := v_snapshot is not null
      and util.dataset_derivative_rebuild_primary_matches(v_request);
    v_terminal_snapshot_ok := coalesce(
      v_request.status = 'completed'
      and v_request.completed_snapshot_sha256 is not null
      and v_snapshot->>'snapshot_sha256'
        is not distinct from v_request.completed_snapshot_sha256,
      false
    );
    v_derivative_fresh := coalesce(
      v_snapshot->>'extracted_md_sha256' is not null
      and v_snapshot->>'embedding_ft_sha256' is not null
      and (v_snapshot->>'embedding_ft_at')::timestamp with time zone
        > coalesce(
          v_request.before_embedding_ft_at,
          '-infinity'::timestamp with time zone
        ),
      false
    );
    v_lifecycle_ok := coalesce(
      v_request.phase = 'completed'
      and v_request.markdown_request_id is not null
      and v_request.markdown_dispatched_at is not null
      and v_request.markdown_response_status between 200 and 299
      and v_request.markdown_response_received_at
        >= v_request.markdown_dispatched_at
      and v_request.markdown_proposal_id is not null
      and v_request.accepted_extracted_md_sha256 is not null
      and v_request.embedding_queue_msg_id is not null
      and v_request.embedding_queued_at
        >= v_request.markdown_response_received_at
      and v_request.embedding_proposal_id is not null
      and v_request.completed_at >= v_request.embedding_queued_at
      and v_request.terminal_at >= v_request.completed_at
      and v_request.drained_at >= v_request.completed_at,
      false
    );

    select
      (
        select count(*) = 2
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.request_id = v_request.id
          and proposal.status <> 'discarded'
      )
      and exists (
        select 1
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.id = v_request.markdown_proposal_id
          and proposal.request_id = v_request.id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.captured_at >= v_request.markdown_dispatched_at
          and proposal.extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = v_snapshot->>'extracted_md_sha256'
      )
      and exists (
        select 1
        from util.dataset_derivative_rebuild_proposals as proposal
        where proposal.id = v_request.embedding_proposal_id
          and proposal.request_id = v_request.id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.captured_at >= v_request.embedding_queued_at
          and proposal.source_extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = v_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at
            = (v_snapshot->>'embedding_ft_at')::timestamp with time zone
      )
    into v_proposals_ok;

    select count(*)::integer
    into v_http_residue
    from net.http_request_queue as request
    where util.dataset_derivative_rebuild_http_body_matches(
      request.body,
      v_request.target_table,
      v_request.target_id,
      v_request.target_version
    );

    select count(*)::integer
    into v_embedding_residue
    from pgmq.q_embedding_jobs as job
    where job.message->>'id' = v_request.target_id::text
      and btrim(job.message->>'version') = v_request.target_version
      and job.message->>'schema' = 'public'
      and job.message->>'table' = v_request.target_table
      and job.message->>'embeddingColumn' = 'embedding_ft';

    select count(*)::integer
    into v_pending_residue
    from util.pending_embedding_jobs as pending
    where pending.schema_name = 'public'
      and pending.table_name = v_request.target_table
      and pending.record_id = v_request.target_id::text
      and btrim(pending.record_version) = v_request.target_version
      and pending.embedding_column = 'embedding_ft'
      and pending.status = 'pending';

    select count(*)::integer
    into v_failure_residue
    from util.embedding_job_failures as failure
    where failure.msg_id = v_request.embedding_queue_msg_id;

    select count(*)::integer
    into v_fence_residue
    from util.dataset_derivative_rebuild_requests as active_request
    where active_request.id <> v_request.id
      and active_request.target_table = v_request.target_table
      and active_request.target_id = v_request.target_id
      and active_request.target_version = v_request.target_version
      and active_request.status not in ('completed', 'stale', 'failed');

    select count(*) = 1
    into v_terminal_audit_ok
    from private.command_audit_log as audit
    where audit.command = 'cmd_dataset_derivative_rebuild_terminal'
      and audit.actor_user_id = p_actor_user_id
      and audit.target_table = v_request.target_table
      and audit.target_id = v_request.target_id
      and audit.target_version = v_request.target_version
      and audit.payload->>'request_id' = v_request.id::text
      and audit.payload->>'status' = 'completed';

    v_target_ok := coalesce(v_primary_ok
      and v_terminal_snapshot_ok
      and v_derivative_fresh
      and v_lifecycle_ok
      and v_proposals_ok
      and v_terminal_audit_ok
      and v_http_residue = 0
      and v_embedding_residue = 0
      and v_pending_residue = 0
      and v_failure_residue = 0
      and v_fence_residue = 0, false);

    if not v_target_ok then
      v_invalid_proof_count := v_invalid_proof_count + 1;
      if v_request.status = 'completed' then
        v_completed_invalid_proof_count :=
          v_completed_invalid_proof_count + 1;
      end if;
    end if;

    v_targets := v_targets || jsonb_build_array(
      jsonb_build_object(
        'ordinal', v_request.batch_ordinal,
        'request_id', v_request.id,
        'table', v_request.target_table,
        'id', v_request.target_id,
        'version', v_request.target_version,
        'status', v_request.status,
        'phase', v_request.phase,
        'source_baseline_snapshot_sha256',
          v_request.source_baseline_snapshot_sha256,
        'expected_snapshot_sha256', v_request.expected_snapshot_sha256,
        'completed_snapshot_sha256',
          v_request.completed_snapshot_sha256,
        'primary_matches', v_primary_ok,
        'terminal_snapshot_matches', v_terminal_snapshot_ok,
        'proposals_committed', v_proposals_ok,
        'derivative_fresh', v_derivative_fresh,
        'lifecycle_complete', v_lifecycle_ok,
        'terminal_audit_present', v_terminal_audit_ok,
        'residue', jsonb_build_object(
          'http_requests', v_http_residue,
          'embedding_jobs', v_embedding_residue,
          'pending_jobs', v_pending_residue,
          'failure_rows', v_failure_residue,
          'other_active_fences', v_fence_residue
        ),
        'causal_terminal_proof', v_target_ok
      )
    );
  end loop;

  if v_failed_count > 0 then
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CHILD_FAILED';
  elsif v_completed_invalid_proof_count > 0 then
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  elsif v_nonterminal_count > 0 then
    v_status := 'pending';
    v_code := 'DERIVATIVE_BATCH_PENDING';
  elsif v_completed_count = 50 and v_invalid_proof_count = 0 then
    v_status := 'completed';
    v_code := 'DERIVATIVE_BATCH_COMPLETED';
  else
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  end if;

  return jsonb_build_object(
    'ok', v_status <> 'failed',
    'schema_version', 'dataset-derivative-rebuild-batch-status.v1',
    'batch_id', p_batch_id,
    'status', v_status,
    'code', v_code,
    'proof_level', 'causal_terminal',
    'proof_deferred', false,
    'causal_terminal_proof',
      v_status = 'completed' and v_invalid_proof_count = 0,
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'completed_count', v_completed_count,
    'nonterminal_count', v_nonterminal_count,
    'failed_count', v_failed_count,
    'invalid_proof_count', v_invalid_proof_count,
    'completed_invalid_proof_count', v_completed_invalid_proof_count,
    'targets', v_targets
  );
end;
$$;

ALTER FUNCTION "util"."read_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."read_dataset_derivative_rebuild_batch"("p_actor_user_id" "uuid", "p_batch_id" "uuid") FROM PUBLIC;
