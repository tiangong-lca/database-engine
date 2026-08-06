CREATE OR REPLACE FUNCTION "util"."read_dataset_derivative_rebuild_batch_any"("p_actor_user_id" "uuid", "p_batch_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_request util.dataset_derivative_rebuild_requests%rowtype;
  v_snapshot jsonb;
  v_targets jsonb := '[]'::jsonb;
  v_target_count integer;
  v_flow_count integer;
  v_process_count integer;
  v_completed_count integer;
  v_nonterminal_count integer;
  v_failed_count integer;
  v_distinct_ordinal_count integer;
  v_distinct_target_count integer;
  v_distinct_plan_count integer;
  v_distinct_summary_count integer;
  v_distinct_declared_count integer;
  v_declared_count integer;
  v_min_ordinal integer;
  v_max_ordinal integer;
  v_invalid_proof_count integer := 0;
  v_primary_ok boolean;
  v_snapshot_ok boolean;
  v_derivative_fresh boolean;
  v_lifecycle_ok boolean;
  v_proposals_ok boolean;
  v_terminal_audit_ok boolean;
  v_residue jsonb;
  v_target_ok boolean;
  v_status text;
  v_code text;
  v_batch_rows integer;
  v_single_rows integer;
  v_is_batch boolean;
begin
  if p_actor_user_id is null or p_batch_id is null then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_READ_INVALID_REQUEST',
      'causal_terminal_proof', false
    );
  end if;

  -- A Step 3 primary transaction always records a protected batch id.  If
  -- that child later reaches a terminal failed/stale state, a separately
  -- frozen and approved derivative-only compensation is admitted through the
  -- retained single-target RPC and is identified by its request id.  This
  -- reader accepts either exact identity, never a loose target lookup.
  select
    count(*) filter (where request.batch_id = p_batch_id)::integer,
    count(*) filter (
      where request.batch_id is null and request.id = p_batch_id
    )::integer
  into v_batch_rows, v_single_rows
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and (request.batch_id = p_batch_id or request.id = p_batch_id);

  if (v_batch_rows > 0 and v_single_rows > 0)
    or (v_batch_rows = 0 and v_single_rows <> 1) then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_REFERENCE_AMBIGUOUS',
      'causal_terminal_proof', false
    );
  end if;
  v_is_batch := v_batch_rows > 0;

  select
    count(*)::integer,
    count(*) filter (where target_table = 'flows')::integer,
    count(*) filter (where target_table = 'processes')::integer,
    count(*) filter (where status = 'completed')::integer,
    count(*) filter (where status not in ('completed', 'stale', 'failed'))::integer,
    count(*) filter (where status in ('stale', 'failed'))::integer,
    count(distinct coalesce(batch_ordinal, 1))::integer,
    count(distinct (target_table || ':' || target_id::text || '@' || target_version))::integer,
    count(distinct plan_request_sha256)::integer,
    count(distinct summary_audit_id)::integer,
    count(distinct coalesce(batch_target_count, 1))::integer,
    min(coalesce(batch_target_count, 1))::integer,
    min(coalesce(batch_ordinal, 1))::integer,
    max(coalesce(batch_ordinal, 1))::integer
  into
    v_target_count, v_flow_count, v_process_count, v_completed_count,
    v_nonterminal_count, v_failed_count, v_distinct_ordinal_count,
    v_distinct_target_count, v_distinct_plan_count, v_distinct_summary_count,
    v_distinct_declared_count, v_declared_count, v_min_ordinal,
    v_max_ordinal
  from util.dataset_derivative_rebuild_requests as request
  where request.actor_user_id = p_actor_user_id
    and (
      (v_is_batch and request.batch_id = p_batch_id)
      or (not v_is_batch and request.batch_id is null
        and request.id = p_batch_id)
    );

  if v_target_count not between 1 and 50
    or v_flow_count + v_process_count <> v_target_count
    or v_distinct_ordinal_count <> v_target_count
    or v_distinct_target_count <> v_target_count
    or v_distinct_plan_count <> 1
    or v_distinct_summary_count <> 1
    or v_distinct_declared_count <> 1
    or v_declared_count <> v_target_count
    or v_min_ordinal <> 1
    or v_max_ordinal <> v_target_count then
    return jsonb_build_object(
      'ok', false,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'reference_kind', case when v_is_batch then 'batch' else 'request' end,
      'batch_id', case when v_is_batch then p_batch_id else null end,
      'request_id', case when v_is_batch then null else p_batch_id end,
      'status', 'failed',
      'code', 'DERIVATIVE_BATCH_TARGET_SET_MISMATCH',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count
    );
  end if;

  if v_nonterminal_count > 0 or v_failed_count > 0 then
    select coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', coalesce(request.batch_ordinal, 1),
      'request_id', request.id,
      'table', request.target_table,
      'id', request.target_id,
      'version', request.target_version,
      'status', request.status,
      'phase', request.phase,
      'error', request.last_error,
      'causal_terminal_proof', false
    ) order by request.batch_ordinal), '[]'::jsonb)
    into v_targets
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and (
        (v_is_batch and request.batch_id = p_batch_id)
        or (not v_is_batch and request.batch_id is null
          and request.id = p_batch_id)
      );

    return jsonb_build_object(
      'ok', v_failed_count = 0,
      'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
      'reference_id', p_batch_id,
      'reference_kind', case when v_is_batch then 'batch' else 'request' end,
      'batch_id', case when v_is_batch then p_batch_id else null end,
      'request_id', case when v_is_batch then null else p_batch_id end,
      'status', case when v_failed_count > 0 then 'failed' else 'pending' end,
      'code', case when v_failed_count > 0
        then 'DERIVATIVE_BATCH_CHILD_FAILED'
        else 'DERIVATIVE_BATCH_PENDING' end,
      'proof_level', 'status_only',
      'causal_terminal_proof', false,
      'target_count', v_target_count,
      'flow_count', v_flow_count,
      'process_count', v_process_count,
      'completed_count', v_completed_count,
      'nonterminal_count', v_nonterminal_count,
      'failed_count', v_failed_count,
      'targets', v_targets
    );
  end if;

  for v_request in
    select request.*
    from util.dataset_derivative_rebuild_requests as request
    where request.actor_user_id = p_actor_user_id
      and (
        (v_is_batch and request.batch_id = p_batch_id)
        or (not v_is_batch and request.batch_id is null
          and request.id = p_batch_id)
      )
    order by coalesce(request.batch_ordinal, 1)
  loop
    begin
      v_snapshot := util.dataset_derivative_rebuild_snapshot(
        v_request.target_table,
        v_request.target_id,
        v_request.target_version
      );
    exception when others then
      v_snapshot := null;
    end;

    v_primary_ok := v_snapshot is not null
      and util.dataset_derivative_rebuild_primary_matches(v_request);
    v_snapshot_ok := coalesce(
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
        > coalesce(v_request.before_embedding_ft_at, '-infinity'::timestamp with time zone),
      false
    );
    v_lifecycle_ok := coalesce(
      v_request.phase = 'completed'
      and v_request.markdown_request_id is not null
      and v_request.markdown_dispatched_at is not null
      and v_request.markdown_response_status between 200 and 299
      and v_request.markdown_response_received_at >= v_request.markdown_dispatched_at
      and v_request.markdown_proposal_id is not null
      and v_request.accepted_extracted_md_sha256 is not null
      and v_request.embedding_queue_msg_id is not null
      and v_request.embedding_queued_at >= v_request.markdown_response_received_at
      and v_request.embedding_proposal_id is not null
      and v_request.completed_at >= v_request.embedding_queued_at
      and v_request.terminal_at >= v_request.completed_at
      and v_request.drained_at >= v_request.completed_at,
      false
    );

    select
      count(*) = 2
      and count(*) filter (
        where proposal.id = v_request.markdown_proposal_id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = v_snapshot->>'extracted_md_sha256'
      ) = 1
      and count(*) filter (
        where proposal.id = v_request.embedding_proposal_id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.source_extracted_md_sha256
            = v_request.accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = v_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at
            = (v_snapshot->>'embedding_ft_at')::timestamp with time zone
      ) = 1
    into v_proposals_ok
    from util.dataset_derivative_rebuild_proposals as proposal
    where proposal.request_id = v_request.id
      and proposal.status <> 'discarded';

    select jsonb_build_object(
      'http_requests', (
        select count(*)
        from net.http_request_queue as request
        where util.dataset_derivative_rebuild_http_body_matches(
          request.body, v_request.target_table,
          v_request.target_id, v_request.target_version
        )
      ),
      'embedding_jobs', (
        select count(*)
        from pgmq.q_embedding_jobs as job
        where job.message->>'id' = v_request.target_id::text
          and btrim(job.message->>'version') = v_request.target_version
          and job.message->>'schema' = 'public'
          and job.message->>'table' = v_request.target_table
          and job.message->>'embeddingColumn' = 'embedding_ft'
      ),
      'pending_jobs', (
        select count(*)
        from util.pending_embedding_jobs as pending
        where pending.schema_name = 'public'
          and pending.table_name = v_request.target_table
          and pending.record_id = v_request.target_id::text
          and btrim(pending.record_version) = v_request.target_version
          and pending.embedding_column = 'embedding_ft'
          and pending.status = 'pending'
      ),
      'failure_rows', (
        select count(*)
        from util.embedding_job_failures as failure
        where failure.msg_id = v_request.embedding_queue_msg_id
      ),
      'other_active_fences', (
        select count(*)
        from util.dataset_derivative_rebuild_requests as active
        where active.id <> v_request.id
          and active.target_table = v_request.target_table
          and active.target_id = v_request.target_id
          and active.target_version = v_request.target_version
          and active.status not in ('completed', 'stale', 'failed')
      )
    ) into v_residue;

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

    v_target_ok := coalesce(
      v_primary_ok and v_snapshot_ok and v_derivative_fresh
      and v_lifecycle_ok and v_proposals_ok and v_terminal_audit_ok
      and (v_residue->>'http_requests')::integer = 0
      and (v_residue->>'embedding_jobs')::integer = 0
      and (v_residue->>'pending_jobs')::integer = 0
      and (v_residue->>'failure_rows')::integer = 0
      and (v_residue->>'other_active_fences')::integer = 0,
      false
    );
    if not v_target_ok then
      v_invalid_proof_count := v_invalid_proof_count + 1;
    end if;

    v_targets := v_targets || jsonb_build_array(jsonb_build_object(
      'ordinal', coalesce(v_request.batch_ordinal, 1),
      'request_id', v_request.id,
      'table', v_request.target_table,
      'id', v_request.target_id,
      'version', v_request.target_version,
      'status', v_request.status,
      'phase', v_request.phase,
      'source_baseline_snapshot_sha256',
        v_request.source_baseline_snapshot_sha256,
      'expected_snapshot_sha256', v_request.expected_snapshot_sha256,
      'completed_snapshot_sha256', v_request.completed_snapshot_sha256,
      'primary_matches', v_primary_ok,
      'terminal_snapshot_matches', v_snapshot_ok,
      'proposals_committed', v_proposals_ok,
      'derivative_fresh', v_derivative_fresh,
      'lifecycle_complete', v_lifecycle_ok,
      'terminal_audit_present', v_terminal_audit_ok,
      'residue', v_residue,
      'causal_terminal_proof', v_target_ok
    ));
  end loop;

  if v_completed_count = v_target_count and v_invalid_proof_count = 0 then
    v_status := 'completed';
    v_code := 'DERIVATIVE_BATCH_COMPLETED';
  else
    v_status := 'failed';
    v_code := 'DERIVATIVE_BATCH_CAUSAL_PROOF_MISMATCH';
  end if;

  return jsonb_build_object(
    'ok', v_status = 'completed',
    'schema_version', 'dataset-derivative-rebuild-batch-status.v2',
    'reference_id', p_batch_id,
    'reference_kind', case when v_is_batch then 'batch' else 'request' end,
    'batch_id', case when v_is_batch then p_batch_id else null end,
    'request_id', case when v_is_batch then null else p_batch_id end,
    'status', v_status,
    'code', v_code,
    'proof_level', 'causal_terminal',
    'causal_terminal_proof', v_status = 'completed',
    'target_count', v_target_count,
    'flow_count', v_flow_count,
    'process_count', v_process_count,
    'completed_count', v_completed_count,
    'nonterminal_count', v_nonterminal_count,
    'failed_count', v_failed_count,
    'invalid_proof_count', v_invalid_proof_count,
    'targets', v_targets,
    'proof_sha256', util.dataset_flow_identity_sha256(v_targets)
  );
end;
$$;

ALTER FUNCTION "util"."read_dataset_derivative_rebuild_batch_any"("p_actor_user_id" "uuid", "p_batch_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."read_dataset_derivative_rebuild_batch_any"("p_actor_user_id" "uuid", "p_batch_id" "uuid") FROM PUBLIC;
