CREATE OR REPLACE FUNCTION "util"."read_dataset_flow_identity_derivative_set"("p_actor_user_id" "uuid", "p_scope_id" "uuid") RETURNS "jsonb"
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
  with scope_row as (
    select scope.*
    from util.dataset_flow_identity_scopes as scope
    where scope.id = p_scope_id
      and scope.actor_user_id = p_actor_user_id
  ), base as materialized (
    select
      ledger.ordinal,
      ledger.process_id,
      ledger.process_version,
      ledger.derivative_batch_id,
      ledger.after_payload_sha256,
      ledger.manifest,
      primary_audit.created_at as primary_audit_created_at,
      process.modified_at as current_modified_at,
      snapshot.value as current_snapshot,
      original as original_request,
      original_action.payload as original_action_payload,
      original_summary.payload as original_summary_payload,
      scope.plan_sha256 as scope_plan_sha256,
      scope.operation_id as scope_operation_id
    from scope_row as scope
    join util.dataset_flow_identity_process_ledger as ledger
      on ledger.scope_id = scope.id
    join public.processes as process
      on process.id = ledger.process_id
      and btrim(process.version::text) = ledger.process_version
      and process.user_id = p_actor_user_id
      and process.state_code = 0
    cross join lateral (
      select util.dataset_derivative_rebuild_snapshot(process) as value
    ) as snapshot
    join private.command_audit_log as primary_audit
      on primary_audit.id = ledger.audit_id
      and primary_audit.actor_user_id = p_actor_user_id
      and primary_audit.command
        = 'cmd_dataset_flow_identity_process_rewrite_guarded'
    left join util.dataset_derivative_rebuild_requests as original
      on original.actor_user_id = p_actor_user_id
      and original.batch_id = ledger.derivative_batch_id
      and original.target_table = 'processes'
      and original.target_id = ledger.process_id
      and original.target_version = ledger.process_version
    left join private.command_audit_log as original_action
      on original_action.id = original.action_audit_id
    left join private.command_audit_log as original_summary
      on original_summary.id = original.summary_audit_id
  ), compensation as materialized (
    select base.ordinal, candidate.request_row
    from base
    left join lateral (
      select request as request_row
      from util.dataset_derivative_rebuild_requests as request
      where request.actor_user_id = p_actor_user_id
        and request.batch_id is null
        and request.target_table = 'processes'
        and request.target_id = base.process_id
        and request.target_version = base.process_version
        and request.plan_sha256 <> base.scope_plan_sha256
        and request.expected_json_ordered_sha256
          = base.current_snapshot->>'json_ordered_sha256'
        and request.reason_code = 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || base.ordinal::text
        and request.operation_id = request.action_id
        and request.operation_id ~ (
          '^FLOW_IDENTITY_SCOPE_COMPENSATION:' || p_scope_id::text || ':'
          || base.ordinal::text
          || ':[A-Za-z0-9][A-Za-z0-9._-]{0,63}$'
        )
        and request.admitted_at > base.primary_audit_created_at
        and exists (
          select 1
          from private.command_audit_log as action_audit
          where action_audit.id = request.action_audit_id
            and action_audit.actor_user_id = p_actor_user_id
            and action_audit.command
              = 'cmd_dataset_derivative_rebuild_plan_guarded'
            and action_audit.target_table = 'processes'
            and action_audit.target_id = base.process_id
            and action_audit.target_version = base.process_version
            and action_audit.payload->>'request_id' = request.id::text
            and action_audit.payload->>'plan_sha256' = request.plan_sha256
            and action_audit.payload->>'operation_id' = request.operation_id
            and action_audit.payload->>'action_id' = request.action_id
            and action_audit.payload->>'reason_code' = request.reason_code
            and action_audit.payload->>'expected_snapshot_sha256'
              = request.expected_snapshot_sha256
        )
        and exists (
          select 1
          from private.command_audit_log as summary_audit
          where summary_audit.id = request.summary_audit_id
            and summary_audit.actor_user_id = p_actor_user_id
            and summary_audit.command
              = 'cmd_dataset_derivative_rebuild_plan_guarded'
            and summary_audit.target_table is null
            and summary_audit.payload->>'request_id' = request.id::text
            and summary_audit.payload->>'plan_sha256' = request.plan_sha256
            and summary_audit.payload->>'operation_id' = request.operation_id
            and summary_audit.payload->>'action_count' = '1'
            and summary_audit.payload->>'accepted_count' = '1'
        )
      order by request.admitted_at desc, request.id desc
      limit 1
    ) as candidate on true
    where (candidate.request_row).id is not null
  ), candidate as materialized (
    select
      base.ordinal,
      base.process_id,
      base.process_version,
      base.derivative_batch_id,
      base.after_payload_sha256,
      base.current_modified_at,
      base.current_snapshot,
      'protected_batch'::text as reference_kind,
      base.original_request as request_row,
      coalesce(
        (base.original_request).id is not null
        and (base.original_request).batch_id = base.derivative_batch_id
        and (base.original_request).batch_ordinal = 1
        and (base.original_request).batch_target_count = 1
        and (base.original_request).plan_sha256 = base.scope_plan_sha256
        and (base.original_request).operation_id = base.scope_operation_id
        and (base.original_request).reason_code = 'FLOW_IDENTITY_SCOPE:'
          || p_scope_id::text || ':' || base.ordinal::text
        and (base.original_request).admitted_at
          > base.primary_audit_created_at
        and base.original_action_payload->>'batch_id'
          = base.derivative_batch_id::text
        and base.original_action_payload->>'request_id'
          = (base.original_request).id::text
        and base.original_summary_payload->>'batch_id'
          = base.derivative_batch_id::text,
        false
      ) as lineage_ok
    from base
    union all
    select
      base.ordinal,
      base.process_id,
      base.process_version,
      base.derivative_batch_id,
      base.after_payload_sha256,
      base.current_modified_at,
      base.current_snapshot,
      'separate_compensation',
      compensation.request_row,
      true
    from base
    join compensation using (ordinal)
  ), proposal_proof as (
    select
      candidate.ordinal,
      candidate.reference_kind,
      count(proposal.*) filter (
        where proposal.status <> 'discarded'
      ) = 2
      and count(proposal.*) filter (
        where proposal.status <> 'discarded'
          and proposal.id = (candidate.request_row).markdown_proposal_id
          and proposal.proposal_kind = 'markdown'
          and proposal.status = 'committed'
          and proposal.extracted_md_sha256
            = (candidate.request_row).accepted_extracted_md_sha256
          and proposal.extracted_md_sha256
            = candidate.current_snapshot->>'extracted_md_sha256'
      ) = 1
      and count(proposal.*) filter (
        where proposal.status <> 'discarded'
          and proposal.id = (candidate.request_row).embedding_proposal_id
          and proposal.proposal_kind = 'embedding'
          and proposal.status = 'committed'
          and proposal.source_extracted_md_sha256
            = (candidate.request_row).accepted_extracted_md_sha256
          and proposal.embedding_ft_sha256
            = candidate.current_snapshot->>'embedding_ft_sha256'
          and proposal.embedding_ft_at =
            (candidate.current_snapshot->>'embedding_ft_at')::timestamp with time zone
      ) = 1 as proposals_ok
    from candidate
    left join util.dataset_derivative_rebuild_proposals as proposal
      on proposal.request_id = (candidate.request_row).id
    group by candidate.ordinal, candidate.reference_kind,
      candidate.request_row, candidate.current_snapshot
  ), terminal_audit_proof as (
    select candidate.ordinal, candidate.reference_kind,
      count(audit.*) = 1 as terminal_audit_ok
    from candidate
    left join private.command_audit_log as audit
      on audit.command = 'cmd_dataset_derivative_rebuild_terminal'
      and audit.actor_user_id = p_actor_user_id
      and audit.target_table = 'processes'
      and audit.target_id = candidate.process_id
      and audit.target_version = candidate.process_version
      and audit.payload->>'request_id' = (candidate.request_row).id::text
      and audit.payload->>'status' = 'completed'
    group by candidate.ordinal, candidate.reference_kind
  ), http_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(request.*)::integer as residue_count
    from candidate
    left join net.http_request_queue as request
      on util.dataset_derivative_rebuild_http_body_matches(
        request.body, 'processes', candidate.process_id,
        candidate.process_version
      )
    group by candidate.ordinal, candidate.reference_kind
  ), embedding_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(job.*)::integer as residue_count
    from candidate
    left join pgmq.q_embedding_jobs as job
      on job.message->>'id' = candidate.process_id::text
      and btrim(job.message->>'version') = candidate.process_version
      and job.message->>'schema' = 'public'
      and job.message->>'table' = 'processes'
      and job.message->>'embeddingColumn' = 'embedding_ft'
    group by candidate.ordinal, candidate.reference_kind
  ), pending_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(pending.*)::integer as residue_count
    from candidate
    left join util.pending_embedding_jobs as pending
      on pending.schema_name = 'public'
      and pending.table_name = 'processes'
      and pending.record_id = candidate.process_id::text
      and btrim(pending.record_version) = candidate.process_version
      and pending.embedding_column = 'embedding_ft'
      and pending.status = 'pending'
    group by candidate.ordinal, candidate.reference_kind
  ), failure_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(failure.*)::integer as residue_count
    from candidate
    left join util.embedding_job_failures as failure
      on failure.msg_id = (candidate.request_row).embedding_queue_msg_id
    group by candidate.ordinal, candidate.reference_kind
  ), fence_residue as (
    select candidate.ordinal, candidate.reference_kind,
      count(active.*)::integer as residue_count
    from candidate
    left join util.dataset_derivative_rebuild_requests as active
      on active.id <> (candidate.request_row).id
      and active.target_table = 'processes'
      and active.target_id = candidate.process_id
      and active.target_version = candidate.process_version
      and active.status not in ('completed', 'stale', 'failed')
    group by candidate.ordinal, candidate.reference_kind
  ), evaluated as materialized (
    select
      candidate.*,
      coalesce(proposal_proof.proposals_ok, false) as proposals_ok,
      coalesce(terminal_audit_proof.terminal_audit_ok, false)
        as terminal_audit_ok,
      jsonb_build_object(
        'http_requests', coalesce(http_residue.residue_count, 0),
        'embedding_jobs', coalesce(embedding_residue.residue_count, 0),
        'pending_jobs', coalesce(pending_residue.residue_count, 0),
        'failure_rows', coalesce(failure_residue.residue_count, 0),
        'other_active_fences', coalesce(fence_residue.residue_count, 0)
      ) as residue,
      coalesce(
        candidate.lineage_ok
        and (candidate.request_row).status = 'completed'
        and candidate.current_snapshot->>'user_id' = p_actor_user_id::text
        and candidate.current_snapshot->>'state_code' = '0'
        and candidate.current_modified_at
          = (candidate.request_row).expected_modified_at
        and candidate.current_snapshot->>'json_sha256'
          = (candidate.request_row).expected_json_sha256
        and candidate.current_snapshot->>'json_ordered_sha256'
          = (candidate.request_row).expected_json_ordered_sha256
        and candidate.current_snapshot->>'snapshot_sha256'
          = (candidate.request_row).completed_snapshot_sha256
        and candidate.current_snapshot->>'extracted_md_sha256' is not null
        and candidate.current_snapshot->>'embedding_ft_sha256' is not null
        and (candidate.current_snapshot->>'embedding_ft_at')::timestamp with time zone
          > coalesce((candidate.request_row).before_embedding_ft_at,
            '-infinity'::timestamp with time zone)
        and (candidate.request_row).phase = 'completed'
        and (candidate.request_row).markdown_request_id is not null
        and (candidate.request_row).markdown_dispatched_at is not null
        and (candidate.request_row).markdown_response_status between 200 and 299
        and (candidate.request_row).markdown_response_received_at
          >= (candidate.request_row).markdown_dispatched_at
        and (candidate.request_row).markdown_proposal_id is not null
        and (candidate.request_row).accepted_extracted_md_sha256 is not null
        and (candidate.request_row).embedding_queue_msg_id is not null
        and (candidate.request_row).embedding_queued_at
          >= (candidate.request_row).markdown_response_received_at
        and (candidate.request_row).embedding_proposal_id is not null
        and (candidate.request_row).completed_at
          >= (candidate.request_row).embedding_queued_at
        and (candidate.request_row).terminal_at
          >= (candidate.request_row).completed_at
        and (candidate.request_row).drained_at
          >= (candidate.request_row).completed_at
        and coalesce(proposal_proof.proposals_ok, false)
        and coalesce(terminal_audit_proof.terminal_audit_ok, false)
        and coalesce(http_residue.residue_count, 0) = 0
        and coalesce(embedding_residue.residue_count, 0) = 0
        and coalesce(pending_residue.residue_count, 0) = 0
        and coalesce(failure_residue.residue_count, 0) = 0
        and coalesce(fence_residue.residue_count, 0) = 0,
        false
      ) as causal_terminal_proof
    from candidate
    left join proposal_proof using (ordinal, reference_kind)
    left join terminal_audit_proof using (ordinal, reference_kind)
    left join http_residue using (ordinal, reference_kind)
    left join embedding_residue using (ordinal, reference_kind)
    left join pending_residue using (ordinal, reference_kind)
    left join failure_residue using (ordinal, reference_kind)
    left join fence_residue using (ordinal, reference_kind)
  ), ranked as (
    select evaluated.*,
      case
        when evaluated.reference_kind = 'protected_batch'
          and (
            evaluated.causal_terminal_proof
            or (evaluated.request_row).status
              not in ('completed', 'stale', 'failed')
          ) then 0
        when evaluated.reference_kind = 'separate_compensation' then 1
        else 2
      end as proof_rank
    from evaluated
  ), effective as (
    select distinct on (ranked.ordinal) ranked.*
    from ranked
    order by ranked.ordinal, ranked.proof_rank
  ), target_payload as (
    select effective.ordinal,
      jsonb_build_object(
        'ordinal', effective.ordinal,
        'id', effective.process_id,
        'version', effective.process_version,
        'original_batch_id', effective.derivative_batch_id,
        'effective_reference_id', (effective.request_row).id,
        'effective_reference_kind', effective.reference_kind,
        'status', case
          when effective.causal_terminal_proof then 'completed'
          when (effective.request_row).status
            not in ('completed', 'stale', 'failed') then 'pending'
          else 'failed' end,
        'request_status', coalesce((effective.request_row).status, 'missing'),
        'phase', coalesce((effective.request_row).phase, 'missing'),
        'lineage_ok', effective.lineage_ok,
        'proposals_committed', effective.proposals_ok,
        'terminal_audit_present', effective.terminal_audit_ok,
        'residue', effective.residue,
        'current_snapshot_sha256',
          effective.current_snapshot->>'snapshot_sha256',
        'current_json_ordered_sha256',
          effective.current_snapshot->>'json_ordered_sha256',
        'causal_terminal_proof', effective.causal_terminal_proof
      ) as value,
      jsonb_build_object(
        'ordinal', effective.ordinal,
        'table', 'processes',
        'id', effective.process_id,
        'version', effective.process_version,
        'original_batch_id', effective.derivative_batch_id,
        'original_status', case
          when (base.original_request).id is null then 'missing'
          when (base.original_request).status = 'stale' then 'stale'
          else 'failed' end,
        'original_code', coalesce(
          nullif(btrim((base.original_request).last_error->>'code'), ''),
          case
            when (base.original_request).id is null
              then 'DERIVATIVE_BATCH_CHILD_MISSING'
            when (base.original_request).status = 'stale'
              then 'DERIVATIVE_BATCH_CHILD_STALE'
            when (base.original_request).status = 'failed'
              then 'DERIVATIVE_BATCH_CHILD_FAILED'
            else 'DERIVATIVE_CAUSAL_TERMINAL_PROOF_FAILED'
          end
        ),
        'latest_compensation_request_id', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).id else null end,
        'latest_compensation_status', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).status else null end,
        'latest_compensation_plan_sha256', case
          when effective.reference_kind = 'separate_compensation'
            then (effective.request_row).plan_sha256 else null end,
        'desired_payload_sha256', effective.after_payload_sha256,
        'current_json_ordered_sha256',
          effective.current_snapshot->>'json_ordered_sha256',
        'current_snapshot_sha256',
          effective.current_snapshot->>'snapshot_sha256',
        'current_modified_at', effective.current_snapshot->>'modified_at',
        'components', jsonb_build_array('extracted_md', 'embedding_ft'),
        'reason_code', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || effective.ordinal::text,
        'operation_id_prefix', 'FLOW_IDENTITY_SCOPE_COMPENSATION:'
          || p_scope_id::text || ':' || effective.ordinal::text || ':',
        'requires_new_plan_freeze_approval', true,
        'automatic_retry', false
      ) as compensation_value,
      effective.causal_terminal_proof,
      coalesce(
        (effective.request_row).status
          not in ('completed', 'stale', 'failed'),
        false
      ) as is_pending
    from effective
    join base using (ordinal)
  ), aggregate_payload as (
    select
      count(*)::integer as target_count,
      count(*) filter (where causal_terminal_proof)::integer
        as completed_count,
      count(*) filter (
        where not causal_terminal_proof and is_pending
      )::integer as pending_count,
      count(*) filter (
        where not causal_terminal_proof and not is_pending
      )::integer as failed_count,
      coalesce(jsonb_agg(value order by ordinal), '[]'::jsonb) as targets,
      coalesce(jsonb_agg(compensation_value order by ordinal) filter (
        where not causal_terminal_proof and not is_pending
      ), '[]'::jsonb) as compensation_targets,
      coalesce((select process_count from scope_row), 0)::integer
        as expected_target_count
    from target_payload
  )
  select jsonb_build_object(
    'ok', aggregate_payload.target_count > 0
      and aggregate_payload.target_count
        = aggregate_payload.expected_target_count
      and aggregate_payload.completed_count
        + aggregate_payload.pending_count
        + aggregate_payload.failed_count
        = aggregate_payload.target_count
      and aggregate_payload.failed_count = 0,
    'schema_version', 'dataset-flow-identity-derivative-set-proof.v1',
    'scope_id', p_scope_id,
    'status', case
      when aggregate_payload.target_count = 0
        or aggregate_payload.target_count
          <> aggregate_payload.expected_target_count
        or aggregate_payload.completed_count
          + aggregate_payload.pending_count
          + aggregate_payload.failed_count
          <> aggregate_payload.target_count then 'failed'
      when aggregate_payload.failed_count > 0 then 'compensation_required'
      when aggregate_payload.pending_count > 0 then 'pending'
      else 'completed' end,
    'target_count', aggregate_payload.target_count,
    'completed_count', aggregate_payload.completed_count,
    'pending_count', aggregate_payload.pending_count,
    'failed_count', aggregate_payload.failed_count,
    'causal_terminal_proof', aggregate_payload.target_count > 0
      and aggregate_payload.target_count
        = aggregate_payload.expected_target_count
      and aggregate_payload.completed_count = aggregate_payload.target_count
      and aggregate_payload.pending_count = 0
      and aggregate_payload.failed_count = 0,
    'targets', aggregate_payload.targets,
    'compensation_targets', aggregate_payload.compensation_targets,
    'proof_sha256', util.dataset_flow_identity_sha256(
      aggregate_payload.targets
    )
  )
  from aggregate_payload
$_$;

ALTER FUNCTION "util"."read_dataset_flow_identity_derivative_set"("p_actor_user_id" "uuid", "p_scope_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."read_dataset_flow_identity_derivative_set"("p_actor_user_id" "uuid", "p_scope_id" "uuid") FROM PUBLIC;
