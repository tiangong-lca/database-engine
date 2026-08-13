CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_scope_read_core_v1"("p_scope_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_scope util.dataset_flow_identity_scopes%rowtype;
  v_processes jsonb;
  v_protected_proof jsonb;
  v_derivative_set_proof jsonb;
  v_completed integer;
  v_pending integer;
  v_failed integer;
  v_derivative_pending integer;
  v_derivative_failed integer;
  v_compensation_targets jsonb := '[]'::jsonb;
  v_next_ordinal integer;
  v_status text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'AUTH_REQUIRED', 'status', 401,
      'message', 'Authentication required'
    );
  end if;
  if p_scope_id is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'FLOW_IDENTITY_SCOPE_READ_INVALID_REQUEST', 'status', 400,
      'message', 'Scope ID is required'
    );
  end if;

  select scope.*
  into v_scope
  from util.dataset_flow_identity_scopes as scope
  where scope.id = p_scope_id and scope.actor_user_id = v_actor;
  if v_scope.id is null then
    return jsonb_build_object(
      'ok', false, 'command', 'cmd_dataset_flow_identity_scope_read',
      'code', 'FLOW_IDENTITY_SCOPE_NOT_FOUND', 'status', 404,
      'message', 'No actor-owned Step 3 scope exists'
    );
  end if;

  select
    count(*) filter (where ledger.status = 'completed')::integer,
    count(*) filter (where ledger.status = 'pending')::integer,
    count(*) filter (where ledger.status = 'failed')::integer,
    count(*) filter (
      where child.status is not null
        and child.status not in ('completed', 'stale', 'failed')
    )::integer,
    count(*) filter (where child.status in ('stale', 'failed'))::integer,
    min(ledger.ordinal) filter (where ledger.status = 'pending')::integer,
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', ledger.ordinal,
      'id', ledger.process_id,
      'version', ledger.process_version,
      'status', ledger.status,
      'process_request_sha256', ledger.process_request_sha256,
      'rewrite_count', ledger.rewrite_count,
      'audit_id', case when ledger.audit_id is null
        then null else ledger.audit_id::text end,
      'before_payload_sha256', ledger.before_payload_sha256,
      'after_payload_sha256', ledger.after_payload_sha256,
      'derivative_batch_id', ledger.derivative_batch_id,
      'derivative_request_id', child.id,
      'derivative_status', case when ledger.derivative_batch_id is null
        then null else coalesce(child.status, 'missing') end,
      'causal_terminal_proof', false,
      'completed_at', ledger.completed_at,
      'last_error', coalesce(ledger.last_error, child.last_error)
    ) order by ledger.ordinal), '[]'::jsonb)
  into
    v_completed, v_pending, v_failed, v_derivative_pending,
    v_derivative_failed, v_next_ordinal, v_processes
  from util.dataset_flow_identity_process_ledger as ledger
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version
  where ledger.scope_id = p_scope_id;

  v_derivative_set_proof :=
    util.read_dataset_flow_identity_derivative_set(v_actor, p_scope_id);
  v_derivative_pending := coalesce(
    (v_derivative_set_proof->>'pending_count')::integer, 0
  );
  v_derivative_failed := coalesce(
    (v_derivative_set_proof->>'failed_count')::integer, 0
  );

  select coalesce(jsonb_agg(
    target.value || jsonb_build_object(
      'original_request_id', child.id,
      'original_error', child.last_error
    ) order by (target.value->>'ordinal')::integer
  ), '[]'::jsonb)
  into v_compensation_targets
  from jsonb_array_elements(
    coalesce(
      v_derivative_set_proof->'compensation_targets', '[]'::jsonb
    )
  ) as target(value)
  join util.dataset_flow_identity_process_ledger as ledger
    on ledger.scope_id = p_scope_id
    and ledger.ordinal = (target.value->>'ordinal')::integer
  left join util.dataset_derivative_rebuild_requests as child
    on child.actor_user_id = v_actor
    and child.batch_id = ledger.derivative_batch_id
    and child.target_table = 'processes'
    and child.target_id = ledger.process_id
    and child.target_version = ledger.process_version;

  v_protected_proof := util.dataset_flow_identity_protected_closure(
    v_actor, v_scope.protected_closure
  );
  v_status := case
    when v_scope.status = 'cancelled' then 'cancelled'
    when v_scope.status = 'failed' or v_failed > 0 then 'failed'
    when v_completed = 0 then 'sealed'
    when v_pending > 0 then 'running'
    when v_completed = v_scope.process_count
      and coalesce(
        (v_derivative_set_proof->>'causal_terminal_proof')::boolean,
        false
      )
      then case when v_scope.status = 'completed'
        then 'completed' else 'primary_complete' end
    when v_completed = v_scope.process_count then 'derivatives_pending'
    else v_scope.status
  end;

  return jsonb_build_object(
    'ok', v_status <> 'failed',
    'command', 'cmd_dataset_flow_identity_scope_read',
    'schema_version', 'dataset-flow-identity-scope-status.v1',
    'scope_id', v_scope.id,
    'operation_id', v_scope.operation_id,
    'plan_sha256', v_scope.plan_sha256,
    'scope_proof_sha256', v_scope.scope_proof_sha256,
    'status', v_status,
    'process_count', v_scope.process_count,
    'completed_process_count', v_completed,
    'pending_process_count', v_pending,
    'failed_process_count', v_failed,
    'next_ordinal', coalesce(v_next_ordinal, v_scope.process_count + 1),
    'rewrite_count', v_scope.rewrite_count,
    'completed_rewrite_count', coalesce((
      select sum(ledger.rewrite_count)::integer
      from util.dataset_flow_identity_process_ledger as ledger
      where ledger.scope_id = p_scope_id and ledger.status = 'completed'
    ), 0),
    'primary_complete', v_completed = v_scope.process_count,
    'cancellable', v_completed = 0
      and v_scope.status not in ('completed', 'cancelled', 'failed'),
    'strict_continuation_required', v_completed > 0
      and v_completed < v_scope.process_count,
    'derivatives_current', v_status = 'completed',
    'derivative_pending_count', v_derivative_pending,
    'derivative_failed_count', v_derivative_failed,
    'derivative_set_proof', v_derivative_set_proof,
    'derivative_proof_set_sha256',
      v_derivative_set_proof->>'proof_sha256',
    'compensation_required', v_derivative_failed > 0,
    'compensation_targets', v_compensation_targets,
    'protected_closure_current', coalesce(
      (v_protected_proof->>'ok')::boolean, false
    ),
    'protected_closure_proof', v_protected_proof,
    'processes', v_processes,
    'terminal_proof_sha256', case when v_status = 'completed'
      then v_scope.terminal_proof_sha256 else null end,
    'completed_at', case when v_status = 'completed'
      then v_scope.completed_at else null end
  ) || case when v_derivative_failed > 0 then jsonb_build_object(
    'code', 'FLOW_IDENTITY_DERIVATIVE_COMPENSATION_REQUIRED'
  ) else '{}'::jsonb end;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_scope_read_core_v1"("p_scope_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_scope_read_core_v1"("p_scope_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_scope_read_core_v1"("p_scope_id" "uuid") TO "api_internal_executor";
