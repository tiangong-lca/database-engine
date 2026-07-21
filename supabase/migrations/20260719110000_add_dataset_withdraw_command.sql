create or replace function public.cmd_dataset_withdraw(
  p_table text,
  p_id uuid,
  p_version text,
  p_reason text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_current_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_updated_row jsonb;
  v_reason text := btrim(coalesce(p_reason, ''));
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table not in ('sources', 'flows', 'processes') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Only sources, flows, and processes can be withdrawn'
    );
  end if;

  if v_reason = '' or length(v_reason) > 4000 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WITHDRAW_REASON',
      'status', 400,
      'message', 'A non-empty withdrawal reason of at most 4000 characters is required'
    );
  end if;

  if jsonb_typeof(coalesce(p_audit, '{}'::jsonb)) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_AUDIT_PAYLOAD',
      'status', 400,
      'message', 'Audit payload must be a JSON object'
    );
  end if;

  execute format(
    'select to_jsonb(t)
       from public.%I as t
      where t.id = $1
        and t.version = $2
      for update of t',
    p_table
  )
    into v_current_row
    using p_id, p_version;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_current_row->>'user_id', '')::uuid;
  v_state_code := coalesce((v_current_row->>'state_code')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can withdraw this dataset'
    );
  end if;

  if v_state_code = 0 then
    return jsonb_build_object(
      'ok', true,
      'changed', false,
      'code', 'DATASET_ALREADY_DRAFT',
      'data', v_current_row
    );
  end if;

  if v_state_code < 100 or v_state_code >= 200 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_WITHDRAW_REQUIRES_PUBLISHED_STATE',
      'status', 403,
      'message', 'Only public datasets in state 100 through 199 can be withdrawn',
      'details', jsonb_build_object(
        'state_code', v_state_code
      )
    );
  end if;

  execute format(
    'update public.%I as t
        set state_code = 0,
            rule_verification = false,
            modified_at = now()
      where t.id = $1
        and t.version = $2
    returning to_jsonb(t)',
    p_table
  )
    into v_updated_row
    using p_id, p_version;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_withdraw',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb)
      || jsonb_build_object(
        'reason', v_reason,
        'from_state_code', v_state_code,
        'to_state_code', 0
      )
  );

  return jsonb_build_object(
    'ok', true,
    'changed', true,
    'data', v_updated_row
  );
end;
$$;
comment on function public.cmd_dataset_withdraw(text, uuid, text, text, jsonb)
  is 'Owner-authenticated, audited withdrawal of public source, flow, or process datasets to draft state without changing dataset JSON.';
revoke all on function public.cmd_dataset_withdraw(text, uuid, text, text, jsonb) from public;
revoke all on function public.cmd_dataset_withdraw(text, uuid, text, text, jsonb) from anon;
grant execute on function public.cmd_dataset_withdraw(text, uuid, text, text, jsonb) to authenticated;
grant execute on function public.cmd_dataset_withdraw(text, uuid, text, text, jsonb) to service_role;
