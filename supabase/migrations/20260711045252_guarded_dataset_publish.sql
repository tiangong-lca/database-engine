create unique index command_audit_log_support_approval_replay_idx
  on public.command_audit_log (
    actor_user_id,
    target_table,
    target_id,
    target_version,
    (payload ->> 'plan_sha256'),
    (payload ->> 'operation_id'),
    (payload ->> 'action_id')
  )
  where command = 'cmd_dataset_support_approve_guarded';

create or replace function public.cmd_dataset_support_approve_guarded(
  p_table text,
  p_id uuid,
  p_version text,
  p_expected_modified_at timestamptz,
  p_expected_json_ordered jsonb,
  p_audit jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $$
declare
  v_actor uuid := auth.uid();
  v_current_reviewer_email text;
  v_recorded_reviewer_email text;
  v_owner uuid;
  v_state_code integer;
  v_modified_at timestamptz;
  v_json_ordered jsonb;
  v_current_row jsonb;
  v_plan_sha256 text;
  v_operation_id text;
  v_action_id text;
  v_audit_payload jsonb;
  v_prior_audit_id bigint;
  v_audit_id bigint;
  v_mismatches text[] := array[]::text[];
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can approve support publication'
    );
  end if;

  select lower(nullif(btrim(auth_user.email), ''))
    into v_current_reviewer_email
    from auth.users as auth_user
   where auth_user.id = v_actor
     and auth_user.email_confirmed_at is not null;

  if v_current_reviewer_email is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_EMAIL_REQUIRED',
      'status', 409,
      'message', 'The authenticated reviewer must have a verified account email'
    );
  end if;

  if p_table is null or p_table not in ('unitgroups', 'flowproperties') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Guarded support approval supports only unitgroups and flowproperties'
    );
  end if;

  if p_expected_modified_at is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_EXPECTED_MODIFIED_AT_REQUIRED',
      'status', 400,
      'message', 'expectedModifiedAt is required'
    );
  end if;

  if p_expected_json_ordered is null
    or jsonb_typeof(p_expected_json_ordered) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_EXPECTED_JSON_ORDERED_INVALID',
      'status', 400,
      'message', 'expectedJsonOrdered must be a JSON object'
    );
  end if;

  if jsonb_typeof(p_audit) is distinct from 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_AUDIT_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'A plan/action audit correlation object is required'
    );
  end if;

  v_plan_sha256 := nullif(btrim(p_audit->>'plan_sha256'), '');
  v_operation_id := nullif(btrim(p_audit->>'operation_id'), '');
  v_action_id := nullif(btrim(p_audit->>'action_id'), '');
  if v_plan_sha256 is null
    or v_plan_sha256 !~ '^[a-f0-9]{64}$'
    or v_operation_id is null
    or octet_length(v_operation_id) > 512
    or v_action_id is null
    or octet_length(v_action_id) > 512 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_AUDIT_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'plan_sha256, operation_id, and action_id are required for support approval'
    );
  end if;

  execute format(
    'select t.user_id,
            t.state_code,
            t.modified_at,
            t.json_ordered::jsonb,
            to_jsonb(t)
       from public.%I as t
      where t.id = $1
        and t.version = $2
      for share of t',
    p_table
  )
    into v_owner, v_state_code, v_modified_at, v_json_ordered, v_current_row
    using p_id, p_version;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_owner is null or v_owner = v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'INDEPENDENT_REVIEWER_REQUIRED',
      'status', 409,
      'message', 'Support publication must be approved by a review admin other than the dataset owner'
    );
  end if;

  if v_state_code is distinct from 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_REQUIRES_DRAFT',
      'status', 409,
      'message', 'Support approval requires state_code=0',
      'details', jsonb_build_object('state_code', v_state_code)
    );
  end if;

  if v_modified_at is distinct from p_expected_modified_at then
    v_mismatches := array_append(v_mismatches, 'modified_at');
  end if;

  if v_json_ordered is distinct from p_expected_json_ordered then
    v_mismatches := array_append(v_mismatches, 'json_ordered');
  end if;

  if cardinality(v_mismatches) > 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_SUPPORT_APPROVAL_PRECONDITION_FAILED',
      'status', 409,
      'message', 'Dataset changed after the support approval request was planned',
      'details', jsonb_build_object(
        'mismatches', to_jsonb(v_mismatches),
        'expected_modified_at', to_jsonb(p_expected_modified_at),
        'actual_modified_at', to_jsonb(v_modified_at)
      )
    );
  end if;

  v_audit_payload := p_audit || jsonb_build_object(
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'action_id', v_action_id,
    'decision', 'approved_for_publication',
    'reviewer_role', 'review-admin',
    'reviewer_email', v_current_reviewer_email,
    'target_owner_user_id', v_owner,
    'expected_modified_at', p_expected_modified_at,
    'expected_json_ordered', p_expected_json_ordered
  );

  select
    audit_log.id,
    audit_log.payload->>'reviewer_email'
    into v_prior_audit_id, v_recorded_reviewer_email
    from public.command_audit_log as audit_log
   where audit_log.command = 'cmd_dataset_support_approve_guarded'
     and audit_log.actor_user_id = v_actor
     and audit_log.target_table = p_table
     and audit_log.target_id = p_id
     and audit_log.target_version = p_version
     and audit_log.payload->>'plan_sha256' = v_plan_sha256
     and audit_log.payload->>'operation_id' = v_operation_id
     and audit_log.payload->>'action_id' = v_action_id
     and audit_log.payload->>'decision' = 'approved_for_publication'
     and audit_log.payload->>'reviewer_role' = 'review-admin'
     and audit_log.payload->>'target_owner_user_id' = v_owner::text
     and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
     and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered
   order by audit_log.id desc
   limit 1;

  if v_prior_audit_id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'approval_audit_id', v_prior_audit_id::text,
        'reviewer_user_id', v_actor,
        'reviewer_email', v_recorded_reviewer_email,
        'target_owner_user_id', v_owner,
        'target', v_current_row
      ),
      'audit_id', v_prior_audit_id::text,
      'idempotent_replay', true
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_support_approve_guarded',
    v_actor,
    p_table,
    p_id,
    p_version,
    v_audit_payload
  )
  on conflict do nothing
  returning id into v_audit_id;

  if v_audit_id is null then
    select
      audit_log.id,
      audit_log.payload->>'reviewer_email'
      into v_prior_audit_id, v_recorded_reviewer_email
      from public.command_audit_log as audit_log
     where audit_log.command = 'cmd_dataset_support_approve_guarded'
       and audit_log.actor_user_id = v_actor
       and audit_log.target_table = p_table
       and audit_log.target_id = p_id
       and audit_log.target_version = p_version
       and audit_log.payload->>'plan_sha256' = v_plan_sha256
       and audit_log.payload->>'operation_id' = v_operation_id
       and audit_log.payload->>'action_id' = v_action_id
       and audit_log.payload->>'decision' = 'approved_for_publication'
       and audit_log.payload->>'reviewer_role' = 'review-admin'
       and audit_log.payload->>'target_owner_user_id' = v_owner::text
       and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
       and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered
     limit 1;

    if v_prior_audit_id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_SUPPORT_APPROVAL_CORRELATION_CONFLICT',
        'status', 409,
        'message', 'This plan action is already bound to a different support approval snapshot'
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'approval_audit_id', v_prior_audit_id::text,
        'reviewer_user_id', v_actor,
        'reviewer_email', v_recorded_reviewer_email,
        'target_owner_user_id', v_owner,
        'target', v_current_row
      ),
      'audit_id', v_prior_audit_id::text,
      'idempotent_replay', true
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'approval_audit_id', v_audit_id::text,
      'reviewer_user_id', v_actor,
      'reviewer_email', v_current_reviewer_email,
      'target_owner_user_id', v_owner,
      'target', v_current_row
    ),
    'audit_id', v_audit_id::text,
    'idempotent_replay', false
  );
end;
$$;

alter function public.cmd_dataset_support_approve_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  owner to postgres;

revoke all on function public.cmd_dataset_support_approve_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  from public, anon, authenticated, service_role;

grant execute on function public.cmd_dataset_support_approve_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  to authenticated, service_role;

comment on function public.cmd_dataset_support_approve_guarded(text, uuid, text, timestamptz, jsonb, jsonb) is
  'Records an immutable independent review-admin approval for one exact owner-draft unit group or flow property publication plan action.';

create index command_audit_log_guarded_publish_replay_idx
  on public.command_audit_log (
    actor_user_id,
    target_table,
    target_id,
    target_version,
    (payload ->> 'plan_sha256'),
    (payload ->> 'operation_id'),
    (payload ->> 'action_id'),
    (payload ->> 'approval_audit_id'),
    id desc
  )
  where command = 'cmd_dataset_publish_guarded';

create or replace function public.cmd_dataset_publish_guarded(
  p_table text,
  p_id uuid,
  p_version text,
  p_expected_modified_at timestamptz,
  p_expected_json_ordered jsonb,
  p_audit jsonb
) returns jsonb
language plpgsql
security definer
set search_path to 'public', 'pg_temp'
as $$
declare
  v_actor uuid := auth.uid();
  v_state_code integer;
  v_modified_at timestamptz;
  v_json_ordered jsonb;
  v_current_row jsonb;
  v_updated_row jsonb;
  v_plan_sha256 text;
  v_operation_id text;
  v_action_id text;
  v_approval_audit_id_text text;
  v_expected_approval_reviewer_user_id text;
  v_expected_approval_reviewer_email text;
  v_approval_audit_id bigint;
  v_approval_reviewer_id uuid;
  v_approval_reviewer_email text;
  v_audit_payload jsonb;
  v_prior_audit_id bigint;
  v_audit_id bigint;
  v_mismatches text[] := array[]::text[];
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table is null or p_table not in ('unitgroups', 'flowproperties') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Guarded publish supports only unitgroups and flowproperties'
    );
  end if;

  if p_expected_modified_at is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_EXPECTED_MODIFIED_AT_REQUIRED',
      'status', 400,
      'message', 'expectedModifiedAt is required'
    );
  end if;

  if p_expected_json_ordered is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_EXPECTED_JSON_ORDERED_REQUIRED',
      'status', 400,
      'message', 'expectedJsonOrdered is required'
    );
  end if;

  if jsonb_typeof(p_expected_json_ordered) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_EXPECTED_JSON_ORDERED_INVALID',
      'status', 400,
      'message', 'expectedJsonOrdered must be a JSON object'
    );
  end if;

  if jsonb_typeof(p_audit) is distinct from 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_AUDIT_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'A plan/action audit correlation object is required'
    );
  end if;

  v_plan_sha256 := nullif(btrim(p_audit->>'plan_sha256'), '');
  v_operation_id := nullif(btrim(p_audit->>'operation_id'), '');
  v_action_id := nullif(btrim(p_audit->>'action_id'), '');
  v_approval_audit_id_text := nullif(btrim(p_audit->>'approval_audit_id'), '');
  v_expected_approval_reviewer_user_id := nullif(
    btrim(p_audit->>'approval_reviewer_user_id'),
    ''
  );
  v_expected_approval_reviewer_email := lower(nullif(
    btrim(p_audit->>'approval_reviewer_email'),
    ''
  ));

  if v_plan_sha256 is null
    or v_plan_sha256 !~ '^[a-f0-9]{64}$'
    or v_operation_id is null
    or octet_length(v_operation_id) > 512
    or v_action_id is null
    or octet_length(v_action_id) > 512 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_AUDIT_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'plan_sha256, operation_id, and action_id are required for guarded publish'
    );
  end if;

  execute format(
    'select t.state_code,
            t.modified_at,
            t.json_ordered::jsonb,
            to_jsonb(t)
       from public.%I as t
      where t.id = $1
        and t.version = $2
        and t.user_id = $3
      for update of t',
    p_table
  )
    into v_state_code, v_modified_at, v_json_ordered, v_current_row
    using p_id, p_version, v_actor;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_state_code = 100 then
    if v_json_ordered is distinct from p_expected_json_ordered then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_PUBLISH_REPLAY_PAYLOAD_MISMATCH',
        'status', 409,
        'message', 'Published dataset payload does not match the guarded action payload'
      );
    end if;
  else
    if v_state_code is distinct from 0 then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_PUBLISH_REQUIRES_DRAFT',
        'status', 409,
        'message', 'Guarded publish requires state_code=0',
        'details', jsonb_build_object('state_code', v_state_code)
      );
    end if;

    if v_modified_at is distinct from p_expected_modified_at then
      v_mismatches := array_append(v_mismatches, 'modified_at');
    end if;

    if v_json_ordered is distinct from p_expected_json_ordered then
      v_mismatches := array_append(v_mismatches, 'json_ordered');
    end if;

    if cardinality(v_mismatches) > 0 then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_PUBLISH_PRECONDITION_FAILED',
        'status', 409,
        'message', 'Dataset changed after the guarded publish action was planned',
        'details', jsonb_build_object(
          'mismatches', to_jsonb(v_mismatches),
          'expected_modified_at', to_jsonb(p_expected_modified_at),
          'actual_modified_at', to_jsonb(v_modified_at)
        )
      );
    end if;

  end if;

  if v_approval_audit_id_text is null
    or jsonb_typeof(p_audit->'approval_audit_id') is distinct from 'string'
    or octet_length(v_approval_audit_id_text) > 18
    or v_approval_audit_id_text !~ '^[1-9][0-9]*$'
    or v_expected_approval_reviewer_user_id is null
    or jsonb_typeof(p_audit->'approval_reviewer_user_id') is distinct from 'string'
    or octet_length(v_expected_approval_reviewer_user_id) > 36
    or v_expected_approval_reviewer_email is null
    or jsonb_typeof(p_audit->'approval_reviewer_email') is distinct from 'string'
    or octet_length(v_expected_approval_reviewer_email) > 320 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_APPROVAL_REQUIRED',
      'status', 409,
      'message', 'A durable approval audit id and its exact reviewer identity are required'
    );
  end if;

  v_approval_audit_id := v_approval_audit_id_text::bigint;

  v_audit_payload := p_audit || jsonb_build_object(
    'plan_sha256', v_plan_sha256,
    'operation_id', v_operation_id,
    'action_id', v_action_id,
    'approval_audit_id', v_approval_audit_id::text,
    'expected_modified_at', p_expected_modified_at,
    'expected_json_ordered', p_expected_json_ordered
  );

  select audit_log.id
    into v_prior_audit_id
    from public.command_audit_log as audit_log
   where audit_log.command = 'cmd_dataset_publish_guarded'
     and audit_log.actor_user_id = v_actor
     and audit_log.target_table = p_table
     and audit_log.target_id = p_id
     and audit_log.target_version = p_version
     and audit_log.payload->>'plan_sha256' = v_plan_sha256
     and audit_log.payload->>'operation_id' = v_operation_id
     and audit_log.payload->>'action_id' = v_action_id
     and audit_log.payload->>'approval_audit_id' = v_approval_audit_id::text
     and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
     and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered
   order by audit_log.id desc
   limit 1;

  if v_state_code = 0 and v_prior_audit_id is not null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_AUDIT_STATE_CONFLICT',
      'status', 409,
      'message', 'A matching committed audit record exists but the dataset is still a draft'
    );
  end if;

  select
    audit_log.actor_user_id,
    audit_log.payload->>'reviewer_email'
    into v_approval_reviewer_id, v_approval_reviewer_email
    from public.command_audit_log as audit_log
   where audit_log.id = v_approval_audit_id
     and audit_log.command = 'cmd_dataset_support_approve_guarded'
     and audit_log.actor_user_id <> v_actor
     and public.cmd_review_is_review_admin(audit_log.actor_user_id)
     and audit_log.target_table = p_table
     and audit_log.target_id = p_id
     and audit_log.target_version = p_version
     and audit_log.payload->>'plan_sha256' = v_plan_sha256
     and audit_log.payload->>'operation_id' = v_operation_id
     and audit_log.payload->>'action_id' = v_action_id
     and audit_log.payload->>'decision' = 'approved_for_publication'
     and audit_log.payload->>'reviewer_role' = 'review-admin'
     and nullif(btrim(audit_log.payload->>'reviewer_email'), '') is not null
     and audit_log.payload->>'target_owner_user_id' = v_actor::text
     and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
     and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered;

  if v_approval_reviewer_id is null
    or v_approval_reviewer_id::text <> v_expected_approval_reviewer_user_id
    or lower(v_approval_reviewer_email) <> v_expected_approval_reviewer_email then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_APPROVAL_INVALID',
      'status', 409,
      'message', 'The support approval is missing, stale, forged, unauthorized, or bound to a different action'
    );
  end if;

  v_audit_payload := v_audit_payload || jsonb_build_object(
    'approval_audit_id', v_approval_audit_id::text,
    'approval_reviewer_user_id', v_approval_reviewer_id,
    'approval_reviewer_email', v_approval_reviewer_email
  );

  if v_state_code = 100 then
    if v_prior_audit_id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'DATASET_PUBLISH_REPLAY_UNPROVEN',
        'status', 409,
        'message', 'Published dataset has no matching committed guarded-publish audit record'
      );
    end if;

    return jsonb_build_object(
      'ok', true,
      'data', v_current_row,
      'audit_id', v_prior_audit_id::text,
      'approval_audit_id', v_approval_audit_id::text,
      'approval_reviewer_user_id', v_approval_reviewer_id,
      'approval_reviewer_email', v_approval_reviewer_email,
      'idempotent_replay', true
    );
  end if;
  execute format(
    'update public.%I as t
        set state_code = 100,
            modified_at = now()
      where t.id = $1
        and t.version = $2
        and t.user_id = $3
    returning to_jsonb(t)',
    p_table
  )
    into v_updated_row
    using p_id, p_version, v_actor;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_publish_guarded',
    v_actor,
    p_table,
    p_id,
    p_version,
    v_audit_payload
  )
  returning id into v_audit_id;

  return jsonb_build_object(
    'ok', true,
    'data', v_updated_row,
    'audit_id', v_audit_id::text,
    'approval_audit_id', v_approval_audit_id::text,
    'approval_reviewer_user_id', v_approval_reviewer_id,
    'approval_reviewer_email', v_approval_reviewer_email,
    'idempotent_replay', false
  );
end;
$$;

alter function public.cmd_dataset_publish_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  owner to postgres;

revoke all on function public.cmd_dataset_publish_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  from public, anon, authenticated, service_role;

grant execute on function public.cmd_dataset_publish_guarded(text, uuid, text, timestamptz, jsonb, jsonb)
  to authenticated, service_role;

comment on function public.cmd_dataset_publish_guarded(text, uuid, text, timestamptz, jsonb, jsonb) is
  'Publishes an owned draft unit group or flow property only with an exact independent review-admin approval, locked optimistic preconditions, and audit-proven idempotent replay.';

create or replace function public.qry_dataset_publish_guarded_proof(
  p_table text,
  p_id uuid,
  p_version text,
  p_expected_modified_at timestamptz,
  p_expected_json_ordered jsonb,
  p_audit jsonb
) returns jsonb
language plpgsql
stable
security definer
set search_path to 'public', 'pg_temp'
as $$
declare
  v_actor uuid := auth.uid();
  v_state_code integer;
  v_json_ordered jsonb;
  v_current_row jsonb;
  v_plan_sha256 text;
  v_operation_id text;
  v_action_id text;
  v_approval_audit_id_text text;
  v_publish_audit_id_text text;
  v_approval_audit_id bigint;
  v_publish_audit_id bigint;
  v_publish_reviewer_user_id text;
  v_publish_reviewer_email text;
  v_approval_reviewer_user_id text;
  v_approval_reviewer_email text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table is null or p_table not in ('unitgroups', 'flowproperties') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Guarded publish proof supports only unitgroups and flowproperties'
    );
  end if;

  if p_expected_modified_at is null
    or p_expected_json_ordered is null
    or jsonb_typeof(p_expected_json_ordered) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_SNAPSHOT_REQUIRED',
      'status', 400,
      'message', 'The exact planned modified_at and JSON object are required'
    );
  end if;

  if jsonb_typeof(p_audit) is distinct from 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'The exact approval and publication audit correlation is required'
    );
  end if;

  v_plan_sha256 := nullif(btrim(p_audit->>'plan_sha256'), '');
  v_operation_id := nullif(btrim(p_audit->>'operation_id'), '');
  v_action_id := nullif(btrim(p_audit->>'action_id'), '');
  v_approval_audit_id_text := nullif(btrim(p_audit->>'approval_audit_id'), '');
  v_publish_audit_id_text := nullif(btrim(p_audit->>'publish_audit_id'), '');

  if v_plan_sha256 is null
    or v_plan_sha256 !~ '^[a-f0-9]{64}$'
    or v_operation_id is null
    or octet_length(v_operation_id) > 512
    or v_action_id is null
    or octet_length(v_action_id) > 512
    or v_approval_audit_id_text is null
    or jsonb_typeof(p_audit->'approval_audit_id') is distinct from 'string'
    or octet_length(v_approval_audit_id_text) > 18
    or v_approval_audit_id_text !~ '^[1-9][0-9]*$'
    or v_publish_audit_id_text is null
    or jsonb_typeof(p_audit->'publish_audit_id') is distinct from 'string'
    or octet_length(v_publish_audit_id_text) > 18
    or v_publish_audit_id_text !~ '^[1-9][0-9]*$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_CORRELATION_REQUIRED',
      'status', 400,
      'message', 'plan_sha256, operation_id, action_id, approval_audit_id, and publish_audit_id are required'
    );
  end if;

  v_approval_audit_id := v_approval_audit_id_text::bigint;
  v_publish_audit_id := v_publish_audit_id_text::bigint;

  execute format(
    'select t.state_code,
            t.json_ordered::jsonb,
            to_jsonb(t)
       from public.%I as t
      where t.id = $1
        and t.version = $2
        and t.user_id = $3',
    p_table
  )
    into v_state_code, v_json_ordered, v_current_row
    using p_id, p_version, v_actor;

  if v_current_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_state_code is distinct from 100
    or v_json_ordered is distinct from p_expected_json_ordered then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_STATE_MISMATCH',
      'status', 409,
      'message', 'The owner dataset is not the exact published payload from the frozen action'
    );
  end if;

  select
    audit_log.payload->>'approval_reviewer_user_id',
    audit_log.payload->>'approval_reviewer_email'
    into v_publish_reviewer_user_id, v_publish_reviewer_email
    from public.command_audit_log as audit_log
   where audit_log.id = v_publish_audit_id
     and audit_log.command = 'cmd_dataset_publish_guarded'
     and audit_log.actor_user_id = v_actor
     and audit_log.target_table = p_table
     and audit_log.target_id = p_id
     and audit_log.target_version = p_version
     and audit_log.payload->>'plan_sha256' = v_plan_sha256
     and audit_log.payload->>'operation_id' = v_operation_id
     and audit_log.payload->>'action_id' = v_action_id
     and audit_log.payload->>'approval_audit_id' = v_approval_audit_id::text
     and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
     and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered
     and nullif(btrim(audit_log.payload->>'approval_reviewer_user_id'), '') is not null
     and nullif(btrim(audit_log.payload->>'approval_reviewer_email'), '') is not null;

  if v_publish_reviewer_user_id is null or v_publish_reviewer_email is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_INVALID',
      'status', 409,
      'message', 'No exact committed guarded publication audit proof exists'
    );
  end if;

  select
    audit_log.actor_user_id::text,
    audit_log.payload->>'reviewer_email'
    into v_approval_reviewer_user_id, v_approval_reviewer_email
    from public.command_audit_log as audit_log
   where audit_log.id = v_approval_audit_id
     and audit_log.command = 'cmd_dataset_support_approve_guarded'
     and audit_log.actor_user_id::text = v_publish_reviewer_user_id
     and audit_log.actor_user_id <> v_actor
     and audit_log.target_table = p_table
     and audit_log.target_id = p_id
     and audit_log.target_version = p_version
     and audit_log.payload->>'plan_sha256' = v_plan_sha256
     and audit_log.payload->>'operation_id' = v_operation_id
     and audit_log.payload->>'action_id' = v_action_id
     and audit_log.payload->>'decision' = 'approved_for_publication'
     and audit_log.payload->>'reviewer_role' = 'review-admin'
     and audit_log.payload->>'reviewer_email' = v_publish_reviewer_email
     and audit_log.payload->>'target_owner_user_id' = v_actor::text
     and audit_log.payload->'expected_modified_at' = to_jsonb(p_expected_modified_at)
     and audit_log.payload->'expected_json_ordered' = p_expected_json_ordered;

  if v_approval_reviewer_user_id is null
    or v_approval_reviewer_email is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_PUBLISH_PROOF_INVALID',
      'status', 409,
      'message', 'The publication audit is not bound to an exact independent approval audit'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'proof_verified', true,
      'publish_audit_id', v_publish_audit_id::text,
      'approval_audit_id', v_approval_audit_id::text,
      'approval_reviewer_user_id', v_approval_reviewer_user_id,
      'approval_reviewer_email', v_approval_reviewer_email,
      'target', v_current_row
    )
  );
end;
$$;

alter function public.qry_dataset_publish_guarded_proof(text, uuid, text, timestamptz, jsonb, jsonb)
  owner to postgres;

revoke all on function public.qry_dataset_publish_guarded_proof(text, uuid, text, timestamptz, jsonb, jsonb)
  from public, anon, authenticated, service_role;

grant execute on function public.qry_dataset_publish_guarded_proof(text, uuid, text, timestamptz, jsonb, jsonb)
  to authenticated;

comment on function public.qry_dataset_publish_guarded_proof(text, uuid, text, timestamptz, jsonb, jsonb) is
  'Read-only owner-scoped proof that an exact published support payload is bound to one exact independent approval audit and guarded publication audit.';
