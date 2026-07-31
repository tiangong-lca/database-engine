-- Atomic command surface for root/reference review v2.

create or replace function private.review_dataset_can_read_v1(
  p_actor uuid,
  p_target_table text,
  p_target_row jsonb
)
returns boolean
language sql
stable
security definer
set search_path = ''
as $$
  select p_actor is not null and (
    coalesce((p_target_row->>'state_code')::integer, 0) >= 100
    or nullif(p_target_row->>'user_id', '')::uuid = p_actor
    or exists (
      select 1
      from public.roles as membership
      where membership.user_id = p_actor
        and membership.team_id = nullif(p_target_row->>'team_id', '')::uuid
        and membership.role in ('owner', 'admin', 'member')
    )
  )
$$;

alter function private.review_dataset_can_read_v1(uuid, text, jsonb)
  owner to postgres;
revoke all on function private.review_dataset_can_read_v1(uuid, text, jsonb)
  from public, anon, authenticated, service_role;

create or replace function private.review_build_json_v1(
  p_target_table text,
  p_target_row jsonb,
  p_owner_id uuid,
  p_action text,
  p_actor uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_team_name jsonb;
  v_owner_meta jsonb;
  v_actor_meta jsonb;
begin
  select coalesce(team_row.json->'title', team_row.json->'name')
  into v_team_name
  from public.teams as team_row
  where team_row.id = nullif(p_target_row->>'team_id', '')::uuid;

  select user_row.raw_user_meta_data
  into v_owner_meta
  from public.users as user_row
  where user_row.id = p_owner_id;

  select user_row.raw_user_meta_data
  into v_actor_meta
  from public.users as user_row
  where user_row.id = p_actor;

  return jsonb_build_object(
    'review_kind', case
      when p_action = 'submit_reference_review' then 'reference'
      else 'root'
    end,
    'data', jsonb_build_object(
      'id', p_target_row->>'id',
      'version', p_target_row->>'version',
      'table', p_target_table,
      'name', public.cmd_review_get_dataset_name(
        p_target_table,
        p_target_row
      )
    ),
    'team', jsonb_build_object(
      'id', p_target_row->>'team_id',
      'name', v_team_name
    ),
    'user', jsonb_build_object(
      'id', p_owner_id,
      'name', coalesce(
        nullif(v_owner_meta->>'display_name', ''),
        nullif(v_owner_meta->>'email', '')
      ),
      'email', nullif(v_owner_meta->>'email', '')
    ),
    'comment', jsonb_build_object('message', ''),
    'logs', jsonb_build_array(jsonb_build_object(
      'action', p_action,
      'time', to_jsonb(now()),
      'user', jsonb_build_object(
        'id', p_actor,
        'display_name', coalesce(
          nullif(v_actor_meta->>'display_name', ''),
          nullif(v_actor_meta->>'email', '')
        )
      )
    ))
  );
end;
$$;

alter function private.review_build_json_v1(text, jsonb, uuid, text, uuid)
  owner to postgres;
revoke all on function private.review_build_json_v1(
  text, jsonb, uuid, text, uuid
) from public, anon, authenticated, service_role;

create or replace function private.review_notify_event_v1(
  p_event_type text,
  p_review_id uuid,
  p_recipient_user_id uuid,
  p_sender_user_id uuid,
  p_target_table text,
  p_target_id uuid,
  p_target_version text,
  p_root_review_id uuid default null,
  p_scope_version integer default null,
  p_reason_code text default null
)
returns text
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_event_key text;
begin
  if p_recipient_user_id is null then
    raise exception using
      errcode = '23502',
      message = 'NOTIFICATION_RECIPIENT_UNRESOLVED';
  end if;

  v_event_key := encode(
    extensions.digest(
      convert_to(
        concat_ws(
          '|',
          p_event_type,
          p_review_id::text,
          p_recipient_user_id::text,
          coalesce(p_root_review_id::text, ''),
          coalesce(p_scope_version::text, '')
        ),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  );

  insert into public.notifications (
    recipient_user_id,
    sender_user_id,
    type,
    dataset_type,
    dataset_id,
    dataset_version,
    json
  )
  values (
    p_recipient_user_id,
    coalesce(p_sender_user_id, p_recipient_user_id),
    'review_event',
    p_target_table,
    p_target_id,
    p_target_version,
    jsonb_strip_nulls(jsonb_build_object(
      'event_key', v_event_key,
      'event_type', p_event_type,
      'review_id', p_review_id,
      'root_review_id', p_root_review_id,
      'scope_version', p_scope_version,
      'reason_code', p_reason_code,
      'target_table', p_target_table,
      'target_id', p_target_id,
      'target_version', p_target_version
    ))
  )
  on conflict (
    recipient_user_id,
    type,
    (json->>'event_key')
  ) where nullif(json->>'event_key', '') is not null
  do nothing;

  return v_event_key;
end;
$$;

alter function private.review_notify_event_v1(
  text, uuid, uuid, uuid, text, uuid, text, uuid, integer, text
) owner to postgres;
revoke all on function private.review_notify_event_v1(
  text, uuid, uuid, uuid, text, uuid, text, uuid, integer, text
) from public, anon, authenticated, service_role;

create or replace function private.review_get_or_create_reference_v1(
  p_target_table text,
  p_target_row jsonb,
  p_checksum text,
  p_actor uuid
)
returns public.reviews
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_reference public.reviews%rowtype;
  v_owner_id uuid := nullif(p_target_row->>'user_id', '')::uuid;
  v_team_id uuid := nullif(p_target_row->>'team_id', '')::uuid;
  v_state integer := coalesce((p_target_row->>'state_code')::integer, 0);
begin
  if v_owner_id is null then
    raise exception using
      errcode = '23502',
      message = 'REFERENCE_OWNER_UNRESOLVED';
  end if;

  if exists (
    select 1
    from public.reviews as rejected
    where rejected.review_kind = 'reference'
      and rejected.target_table = p_target_table
      and rejected.data_id = (p_target_row->>'id')::uuid
      and btrim(rejected.data_version::text) = p_target_row->>'version'
      and rejected.submitted_revision_checksum = p_checksum
      and rejected.state_code = -1
  ) then
    raise exception using
      errcode = '23505',
      message = 'REFERENCE_REVISION_REJECTED_UNCHANGED';
  end if;

  select reference_row.*
  into v_reference
  from public.reviews as reference_row
  where reference_row.review_kind = 'reference'
    and reference_row.target_table = p_target_table
    and reference_row.data_id = (p_target_row->>'id')::uuid
    and btrim(reference_row.data_version::text) = p_target_row->>'version'
    and reference_row.submitted_revision_checksum = p_checksum
    and reference_row.state_code in (0, 1, 2)
  order by reference_row.state_code desc, reference_row.created_at
  limit 1
  for update;

  if found then
    return v_reference;
  end if;

  begin
    insert into public.reviews (
      id,
      data_id,
      data_version,
      state_code,
      reviewer_id,
      json,
      review_kind,
      target_table,
      submitted_revision_checksum,
      approved_revision_checksum,
      target_owner_id,
      target_team_id
    )
    values (
      gen_random_uuid(),
      (p_target_row->>'id')::uuid,
      p_target_row->>'version',
      case when v_state >= 100 then 2 else 0 end,
      '[]'::jsonb,
      private.review_build_json_v1(
        p_target_table,
        p_target_row,
        v_owner_id,
        'submit_reference_review',
        p_actor
      ),
      'reference',
      p_target_table,
      p_checksum,
      case when v_state >= 100 then p_checksum else null end,
      v_owner_id,
      v_team_id
    )
    returning * into v_reference;
  exception
    when unique_violation then
      select reference_row.*
      into strict v_reference
      from public.reviews as reference_row
      where reference_row.review_kind = 'reference'
        and reference_row.target_table = p_target_table
        and reference_row.data_id = (p_target_row->>'id')::uuid
        and btrim(reference_row.data_version::text) = p_target_row->>'version'
        and reference_row.submitted_revision_checksum = p_checksum
        and reference_row.state_code in (0, 1, 2)
      order by reference_row.state_code desc, reference_row.created_at
      limit 1
      for update;
  end;

  return v_reference;
end;
$$;

alter function private.review_get_or_create_reference_v1(
  text, jsonb, text, uuid
) owner to postgres;
revoke all on function private.review_get_or_create_reference_v1(
  text, jsonb, text, uuid
) from public, anon, authenticated, service_role;

create or replace function private.review_replace_reference_item_v1(
  p_items jsonb,
  p_target_table text,
  p_target_id uuid,
  p_target_version text,
  p_checksum text,
  p_reference_review_id uuid,
  p_owner_id uuid,
  p_team_id uuid
)
returns jsonb
language sql
immutable
set search_path = ''
as $$
  select coalesce(jsonb_agg(
    case
      when item.value->>'item_kind' = 'reference'
        and item.value->>'target_table' = p_target_table
        and item.value->>'data_id' = p_target_id::text
        and item.value->>'data_version' = p_target_version
      then item.value || jsonb_build_object(
        'submitted_revision_checksum', p_checksum,
        'reference_review_id', p_reference_review_id,
        'target_owner_id', p_owner_id,
        'target_team_id', p_team_id,
        'introduced_by', 'reference_repair'
      )
      else item.value
    end
    order by item.ordinality
  ), '[]'::jsonb)
  from jsonb_array_elements(coalesce(p_items, '[]'::jsonb))
    with ordinality as item(value, ordinality)
$$;

alter function private.review_replace_reference_item_v1(
  jsonb, text, uuid, text, text, uuid, uuid, uuid
) owner to postgres;
revoke all on function private.review_replace_reference_item_v1(
  jsonb, text, uuid, text, text, uuid, uuid, uuid
) from public, anon, authenticated, service_role;

create or replace function public.cmd_review_submit_v2(
  p_target_table text,
  p_target_id uuid,
  p_target_version text,
  p_gate_context jsonb default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_table text := lower(coalesce(p_target_table, ''));
  v_root_row jsonb;
  v_checksum text;
  v_gate_assertion jsonb;
  v_root_review_id uuid := gen_random_uuid();
  v_root_review public.reviews%rowtype;
  v_reference public.reviews%rowtype;
  v_target record;
  v_target_checksum text;
  v_items jsonb := '[]'::jsonb;
  v_item jsonb;
  v_scope_history jsonb;
  v_affected jsonb := '[]'::jsonb;
  v_reference_ids uuid[] := array[]::uuid[];
  v_old_reference_ids uuid[] := array[]::uuid[];
  v_impacted_root public.reviews%rowtype;
  v_current_snapshot jsonb;
  v_repair_items jsonb;
  v_conflict_version text;
  v_event_key text;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if v_table not in (
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties',
    'flows',
    'processes',
    'lifecyclemodels'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Unsupported dataset table for review submission'
    );
  end if;

  v_root_row := public.cmd_review_get_dataset_row(
    v_table,
    p_target_id,
    p_target_version,
    true
  );

  if v_root_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if nullif(v_root_row->>'user_id', '')::uuid is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROOT_DATASET_NOT_OWNED',
      'status', 403,
      'message', 'Only the dataset owner can submit review'
    );
  end if;

  if coalesce((v_root_row->>'state_code')::integer, 0) <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', case
        when coalesce((v_root_row->>'state_code')::integer, 0) = 100
          then 'APPROVED_DATASET_IMMUTABLE'
        else 'DATA_UNDER_REVIEW'
      end,
      'status', 409,
      'message', 'Only a Draft dataset can be submitted'
    );
  end if;

  v_checksum := public.review_revision_fingerprint_v1(v_table, v_root_row);

  if v_table = 'processes' then
    if nullif(p_gate_context->>'reviewSubmitJobId', '') is not null then
      if not exists (
        select 1
        from public.dataset_review_submit_requests as submit_job
        left join public.worker_jobs as gate_job
          on gate_job.id = submit_job.gate_worker_job_id
        where submit_job.id =
            (p_gate_context->>'reviewSubmitJobId')::uuid
          and submit_job.requested_by = v_actor
          and submit_job.dataset_table = v_table
          and submit_job.dataset_id = p_target_id
          and submit_job.dataset_version = p_target_version
          and submit_job.revision_checksum = v_checksum
          and submit_job.status in ('submitting', 'submitted')
          and (
            (
              submit_job.gate_worker_job_id is not null
              and gate_job.status = 'completed'
              and gate_job.result_json->>'status' = 'passed'
              and gate_job.result_json
                #>> '{datasetRevision,revisionChecksum}' = v_checksum
            )
            or submit_job.gate_run_id is not null
          )
      ) then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_SUBMIT_JOB_GATE_INVALID',
          'status', 409,
          'message', 'Review-submit job does not prove a passed Process Gate'
        );
      end if;
    else
      if p_gate_context is null
        or nullif(p_gate_context->>'reviewSubmitGateRunId', '') is null
        or nullif(p_gate_context->>'revisionChecksum', '') is null then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_GATE_REQUIRED',
          'status', 409,
          'message', 'Process review submission requires a passed Gate'
        );
      end if;

      if p_gate_context->>'revisionChecksum' <> v_checksum then
        return jsonb_build_object(
          'ok', false,
          'code', 'REVIEW_GATE_CHECKSUM_MISMATCH',
          'status', 409,
          'message', 'Gate checksum does not match the current Process revision'
        );
      end if;

      v_gate_assertion := public.cmd_dataset_assert_review_submit_gate_passed(
        v_table,
        p_target_id,
        p_target_version,
        (p_gate_context->>'reviewSubmitGateRunId')::uuid,
        p_gate_context->>'revisionChecksum',
        coalesce(
          nullif(p_gate_context->>'policyProfile', ''),
          'review_submit_fast.v1'
        ),
        coalesce(
          nullif(p_gate_context->>'reportSchemaVersion', ''),
          'review_submit_gate_report.v1'
        )
      );

      if coalesce((v_gate_assertion->>'ok')::boolean, false) is false then
        return v_gate_assertion;
      end if;
    end if;
  elsif p_gate_context is not null
    and p_gate_context <> '{}'::jsonb
    and p_gate_context <> 'null'::jsonb then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_GATE_NOT_APPLICABLE',
      'status', 400,
      'message', 'Gate context is only accepted for Process datasets'
    );
  end if;

  if exists (
    select 1
    from public.reviews as rejected
    where rejected.review_kind = 'reference'
      and rejected.target_table = v_table
      and rejected.data_id = p_target_id
      and btrim(rejected.data_version::text) = p_target_version
      and rejected.submitted_revision_checksum = v_checksum
      and rejected.state_code = -1
      and exists (
        select 1
        from public.reviews as root_review
        where root_review.review_kind = 'root'
          and root_review.current_reference_review_ids
            @> array[rejected.id]::uuid[]
      )
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REFERENCE_REVISION_REJECTED_UNCHANGED',
      'status', 409,
      'message', 'The rejected reference must be changed before resubmission'
    );
  end if;

  select coalesce(array_agg(rejected.id order by rejected.id), array[]::uuid[])
  into v_old_reference_ids
  from public.reviews as rejected
  where rejected.review_kind = 'reference'
    and rejected.target_table = v_table
    and rejected.data_id = p_target_id
    and btrim(rejected.data_version::text) = p_target_version
    and rejected.state_code = -1
    and exists (
      select 1
      from public.reviews as root_review
      where root_review.review_kind = 'root'
        and root_review.current_reference_review_ids
          @> array[rejected.id]::uuid[]
    );

  if cardinality(v_old_reference_ids) > 0 then
    v_reference := private.review_get_or_create_reference_v1(
      v_table,
      v_root_row,
      v_checksum,
      v_actor
    );

    perform set_config('app.review_controlled_write', 'on', true);
    execute format(
      'update public.%I
          set state_code = 20,
              modified_at = now()
        where id = $1
          and version = $2
          and state_code = 0',
      v_table
    ) using p_target_id, p_target_version;
    perform set_config('app.review_controlled_write', 'off', true);

    for v_impacted_root in
      select root_review.*
      from public.reviews as root_review
      where root_review.review_kind = 'root'
        and root_review.current_reference_review_ids && v_old_reference_ids
      order by root_review.id
      for update
    loop
      v_current_snapshot := public.review_scope_current_snapshot_v1(
        v_impacted_root.scope_history
      );
      v_repair_items := private.review_replace_reference_item_v1(
        v_current_snapshot->'items',
        v_table,
        p_target_id,
        p_target_version,
        v_checksum,
        v_reference.id,
        nullif(v_root_row->>'user_id', '')::uuid,
        nullif(v_root_row->>'team_id', '')::uuid
      );

      perform public.review_append_scope_snapshot_v1(
        v_impacted_root.id,
        'reference_repair',
        v_current_snapshot->>'root_revision_checksum',
        v_repair_items,
        v_actor
      );

      perform private.review_notify_event_v1(
        'reference_repaired',
        v_reference.id,
        v_impacted_root.target_owner_id,
        v_actor,
        v_table,
        p_target_id,
        p_target_version,
        v_impacted_root.id,
        (v_impacted_root.scope_history->>'current_version')::integer + 1,
        null
      );
    end loop;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_review_submit_v2',
      v_actor,
      v_table,
      p_target_id,
      p_target_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_id', v_reference.id,
        'review_kind', 'reference',
        'submission_mode', 'reference_repair',
        'replaced_reference_review_ids', to_jsonb(v_old_reference_ids)
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'reviewId', v_reference.id,
        'reviewKind', 'reference',
        'submissionMode', 'reference_repair'
      )
    );
  end if;

  if exists (
    select 1
    from public.reviews as active_root
    where active_root.review_kind = 'root'
      and active_root.target_table = v_table
      and active_root.data_id = p_target_id
      and btrim(active_root.data_version::text) = p_target_version
      and active_root.state_code in (0, 1)
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'An active Root Review already exists'
    );
  end if;

  create temporary table if not exists review_submit_v2_targets (
    table_name text not null,
    dataset_id uuid not null,
    dataset_version text not null,
    state_code integer not null,
    reviews jsonb,
    dataset_row jsonb not null,
    is_root boolean not null default false,
    primary key (table_name, dataset_id, dataset_version)
  ) on commit drop;
  truncate table review_submit_v2_targets;

  insert into review_submit_v2_targets
  select *
  from public.cmd_review_collect_dataset_targets(
    jsonb_build_array(jsonb_build_object(
      'table', v_table,
      'id', p_target_id,
      'version', p_target_version,
      'is_root', true
    )),
    true
  );

  if not exists (
    select 1
    from review_submit_v2_targets
    where is_root
      and table_name = v_table
      and dataset_id = p_target_id
      and dataset_version = p_target_version
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  if v_table in ('processes', 'lifecyclemodels') then
    v_gate_assertion := public.cmd_review_assert_lifecycle_closure(
      jsonb_build_array(jsonb_build_object(
        'table', v_table,
        'id', p_target_id,
        'version', p_target_version
      )),
      'submit',
      v_actor
    );
    if v_gate_assertion is not null then
      return v_gate_assertion;
    end if;
  end if;

  for v_target in
    select *
    from review_submit_v2_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    if nullif(v_target.dataset_row->>'user_id', '') is null then
      return jsonb_build_object(
        'ok', false,
        'code', case when v_target.is_root
          then 'ROOT_OWNER_UNRESOLVED'
          else 'REFERENCE_OWNER_UNRESOLVED'
        end,
        'status', 409,
        'message', 'Dataset owner could not be resolved'
      );
    end if;

    if not v_target.is_root
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor
      and not private.review_dataset_can_read_v1(
        v_actor,
        v_target.table_name,
        v_target.dataset_row
      ) then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_ACCESS_DENIED',
        'status', 403,
        'message', 'A referenced dataset is not accessible'
      );
    end if;

    select btrim(active_reference.data_version::text)
    into v_conflict_version
    from public.reviews as active_reference
    where active_reference.review_kind = 'reference'
      and active_reference.target_table = v_target.table_name
      and active_reference.data_id = v_target.dataset_id
      and btrim(active_reference.data_version::text)
        <> v_target.dataset_version
      and active_reference.state_code in (0, 1)
    order by active_reference.created_at desc
    limit 1;

    if v_conflict_version is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REFERENCE_REVISION_CONFLICT',
        'status', 409,
        'message', 'Another version of a referenced dataset is under review'
      );
    end if;

    v_target_checksum := public.review_revision_fingerprint_v1(
      v_target.table_name,
      v_target.dataset_row
    );

    if v_target.is_root then
      v_item := jsonb_build_object(
        'item_kind', 'root',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', null,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'root',
        'relation_path', '$',
        'introduced_by', 'submitted_data',
        'introduced_field_path', null
      );
    else
      begin
        v_reference := private.review_get_or_create_reference_v1(
          v_target.table_name,
          v_target.dataset_row,
          v_target_checksum,
          v_actor
        );
      exception
        when unique_violation then
          if sqlerrm = 'REFERENCE_REVISION_REJECTED_UNCHANGED' then
            return jsonb_build_object(
              'ok', false,
              'code', 'REFERENCE_REVISION_REJECTED_UNCHANGED',
              'status', 409,
              'message', 'A rejected reference revision must be changed'
            );
          end if;
          raise;
      end;

      v_reference_ids := array_append(v_reference_ids, v_reference.id);
      v_item := jsonb_build_object(
        'item_kind', 'reference',
        'target_table', v_target.table_name,
        'data_id', v_target.dataset_id,
        'data_version', v_target.dataset_version,
        'submitted_revision_checksum', v_target_checksum,
        'reference_review_id', v_reference.id,
        'target_owner_id', v_target.dataset_row->>'user_id',
        'target_team_id', v_target.dataset_row->'team_id',
        'relation_type', 'dependency',
        'relation_path', '$',
        'introduced_by', 'submitted_data',
        'introduced_field_path', null
      );
    end if;

    v_items := v_items || jsonb_build_array(v_item);
  end loop;

  v_scope_history := jsonb_build_object(
    'schema_version', 'review_scope.v1',
    'current_version', 1,
    'snapshots', jsonb_build_array(jsonb_build_object(
      'version_no', 1,
      'scope_basis', 'submitted',
      'root_revision_checksum', v_checksum,
      'scope_checksum', public.review_scope_checksum_v1(v_items),
      'created_by', v_actor,
      'created_at', to_jsonb(now()),
      'items', v_items
    ))
  );

  insert into public.reviews (
    id,
    data_id,
    data_version,
    state_code,
    reviewer_id,
    json,
    review_kind,
    target_table,
    submitted_revision_checksum,
    target_owner_id,
    target_team_id,
    scope_schema_version,
    scope_history
  )
  values (
    v_root_review_id,
    p_target_id,
    p_target_version,
    0,
    '[]'::jsonb,
    private.review_build_json_v1(
      v_table,
      v_root_row,
      v_actor,
      'submit_review',
      v_actor
    ),
    'root',
    v_table,
    v_checksum,
    v_actor,
    nullif(v_root_row->>'team_id', '')::uuid,
    'review_scope.v1',
    v_scope_history
  )
  returning * into v_root_review;

  perform public.review_validate_scope_history_v1(
    v_root_review_id,
    v_scope_history
  );

  perform set_config('app.review_controlled_write', 'on', true);
  for v_target in
    select *
    from review_submit_v2_targets
    order by is_root desc, table_name, dataset_id, dataset_version
  loop
    execute format(
      'update public.%I
          set state_code = case when state_code < 20 then 20 else state_code end,
              reviews = case
                when state_code < 100
                  then public.cmd_review_append_review_ref(reviews, $1)
                else reviews
              end,
              modified_at = now()
        where id = $2
          and version = $3',
      v_target.table_name
    ) using v_root_review_id, v_target.dataset_id, v_target.dataset_version;

    if not v_target.is_root
      and v_target.state_code < 20
      and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor then
      v_event_key := private.review_notify_event_v1(
        'reference_entered_review',
        (
          select item.reference_review_id
          from public.review_scope_current_items_v1(v_scope_history) as item
          where item.item_kind = 'reference'
            and item.target_table = v_target.table_name
            and item.data_id = v_target.dataset_id
            and item.data_version = v_target.dataset_version
          limit 1
        ),
        nullif(v_target.dataset_row->>'user_id', '')::uuid,
        v_actor,
        v_target.table_name,
        v_target.dataset_id,
        v_target.dataset_version,
        v_root_review_id,
        1,
        null
      );
    end if;

    v_affected := v_affected || jsonb_build_array(jsonb_build_object(
      'table', v_target.table_name,
      'id', v_target.dataset_id,
      'version', v_target.dataset_version,
      'previous_state_code', v_target.state_code,
      'state_code', case
        when v_target.state_code < 20 then 20
        else v_target.state_code
      end
    ));
  end loop;
  perform set_config('app.review_controlled_write', 'off', true);

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_review_submit_v2',
    v_actor,
    v_table,
    p_target_id,
    p_target_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_id', v_root_review_id,
      'review_kind', 'root',
      'submission_mode', 'root',
      'scope_version', 1,
      'reference_review_ids', to_jsonb(v_reference_ids),
      'affected_datasets', v_affected
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'reviewId', v_root_review_id,
      'reviewKind', 'root',
      'submissionMode', 'root',
      'review', to_jsonb(v_root_review),
      'affectedDatasets', v_affected
    )
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    if sqlerrm in (
      'REFERENCE_REVISION_REJECTED_UNCHANGED',
      'REFERENCE_OWNER_UNRESOLVED'
    ) then
      return jsonb_build_object(
        'ok', false,
        'code', sqlerrm,
        'status', 409,
        'message', 'Review submission could not be completed'
      );
    end if;
    raise;
end;
$$;

alter function public.cmd_review_submit_v2(
  text, uuid, text, jsonb, jsonb
) owner to postgres;
revoke all on function public.cmd_review_submit_v2(
  text, uuid, text, jsonb, jsonb
) from public, anon;
grant execute on function public.cmd_review_submit_v2(
  text, uuid, text, jsonb, jsonb
) to authenticated, service_role;

-- The existing Process coordinator validates its worker Gate before invoking
-- cmd_review_submit_without_gate. Preserve that coordinator and route only its
-- authenticated job hand-off into the v2 atomic submit command.
alter function public.cmd_review_submit_without_gate(
  text, uuid, text, jsonb
) rename to cmd_review_submit_without_gate_pre_v2;

revoke all on function public.cmd_review_submit_without_gate_pre_v2(
  text, uuid, text, jsonb
) from public, anon, authenticated, service_role;

create function public.cmd_review_submit_without_gate(
  p_table text,
  p_id uuid,
  p_version text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
begin
  if lower(coalesce(p_table, '')) = 'processes'
    and p_audit->>'source' = 'cmd_review_submit_from_job'
    and nullif(p_audit->>'review_submit_job_id', '') is not null then
    return public.cmd_review_submit_v2(
      p_table,
      p_id,
      p_version,
      jsonb_build_object(
        'reviewSubmitJobId',
        p_audit->>'review_submit_job_id'
      ),
      p_audit
    );
  end if;

  return public.cmd_review_submit_without_gate_pre_v2(
    p_table,
    p_id,
    p_version,
    p_audit
  );
end;
$$;

alter function public.cmd_review_submit_without_gate(
  text, uuid, text, jsonb
) owner to postgres;
revoke all on function public.cmd_review_submit_without_gate(
  text, uuid, text, jsonb
) from public, authenticated, service_role;
grant execute on function public.cmd_review_submit_without_gate(
  text, uuid, text, jsonb
) to anon;

create or replace function private.review_assert_all_reviewers_completed_v1(
  p_review public.reviews
)
returns void
language plpgsql
stable
security definer
set search_path = ''
as $$
begin
  if pg_catalog.jsonb_array_length(
    coalesce(p_review.reviewer_id, '[]'::jsonb)
  ) = 0 then
    raise exception using
      errcode = '55000',
      message = 'REVIEWER_REQUIRED';
  end if;

  if exists (
    select 1
    from pg_catalog.jsonb_array_elements_text(
      coalesce(p_review.reviewer_id, '[]'::jsonb)
    ) as reviewer(value)
    left join public.comments as comment_row
      on comment_row.review_id = p_review.id
      and comment_row.reviewer_id = reviewer.value::uuid
      and comment_row.state_code <> -2
    where comment_row.reviewer_id is null
      or comment_row.state_code not in (1, -3)
  ) then
    raise exception using
      errcode = '55000',
      message = 'REVIEWERS_NOT_COMPLETED';
  end if;
end;
$$;

alter function private.review_assert_all_reviewers_completed_v1(public.reviews)
  owner to postgres;
revoke all on function private.review_assert_all_reviewers_completed_v1(
  public.reviews
) from public, anon, authenticated, service_role;

create or replace function public.cmd_simple_review_submit_decision(
  p_review_id uuid,
  p_decision text,
  p_reason text default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_reason text := btrim(coalesce(p_reason, ''));
begin
  if v_actor is null then
    return jsonb_build_object('ok', false, 'code', 'AUTH_REQUIRED', 'status', 401);
  end if;

  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found
    or not (
      v_review.review_kind = 'reference'
      or (
        v_review.review_kind = 'root'
        and v_review.target_table in (
          'contacts',
          'sources',
          'unitgroups',
          'flowproperties',
          'flows'
        )
      )
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SIMPLE_REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_review.state_code <> 1
    or not coalesce(v_review.reviewer_id, '[]'::jsonb)
      @> jsonb_build_array(v_actor::text) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403
    );
  end if;

  if lower(p_decision) = 'approve' and v_reason <> '' then
    return jsonb_build_object(
      'ok', false,
      'code', 'SIMPLE_REVIEW_DECISION_PAYLOAD_INVALID',
      'status', 400
    );
  elsif lower(p_decision) = 'reject' and v_reason = '' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_REJECT_REASON_REQUIRED',
      'status', 400
    );
  elsif lower(p_decision) not in ('approve', 'reject') then
    return jsonb_build_object(
      'ok', false,
      'code', 'SIMPLE_REVIEW_DECISION_INVALID',
      'status', 400
    );
  end if;

  insert into public.comments (
    review_id,
    reviewer_id,
    json,
    state_code
  )
  values (
    p_review_id,
    v_actor,
    jsonb_strip_nulls(jsonb_build_object(
      'decision', lower(p_decision),
      'reason', nullif(v_reason, ''),
      'submitted_revision_checksum', v_review.submitted_revision_checksum
    ))::json,
    case when lower(p_decision) = 'approve' then 1 else -3 end
  )
  on conflict (review_id, reviewer_id) do update
  set json = excluded.json,
      state_code = excluded.state_code,
      modified_at = now()
  returning * into v_comment;

  update public.reviews
  set json = public.cmd_review_append_log(
        coalesce(json, '{}'::jsonb),
        case when lower(p_decision) = 'approve'
          then 'simple_reviewer_approved'
          else 'simple_reviewer_rejected'
        end,
        v_actor,
        jsonb_build_object(
          'submitted_revision_checksum',
          v_review.submitted_revision_checksum
        )
      ),
      modified_at = now()
  where id = p_review_id
  returning * into v_review;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_simple_review_submit_decision',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'decision', lower(p_decision),
      'comment_state_code', case
        when lower(p_decision) = 'approve' then 1 else -3
      end
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'comment', to_jsonb(v_comment)
    )
  );
end;
$$;

alter function public.cmd_simple_review_submit_decision(
  uuid, text, text, jsonb
) owner to postgres;
revoke all on function public.cmd_simple_review_submit_decision(
  uuid, text, text, jsonb
) from public, anon;
grant execute on function public.cmd_simple_review_submit_decision(
  uuid, text, text, jsonb
) to authenticated, service_role;

create or replace function private.review_notify_impacted_roots_v1(
  p_reference_review public.reviews,
  p_event_type text,
  p_actor uuid,
  p_reason_code text default null
)
returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_root public.reviews%rowtype;
begin
  for v_root in
    select root_review.*
    from public.reviews as root_review
    where root_review.review_kind = 'root'
      and root_review.current_reference_review_ids
        @> array[p_reference_review.id]::uuid[]
    order by root_review.id
  loop
    perform private.review_notify_event_v1(
      p_event_type,
      p_reference_review.id,
      v_root.target_owner_id,
      p_actor,
      p_reference_review.target_table,
      p_reference_review.data_id,
      btrim(p_reference_review.data_version::text),
      v_root.id,
      (v_root.scope_history->>'current_version')::integer,
      p_reason_code
    );
  end loop;
end;
$$;

alter function private.review_notify_impacted_roots_v1(
  public.reviews, text, uuid, text
) owner to postgres;
revoke all on function private.review_notify_impacted_roots_v1(
  public.reviews, text, uuid, text
) from public, anon, authenticated, service_role;

create or replace function public.cmd_review_finalize_approve(
  p_review_id uuid,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_target_row jsonb;
  v_current_checksum text;
  v_approved_checksum text;
  v_review_items jsonb := '[]'::jsonb;
  v_compliance_items jsonb := '[]'::jsonb;
  v_current_snapshot jsonb;
  v_approved_items jsonb;
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403
    );
  end if;

  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found or v_review.review_kind not in ('root', 'reference') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_review.state_code <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409
    );
  end if;

  begin
    perform private.review_assert_all_reviewers_completed_v1(v_review);
  exception
    when sqlstate '55000' then
      return jsonb_build_object(
        'ok', false,
        'code', sqlerrm,
        'status', 409
      );
  end;

  v_target_row := public.cmd_review_get_dataset_row(
    v_review.target_table,
    v_review.data_id,
    btrim(v_review.data_version::text),
    true
  );

  if v_target_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_TARGET_NOT_FOUND',
      'status', 404
    );
  end if;

  if coalesce((v_target_row->>'state_code')::integer, 0) not in (20, 100) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_TARGET_NOT_IN_REVIEW',
      'status', 409
    );
  end if;

  v_current_checksum := public.review_revision_fingerprint_v1(
    v_review.target_table,
    v_target_row
  );
  if v_current_checksum <> v_review.submitted_revision_checksum then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_MISMATCH',
      'status', 409
    );
  end if;

  perform set_config('app.review_controlled_write', 'on', true);

  if v_review.review_kind = 'root'
    and v_review.target_table in ('processes', 'lifecyclemodels') then
    select coalesce(jsonb_agg(review_item.value), '[]'::jsonb)
    into v_review_items
    from public.comments as comment_row
    cross join lateral jsonb_array_elements(
      public.cmd_review_json_array(
        to_jsonb(comment_row.json)
          #> '{modellingAndValidation,validation,review}'
      )
    ) as review_item(value)
    where comment_row.review_id = p_review_id
      and comment_row.state_code = 1;

    select coalesce(jsonb_agg(compliance_item.value), '[]'::jsonb)
    into v_compliance_items
    from public.comments as comment_row
    cross join lateral jsonb_array_elements(
      public.cmd_review_json_array(
        to_jsonb(comment_row.json)
          #> '{modellingAndValidation,complianceDeclarations,compliance}'
      )
    ) as compliance_item(value)
    where comment_row.review_id = p_review_id
      and comment_row.state_code = 1;

    v_target_row := public.cmd_review_apply_mv_payload(
      v_review.target_table,
      v_review.data_id,
      btrim(v_review.data_version::text),
      v_review_items,
      v_compliance_items
    );
  end if;

  v_approved_checksum := public.review_revision_fingerprint_v1(
    v_review.target_table,
    v_target_row
  );

  if v_review.review_kind = 'root'
    and v_review.target_table not in ('processes', 'lifecyclemodels')
    and v_approved_checksum <> v_review.submitted_revision_checksum then
    perform set_config('app.review_controlled_write', 'off', true);
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_MISMATCH',
      'status', 409
    );
  end if;

  if v_review.review_kind = 'reference' then
    v_approved_checksum := v_review.submitted_revision_checksum;
  else
    v_current_snapshot := public.review_scope_current_snapshot_v1(
      v_review.scope_history
    );
    v_approved_items := (
      select jsonb_agg(
        case when item.value->>'item_kind' = 'root'
          then item.value || jsonb_build_object(
            'submitted_revision_checksum', v_approved_checksum
          )
          else item.value
        end
        order by item.ordinality
      )
      from jsonb_array_elements(v_current_snapshot->'items')
        with ordinality as item(value, ordinality)
    );

    perform public.review_append_scope_snapshot_v1(
      p_review_id,
      'approved',
      v_approved_checksum,
      v_approved_items,
      v_actor
    );
  end if;

  execute format(
    'update public.%I
        set state_code = 100,
            modified_at = now()
      where id = $1
        and version = $2
        and state_code in (20, 100)',
    v_review.target_table
  ) using v_review.data_id, btrim(v_review.data_version::text);

  update public.comments
  set state_code = 2,
      modified_at = now()
  where review_id = p_review_id
    and state_code <> -2;

  update public.reviews
  set state_code = 2,
      approved_revision_checksum = v_approved_checksum,
      json = public.cmd_review_append_log(
        coalesce(json, '{}'::jsonb),
        'approved',
        v_actor,
        jsonb_build_object(
          'approved_revision_checksum',
          v_approved_checksum
        )
      ),
      modified_at = now()
  where id = p_review_id
  returning * into v_review;

  if v_review.review_kind = 'reference' then
    perform private.review_notify_impacted_roots_v1(
      v_review,
      'reference_approved',
      v_actor,
      null
    );
  end if;

  perform set_config('app.review_controlled_write', 'off', true);

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_finalize_approve',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_kind', v_review.review_kind,
      'approved_revision_checksum', v_approved_checksum,
      'target_state_code', 100
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object('review', to_jsonb(v_review))
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    raise;
end;
$$;

alter function public.cmd_review_finalize_approve(uuid, jsonb)
  owner to postgres;
revoke all on function public.cmd_review_finalize_approve(uuid, jsonb)
  from public, anon;
grant execute on function public.cmd_review_finalize_approve(uuid, jsonb)
  to authenticated, service_role;

create or replace function public.cmd_review_finalize_reject(
  p_review_id uuid,
  p_reason text,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_reason text := btrim(coalesce(p_reason, ''));
  v_other_active boolean;
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403
    );
  end if;

  if v_reason = '' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_REJECT_REASON_REQUIRED',
      'status', 400
    );
  end if;

  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found or v_review.review_kind not in ('root', 'reference') then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_review.state_code not in (0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409
    );
  end if;

  select exists (
    select 1
    from public.reviews as active_review
    where active_review.id <> p_review_id
      and active_review.target_table = v_review.target_table
      and active_review.data_id = v_review.data_id
      and btrim(active_review.data_version::text)
        = btrim(v_review.data_version::text)
      and active_review.state_code in (0, 1)
  )
  into v_other_active;

  perform set_config('app.review_controlled_write', 'on', true);
  if not v_other_active then
    execute format(
      'update public.%I
          set state_code = 0,
              modified_at = now()
        where id = $1
          and version = $2
          and state_code = 20',
      v_review.target_table
    ) using v_review.data_id, btrim(v_review.data_version::text);
  end if;

  update public.comments
  set state_code = -1,
      modified_at = now()
  where review_id = p_review_id
    and state_code <> -2;

  update public.reviews
  set state_code = -1,
      json = public.cmd_review_append_log(
        jsonb_set(
          coalesce(json, '{}'::jsonb),
          '{comment}',
          coalesce(json->'comment', '{}'::jsonb)
            || jsonb_build_object('message', v_reason),
          true
        ),
        'rejected',
        v_actor,
        jsonb_build_object('reason', v_reason)
      ),
      modified_at = now()
  where id = p_review_id
  returning * into v_review;
  perform set_config('app.review_controlled_write', 'off', true);

  perform private.review_notify_event_v1(
    case when v_review.review_kind = 'root'
      then 'root_rejected'
      else 'reference_rejected'
    end,
    v_review.id,
    v_review.target_owner_id,
    v_actor,
    v_review.target_table,
    v_review.data_id,
    btrim(v_review.data_version::text),
    case when v_review.review_kind = 'root' then v_review.id else null end,
    case when v_review.review_kind = 'root'
      then (v_review.scope_history->>'current_version')::integer
      else null
    end,
    'ADMIN_REJECTED'
  );

  if v_review.review_kind = 'reference' then
    perform private.review_notify_impacted_roots_v1(
      v_review,
      'reference_rejected',
      v_actor,
      'ADMIN_REJECTED'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_finalize_reject',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_kind', v_review.review_kind,
      'reason', v_reason,
      'target_released', not v_other_active
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review),
      'targetReleased', not v_other_active
    )
  );
exception
  when others then
    perform set_config('app.review_controlled_write', 'off', true);
    raise;
end;
$$;

alter function public.cmd_review_finalize_reject(uuid, text, jsonb)
  owner to postgres;
revoke all on function public.cmd_review_finalize_reject(uuid, text, jsonb)
  from public, anon;
grant execute on function public.cmd_review_finalize_reject(uuid, text, jsonb)
  to authenticated, service_role;

-- Keep assignment behavior and its audit shape, but prevent migrated legacy
-- records and terminal v2 records from re-entering an active queue.
alter function public.cmd_review_assign_reviewers(
  uuid, jsonb, timestamptz, jsonb
) rename to cmd_review_assign_reviewers_v1_legacy;

revoke all on function public.cmd_review_assign_reviewers_v1_legacy(
  uuid, jsonb, timestamptz, jsonb
) from public, anon, authenticated, service_role;

create function public.cmd_review_assign_reviewers(
  p_review_id uuid,
  p_reviewer_ids jsonb,
  p_deadline timestamptz default null,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_review public.reviews%rowtype;
begin
  select review_row.*
  into v_review
  from public.reviews as review_row
  where review_row.id = p_review_id;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404
    );
  end if;

  if v_review.review_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'LEGACY_REVIEW_READ_ONLY',
      'status', 409
    );
  end if;

  if v_review.state_code not in (0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409
    );
  end if;

  return public.cmd_review_assign_reviewers_v1_legacy(
    p_review_id,
    p_reviewer_ids,
    p_deadline,
    p_audit
  );
end;
$$;

alter function public.cmd_review_assign_reviewers(
  uuid, jsonb, timestamptz, jsonb
) owner to postgres;
revoke all on function public.cmd_review_assign_reviewers(
  uuid, jsonb, timestamptz, jsonb
) from public, anon;
grant execute on function public.cmd_review_assign_reviewers(
  uuid, jsonb, timestamptz, jsonb
) to authenticated, service_role;

create or replace function public.qry_review_admin_queue_items_v2(
  p_status text default null,
  p_page integer default 1,
  p_page_size integer default 20
)
returns table (
  id uuid,
  review_kind text,
  target_table text,
  data_id uuid,
  data_version text,
  state_code integer,
  target_owner_id uuid,
  target_team_id uuid,
  submitted_revision_checksum text,
  reviewer_id jsonb,
  deadline timestamptz,
  reference_count integer,
  completed_reviewer_count integer,
  modified_at timestamptz,
  total_count bigint
)
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not public.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    review_row.id,
    review_row.review_kind,
    review_row.target_table,
    review_row.data_id,
    btrim(review_row.data_version::text),
    review_row.state_code,
    review_row.target_owner_id,
    review_row.target_team_id,
    review_row.submitted_revision_checksum,
    coalesce(review_row.reviewer_id, '[]'::jsonb),
    review_row.deadline,
    cardinality(review_row.current_reference_review_ids)::integer,
    count(comment_row.reviewer_id)
      filter (where comment_row.state_code in (1, -3, 2))::integer,
    review_row.modified_at,
    count(*) over ()
  from public.reviews as review_row
  left join public.comments as comment_row
    on comment_row.review_id = review_row.id
    and comment_row.state_code <> -2
  where review_row.review_kind in ('root', 'reference')
    and (
      p_status is null
      or (p_status = 'unassigned' and review_row.state_code = 0)
      or (p_status = 'assigned' and review_row.state_code = 1)
      or (p_status = 'approved' and review_row.state_code = 2)
      or (p_status = 'rejected' and review_row.state_code = -1)
    )
  group by review_row.id
  order by review_row.modified_at desc, review_row.id
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 20), 1)
  limit least(greatest(coalesce(p_page_size, 20), 1), 100);
end;
$$;

alter function public.qry_review_admin_queue_items_v2(
  text, integer, integer
) owner to postgres;
revoke all on function public.qry_review_admin_queue_items_v2(
  text, integer, integer
) from public, anon;
grant execute on function public.qry_review_admin_queue_items_v2(
  text, integer, integer
) to authenticated, service_role;

create or replace function public.qry_review_member_queue_items_v2(
  p_status text default null,
  p_page integer default 1,
  p_page_size integer default 20
)
returns table (
  id uuid,
  review_kind text,
  target_table text,
  data_id uuid,
  data_version text,
  state_code integer,
  submitted_revision_checksum text,
  my_comment_state_code integer,
  deadline timestamptz,
  modified_at timestamptz,
  total_count bigint
)
language sql
stable
security definer
set search_path = ''
as $$
  select
    review_row.id,
    review_row.review_kind,
    review_row.target_table,
    review_row.data_id,
    btrim(review_row.data_version::text),
    review_row.state_code,
    review_row.submitted_revision_checksum,
    comment_row.state_code,
    review_row.deadline,
    review_row.modified_at,
    count(*) over ()
  from public.reviews as review_row
  join public.comments as comment_row
    on comment_row.review_id = review_row.id
    and comment_row.reviewer_id = auth.uid()
    and comment_row.state_code <> -2
  where review_row.review_kind in ('root', 'reference')
    and coalesce(review_row.reviewer_id, '[]'::jsonb)
      @> jsonb_build_array(auth.uid()::text)
    and (
      p_status is null
      or (p_status = 'pending' and comment_row.state_code = 0)
      or (p_status = 'completed' and comment_row.state_code in (1, -3, 2))
    )
  order by review_row.modified_at desc, review_row.id
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 20), 1)
  limit least(greatest(coalesce(p_page_size, 20), 1), 100)
$$;

alter function public.qry_review_member_queue_items_v2(
  text, integer, integer
) owner to postgres;
revoke all on function public.qry_review_member_queue_items_v2(
  text, integer, integer
) from public, anon;
grant execute on function public.qry_review_member_queue_items_v2(
  text, integer, integer
) to authenticated, service_role;

-- Data notifications continue to surface Review json.comment. For shared
-- reference events, notifications supplies visibility while the Review remains
-- the source of the final Admin reason.
create or replace function public.qry_notification_get_my_data_items(
  p_page integer default 1,
  p_page_size integer default 10,
  p_days integer default 3
)
returns table (
  id uuid,
  state_code integer,
  "json" jsonb,
  modified_at timestamptz,
  total_count integer
)
language sql
stable
security definer
set search_path = ''
as $$
  with visible_reviews as (
    select review_row.id, review_row.state_code, review_row.json,
      review_row.modified_at
    from public.reviews as review_row
    where coalesce(review_row.json->'user'->>'id', '') = auth.uid()::text
      and review_row.state_code in (1, -1, 2)
    union
    select review_row.id, review_row.state_code, review_row.json,
      greatest(review_row.modified_at, notification_row.modified_at)
    from public.notifications as notification_row
    join public.reviews as review_row
      on review_row.id = nullif(notification_row.json->>'review_id', '')::uuid
    where notification_row.recipient_user_id = auth.uid()
      and notification_row.type = 'review_event'
      and review_row.state_code in (-1, 2)
  )
  select
    visible_review.id,
    visible_review.state_code,
    coalesce(visible_review.json, '{}'::jsonb),
    visible_review.modified_at,
    count(*) over ()::integer
  from visible_reviews as visible_review
  where coalesce(p_days, 3) <= 0
    or visible_review.modified_at >= now()
      - make_interval(days => greatest(coalesce(p_days, 3), 0))
  order by visible_review.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1)
$$;

create or replace function public.qry_notification_get_my_data_count(
  p_days integer default 3,
  p_last_view_at timestamptz default null
)
returns integer
language sql
stable
security definer
set search_path = ''
as $$
  with visible_reviews as (
    select review_row.id, review_row.modified_at
    from public.reviews as review_row
    where coalesce(review_row.json->'user'->>'id', '') = auth.uid()::text
      and review_row.state_code in (1, -1, 2)
    union
    select review_row.id,
      greatest(review_row.modified_at, notification_row.modified_at)
    from public.notifications as notification_row
    join public.reviews as review_row
      on review_row.id = nullif(notification_row.json->>'review_id', '')::uuid
    where notification_row.recipient_user_id = auth.uid()
      and notification_row.type = 'review_event'
      and review_row.state_code in (-1, 2)
  )
  select count(*)::integer
  from visible_reviews
  where (
    p_last_view_at is not null and modified_at > p_last_view_at
  ) or (
    p_last_view_at is null and (
      coalesce(p_days, 3) <= 0
      or modified_at >= now()
        - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  )
$$;

comment on function public.cmd_review_submit_v2(
  text, uuid, text, jsonb, jsonb
) is
  'Unified seven-type Open Data review submission. The database alone resolves Root Review versus rejected Reference Review repair.';
comment on function public.cmd_review_finalize_approve(uuid, jsonb) is
  'Review Admin final approval. Reviewer decisions are advisory: completion is required, unanimous approval is not.';
comment on function public.cmd_review_finalize_reject(uuid, text, jsonb) is
  'Review Admin final rejection from Unassigned or Assigned. All non-revoked comments close as -1 and the reason remains in reviews.json.comment.';
