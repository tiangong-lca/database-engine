-- Issue #446 follow-up: Reviewer metadata drafts participate in the current
-- relationship closure, so saving a draft must provision the same independent
-- Reference Reviews and append-only Root candidate hints as final submission.

create or replace function api.cmd_review_save_comment_draft(
  p_review_id uuid,
  p_json jsonb,
  p_audit jsonb default '{}'::jsonb
)
returns jsonb
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_actor uuid := auth.uid();
  v_review private.reviews%rowtype;
  v_comment private.comments%rowtype;
  v_comment_json jsonb := coalesce(p_json, '{}'::jsonb);
  v_review_json jsonb;
  v_ref record;
  v_target record;
  v_reference private.reviews%rowtype;
  v_ref_roots jsonb := '[]'::jsonb;
  v_checksum text;
  v_affected jsonb := '[]'::jsonb;
begin
  if v_actor is null then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if coalesce(pg_catalog.jsonb_typeof(v_comment_json), 'null') <> 'object' then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_JSON',
      'status', 400,
      'message', 'comment json must be an object'
    );
  end if;

  select review_row.*
  into v_review
  from private.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 1) then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Review comments can only be edited for assigned or rejected reviews',
      'details', pg_catalog.jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not api.cmd_review_json_array(v_review.reviewer_id)
    @> pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_actor::text)) then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only assigned reviewers can edit review comments'
    );
  end if;

  select comment_row.*
  into v_comment
  from private.comments as comment_row
  where comment_row.review_id = p_review_id
    and comment_row.reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409,
      'message', 'This reviewer comment can no longer be edited',
      'details', pg_catalog.jsonb_build_object(
        'state_code', v_comment.state_code
      )
    );
  end if;

  if v_review.review_kind = 'root'
    and v_review.target_table in ('processes', 'lifecyclemodels') then
    for v_ref in
      select extracted.*
      from api.cmd_review_extract_refs(v_comment_json) as extracted
    loop
      if api.cmd_review_ref_type_to_table(v_ref.ref_type) is not null then
        v_ref_roots := v_ref_roots || pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'table', api.cmd_review_ref_type_to_table(v_ref.ref_type),
            'id', v_ref.ref_object_id,
            'version', v_ref.ref_version,
            'is_root', false
          )
        );
      end if;
    end loop;

    for v_target in
      select collected.*
      from api.cmd_review_collect_dataset_targets(
        v_ref_roots,
        true
      ) as collected
      order by collected.table_name, collected.dataset_id,
        collected.dataset_version
    loop
      if nullif(v_target.dataset_row->>'user_id', '') is null then
        return pg_catalog.jsonb_build_object(
          'ok', false,
          'code', 'REFERENCE_OWNER_UNRESOLVED',
          'status', 409
        );
      end if;

      if nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor
        and not private.review_dataset_can_read_v1(
          v_actor,
          v_target.table_name,
          v_target.dataset_row
        ) then
        return pg_catalog.jsonb_build_object(
          'ok', false,
          'code', 'REFERENCE_ACCESS_DENIED',
          'status', 403
        );
      end if;

      v_checksum := private.review_revision_fingerprint_v1(
        v_target.table_name,
        v_target.dataset_row
      );
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_checksum,
        v_actor
      );

      perform pg_catalog.set_config('app.review_controlled_write', 'on', true);
      execute pg_catalog.format(
        'update public.%I
            set state_code = case when state_code < 20 then 20 else state_code end,
                reviews = case when state_code < 100
                  then api.cmd_review_append_review_ref(reviews, $1)
                  else reviews
                end,
                modified_at = now()
          where id = $2
            and version = $3',
        v_target.table_name
      ) using p_review_id, v_target.dataset_id, v_target.dataset_version;
      perform pg_catalog.set_config('app.review_controlled_write', 'off', true);

      if v_target.state_code < 20
        and nullif(v_target.dataset_row->>'user_id', '')::uuid <> v_actor then
        perform private.review_notify_event_v1(
          'reference_entered_review',
          v_reference.id,
          nullif(v_target.dataset_row->>'user_id', '')::uuid,
          v_actor,
          v_target.table_name,
          v_target.dataset_id,
          v_target.dataset_version,
          null,
          null,
          null
        );
      end if;

      v_affected := v_affected || pg_catalog.jsonb_build_array(
        pg_catalog.jsonb_build_object(
          'table', v_target.table_name,
          'id', v_target.dataset_id,
          'version', v_target.dataset_version,
          'reference_review_id', v_reference.id
        )
      );
    end loop;
  end if;

  if v_comment.review_id is null then
    insert into private.comments (
      review_id,
      reviewer_id,
      json,
      state_code
    )
    values (
      p_review_id,
      v_actor,
      v_comment_json::json,
      case
        when v_review.state_code = -1 then -1
        else 0
      end
    )
    returning * into v_comment;
  else
    update private.comments
    set json = v_comment_json::json,
        modified_at = pg_catalog.now()
    where review_id = p_review_id
      and reviewer_id = v_actor
    returning * into v_comment;
  end if;

  v_review_json := api.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'submit_comments_temporary',
    v_actor,
    pg_catalog.jsonb_build_object('reviewer_id', v_actor)
  );

  update private.reviews
  set json = v_review_json,
      modified_at = pg_catalog.now()
  where id = p_review_id
  returning * into v_review;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_save_comment_draft',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || pg_catalog.jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', v_comment.state_code,
      'affected_datasets', (
        select coalesce(
          pg_catalog.jsonb_agg(affected.value - 'reference_review_id'),
          '[]'::jsonb
        )
        from pg_catalog.jsonb_array_elements(v_affected) as affected(value)
      )
    )
  );

  return pg_catalog.jsonb_build_object(
    'ok', true,
    'data', pg_catalog.jsonb_build_object(
      'review', pg_catalog.to_jsonb(v_review),
      'comment', pg_catalog.to_jsonb(v_comment),
      'affected_datasets', v_affected
    )
  );
exception
  when others then
    perform pg_catalog.set_config('app.review_controlled_write', 'off', true);
    raise;
end;
$$;

alter function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  owner to postgres;
revoke all on function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  from public, anon;
grant execute on function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  to authenticated, api_internal_executor;

-- Candidate hints are now maintained by Root submission, final Comment
-- submission, and draft Comment saves. Restrict grouped derivation to roots
-- that directly match the tab or are hinted by a matching Reference Review.
do $migration$
declare
  v_signature text;
  v_definition text;
  v_all_root_fallback text := $fragment$
    union
    select root_review.id
    from private.reviews as root_review
    where root_review.review_kind = 'root'$fragment$;
begin
  foreach v_signature in array array[
    'api.qry_review_get_admin_root_queue_items_v2(text,integer,integer,text,text)',
    'api.qry_review_get_member_root_queue_items_v2(text,integer,integer,text,text)'
  ]
  loop
    select pg_catalog.pg_get_functiondef(v_signature::pg_catalog.regprocedure)
    into v_definition;

    if pg_catalog.strpos(v_definition, v_all_root_fallback) = 0 then
      raise exception using
        errcode = '55000',
        message = 'EXPECTED_ALL_ROOT_QUEUE_FALLBACK_NOT_FOUND',
        detail = v_signature;
    end if;

    execute pg_catalog.replace(v_definition, v_all_root_fallback, '');
  end loop;
end;
$migration$;

comment on function api.qry_review_get_admin_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Current-state Admin queue grouped by candidate Root Review. Direct or candidate-hinted child matches can include a parent; unrelated roots are not derived.';

comment on function api.qry_review_get_member_root_queue_items_v2(
  text, integer, integer, text, text
) is
  'Current-state Member queue grouped by candidate Root Review. Direct or candidate-hinted child matches can include a parent; unrelated roots are not derived.';

-- Repair already-saved, non-revoked Reviewer metadata drafts. This is
-- idempotent: Reference Review reuse is checksum-based and candidate hints are
-- append-only with duplicate suppression. Notifications and command audit rows
-- are intentionally not replayed by this migration.
do $reconcile$
declare
  v_comment record;
  v_ref record;
  v_target record;
  v_reference private.reviews%rowtype;
  v_ref_roots jsonb;
  v_checksum text;
begin
  for v_comment in
    select
      comment_row.review_id,
      comment_row.reviewer_id,
      comment_row.json::jsonb as comment_json
    from private.comments as comment_row
    join private.reviews as root_review
      on root_review.id = comment_row.review_id
      and root_review.review_kind = 'root'
      and root_review.target_table in ('processes', 'lifecyclemodels')
      and root_review.state_code in (0, 1)
    where comment_row.state_code <> -2
    order by comment_row.review_id, comment_row.reviewer_id
  loop
    v_ref_roots := '[]'::jsonb;

    for v_ref in
      select extracted.*
      from api.cmd_review_extract_refs(
        coalesce(v_comment.comment_json, '{}'::jsonb)
      ) as extracted
    loop
      if api.cmd_review_ref_type_to_table(v_ref.ref_type) is not null then
        v_ref_roots := v_ref_roots || pg_catalog.jsonb_build_array(
          pg_catalog.jsonb_build_object(
            'table', api.cmd_review_ref_type_to_table(v_ref.ref_type),
            'id', v_ref.ref_object_id,
            'version', v_ref.ref_version,
            'is_root', false
          )
        );
      end if;
    end loop;

    for v_target in
      select collected.*
      from api.cmd_review_collect_dataset_targets(v_ref_roots, true) as collected
      order by collected.table_name, collected.dataset_id,
        collected.dataset_version
    loop
      v_checksum := private.review_revision_fingerprint_v1(
        v_target.table_name,
        v_target.dataset_row
      );
      v_reference := private.review_get_or_create_reference_v1(
        v_target.table_name,
        v_target.dataset_row,
        v_checksum,
        v_comment.reviewer_id
      );

      perform pg_catalog.set_config('app.review_controlled_write', 'on', true);
      execute pg_catalog.format(
        'update public.%I
            set state_code = case when state_code < 20 then 20 else state_code end,
                reviews = case when state_code < 100
                  then api.cmd_review_append_review_ref(reviews, $1)
                  else reviews
                end,
                modified_at = now()
          where id = $2
            and version = $3',
        v_target.table_name
      ) using v_comment.review_id, v_target.dataset_id,
        v_target.dataset_version;
      perform pg_catalog.set_config('app.review_controlled_write', 'off', true);
    end loop;
  end loop;
exception
  when others then
    perform pg_catalog.set_config('app.review_controlled_write', 'off', true);
    raise;
end;
$reconcile$;
