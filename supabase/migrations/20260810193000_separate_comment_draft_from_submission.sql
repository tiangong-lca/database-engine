-- Issue #446 follow-up: temporarily storing a reviewer Comment must not
-- provision Reference Reviews. Only formal Comment submission makes its
-- references part of the current Root/Reference relationship.

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
      'comment_state_code', v_comment.state_code
    )
  );

  return pg_catalog.jsonb_build_object(
    'ok', true,
    'data', pg_catalog.jsonb_build_object(
      'review', pg_catalog.to_jsonb(v_review),
      'comment', pg_catalog.to_jsonb(v_comment)
    )
  );
end;
$$;

alter function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  owner to postgres;
revoke all on function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  from public, anon;
grant execute on function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb)
  to authenticated, api_internal_executor;

comment on function api.cmd_review_save_comment_draft(uuid, jsonb, jsonb) is
  'Temporarily stores editable reviewer Comment JSON and audit metadata without provisioning Reference Reviews or Root candidate hints.';

do $migration$
declare
  v_definition text;
  v_draft_filter text :=
    '      and comment_row.state_code <> -2';
  v_submitted_filter text :=
    '      and comment_row.state_code in (1, -3, 2)';
begin
  select pg_catalog.pg_get_functiondef(
    'private.review_resolve_current_reference_targets_v1(uuid[])'
      ::pg_catalog.regprocedure
  )
  into v_definition;

  if pg_catalog.strpos(v_definition, v_draft_filter) = 0 then
    raise exception using
      errcode = '55000',
      message = 'EXPECTED_COMMENT_RELATIONSHIP_FILTER_NOT_FOUND';
  end if;

  execute pg_catalog.replace(
    v_definition,
    v_draft_filter,
    v_submitted_filter
  );
end;
$migration$;

comment on function private.review_resolve_current_reference_targets_v1(
  uuid[]
) is
  'Set-oriented recursive current JSON/submitted-Comment closure for multiple Root Reviews; state-zero drafts are excluded and no relationship is persisted.';
