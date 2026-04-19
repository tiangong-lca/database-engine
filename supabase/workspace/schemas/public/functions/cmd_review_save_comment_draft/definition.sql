CREATE OR REPLACE FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_comment_json jsonb := coalesce(p_json, '{}'::jsonb);
  v_review_json jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if coalesce(jsonb_typeof(v_comment_json), 'null') <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_JSON',
      'status', 400,
      'message', 'comment json must be an object'
    );
  end if;

  select *
    into v_review
  from public.reviews
  where id = p_review_id
  for update;

  if not found then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code not in (-1, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Review comments can only be edited for assigned or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(v_actor::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only assigned reviewers can edit review comments'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = v_actor
  for update;

  if found and v_comment.state_code in (-2, 2) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_COMMENT_STATE',
      'status', 409,
      'message', 'This reviewer comment can no longer be edited',
      'details', jsonb_build_object(
        'state_code', v_comment.state_code
      )
    );
  end if;

  if not found then
    insert into public.comments (
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
    returning *
      into v_comment;
  else
    update public.comments
      set json = v_comment_json::json,
          modified_at = now()
    where review_id = p_review_id
      and reviewer_id = v_actor
    returning *
      into v_comment;
  end if;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'submit_comments_temporary',
    v_actor,
    jsonb_build_object(
      'reviewer_id', v_actor
    )
  );

  update public.reviews
    set json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  insert into public.command_audit_log (
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
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', v_actor,
      'comment_state_code', v_comment.state_code
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

ALTER FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_save_comment_draft"("p_review_id" "uuid", "p_json" "jsonb", "p_audit" "jsonb") TO "service_role";
