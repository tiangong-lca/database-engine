CREATE OR REPLACE FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_comment public.comments%rowtype;
  v_remaining_reviewer_ids jsonb;
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

  if not public.cmd_review_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_ADMIN_REQUIRED',
      'status', 403,
      'message', 'Only review admins can revoke reviewers'
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

  if v_review.state_code <> 1 then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewers can only be revoked from assigned reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  if not public.cmd_review_json_array(v_review.reviewer_id) @> jsonb_build_array(to_jsonb(p_reviewer_id::text)) then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_NOT_ASSIGNED',
      'status', 409,
      'message', 'Reviewer is not currently assigned to this review'
    );
  end if;

  select *
    into v_comment
  from public.comments
  where review_id = p_review_id
    and reviewer_id = p_reviewer_id
  for update;

  if found and v_comment.state_code <> 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_ALREADY_RESPONDED',
      'status', 409,
      'message', 'Only pending reviewers can be revoked',
      'details', jsonb_build_object(
        'comment_state_code', v_comment.state_code
      )
    );
  end if;

  select coalesce(
    jsonb_agg(to_jsonb(value) order by ordinality),
    '[]'::jsonb
  )
    into v_remaining_reviewer_ids
  from jsonb_array_elements_text(public.cmd_review_json_array(v_review.reviewer_id))
       with ordinality as reviewer_ids(value, ordinality)
  where reviewer_ids.value <> p_reviewer_id::text;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'revoke_reviewer',
    v_actor,
    jsonb_build_object(
      'reviewer_id', p_reviewer_id
    )
  );

  update public.reviews
    set reviewer_id = v_remaining_reviewer_ids,
        state_code = case
          when jsonb_array_length(v_remaining_reviewer_ids) = 0 then 0
          else 1
        end,
        json = v_review_json,
        modified_at = now()
  where id = p_review_id
  returning *
    into v_review;

  update public.comments
    set state_code = -2,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id = p_reviewer_id;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_review_revoke_reviewer',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_id', p_reviewer_id,
      'remaining_reviewer_ids', v_remaining_reviewer_ids
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review)
    )
  );
end;
$$;

ALTER FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_revoke_reviewer"("p_review_id" "uuid", "p_reviewer_id" "uuid", "p_audit" "jsonb") TO "service_role";
