CREATE OR REPLACE FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_review public.reviews%rowtype;
  v_reviewer_ids jsonb;
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
      'message', 'Only review admins can manage reviewer assignments'
    );
  end if;

  if coalesce(jsonb_typeof(p_reviewer_ids), 'null') not in ('null', 'array') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must be an array of UUID strings'
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements_text(coalesce(p_reviewer_ids, '[]'::jsonb)) as reviewer_ids(value)
    where reviewer_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must contain valid UUID strings only'
    );
  end if;

  v_reviewer_ids := public.cmd_review_normalize_reviewer_ids(coalesce(p_reviewer_ids, '[]'::jsonb));

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

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewer assignments can only be changed for unassigned or active reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'assign_reviewers_temporary',
    v_actor,
    jsonb_build_object(
      'reviewer_ids', v_reviewer_ids
    )
  );

  update public.reviews
    set reviewer_id = v_reviewer_ids,
        json = v_review_json,
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
    'cmd_review_save_assignment_draft',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'reviewer_ids', v_reviewer_ids
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'review', to_jsonb(v_review)
    )
  );
end;
$_$;

ALTER FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_save_assignment_draft"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_audit" "jsonb") TO "service_role";
