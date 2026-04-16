CREATE OR REPLACE FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
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
      'message', 'Only review admins can assign reviewers'
    );
  end if;

  if coalesce(jsonb_typeof(p_reviewer_ids), 'null') <> 'array' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must be an array of UUID strings'
    );
  end if;

  if exists (
    select 1
    from jsonb_array_elements_text(p_reviewer_ids) as reviewer_ids(value)
    where reviewer_ids.value !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEWER_IDS',
      'status', 400,
      'message', 'reviewerIds must contain valid UUID strings only'
    );
  end if;

  v_reviewer_ids := public.cmd_review_normalize_reviewer_ids(p_reviewer_ids);

  if jsonb_array_length(v_reviewer_ids) = 0 then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 400,
      'message', 'At least one reviewer is required'
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

  if v_review.state_code not in (-1, 0, 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_STATE',
      'status', 409,
      'message', 'Reviewers can only be assigned for pending or rejected reviews',
      'details', jsonb_build_object(
        'state_code', v_review.state_code
      )
    );
  end if;

  create temporary table if not exists cmd_review_assignment_active_reviewers (
    reviewer_id uuid primary key
  ) on commit drop;

  truncate table cmd_review_assignment_active_reviewers;

  insert into cmd_review_assignment_active_reviewers (reviewer_id)
  select value::uuid
  from jsonb_array_elements_text(v_reviewer_ids) as reviewer_ids(value);

  update public.comments
    set state_code = -2,
        modified_at = now()
  where review_id = p_review_id
    and reviewer_id not in (
      select reviewer_id
      from cmd_review_assignment_active_reviewers
    )
    and state_code = 0;

  insert into public.comments (
    review_id,
    reviewer_id,
    state_code
  )
  select
    p_review_id,
    reviewer_id,
    0
  from cmd_review_assignment_active_reviewers
  on conflict (review_id, reviewer_id) do update
    set state_code = case
      when public.comments.state_code in (-2, -1) then 0
      else public.comments.state_code
    end,
        modified_at = now();

  v_review_json := public.cmd_review_append_log(
    coalesce(v_review.json, '{}'::jsonb),
    'assign_reviewers',
    v_actor,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewer_ids', v_reviewer_ids,
        'deadline', case
          when p_deadline is null then null
          else to_jsonb(p_deadline)
        end
      )
    )
  );

  update public.reviews
    set reviewer_id = v_reviewer_ids,
        state_code = 1,
        deadline = p_deadline,
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
    'cmd_review_assign_reviewers',
    v_actor,
    'reviews',
    p_review_id,
    coalesce(p_audit, '{}'::jsonb) || jsonb_strip_nulls(
      jsonb_build_object(
        'reviewer_ids', v_reviewer_ids,
        'deadline', case
          when p_deadline is null then null
          else to_jsonb(p_deadline)
        end
      )
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

ALTER FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "service_role";
