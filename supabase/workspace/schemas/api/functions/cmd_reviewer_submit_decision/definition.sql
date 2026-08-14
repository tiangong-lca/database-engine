CREATE OR REPLACE FUNCTION "api"."cmd_reviewer_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_review private.reviews%rowtype;
  v_comment private.comments%rowtype;
  v_decision text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_decision, '')));
  v_reason text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_comment_json jsonb;
  v_is_simple boolean;
begin
  if v_actor is null then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if v_decision not in ('approve', 'reject') then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_DECISION_INVALID',
      'status', 400,
      'message', 'decision must be approve or reject'
    );
  end if;

  if v_decision = 'approve' and v_reason <> '' then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_DECISION_PAYLOAD_INVALID',
      'status', 400,
      'message', 'approve does not accept a reason'
    );
  end if;

  if v_decision = 'reject' and v_reason = '' then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_REJECT_REASON_REQUIRED',
      'status', 400,
      'message', 'reason is required for reject'
    );
  end if;

  if pg_catalog.length(v_reason) > 1000 then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_REJECT_REASON_TOO_LONG',
      'status', 400,
      'message', 'reason must not exceed 1000 characters'
    );
  end if;

  select review_row.*
  into v_review
  from private.reviews as review_row
  where review_row.id = p_review_id
  for update;

  if not found or v_review.review_kind not in ('root', 'reference') then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_NOT_FOUND',
      'status', 404,
      'message', 'Review not found'
    );
  end if;

  if v_review.state_code <> 1
    or not api.cmd_review_json_array(v_review.reviewer_id)
      @> pg_catalog.jsonb_build_array(pg_catalog.to_jsonb(v_actor::text)) then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEWER_REQUIRED',
      'status', 403,
      'message', 'Only an assigned reviewer can submit a pending decision'
    );
  end if;

  select comment_row.*
  into v_comment
  from private.comments as comment_row
  where comment_row.review_id = p_review_id
    and comment_row.reviewer_id = v_actor
  for update;

  if not found or v_comment.state_code <> 0 then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_COMMENT_NOT_PENDING',
      'status', 409,
      'message', 'Reviewer decision is no longer pending'
    );
  end if;

  v_is_simple := v_review.review_kind = 'reference'
    or v_review.target_table in (
      'contacts',
      'sources',
      'unitgroups',
      'flowproperties',
      'flows'
    );

  if v_is_simple then
    return api.cmd_simple_review_submit_decision(
      p_review_id,
      v_decision,
      case when v_decision = 'reject' then v_reason else null end,
      coalesce(p_audit, '{}'::jsonb)
        || pg_catalog.jsonb_build_object('reviewer_decision_command', true)
    );
  end if;

  v_comment_json := coalesce(pg_catalog.to_jsonb(v_comment.json), '{}'::jsonb);

  if v_decision = 'approve' and v_comment_json = '{}'::jsonb then
    return pg_catalog.jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_DRAFT_REQUIRED',
      'status', 409,
      'message', 'Process and LifecycleModel approvals require a saved review draft'
    );
  end if;

  if v_decision = 'reject' then
    v_comment_json := pg_catalog.jsonb_set(
      v_comment_json,
      '{comment}',
      coalesce(v_comment_json->'comment', '{}'::jsonb)
        || pg_catalog.jsonb_build_object('message', v_reason),
      true
    );
  end if;

  return api.cmd_review_submit_comment(
    p_review_id,
    v_comment_json,
    case when v_decision = 'approve' then 1 else -3 end,
    coalesce(p_audit, '{}'::jsonb)
      || pg_catalog.jsonb_build_object('reviewer_decision_command', true)
  );
end;
$$;

ALTER FUNCTION "api"."cmd_reviewer_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_reviewer_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_reviewer_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_reviewer_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
