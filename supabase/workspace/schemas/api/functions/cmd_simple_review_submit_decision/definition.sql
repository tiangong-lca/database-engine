CREATE OR REPLACE FUNCTION "api"."cmd_simple_review_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text" DEFAULT NULL::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_review private.reviews%rowtype;
  v_comment private.comments%rowtype;
  v_reason text := btrim(coalesce(p_reason, ''));
begin
  if v_actor is null then
    return jsonb_build_object('ok', false, 'code', 'AUTH_REQUIRED', 'status', 401);
  end if;

  select review_row.*
  into v_review
  from private.reviews as review_row
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

  insert into private.comments (
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

  update private.reviews
  set json = api.cmd_review_append_log(
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

  insert into private.command_audit_log (
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

ALTER FUNCTION "api"."cmd_simple_review_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_simple_review_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_simple_review_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_simple_review_submit_decision"("p_review_id" "uuid", "p_decision" "text", "p_reason" "text", "p_audit" "jsonb") TO "authenticated";
