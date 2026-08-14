CREATE OR REPLACE FUNCTION "api"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone DEFAULT NULL::timestamp with time zone, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_review private.reviews%rowtype;
begin
  select review_row.*
  into v_review
  from private.reviews as review_row
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

  return private.cmd_review_assign_reviewers_v1_legacy(
    p_review_id,
    p_reviewer_ids,
    p_deadline,
    p_audit
  );
end;
$$;

ALTER FUNCTION "api"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_review_assign_reviewers"("p_review_id" "uuid", "p_reviewer_ids" "jsonb", "p_deadline" timestamp with time zone, "p_audit" "jsonb") TO "authenticated";
