CREATE OR REPLACE FUNCTION "private"."review_assert_all_reviewers_completed_v1"("p_review" "private"."reviews") RETURNS "void"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
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
    left join private.comments as comment_row
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

ALTER FUNCTION "private"."review_assert_all_reviewers_completed_v1"("p_review" "private"."reviews") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_assert_all_reviewers_completed_v1"("p_review" "private"."reviews") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_assert_all_reviewers_completed_v1"("p_review" "private"."reviews") TO "api_internal_executor";
