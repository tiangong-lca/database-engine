CREATE OR REPLACE FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_reviews jsonb := case
    when jsonb_typeof(p_existing_reviews) = 'array' then p_existing_reviews
    else '[]'::jsonb
  end;
begin
  if exists (
    select 1
    from jsonb_array_elements(v_reviews) as review_item(value)
    where review_item.value->>'id' = p_review_id::text
  ) then
    return v_reviews;
  end if;

  return v_reviews || jsonb_build_array(
    jsonb_build_object(
      'key', jsonb_array_length(v_reviews),
      'id', p_review_id
    )
  );
end;
$$;

ALTER FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_append_review_ref"("p_existing_reviews" "jsonb", "p_review_id" "uuid") TO "service_role";
