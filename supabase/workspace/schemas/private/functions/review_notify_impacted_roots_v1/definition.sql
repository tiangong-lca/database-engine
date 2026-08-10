CREATE OR REPLACE FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text" DEFAULT NULL::"text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_root record;
begin
  for v_root in
    select impacted.root_review_id, root_review.target_owner_id
    from api.qry_reference_review_impacted_roots(
      p_reference_review.id,
      false
    ) as impacted
    join private.reviews as root_review
      on root_review.id = impacted.root_review_id
    order by impacted.root_review_id
  loop
    perform private.review_notify_event_v1(
      p_event_type,
      p_reference_review.id,
      v_root.target_owner_id,
      p_actor,
      p_reference_review.target_table,
      p_reference_review.data_id,
      pg_catalog.btrim(p_reference_review.data_version::text),
      null,
      null,
      p_reason_code
    );
  end loop;
end;
$$;

ALTER FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") TO "api_internal_executor";
