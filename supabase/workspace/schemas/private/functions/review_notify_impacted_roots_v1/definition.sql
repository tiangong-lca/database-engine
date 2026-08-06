CREATE OR REPLACE FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text" DEFAULT NULL::"text") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_root private.reviews%rowtype;
begin
  for v_root in
    select root_review.*
    from private.reviews as root_review
    where root_review.review_kind = 'root'
      and root_review.current_reference_review_ids
        @> array[p_reference_review.id]::uuid[]
    order by root_review.id
  loop
    perform private.review_notify_event_v1(
      p_event_type,
      p_reference_review.id,
      v_root.target_owner_id,
      p_actor,
      p_reference_review.target_table,
      p_reference_review.data_id,
      btrim(p_reference_review.data_version::text),
      v_root.id,
      (v_root.scope_history->>'current_version')::integer,
      p_reason_code
    );
  end loop;
end;
$$;

ALTER FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_notify_impacted_roots_v1"("p_reference_review" "private"."reviews", "p_event_type" "text", "p_actor" "uuid", "p_reason_code" "text") TO "api_internal_executor";
