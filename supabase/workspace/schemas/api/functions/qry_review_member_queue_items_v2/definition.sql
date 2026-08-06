CREATE OR REPLACE FUNCTION "api"."qry_review_member_queue_items_v2"("p_status" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 20) RETURNS TABLE("id" "uuid", "review_kind" "text", "target_table" "text", "data_id" "uuid", "data_version" "text", "state_code" integer, "submitted_revision_checksum" "text", "my_comment_state_code" integer, "deadline" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  select
    review_row.id,
    review_row.review_kind,
    review_row.target_table,
    review_row.data_id,
    btrim(review_row.data_version::text),
    review_row.state_code,
    review_row.submitted_revision_checksum,
    comment_row.state_code,
    review_row.deadline,
    review_row.modified_at,
    count(*) over ()
  from private.reviews as review_row
  join private.comments as comment_row
    on comment_row.review_id = review_row.id
    and comment_row.reviewer_id = auth.uid()
    and comment_row.state_code <> -2
  where review_row.review_kind in ('root', 'reference')
    and coalesce(review_row.reviewer_id, '[]'::jsonb)
      @> jsonb_build_array(auth.uid()::text)
    and (
      p_status is null
      or (p_status = 'pending' and comment_row.state_code = 0)
      or (p_status = 'completed' and comment_row.state_code in (1, -3, 2))
    )
  order by review_row.modified_at desc, review_row.id
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 20), 1)
  limit least(greatest(coalesce(p_page_size, 20), 1), 100)
$$;

ALTER FUNCTION "api"."qry_review_member_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_member_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_member_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) TO "api_internal_executor";
