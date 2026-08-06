CREATE OR REPLACE FUNCTION "api"."qry_review_admin_queue_items_v2"("p_status" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 20) RETURNS TABLE("id" "uuid", "review_kind" "text", "target_table" "text", "data_id" "uuid", "data_version" "text", "state_code" integer, "target_owner_id" "uuid", "target_team_id" "uuid", "submitted_revision_checksum" "text", "reviewer_id" "jsonb", "deadline" timestamp with time zone, "reference_count" integer, "completed_reviewer_count" integer, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    raise exception using errcode = '42501', message = 'REVIEW_ADMIN_REQUIRED';
  end if;

  return query
  select
    review_row.id,
    review_row.review_kind,
    review_row.target_table,
    review_row.data_id,
    btrim(review_row.data_version::text),
    review_row.state_code,
    review_row.target_owner_id,
    review_row.target_team_id,
    review_row.submitted_revision_checksum,
    coalesce(review_row.reviewer_id, '[]'::jsonb),
    review_row.deadline,
    cardinality(review_row.current_reference_review_ids)::integer,
    count(comment_row.reviewer_id)
      filter (where comment_row.state_code in (1, -3, 2))::integer,
    review_row.modified_at,
    count(*) over ()
  from private.reviews as review_row
  left join private.comments as comment_row
    on comment_row.review_id = review_row.id
    and comment_row.state_code <> -2
  where review_row.review_kind in ('root', 'reference')
    and (
      p_status is null
      or (p_status = 'unassigned' and review_row.state_code = 0)
      or (p_status = 'assigned' and review_row.state_code = 1)
      or (p_status = 'approved' and review_row.state_code = 2)
      or (p_status = 'rejected' and review_row.state_code = -1)
    )
  group by review_row.id
  order by review_row.modified_at desc, review_row.id
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 20), 1)
  limit least(greatest(coalesce(p_page_size, 20), 1), 100);
end;
$$;

ALTER FUNCTION "api"."qry_review_admin_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_admin_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_admin_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer) TO "api_internal_executor";
