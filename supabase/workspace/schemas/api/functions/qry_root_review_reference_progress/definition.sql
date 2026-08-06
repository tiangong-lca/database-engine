CREATE OR REPLACE FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") RETURNS TABLE("reference_review_id" "uuid", "target_table" "text", "data_id" "uuid", "data_version" "text", "submitted_revision_checksum" "text", "state_code" integer, "reviewer_count" integer, "completed_reviewer_count" integer, "relation_paths" "jsonb")
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
    reference_review.id,
    item.target_table,
    item.data_id,
    item.data_version,
    item.submitted_revision_checksum,
    reference_review.state_code,
    pg_catalog.jsonb_array_length(
      coalesce(reference_review.reviewer_id, '[]'::jsonb)
    )::integer,
    count(comment_row.reviewer_id)
      filter (where comment_row.state_code in (1, -3, 2))::integer,
    coalesce(
      pg_catalog.jsonb_agg(distinct item.relation_path)
        filter (where item.relation_path is not null),
      '[]'::jsonb
    )
  from private.reviews as root_review
  cross join lateral private.review_scope_current_items_v1(
    root_review.scope_history
  ) as item
  join private.reviews as reference_review
    on reference_review.id = item.reference_review_id
    and reference_review.review_kind = 'reference'
  left join private.comments as comment_row
    on comment_row.review_id = reference_review.id
    and comment_row.state_code <> -2
  where root_review.id = p_root_review_id
    and root_review.review_kind = 'root'
    and item.item_kind = 'reference'
  group by
    reference_review.id,
    item.target_table,
    item.data_id,
    item.data_version,
    item.submitted_revision_checksum,
    reference_review.state_code,
    reference_review.reviewer_id
  order by item.target_table, item.data_id, item.data_version;
end;
$$;

ALTER FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_root_review_reference_progress"("p_root_review_id" "uuid") TO "api_internal_executor";
