CREATE OR REPLACE FUNCTION "api"."qry_review_get_member_queue_items_v3"("p_status" "text" DEFAULT 'pending'::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 50, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text", "p_display_mode" "text" DEFAULT 'all'::"text", "p_target_table" "text" DEFAULT NULL::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "review_state_code" integer, "review_kind" "text", "target_table" "text", "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_code" integer, "comment_json" "jsonb", "comment_created_at" timestamp with time zone, "comment_modified_at" timestamp with time zone, "root_matches_status" boolean, "root_can_read" boolean, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 50), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case pg_catalog.lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    when 'comment_modified_at' then 'comment_modified_at'
    when 'commentmodifiedat' then 'comment_modified_at'
    else 'modified_at'
  end;
  v_order_dir text := api.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := pg_catalog.lower(coalesce(p_status, 'pending'));
  v_display_mode text := pg_catalog.lower(pg_catalog.btrim(coalesce(p_display_mode, 'all')));
  v_target_table text := nullif(
    pg_catalog.lower(pg_catalog.btrim(coalesce(p_target_table, ''))),
    ''
  );
begin
  if v_actor is null or not api.cmd_review_is_review_member(v_actor) then
    return;
  end if;
  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;
  if v_display_mode not in ('all', 'model_process', 'other') then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_DISPLAY_MODE';
  end if;
  if v_target_table is not null and not (
    v_target_table = any(array[
      'contacts', 'sources', 'unitgroups', 'flowproperties', 'flows',
      'processes', 'lifecyclemodels'
    ]::text[])
  ) then
    raise exception using
      errcode = '22023',
      message = 'INVALID_REVIEW_TARGET_TABLE';
  end if;

  return query
  with q as (
    select
      review_row.id,
      review_row.data_id,
      pg_catalog.btrim(review_row.data_version::text) as data_version,
      review_row.state_code as review_state_code,
      review_row.review_kind,
      review_row.target_table,
      coalesce(review_row.reviewer_id, '[]'::jsonb) as reviewer_id,
      coalesce(review_row.json, '{}'::jsonb) as json,
      review_row.deadline,
      review_row.created_at,
      greatest(review_row.modified_at, comment_row.modified_at) as modified_at,
      comment_row.state_code as comment_state_code,
      coalesce(comment_row.json::jsonb, '{}'::jsonb) as comment_json,
      comment_row.created_at as comment_created_at,
      comment_row.modified_at as comment_modified_at,
      true as root_matches_status,
      true as root_can_read
    from private.comments as comment_row
    join private.reviews as review_row on review_row.id = comment_row.review_id
    where review_row.review_kind in ('root', 'reference')
      and comment_row.reviewer_id = v_actor
      and api.policy_review_can_read(review_row.id, v_actor)
      and (
        v_display_mode = 'all'
        or (
          v_display_mode = 'model_process'
          and review_row.target_table in ('processes', 'lifecyclemodels')
        )
        or (
          v_display_mode = 'other'
          and review_row.target_table not in ('processes', 'lifecyclemodels')
        )
      )
      and (v_target_table is null or review_row.target_table = v_target_table)
      and (
        (v_status = 'pending' and comment_row.state_code = 0 and review_row.state_code > 0)
        or (v_status = 'reviewed' and comment_row.state_code = any(array[1, 2, -3]) and review_row.state_code > 0)
        or (v_status = 'reviewer-rejected' and comment_row.state_code = -1 and review_row.state_code = -1)
      )
  )
  select q.*, pg_catalog.count(*) over() as total_count
  from q
  order by
    case when v_sort_key = 'created_at' and v_order_dir = 'asc' then q.created_at end asc nulls last,
    case when v_sort_key = 'created_at' and v_order_dir = 'desc' then q.created_at end desc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'asc' then q.deadline end asc nulls last,
    case when v_sort_key = 'deadline' and v_order_dir = 'desc' then q.deadline end desc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'asc' then q.review_state_code end asc nulls last,
    case when v_sort_key = 'state_code' and v_order_dir = 'desc' then q.review_state_code end desc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'asc' then q.comment_modified_at end asc nulls last,
    case when v_sort_key = 'comment_modified_at' and v_order_dir = 'desc' then q.comment_modified_at end desc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'asc' then q.modified_at end asc nulls last,
    case when v_sort_key = 'modified_at' and v_order_dir = 'desc' then q.modified_at end desc nulls last,
    q.id
  limit v_limit offset v_offset;
end;
$$;

ALTER FUNCTION "api"."qry_review_get_member_queue_items_v3"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_display_mode" "text", "p_target_table" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_get_member_queue_items_v3"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_display_mode" "text", "p_target_table" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_get_member_queue_items_v3"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_display_mode" "text", "p_target_table" "text") TO "authenticated";

GRANT ALL ON FUNCTION "api"."qry_review_get_member_queue_items_v3"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_display_mode" "text", "p_target_table" "text") TO "api_internal_executor";
