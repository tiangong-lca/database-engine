CREATE OR REPLACE FUNCTION "api"."qry_review_get_admin_root_queue_items_v2"("p_status" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "state_code" integer, "review_kind" "text", "target_table" "text", "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_codes" "jsonb", "root_matches_status" boolean, "root_can_read" boolean, "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_sort_key text := case lower(coalesce(p_sort_by, ''))
    when 'created_at' then 'created_at'
    when 'createat' then 'created_at'
    when 'deadline' then 'deadline'
    when 'state_code' then 'state_code'
    when 'statecode' then 'state_code'
    else 'modified_at'
  end;
  v_order_dir text := api.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, ''));
  v_state_code integer;
begin
  if v_actor is null or not api.cmd_review_is_review_admin(v_actor) then
    return;
  end if;

  case v_status
    when '', 'all' then v_state_code := null;
    when 'unassigned' then v_state_code := 0;
    when 'assigned' then v_state_code := 1;
    when 'admin-rejected' then v_state_code := -1;
    else return;
  end case;

  return query
      with root_matches as (
        select
          root_review.id as root_review_id,
          true as root_matches_status,
          root_review.modified_at as matching_task_modified_at
        from private.reviews as root_review
        where root_review.review_kind = 'root'
          and (v_state_code is null or root_review.state_code = v_state_code)

        union all

        select
          root_review.id as root_review_id,
          false as root_matches_status,
          reference_review.modified_at as matching_task_modified_at
        from private.reviews as reference_review
        join private.reviews as root_review
          on root_review.review_kind = 'root'
          and root_review.current_reference_review_ids
            @> array[reference_review.id]::uuid[]
        where v_state_code is not null
          and reference_review.review_kind = 'reference'
          and reference_review.state_code = v_state_code
      ),
      root_candidates as (
        select
          root_match.root_review_id,
          pg_catalog.bool_or(root_match.root_matches_status)
            as root_matches_status,
          pg_catalog.max(root_match.matching_task_modified_at)
            as matching_task_modified_at
        from root_matches as root_match
        group by root_match.root_review_id
      ),
      q as (
        select
          root_review.id,
          root_review.data_id,
          pg_catalog.btrim(root_review.data_version::text) as data_version,
          root_review.state_code,
          root_review.review_kind,
          root_review.target_table,
          coalesce(root_review.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(root_review.json, '{}'::jsonb) as json,
          root_review.deadline,
          root_review.created_at,
          root_candidate.matching_task_modified_at as modified_at,
          coalesce(root_comments.comment_state_codes, '[]'::jsonb)
            as comment_state_codes,
          root_candidate.root_matches_status,
          true as root_can_read
        from root_candidates as root_candidate
        join private.reviews as root_review
          on root_review.id = root_candidate.root_review_id
          and root_review.review_kind = 'root'
        left join lateral (
          select pg_catalog.jsonb_agg(
            pg_catalog.to_jsonb(root_comment.state_code)
            order by root_comment.created_at, root_comment.reviewer_id
          ) filter (where root_comment.reviewer_id is not null)
            as comment_state_codes
          from private.comments as root_comment
          where root_comment.review_id = root_review.id
        ) as root_comments on true
      )
      select q.*, pg_catalog.count(*) over() as total_count
      from q
      order by
        case when v_sort_key = 'created_at' and v_order_dir = 'asc'
          then q.created_at end asc nulls last,
        case when v_sort_key = 'created_at' and v_order_dir = 'desc'
          then q.created_at end desc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'asc'
          then q.deadline end asc nulls last,
        case when v_sort_key = 'deadline' and v_order_dir = 'desc'
          then q.deadline end desc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'asc'
          then q.state_code end asc nulls last,
        case when v_sort_key = 'state_code' and v_order_dir = 'desc'
          then q.state_code end desc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'asc'
          then q.modified_at end asc nulls last,
        case when v_sort_key = 'modified_at' and v_order_dir = 'desc'
          then q.modified_at end desc nulls last,
        q.id asc
      limit v_limit offset v_offset;
end;
$$;

ALTER FUNCTION "api"."qry_review_get_admin_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_get_admin_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_get_admin_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_review_get_admin_root_queue_items_v2"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
