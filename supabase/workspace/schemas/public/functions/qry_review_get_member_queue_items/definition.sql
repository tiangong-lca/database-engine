CREATE OR REPLACE FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text" DEFAULT 'pending'::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "review_state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_code" integer, "comment_json" "jsonb", "comment_created_at" timestamp with time zone, "comment_modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_review_resolve_queue_order_by(p_sort_by, true);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
  v_status text := lower(coalesce(p_status, 'pending'));
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_review_is_review_member(v_actor) then
    return;
  end if;

  if v_status not in ('pending', 'reviewed', 'reviewer-rejected') then
    return;
  end if;

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code as review_state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          c.state_code as comment_state_code,
          coalesce(c.json::jsonb, '{}'::jsonb) as comment_json,
          c.created_at as comment_created_at,
          c.modified_at as comment_modified_at
        from public.comments as c
        join public.reviews as r
          on r.id = c.review_id
        where c.reviewer_id = $1
          and public.policy_review_can_read(r.id, $1)
          and (
            ($4::text = 'pending' and c.state_code = 0 and r.state_code > 0)
            or ($4::text = 'reviewed' and c.state_code = any (array[1, 2, -3]) and r.state_code > 0)
            or ($4::text = 'reviewer-rejected' and c.state_code = -1 and r.state_code = -1)
          )
      )
      select
        q.id,
        q.data_id,
        q.data_version,
        q.review_state_code,
        q.reviewer_id,
        q.json,
        q.deadline,
        q.created_at,
        q.modified_at,
        q.comment_state_code,
        q.comment_json,
        q.comment_created_at,
        q.comment_modified_at,
        count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_actor, v_limit, v_offset, v_status;
end;
$_$;

ALTER FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_review_get_member_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "service_role";
