CREATE OR REPLACE FUNCTION "api"."qry_review_get_admin_queue_items"("p_status" "text" DEFAULT NULL::"text", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'modified_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("id" "uuid", "data_id" "uuid", "data_version" "text", "state_code" integer, "reviewer_id" "jsonb", "json" "jsonb", "deadline" timestamp with time zone, "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "comment_state_codes" "jsonb", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := api.cmd_review_resolve_queue_order_by(p_sort_by, false);
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

  return query execute format(
    $sql$
      with q as (
        select
          r.id,
          r.data_id,
          r.data_version::text as data_version,
          r.state_code,
          coalesce(r.reviewer_id, '[]'::jsonb) as reviewer_id,
          coalesce(r.json, '{}'::jsonb) as json,
          r.deadline,
          r.created_at,
          r.modified_at,
          coalesce(
            jsonb_agg(to_jsonb(c.state_code)
              order by c.created_at, c.reviewer_id)
              filter (where c.reviewer_id is not null),
            '[]'::jsonb
          ) as comment_state_codes
        from private.reviews as r
        left join private.comments as c on c.review_id = r.id
        where r.review_kind in ('root', 'reference')
          and ($1::integer is null or r.state_code = $1::integer)
        group by r.id
      )
      select q.*, count(*) over() as total_count
      from q
      order by %s %s nulls last, q.id asc
      limit $2 offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_state_code, v_limit, v_offset;
end;
$_$;

ALTER FUNCTION "api"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_review_get_admin_queue_items"("p_status" "text", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "api_internal_executor";
