CREATE OR REPLACE FUNCTION "public"."qry_review_get_member_list"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text", "p_role" "text" DEFAULT NULL::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := public.cmd_membership_resolve_member_order_by(p_sort_by, false);
  v_order_dir text := public.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not public.cmd_membership_is_review_admin(v_actor) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          r.user_id,
          r.team_id,
          r.role::text as role,
          coalesce(u.raw_user_meta_data->>'email', '') as email,
          coalesce(
            nullif(u.raw_user_meta_data->>'display_name', ''),
            u.raw_user_meta_data->>'email',
            '-'
          ) as display_name,
          r.created_at,
          r.modified_at
        from public.roles as r
        left join public.users as u
          on u.id = r.user_id
        where r.team_id = $1
          and r.role in ('review-admin', 'review-member')
          and ($4::text is null or r.role = $4::text)
      )
      select
        m.user_id,
        m.team_id,
        m.role,
        m.email,
        m.display_name,
        m.created_at,
        m.modified_at,
        count(*) over() as total_count
      from members as m
      order by %s %s nulls last, m.user_id asc
      limit $2
      offset $3
    $sql$,
    v_order_by,
    v_order_dir
  )
  using v_team_id, v_limit, v_offset, p_role;
end;
$_$;

ALTER FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_review_get_member_list"("p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text", "p_role" "text") TO "service_role";
