CREATE OR REPLACE FUNCTION "api"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_sort_by" "text" DEFAULT 'created_at'::"text", "p_sort_order" "text" DEFAULT 'desc'::"text") RETURNS TABLE("user_id" "uuid", "team_id" "uuid", "role" "text", "email" "text", "display_name" "text", "created_at" timestamp with time zone, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_page_size, 10), 100));
  v_offset integer := (greatest(coalesce(p_page, 1), 1) - 1) * v_limit;
  v_order_by text := api.cmd_membership_resolve_member_order_by(p_sort_by, false);
  v_order_dir text := api.cmd_membership_resolve_sort_direction(p_sort_order);
begin
  if v_actor is null then
    return;
  end if;

  if not api.cmd_membership_is_team_manager(v_actor, p_team_id) then
    return;
  end if;

  return query execute format(
    $sql$
      with members as (
        select
          membership.user_id,
          membership.team_id,
          membership.role::text as role,
          coalesce(
            nullif(btrim(auth_user.email), ''),
            nullif(btrim(profile.raw_user_meta_data ->> 'email'), ''),
            ''
          ) as email,
          coalesce(
            nullif(btrim(profile.raw_user_meta_data ->> 'display_name'), ''),
            nullif(btrim(profile.raw_user_meta_data ->> 'name'), ''),
            nullif(btrim(auth_user.email), ''),
            nullif(btrim(profile.raw_user_meta_data ->> 'email'), ''),
            '-'
          ) as display_name,
          membership.created_at,
          membership.modified_at
        from private.roles as membership
        left join private.users as profile
          on profile.id = membership.user_id
        left join auth.users as auth_user
          on auth_user.id = membership.user_id
        where membership.team_id = $1
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
  using p_team_id, v_limit, v_offset;
end;
$_$;

ALTER FUNCTION "api"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_team_get_member_list"("p_team_id" "uuid", "p_page" integer, "p_page_size" integer, "p_sort_by" "text", "p_sort_order" "text") TO "authenticated";
