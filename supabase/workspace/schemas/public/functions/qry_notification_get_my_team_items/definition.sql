CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer DEFAULT 3) RETURNS TABLE("team_id" "uuid", "user_id" "uuid", "role" "text", "team_title" "jsonb", "modified_at" timestamp with time zone)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select
    r.team_id,
    r.user_id,
    r.role,
    coalesce(t.json -> 'title', '[]'::jsonb) as team_title,
    r.modified_at
  from public.roles as r
  join public.teams as t
    on t.id = r.team_id
  where r.user_id = auth.uid()
    and r.team_id <> '00000000-0000-0000-0000-000000000000'::uuid
    and (
      coalesce(p_days, 3) <= 0 or
      r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by r.modified_at desc;
$$;

ALTER FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_team_items"("p_days" integer) TO "service_role";
