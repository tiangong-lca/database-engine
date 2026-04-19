CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select count(*)::integer
  from public.reviews as r
  where coalesce(r.json -> 'user' ->> 'id', '') = auth.uid()::text
    and r.state_code in (1, -1, 2)
    and (
      (p_last_view_at is not null and r.modified_at > p_last_view_at) or
      (p_last_view_at is null and (
        coalesce(p_days, 3) <= 0 or
        r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
      ))
    );
$$;

ALTER FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "anon";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "service_role";
