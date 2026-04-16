CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_days" integer DEFAULT 3) RETURNS TABLE("id" "uuid", "state_code" integer, "json" "jsonb", "modified_at" timestamp with time zone, "total_count" integer)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select
    r.id,
    r.state_code,
    coalesce(r.json, '{}'::jsonb) as json,
    r.modified_at,
    count(*) over ()::integer as total_count
  from public.reviews as r
  where coalesce(r.json -> 'user' ->> 'id', '') = auth.uid()::text
    and r.state_code in (1, -1, 2)
    and (
      coalesce(p_days, 3) <= 0 or
      r.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by r.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0) * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1);
$$;

ALTER FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "service_role";
