CREATE OR REPLACE FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_days" integer DEFAULT 3) RETURNS TABLE("id" "uuid", "type" "text", "dataset_type" "text", "dataset_id" "uuid", "dataset_version" "text", "json" "jsonb", "modified_at" timestamp with time zone, "total_count" integer)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select
    n.id,
    n.type,
    n.dataset_type,
    n.dataset_id,
    n.dataset_version,
    n.json,
    n.modified_at,
    count(*) over ()::integer as total_count
  from public.notifications as n
  where n.recipient_user_id = auth.uid()
    and n.type = 'validation_issue'
    and (
      coalesce(p_days, 3) <= 0 or
      n.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  order by n.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0) * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1);
$$;

ALTER FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."qry_notification_get_my_issue_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "service_role";
