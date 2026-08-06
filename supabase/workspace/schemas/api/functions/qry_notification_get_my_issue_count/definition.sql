CREATE OR REPLACE FUNCTION "api"."qry_notification_get_my_issue_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select count(*)::integer
  from private.notifications as n
  where n.recipient_user_id = auth.uid()
    and n.type = 'validation_issue'
    and (
      (p_last_view_at is not null and n.modified_at > p_last_view_at) or
      (p_last_view_at is null and (
        coalesce(p_days, 3) <= 0 or
        n.modified_at >= now() - make_interval(days => greatest(coalesce(p_days, 3), 0))
      ))
    );
$$;

ALTER FUNCTION "api"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_issue_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";
