CREATE OR REPLACE FUNCTION "api"."qry_notification_get_my_data_count"("p_days" integer DEFAULT 3, "p_last_view_at" timestamp with time zone DEFAULT NULL::timestamp with time zone) RETURNS integer
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  with visible_reviews as (
    select review_row.id, review_row.modified_at
    from private.reviews as review_row
    where coalesce(review_row.json->'user'->>'id', '') = auth.uid()::text
      and review_row.state_code in (1, -1, 2)
    union
    select review_row.id,
      greatest(review_row.modified_at, notification_row.modified_at)
    from private.notifications as notification_row
    join private.reviews as review_row
      on review_row.id = nullif(notification_row.json->>'review_id', '')::uuid
    where notification_row.recipient_user_id = auth.uid()
      and notification_row.type = 'review_event'
      and review_row.state_code in (-1, 2)
  )
  select count(*)::integer
  from visible_reviews
  where (
    p_last_view_at is not null and modified_at > p_last_view_at
  ) or (
    p_last_view_at is null and (
      coalesce(p_days, 3) <= 0
      or modified_at >= now()
        - make_interval(days => greatest(coalesce(p_days, 3), 0))
    )
  )
$$;

ALTER FUNCTION "api"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_data_count"("p_days" integer, "p_last_view_at" timestamp with time zone) TO "authenticated";
