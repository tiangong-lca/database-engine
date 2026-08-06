CREATE OR REPLACE FUNCTION "api"."qry_notification_get_my_data_items"("p_page" integer DEFAULT 1, "p_page_size" integer DEFAULT 10, "p_days" integer DEFAULT 3) RETURNS TABLE("id" "uuid", "state_code" integer, "json" "jsonb", "modified_at" timestamp with time zone, "total_count" integer)
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
  with visible_reviews as (
    select review_row.id, review_row.state_code, review_row.json,
      review_row.modified_at
    from private.reviews as review_row
    where coalesce(review_row.json->'user'->>'id', '') = auth.uid()::text
      and review_row.state_code in (1, -1, 2)
    union
    select review_row.id, review_row.state_code, review_row.json,
      greatest(review_row.modified_at, notification_row.modified_at)
    from private.notifications as notification_row
    join private.reviews as review_row
      on review_row.id = nullif(notification_row.json->>'review_id', '')::uuid
    where notification_row.recipient_user_id = auth.uid()
      and notification_row.type = 'review_event'
      and review_row.state_code in (-1, 2)
  )
  select
    visible_review.id,
    visible_review.state_code,
    coalesce(visible_review.json, '{}'::jsonb),
    visible_review.modified_at,
    count(*) over ()::integer
  from visible_reviews as visible_review
  where coalesce(p_days, 3) <= 0
    or visible_review.modified_at >= now()
      - make_interval(days => greatest(coalesce(p_days, 3), 0))
  order by visible_review.modified_at desc
  offset greatest(coalesce(p_page, 1) - 1, 0)
    * greatest(coalesce(p_page_size, 10), 1)
  limit greatest(coalesce(p_page_size, 10), 1)
$$;

ALTER FUNCTION "api"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."qry_notification_get_my_data_items"("p_page" integer, "p_page_size" integer, "p_days" integer) TO "authenticated";
