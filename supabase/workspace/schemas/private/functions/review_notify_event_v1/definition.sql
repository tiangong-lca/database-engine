CREATE OR REPLACE FUNCTION "private"."review_notify_event_v1"("p_event_type" "text", "p_review_id" "uuid", "p_recipient_user_id" "uuid", "p_sender_user_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_root_review_id" "uuid" DEFAULT NULL::"uuid", "p_scope_version" integer DEFAULT NULL::integer, "p_reason_code" "text" DEFAULT NULL::"text") RETURNS "text"
    LANGUAGE "sql" SECURITY DEFINER
    SET "search_path" TO 'pg_catalog', 'pg_temp'
    AS $$
  with payload as (
    select
      p_event_type || '|' ||
      p_review_id::text || '|' ||
      p_recipient_user_id::text || '|' ||
      coalesce(p_root_review_id::text, '') || '|' ||
      coalesce(p_scope_version::text, '') as event_key
  )
  insert into private.notifications (
    recipient_user_id,
    sender_user_id,
    type,
    dataset_type,
    dataset_id,
    dataset_version,
    json
  )
  select
    p_recipient_user_id,
    coalesce(p_sender_user_id, p_recipient_user_id),
    'review_event',
    p_target_table,
    p_target_id,
    p_target_version,
    jsonb_strip_nulls(jsonb_build_object(
      'event_key', payload.event_key,
      'event_type', p_event_type,
      'review_id', p_review_id,
      'root_review_id', p_root_review_id,
      'scope_version', p_scope_version,
      'reason_code', p_reason_code,
      'target_table', p_target_table,
      'target_id', p_target_id,
      'target_version', p_target_version
    ))
  from payload
  on conflict (
    recipient_user_id,
    sender_user_id,
    type,
    dataset_type,
    dataset_id,
    dataset_version
  )
  do update
  set json = excluded.json,
      modified_at = now()
  returning notifications.json->>'event_key';
$$;

ALTER FUNCTION "private"."review_notify_event_v1"("p_event_type" "text", "p_review_id" "uuid", "p_recipient_user_id" "uuid", "p_sender_user_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_root_review_id" "uuid", "p_scope_version" integer, "p_reason_code" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."review_notify_event_v1"("p_event_type" "text", "p_review_id" "uuid", "p_recipient_user_id" "uuid", "p_sender_user_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_root_review_id" "uuid", "p_scope_version" integer, "p_reason_code" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."review_notify_event_v1"("p_event_type" "text", "p_review_id" "uuid", "p_recipient_user_id" "uuid", "p_sender_user_id" "uuid", "p_target_table" "text", "p_target_id" "uuid", "p_target_version" "text", "p_root_review_id" "uuid", "p_scope_version" integer, "p_reason_code" "text") TO "api_internal_executor";
