drop index if exists public.notifications_review_event_recipient_uidx;

create or replace function private.review_notify_event_v1(
  p_event_type text,
  p_review_id uuid,
  p_recipient_user_id uuid,
  p_sender_user_id uuid,
  p_target_table text,
  p_target_id uuid,
  p_target_version text,
  p_root_review_id uuid default null,
  p_scope_version integer default null,
  p_reason_code text default null
)
returns text
language sql
security definer
set search_path = pg_catalog, pg_temp
as $$
  with payload as (
    select
      p_event_type || '|' ||
      p_review_id::text || '|' ||
      p_recipient_user_id::text || '|' ||
      coalesce(p_root_review_id::text, '') || '|' ||
      coalesce(p_scope_version::text, '') as event_key
  )
  insert into public.notifications (
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

alter function private.review_notify_event_v1(
  text, uuid, uuid, uuid, text, uuid, text, uuid, integer, text
) owner to postgres;

revoke all on function private.review_notify_event_v1(
  text, uuid, uuid, uuid, text, uuid, text, uuid, integer, text
) from public, anon, authenticated, service_role;
