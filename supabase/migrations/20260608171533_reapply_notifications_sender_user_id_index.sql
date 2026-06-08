create index concurrently if not exists notifications_sender_user_id_idx
  on public.notifications (sender_user_id);
