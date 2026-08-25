begin;

insert into private.api_capability_grants (
  routine_identity,
  capability_id,
  allow_anon,
  allow_authenticated,
  allow_service_role
)
values
  (
    'api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)',
    'EDGE-AI-WORKER-01',
    false,
    false,
    true
  ),
  (
    'api.svc_ai_tidas_suggestion_read(uuid, uuid)',
    'EDGE-AI-WORKER-01',
    false,
    false,
    true
  )
on conflict (routine_identity) do update
set capability_id = excluded.capability_id,
    allow_anon = excluded.allow_anon,
    allow_authenticated = excluded.allow_authenticated,
    allow_service_role = excluded.allow_service_role;

revoke all on function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)
  from public, anon, authenticated;
revoke all on function api.svc_ai_tidas_suggestion_read(uuid, uuid)
  from public, anon, authenticated;

grant execute on function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)
  to service_role;
grant execute on function api.svc_ai_tidas_suggestion_read(uuid, uuid)
  to service_role;

notify pgrst, 'reload schema';

commit;
