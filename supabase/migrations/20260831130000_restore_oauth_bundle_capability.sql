begin;

-- Persistent Dev exposed both LifecycleModel bundle commands before the OAuth
-- capability foundation, so their manifest rows were repaired as generic CLI
-- routes. Restore the exact least-privilege capability without changing the
-- function ACL or any unrelated route.
update private.api_capability_grants as manifest
set capability_id = 'EDGE-BUNDLE-01'
where pg_catalog.to_regprocedure(manifest.routine_identity) = any (array[
  'api.cmd_lifecycle_model_bundle_save(jsonb)'::regprocedure,
  'api.cmd_lifecycle_model_bundle_delete(uuid,text)'::regprocedure
]);

do $oauth_bundle_capability_invariants$
declare
  v_bundle_count integer;
  v_wrong_count integer;
begin
  select count(*)
  into v_bundle_count
  from private.api_capability_grants as manifest
  where pg_catalog.to_regprocedure(manifest.routine_identity) = any (array[
    'api.cmd_lifecycle_model_bundle_save(jsonb)'::regprocedure,
    'api.cmd_lifecycle_model_bundle_delete(uuid,text)'::regprocedure
  ])
    and manifest.capability_id = 'EDGE-BUNDLE-01'
    and manifest.allow_authenticated
    and not manifest.allow_anon
    and not manifest.allow_service_role;

  if v_bundle_count <> 2 then
    raise exception
      'LifecycleModel OAuth bundle capability repair is incomplete: %/2',
      v_bundle_count;
  end if;

  select count(*)
  into v_wrong_count
  from private.api_capability_grants as manifest
  where pg_catalog.to_regprocedure(manifest.routine_identity) = any (array[
    'api.cmd_lifecycle_model_bundle_save(jsonb)'::regprocedure,
    'api.cmd_lifecycle_model_bundle_delete(uuid,text)'::regprocedure
  ])
    and manifest.capability_id <> 'EDGE-BUNDLE-01';

  if v_wrong_count <> 0 then
    raise exception
      'LifecycleModel OAuth bundle routes retain an overbroad capability';
  end if;
end
$oauth_bundle_capability_invariants$;

commit;
