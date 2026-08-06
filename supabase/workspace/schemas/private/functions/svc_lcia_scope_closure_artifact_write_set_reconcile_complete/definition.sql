CREATE OR REPLACE FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_reconcile_complete"("p_write_set_id" "uuid", "p_reconcile_token" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_write_set private.lcia_scope_closure_artifact_write_sets%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return api.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select * into v_write_set
  from private.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
    return api.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.status = 'cleaned'
     and v_write_set.reconcile_token = p_reconcile_token then
    return jsonb_build_object('ok', true, 'reused', true);
  end if;
  if v_write_set.status <> 'cleanup_pending'
     or v_write_set.reconcile_token is distinct from p_reconcile_token
     or v_write_set.reconcile_expires_at is null
     or v_write_set.reconcile_expires_at < now() then
    return api.lcia_scope_closure_error(
      'artifact_write_set_reconcile_invalid',
      409,
      'Artifact write-set reconcile claim is not current'
    );
  end if;
  update private.lcia_scope_closure_artifact_write_sets
  set status = 'cleaned',
      cleaned_at = now(),
      updated_at = now(),
      reconcile_expires_at = null
  where id = v_write_set.id;
  return jsonb_build_object('ok', true, 'reused', false, 'data',
    private.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_reconcile_complete"("p_write_set_id" "uuid", "p_reconcile_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_reconcile_complete"("p_write_set_id" "uuid", "p_reconcile_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_reconcile_complete"("p_write_set_id" "uuid", "p_reconcile_token" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."svc_lcia_scope_closure_artifact_write_set_reconcile_complete"("p_write_set_id" "uuid", "p_reconcile_token" "uuid") TO "api_internal_executor";
