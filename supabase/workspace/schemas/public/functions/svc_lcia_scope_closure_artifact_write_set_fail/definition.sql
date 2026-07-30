CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_fail"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_error" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_write_set public.lcia_scope_closure_artifact_write_sets%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  if nullif(trim(coalesce(p_error, '')), '') is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_failure_invalid', 400, 'Failure reason is required'
    );
  end if;
  select * into v_write_set
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id
  for update;
  if v_write_set.id is null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_write_set.write_token is distinct from p_write_token then
    return public.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  if v_write_set.contract_version is not null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_fence_required',
      409,
      'Use the worker-lease-fenced v2 failure RPC'
    );
  end if;
  if v_write_set.status <> 'staging' then
    return public.lcia_scope_closure_error(
      'artifact_write_set_token_invalid',
      409,
      'Artifact write-set token is not current'
    );
  end if;
  update public.lcia_scope_closure_artifact_write_sets
  set status = 'cleanup_pending',
      failure_reason = left(trim(p_error), 1000),
      updated_at = now()
  where id = v_write_set.id;
  return jsonb_build_object(
    'ok', true,
    'data', public.lcia_scope_closure_artifact_write_set_json(v_write_set.id)
  );
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_fail"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_error" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_fail"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_error" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_fail"("p_write_set_id" "uuid", "p_write_token" "uuid", "p_error" "text") TO "service_role";
