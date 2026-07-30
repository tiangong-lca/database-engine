CREATE OR REPLACE FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_inspect"("p_write_set_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_data jsonb;
  v_contract_version text;
begin
  if not coalesce(util.is_service_request(), false) then
    return public.lcia_scope_closure_error(
      'service_role_required', 403, 'Service role is required'
    );
  end if;
  select contract_version into v_contract_version
  from public.lcia_scope_closure_artifact_write_sets
  where id = p_write_set_id;
  if not found then
    return public.lcia_scope_closure_error(
      'artifact_write_set_not_found', 404, 'Artifact write-set not found'
    );
  end if;
  if v_contract_version is not null then
    return public.lcia_scope_closure_error(
      'artifact_write_set_v2_status_required',
      409,
      'Use the lease-fenced v2 status RPC'
    );
  end if;
  v_data := public.lcia_scope_closure_artifact_write_set_json(p_write_set_id);
  return jsonb_build_object('ok', true, 'data', v_data);
end;
$$;

ALTER FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_inspect"("p_write_set_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_inspect"("p_write_set_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."svc_lcia_scope_closure_artifact_write_set_inspect"("p_write_set_id" "uuid") TO "service_role";
