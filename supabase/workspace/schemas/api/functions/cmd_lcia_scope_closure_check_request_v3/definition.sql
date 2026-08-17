CREATE OR REPLACE FUNCTION "api"."cmd_lcia_scope_closure_check_request_v3"("p_result_set_id" "uuid", "p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_result jsonb;
  v_closure_check_id uuid;
begin
  if v_actor is null then
    return api.lcia_scope_closure_error(
      'auth_required', 401, 'Authentication required'
    );
  end if;
  if not api.lcia_scope_closure_is_manager() then
    return api.lcia_scope_closure_error(
      'not_data_product_manager', 403,
      'Data product manager role is required'
    );
  end if;
  if not exists (
    select 1 from private.lcia_result_sets where id = p_result_set_id
  ) then
    return api.lcia_scope_closure_error(
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

  v_result := api.cmd_lcia_scope_closure_check_request_v2(
    p_requested_scope,
    p_request_idempotency_token,
    coalesce(p_audit, '{}'::jsonb)
      || jsonb_build_object('resultSetId', p_result_set_id)
  );
  if coalesce((v_result->>'ok')::boolean, false) is not true then
    return v_result;
  end if;

  v_closure_check_id := nullif(
    v_result->'data'->>'closureCheckId', ''
  )::uuid;

  update private.lcia_scope_closure_checks
  set result_set_id = p_result_set_id,
      updated_at = now()
  where id = v_closure_check_id
    and requested_by = v_actor
    and (result_set_id is null or result_set_id = p_result_set_id);

  if not found then
    return api.lcia_scope_closure_error(
      'closure_check_result_set_conflict', 409,
      'Closure check is already bound to another result set'
    );
  end if;

  return jsonb_set(
    v_result,
    '{data,resultSetId}',
    to_jsonb(p_result_set_id),
    true
  );
end;
$$;

ALTER FUNCTION "api"."cmd_lcia_scope_closure_check_request_v3"("p_result_set_id" "uuid", "p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_scope_closure_check_request_v3"("p_result_set_id" "uuid", "p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_scope_closure_check_request_v3"("p_result_set_id" "uuid", "p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lcia_scope_closure_check_request_v3"("p_result_set_id" "uuid", "p_requested_scope" "jsonb", "p_request_idempotency_token" "text", "p_audit" "jsonb") TO "authenticated";
