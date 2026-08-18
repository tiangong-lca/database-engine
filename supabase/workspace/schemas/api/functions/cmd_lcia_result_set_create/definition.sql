CREATE OR REPLACE FUNCTION "api"."cmd_lcia_result_set_create"("p_name" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_result_set private.lcia_result_sets%rowtype;
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
  if coalesce(length(btrim(p_name)), 0) = 0 then
    return api.lcia_scope_closure_error(
      'invalid_result_set_name', 400, 'Result set name is required'
    );
  end if;

  insert into private.lcia_result_sets (name)
  values (btrim(p_name))
  returning * into v_result_set;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'schemaVersion', 'lcia.result-set.v1',
      'resultSetId', v_result_set.id,
      'name', v_result_set.name,
      'createdAt', v_result_set.created_at
    )
  );
end;
$$;

ALTER FUNCTION "api"."cmd_lcia_result_set_create"("p_name" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_lcia_result_set_create"("p_name" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_set_create"("p_name" "text") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_lcia_result_set_create"("p_name" "text") TO "authenticated";
