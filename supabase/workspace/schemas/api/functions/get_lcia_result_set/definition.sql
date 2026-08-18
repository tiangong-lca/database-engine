CREATE OR REPLACE FUNCTION "api"."get_lcia_result_set"("p_result_set_id" "uuid") RETURNS "jsonb"
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
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

  select *
  into v_result_set
  from private.lcia_result_sets
  where id = p_result_set_id;

  if v_result_set.id is null then
    return api.lcia_scope_closure_error(
      'result_set_not_found', 404, 'Result set not found'
    );
  end if;

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

ALTER FUNCTION "api"."get_lcia_result_set"("p_result_set_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."get_lcia_result_set"("p_result_set_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."get_lcia_result_set"("p_result_set_id" "uuid") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."get_lcia_result_set"("p_result_set_id" "uuid") TO "authenticated";
