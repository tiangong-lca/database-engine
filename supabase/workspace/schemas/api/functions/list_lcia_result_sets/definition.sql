CREATE OR REPLACE FUNCTION "api"."list_lcia_result_sets"("p_limit" integer DEFAULT 100) RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_limit integer := greatest(1, least(coalesce(p_limit, 100), 200));
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

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'items', coalesce((
        select jsonb_agg(
          jsonb_build_object(
            'schemaVersion', 'lcia.result-set.v1',
            'resultSetId', result_set.id,
            'name', result_set.name,
            'createdAt', result_set.created_at
          )
          order by result_set.created_at desc, result_set.id desc
        )
        from (
          select id, name, created_at
          from private.lcia_result_sets
          order by created_at desc, id desc
          limit v_limit
        ) result_set
      ), '[]'::jsonb)
    )
  );
end;
$$;

ALTER FUNCTION "api"."list_lcia_result_sets"("p_limit" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."list_lcia_result_sets"("p_limit" integer) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."list_lcia_result_sets"("p_limit" integer) TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."list_lcia_result_sets"("p_limit" integer) TO "authenticated";
