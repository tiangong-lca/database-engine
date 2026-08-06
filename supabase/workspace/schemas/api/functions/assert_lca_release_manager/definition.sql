CREATE OR REPLACE FUNCTION "api"."assert_lca_release_manager"() RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
begin
  if v_actor is null then
    return private.lca_release_error('auth_required', 401, 'Authentication required');
  end if;
  if not private.lca_release_is_manager() then
    return private.lca_release_error(
      'not_data_product_manager',
      403,
      'Data product manager role is required'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'userId', v_actor,
      'role', 'data_product_manager'
    )
  );
end;
$$;

ALTER FUNCTION "api"."assert_lca_release_manager"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."assert_lca_release_manager"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."assert_lca_release_manager"() TO "api_internal_executor";
