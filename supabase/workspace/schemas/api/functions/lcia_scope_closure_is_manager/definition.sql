CREATE OR REPLACE FUNCTION "api"."lcia_scope_closure_is_manager"() RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select api.lcia_result_is_manager()
$$;

ALTER FUNCTION "api"."lcia_scope_closure_is_manager"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."lcia_scope_closure_is_manager"() FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."lcia_scope_closure_is_manager"() TO "api_internal_executor";
