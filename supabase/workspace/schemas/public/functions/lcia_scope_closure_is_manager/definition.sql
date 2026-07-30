CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_is_manager"() RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select public.lcia_result_is_manager()
$$;

ALTER FUNCTION "public"."lcia_scope_closure_is_manager"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_is_manager"() FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lcia_scope_closure_is_manager"() TO "service_role";

GRANT ALL ON FUNCTION "public"."lcia_scope_closure_is_manager"() TO "authenticated";
