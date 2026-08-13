CREATE OR REPLACE FUNCTION "private"."lca_release_is_service_request"() RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select current_user = 'service_role'
      or coalesce(util.is_service_request(), false)
$$;

ALTER FUNCTION "private"."lca_release_is_service_request"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_release_is_service_request"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_release_is_service_request"() TO "api_internal_executor";
