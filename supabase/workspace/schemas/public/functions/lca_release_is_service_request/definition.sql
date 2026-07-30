CREATE OR REPLACE FUNCTION "public"."lca_release_is_service_request"() RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select current_user = 'service_role'
      or coalesce(util.is_service_request(), false)
$$;

ALTER FUNCTION "public"."lca_release_is_service_request"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lca_release_is_service_request"() FROM PUBLIC;
