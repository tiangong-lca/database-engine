CREATE OR REPLACE FUNCTION "private"."lca_release_is_manager"() RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select auth.uid() is not null
     and api.policy_is_current_user_in_roles(
       '00000000-0000-0000-0000-000000000000'::uuid,
       array['data_product_manager']
     )
$$;

ALTER FUNCTION "private"."lca_release_is_manager"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lca_release_is_manager"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lca_release_is_manager"() TO "api_internal_executor";
