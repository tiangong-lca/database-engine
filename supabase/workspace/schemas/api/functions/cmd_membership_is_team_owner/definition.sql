CREATE OR REPLACE FUNCTION "api"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select exists (
    select 1
    from private.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role = 'owner'
  )
$$;

ALTER FUNCTION "api"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_membership_is_team_owner"("p_actor" "uuid", "p_team_id" "uuid") TO "api_internal_executor";
