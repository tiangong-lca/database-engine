CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = p_actor
      and team_id = p_team_id
      and role in ('owner', 'admin')
  )
$$;

ALTER FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_membership_is_team_manager"("p_actor" "uuid", "p_team_id" "uuid") TO "service_role";
