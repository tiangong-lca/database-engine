CREATE OR REPLACE FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select exists (
    select 1
    from public.roles
    where user_id = coalesce(p_actor, auth.uid())
      and team_id = '00000000-0000-0000-0000-000000000000'::uuid
      and role = 'owner'
  )
$$;

ALTER FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_membership_is_system_owner"("p_actor" "uuid") TO "service_role";
