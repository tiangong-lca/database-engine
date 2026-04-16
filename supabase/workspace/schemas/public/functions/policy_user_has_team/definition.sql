CREATE OR REPLACE FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.roles r
    WHERE r.user_id = _user_id
      AND r.role <> 'rejected'
	  and r.team_id <> '00000000-0000-0000-0000-000000000000');
$$;

ALTER FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_user_has_team"("_user_id" "uuid") TO "service_role";
