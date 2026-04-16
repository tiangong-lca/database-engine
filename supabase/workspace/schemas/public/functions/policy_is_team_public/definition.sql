CREATE OR REPLACE FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM public.teams t
    WHERE t.id = _team_id
      AND t.is_public);
$$;

ALTER FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_is_team_public"("_team_id" "uuid") TO "service_role";
