CREATE OR REPLACE FUNCTION "api"."policy_is_team_public"("_team_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM private.teams t
    WHERE t.id = _team_id
      AND t.is_public);
$$;

ALTER FUNCTION "api"."policy_is_team_public"("_team_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_is_team_public"("_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_is_team_public"("_team_id" "uuid") TO "api_internal_executor";
