CREATE OR REPLACE FUNCTION "api"."policy_user_has_team"("_user_id" "uuid") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
  SELECT EXISTS (
    SELECT 1
    FROM private.roles r
    WHERE r.user_id = _user_id
      AND r.role <> 'rejected'
	  and r.team_id <> '00000000-0000-0000-0000-000000000000');
$$;

ALTER FUNCTION "api"."policy_user_has_team"("_user_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_user_has_team"("_user_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_user_has_team"("_user_id" "uuid") TO "api_internal_executor";
