CREATE OR REPLACE FUNCTION "api"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
  SELECT (
	-- 验证当前用户是否为团队管理员或拥有者，被删除用户角色不能为owner角色，自己不能删除自己
	(
		_role <> 'owner' AND _user_id <> auth.uid() AND
		EXISTS (
			SELECT 1
			FROM private.roles r
			WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND (r.role = 'admin' OR r.role = 'owner' OR r.role = 'review-admin'))
	)
  );
$$;

ALTER FUNCTION "api"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "api_internal_executor";
