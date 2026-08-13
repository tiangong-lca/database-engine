CREATE OR REPLACE FUNCTION "api"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions'
    AS $$
  SELECT (
	-- 验证当前用户是否为团队拥有者或管理员
	(
	EXISTS (
		select 1 from private.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id and (r0.role ='admin' or r0.role='owner'))
	and
	(
	-- 切换admin和member
	((_role = 'admin' or _role = 'member') and
	  EXISTS (
		SELECT 1
		FROM private.roles r1
		WHERE r1.user_id = _user_id and r1.team_id = _team_id and (r1.role = 'admin' or r1.role = 'member')))
	or
	-- 重新邀请已经拒绝的用户
	(_role = 'is_invited' and
		EXISTS (
			SELECT 1
			FROM private.roles r2
			WHERE r2.user_id = _user_id and r2.team_id = _team_id and r2.role = 'rejected'))
	))
	or
	-- 验证当前用户，接受邀请或拒绝邀请
	((_role = 'member' or _role = 'rejected') and _user_id = auth.uid() and
	EXISTS (
		select 1 from private.roles r3
		where r3.user_id = _user_id and r3.team_id = _team_id and r3.role ='is_invited'))
	);
$$;

ALTER FUNCTION "api"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_roles_update"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "api_internal_executor";
