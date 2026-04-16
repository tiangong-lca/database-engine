CREATE OR REPLACE FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT (
	-- 验证当前用户是否为团队管理员或拥有者，被删除用户角色不能为owner角色，自己不能删除自己
	(
		_role <> 'owner' AND _user_id <> auth.uid() AND
		EXISTS (
			SELECT 1
			FROM public.roles r
			WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND (r.role = 'admin' OR r.role = 'owner' OR r.role = 'review-admin'))
	)
  );
$$;

ALTER FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_roles_delete"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "service_role";
