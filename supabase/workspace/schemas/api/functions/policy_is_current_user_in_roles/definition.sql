CREATE OR REPLACE FUNCTION "api"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
	-- 增加空数组判断：空数组直接返回 false
    SELECT CASE
        WHEN cardinality(p_roles_to_check) = 0 THEN false  -- cardinality() 获取数组长度
		-- 核心逻辑：用 EXISTS 判断是否存在匹配记录，效率更高（无需聚合，找到即返回）
        ELSE EXISTS (
        SELECT 1
        FROM private.roles r
        WHERE r.user_id = auth.uid()                  -- 匹配当前登录用户
          AND r.team_id = p_team_id                   -- 匹配目标团队
          AND r.role <> 'rejected'::text              -- 排除无效的「拒绝」角色
          AND r.role = ANY(p_roles_to_check)          -- 关键：判断用户角色是否在输入的角色数组中（任意一个匹配即可）
     )
	 END;
$$;

ALTER FUNCTION "api"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."policy_is_current_user_in_roles"("p_team_id" "uuid", "p_roles_to_check" "text"[]) TO "api_internal_executor";
