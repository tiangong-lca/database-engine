CREATE OR REPLACE FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT (
  	-- 验证当前用户是否为团队成员（非拒绝状态）
    EXISTS (
		select 1 from public.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id and r0.role <> 'rejected')

    OR
    -- 验证当前用户是否为审核团队/系统管理团队成员
    (_team_id = '00000000-0000-0000-0000-000000000000'::uuid and
    EXISTS (
		select 1 from public.roles r0
		where r0.user_id = auth.uid() and r0.team_id = _team_id))

    OR
    -- 验证当前团队是否为公开团队的拥有者，用于展示加入团队的联系信息
    _role = 'owner' AND
    EXISTS (
        SELECT 1 FROM public.teams t
        WHERE t.id = _team_id AND t.is_public)
	);
$$;

ALTER FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_roles_select"("_team_id" "uuid", "_role" "text") TO "service_role";
