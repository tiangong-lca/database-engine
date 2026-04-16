CREATE OR REPLACE FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") RETURNS boolean
    LANGUAGE "sql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
  SELECT (
    ((
        -- 验证用户是否已经有团队角色，且角色不为rejected
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND r.role <> 'rejected'
            and r.team_id <> '00000000-0000-0000-0000-000000000000')
        ) = false

    AND
    (
        -- 验证当前用户创建团队时，是否为自己分配owner角色，且团队ID未被使用
        ((
            (_user_id = auth.uid() AND _role = 'owner' AND 
            EXISTS (
                SELECT 1
                FROM public.roles r
                WHERE r.team_id = _team_id) = false)
        ))

        OR
        -- 验证当前用户是否为团队管理员或拥有者，邀请的用户角色是否为is_invited角色
        ((
            _role = 'is_invited' AND 
            EXISTS (
                SELECT 1
                FROM public.roles r
                WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND (r.role = 'admin' OR r.role = 'owner'))
        ))
    ))

    OR
    (
        -- 验证用户是否已经有审核团队角色
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND r.role like 'review-%'
            AND r.team_id = '00000000-0000-0000-0000-000000000000') = false

        AND
        -- 验证当前用户是否为审核管理员，邀请的用户角色是否为review-member角色
        (
        _role = 'review-member' AND _team_id = '00000000-0000-0000-0000-000000000000'::uuid AND
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND r.role = 'review-admin')
        )
    )

    OR
    (
        -- 验证用户是否已经有系统团队角色
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = _user_id
            AND (r.role = 'admin' OR r.role = 'member')
            AND r.team_id = '00000000-0000-0000-0000-000000000000') = false

        AND
        -- 验证当前用户是否为系统管理员，邀请的用户角色是否为member角色
        (
        _role = 'member' AND _team_id = '00000000-0000-0000-0000-000000000000'::uuid AND
        EXISTS (
            SELECT 1
            FROM public.roles r
            WHERE r.user_id = auth.uid() AND r.team_id = _team_id AND r.role = 'admin')
        )
    )

    );
$$;

ALTER FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "anon";

GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "authenticated";

GRANT ALL ON FUNCTION "public"."policy_roles_insert"("_user_id" "uuid", "_team_id" "uuid", "_role" "text") TO "service_role";
