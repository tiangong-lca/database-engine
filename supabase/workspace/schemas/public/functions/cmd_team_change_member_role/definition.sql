CREATE OR REPLACE FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text" DEFAULT NULL::"text", "p_action" "text" DEFAULT 'set'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_existing_role text;
  v_role_row jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null or p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_PAYLOAD',
      'status', 400,
      'message', 'teamId and userId are required'
    );
  end if;

  if p_team_id = '00000000-0000-0000-0000-000000000000'::uuid then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_TEAM_SCOPE',
      'status', 400,
      'message', 'Use system or review member commands for the zero team scope'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = p_team_id
  for update;

  if v_action = 'remove' then
    if v_existing_role is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'ROLE_NOT_FOUND',
        'status', 404,
        'message', 'Role not found'
      );
    end if;

    if not public.policy_roles_delete(p_user_id, p_team_id, v_existing_role) then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot remove this team member'
      );
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = p_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_team_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      p_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'action', 'remove'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'removed', true,
        'user_id', p_user_id,
        'team_id', p_team_id
      )
    );
  end if;

  if v_action <> 'set' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ACTION',
      'status', 400,
      'message', 'Unsupported action'
    );
  end if;

  if p_role = 'is_invited' then
    if v_existing_role = 'rejected' then
      return jsonb_build_object(
        'ok', false,
        'code', 'REINVITE_REQUIRED',
        'status', 409,
        'message', 'Use the reinvite command for rejected members'
      );
    end if;

    if v_existing_role is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'TEAM_MEMBER_ALREADY_EXISTS',
        'status', 409,
        'message', 'The team membership already exists'
      );
    end if;

    if not public.policy_roles_insert(p_user_id, p_team_id, 'is_invited') then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot invite this user to the team'
      );
    end if;

    insert into public.roles (
      user_id,
      team_id,
      role,
      modified_at
    )
    values (
      p_user_id,
      p_team_id,
      'is_invited',
      now()
    )
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif p_role in ('admin', 'member') then
    if not public.cmd_membership_is_team_owner(v_actor, p_team_id) then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'Only the team owner can change active member roles'
      );
    end if;

    if v_existing_role not in ('admin', 'member') then
      return jsonb_build_object(
        'ok', false,
        'code', 'INVALID_ROLE_STATE',
        'status', 409,
        'message', 'Only active team members can be promoted or demoted'
      );
    end if;

    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = p_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ROLE',
      'status', 400,
      'message', 'Unsupported team role transition'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'action', 'set',
      'role', p_role
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_MEMBER_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team membership already exists'
    );
end;
$$;

ALTER FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_team_change_member_role"("p_team_id" "uuid", "p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "service_role";
