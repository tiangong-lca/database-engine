CREATE OR REPLACE FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text" DEFAULT NULL::"text", "p_action" "text" DEFAULT 'set'::"text", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_action text := lower(coalesce(p_action, 'set'));
  v_team_id uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_actor_is_owner boolean;
  v_actor_is_manager boolean;
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

  if p_user_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_ID_REQUIRED',
      'status', 400,
      'message', 'userId is required'
    );
  end if;

  v_actor_is_owner := public.cmd_membership_is_system_owner(v_actor);
  v_actor_is_manager := public.cmd_membership_is_system_manager(v_actor);

  if not v_actor_is_manager then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot manage system members'
    );
  end if;

  select role
    into v_existing_role
  from public.roles
  where user_id = p_user_id
    and team_id = v_team_id
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

    if p_user_id = v_actor or v_existing_role = 'owner' then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The actor cannot remove this system member'
      );
    end if;

    delete from public.roles
    where user_id = p_user_id
      and team_id = v_team_id;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_system_change_member_role',
      v_actor,
      'roles',
      p_user_id,
      v_team_id::text,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'action', 'remove'
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', jsonb_build_object(
        'removed', true,
        'user_id', p_user_id,
        'team_id', v_team_id
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

  if p_role not in ('member', 'admin') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_ROLE',
      'status', 400,
      'message', 'Unsupported system role transition'
    );
  end if;

  if p_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only the system owner can assign admin roles'
    );
  end if;

  if p_role = 'member' and v_existing_role = 'admin' and not v_actor_is_owner then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'Only the system owner can demote an admin'
    );
  end if;

  if v_existing_role is null then
    insert into public.roles (
      user_id,
      team_id,
      role,
      modified_at
    )
    values (
      p_user_id,
      v_team_id,
      p_role,
      now()
    )
    returning to_jsonb(roles.*)
      into v_role_row;
  elsif v_existing_role in ('owner', 'admin', 'member') then
    if v_existing_role = 'owner' then
      return jsonb_build_object(
        'ok', false,
        'code', 'FORBIDDEN',
        'status', 403,
        'message', 'The owner role cannot be modified'
      );
    end if;

    update public.roles
      set role = p_role,
          modified_at = now()
    where user_id = p_user_id
      and team_id = v_team_id
    returning to_jsonb(roles.*)
      into v_role_row;
  else
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
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
    'cmd_system_change_member_role',
    v_actor,
    'roles',
    p_user_id,
    v_team_id::text,
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
      'code', 'ROLE_CONFLICT',
      'status', 409,
      'message', 'The existing zero-team role belongs to another scope'
    );
end;
$$;

ALTER FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_system_change_member_role"("p_user_id" "uuid", "p_role" "text", "p_action" "text", "p_audit" "jsonb") TO "service_role";
