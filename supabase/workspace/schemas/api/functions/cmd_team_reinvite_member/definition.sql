CREATE OR REPLACE FUNCTION "api"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
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

  select role
    into v_existing_role
  from private.roles
  where user_id = p_user_id
    and team_id = p_team_id
  for update;

  if v_existing_role is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'ROLE_NOT_FOUND',
      'status', 404,
      'message', 'Role not found'
    );
  end if;

  if not api.policy_roles_update(p_user_id, p_team_id, 'is_invited') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot reinvite this member'
    );
  end if;

  update private.roles
    set role = 'is_invited',
        modified_at = now()
  where user_id = p_user_id
    and team_id = p_team_id
  returning to_jsonb(roles.*)
    into v_role_row;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_team_reinvite_member',
    v_actor,
    'roles',
    p_user_id,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
end;
$$;

ALTER FUNCTION "api"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_team_reinvite_member"("p_team_id" "uuid", "p_user_id" "uuid", "p_audit" "jsonb") TO "authenticated";
