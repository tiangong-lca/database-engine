CREATE OR REPLACE FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
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

  if not public.policy_roles_update(v_actor, p_team_id, 'rejected') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
    );
  end if;

  update public.roles
    set role = 'rejected',
        modified_at = now()
  where user_id = v_actor
    and team_id = p_team_id
    and role = 'is_invited'
  returning to_jsonb(roles.*)
    into v_role_row;

  if v_role_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVITATION_NOT_FOUND',
      'status', 404,
      'message', 'No matching invitation was found for the actor'
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
    'cmd_team_reject_invitation',
    v_actor,
    'roles',
    v_actor,
    p_team_id::text,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_role_row
  );
end;
$$;

ALTER FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_team_reject_invitation"("p_team_id" "uuid", "p_audit" "jsonb") TO "service_role";
