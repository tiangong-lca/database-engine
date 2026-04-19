CREATE OR REPLACE FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_team_row jsonb;
  v_can_manage boolean;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_team_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ID_REQUIRED',
      'status', 400,
      'message', 'teamId is required'
    );
  end if;

  if p_json is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_JSON_REQUIRED',
      'status', 400,
      'message', 'json is required'
    );
  end if;

  select
    public.cmd_membership_is_team_manager(v_actor, p_team_id) or
    public.cmd_membership_is_system_manager(v_actor)
  into v_can_manage;

  if not coalesce(v_can_manage, false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot update this team profile'
    );
  end if;

  update public.teams
    set json = p_json,
        is_public = coalesce(p_is_public, false),
        modified_at = now()
  where id = p_team_id
  returning to_jsonb(teams.*)
    into v_team_row;

  if v_team_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_NOT_FOUND',
      'status', 404,
      'message', 'Team not found'
    );
  end if;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_team_update_profile',
    v_actor,
    'teams',
    p_team_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_team_row
  );
end;
$$;

ALTER FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_team_update_profile"("p_team_id" "uuid", "p_json" "jsonb", "p_is_public" boolean, "p_audit" "jsonb") TO "service_role";
