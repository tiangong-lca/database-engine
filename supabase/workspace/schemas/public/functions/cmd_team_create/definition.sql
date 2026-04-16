CREATE OR REPLACE FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_team_row jsonb;
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

  if public.policy_user_has_team(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ALREADY_ASSIGNED',
      'status', 409,
      'message', 'The actor already belongs to a team'
    );
  end if;

  if not public.policy_roles_insert(v_actor, p_team_id, 'owner') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor is not allowed to create this team'
    );
  end if;

  delete from public.roles
  where user_id = v_actor
    and role = 'rejected'
    and team_id <> '00000000-0000-0000-0000-000000000000'::uuid;

  insert into public.teams (
    id,
    json,
    rank,
    is_public,
    modified_at
  )
  values (
    p_team_id,
    p_json,
    coalesce(p_rank, -1),
    coalesce(p_is_public, false),
    now()
  )
  returning to_jsonb(teams.*)
    into v_team_row;

  insert into public.roles (
    user_id,
    team_id,
    role,
    modified_at
  )
  values (
    v_actor,
    p_team_id,
    'owner',
    now()
  )
  returning to_jsonb(roles.*)
    into v_role_row;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_team_create',
    v_actor,
    'teams',
    p_team_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', jsonb_build_object(
      'team', v_team_row,
      'owner_role', v_role_row
    )
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'TEAM_ALREADY_EXISTS',
      'status', 409,
      'message', 'The team already exists'
    );
end;
$$;

ALTER FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_team_create"("p_team_id" "uuid", "p_json" "jsonb", "p_rank" integer, "p_is_public" boolean, "p_audit" "jsonb") TO "service_role";
