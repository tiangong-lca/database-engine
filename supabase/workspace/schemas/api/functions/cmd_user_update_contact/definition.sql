CREATE OR REPLACE FUNCTION "api"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb" DEFAULT '{}'::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'api', 'private', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
declare
  v_actor uuid := auth.uid();
  v_user_row jsonb;
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

  if v_actor <> p_user_id and not api.cmd_membership_is_review_admin(v_actor) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FORBIDDEN',
      'status', 403,
      'message', 'The actor cannot update this contact'
    );
  end if;

  update private.users
    set contact = p_contact
  where id = p_user_id
  returning to_jsonb(users.*)
    into v_user_row;

  if v_user_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'USER_NOT_FOUND',
      'status', 404,
      'message', 'User not found'
    );
  end if;

  insert into private.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    payload
  )
  values (
    'cmd_user_update_contact',
    v_actor,
    'users',
    p_user_id,
    coalesce(p_audit, '{}'::jsonb)
  );

  return jsonb_build_object(
    'ok', true,
    'data', v_user_row
  );
end;
$$;

ALTER FUNCTION "api"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") TO "api_internal_executor";

GRANT ALL ON FUNCTION "api"."cmd_user_update_contact"("p_user_id" "uuid", "p_contact" "jsonb", "p_audit" "jsonb") TO "authenticated";
