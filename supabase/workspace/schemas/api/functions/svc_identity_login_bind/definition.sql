CREATE OR REPLACE FUNCTION "api"."svc_identity_login_bind"("p_keycloak_sub" "text", "p_user_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null or p_user_id is null then
    raise exception using errcode = '22023', message = 'IDENTITY_BINDING_REQUIRED';
  end if;
  begin
    insert into private.identity_center_users as current_state (
      keycloak_sub, user_id, status, metadata, created_at, modified_at
    ) values (
      p_keycloak_sub, p_user_id, 'active', '{}'::jsonb, pg_catalog.now(), pg_catalog.now()
    )
    on conflict (keycloak_sub) do update set
      user_id = excluded.user_id,
      status = 'active',
      modified_at = pg_catalog.now()
    where current_state.user_id is null or current_state.user_id = excluded.user_id
    returning * into v_state;
  exception when unique_violation then
    raise exception using errcode = '23505', message = 'IDENTITY_USER_ALREADY_BOUND';
  end;
  if v_state.keycloak_sub is null then
    raise exception using errcode = '23505', message = 'IDENTITY_SUBJECT_ALREADY_BOUND';
  end if;
  return pg_catalog.to_jsonb(v_state);
end
$$;

ALTER FUNCTION "api"."svc_identity_login_bind"("p_keycloak_sub" "text", "p_user_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_login_bind"("p_keycloak_sub" "text", "p_user_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_login_bind"("p_keycloak_sub" "text", "p_user_id" "uuid") TO "service_role";
