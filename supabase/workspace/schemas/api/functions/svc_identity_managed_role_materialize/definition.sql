CREATE OR REPLACE FUNCTION "api"."svc_identity_managed_role_materialize"("p_keycloak_sub" "text", "p_user_id" "uuid") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_system_team_id constant uuid := '00000000-0000-0000-0000-000000000000'::uuid;
  v_managed_roles constant text[] := array['admin', 'review-admin', 'review-member'];
  v_desired_role text;
  v_previous_role text;
  v_effective_role text;
  v_changed boolean := false;
  v_reason text;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null or p_user_id is null then
    raise exception using errcode = '22023', message = 'IDENTITY_BINDING_REQUIRED';
  end if;
  select desired_role::text into v_desired_role
  from private.identity_center_users
  where keycloak_sub = p_keycloak_sub and user_id = p_user_id
  for update;
  if not found then
    raise exception using errcode = 'P0001', message = 'IDENTITY_BINDING_MISMATCH';
  end if;

  select role::text into v_previous_role
  from private.roles
  where user_id = p_user_id and team_id = v_system_team_id
  for update;
  v_effective_role := v_previous_role;

  if v_desired_role = any(v_managed_roles) then
    if v_previous_role is null then
      insert into private.roles as existing_role (user_id, team_id, role, created_at, modified_at)
      values (p_user_id, v_system_team_id, v_desired_role, pg_catalog.now(), pg_catalog.now())
      on conflict (user_id, team_id) do update set
        role = excluded.role,
        modified_at = pg_catalog.now()
      where existing_role.role::text = any(v_managed_roles)
        and existing_role.role::text is distinct from excluded.role::text
      returning role::text into v_effective_role;
      if found then
        v_changed := true;
        v_reason := 'managed_role_materialized';
      else
        select role::text into v_effective_role from private.roles
        where user_id = p_user_id and team_id = v_system_team_id;
        v_reason := case when v_effective_role = v_desired_role
          then 'already_materialized' else 'non_managed_role_preserved' end;
      end if;
    elsif v_previous_role = any(v_managed_roles) and v_previous_role is distinct from v_desired_role then
      update private.roles set role = v_desired_role, modified_at = pg_catalog.now()
      where user_id = p_user_id and team_id = v_system_team_id
        and role::text = any(v_managed_roles)
      returning role::text into v_effective_role;
      v_changed := found;
      v_reason := case when v_changed then 'managed_role_updated' else 'managed_role_changed_concurrently' end;
    else
      v_reason := case when v_previous_role = v_desired_role
        then 'already_materialized' else 'non_managed_role_preserved' end;
    end if;
  elsif v_desired_role is null and v_previous_role = any(v_managed_roles) then
    update private.roles set role = 'member', modified_at = pg_catalog.now()
    where user_id = p_user_id and team_id = v_system_team_id
      and role::text = any(v_managed_roles)
    returning role::text into v_effective_role;
    v_changed := found;
    v_reason := 'managed_role_revoked';
  elsif v_desired_role is null then
    v_reason := case when v_previous_role is null
      then 'no_desired_or_current_role' else 'non_managed_role_preserved' end;
  else
    v_reason := 'unsupported_desired_role';
  end if;

  return pg_catalog.jsonb_build_object(
    'keycloak_sub', p_keycloak_sub,
    'user_id', p_user_id,
    'team_id', v_system_team_id,
    'desired_role', v_desired_role,
    'previous_role', v_previous_role,
    'effective_role', v_effective_role,
    'changed', v_changed,
    'reason', v_reason
  );
end
$$;

ALTER FUNCTION "api"."svc_identity_managed_role_materialize"("p_keycloak_sub" "text", "p_user_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_managed_role_materialize"("p_keycloak_sub" "text", "p_user_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_managed_role_materialize"("p_keycloak_sub" "text", "p_user_id" "uuid") TO "service_role";
