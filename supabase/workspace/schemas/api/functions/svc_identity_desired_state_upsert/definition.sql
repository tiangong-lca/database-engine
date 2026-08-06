CREATE OR REPLACE FUNCTION "api"."svc_identity_desired_state_upsert"("p_keycloak_sub" "text", "p_status" "text" DEFAULT NULL::"text", "p_role_code" "text" DEFAULT NULL::"text", "p_role_operation" "text" DEFAULT 'preserve'::"text", "p_metadata" "jsonb" DEFAULT NULL::"jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_operation text := lower(coalesce(nullif(pg_catalog.btrim(p_role_operation), ''), 'preserve'));
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null then
    raise exception using errcode = '22023', message = 'KEYCLOAK_SUB_REQUIRED';
  end if;
  if p_status is not null and p_status not in ('active', 'disabled', 'revoked', 'deleted') then
    raise exception using errcode = '22023', message = 'INVALID_IDENTITY_STATUS';
  end if;
  if v_operation not in ('preserve', 'set', 'revoke') then
    raise exception using errcode = '22023', message = 'INVALID_ROLE_OPERATION';
  end if;
  if v_operation = 'set' and nullif(pg_catalog.btrim(p_role_code), '') is null then
    raise exception using errcode = '22023', message = 'ROLE_CODE_REQUIRED';
  end if;

  insert into private.identity_center_users as current_state (
    keycloak_sub, status, desired_role, metadata, created_at, modified_at
  ) values (
    p_keycloak_sub,
    coalesce(p_status, 'active'),
    case when v_operation = 'set' then p_role_code else null end,
    coalesce(p_metadata, '{}'::jsonb),
    pg_catalog.now(),
    pg_catalog.now()
  )
  on conflict (keycloak_sub) do update set
    status = coalesce(p_status, current_state.status),
    desired_role = case v_operation
      when 'preserve' then current_state.desired_role
      when 'set' then p_role_code
      when 'revoke' then case
        when p_role_code is null or current_state.desired_role = p_role_code then null
        else current_state.desired_role
      end
    end,
    metadata = coalesce(p_metadata, current_state.metadata),
    modified_at = pg_catalog.now()
  returning * into v_state;

  return pg_catalog.to_jsonb(v_state);
end
$$;

ALTER FUNCTION "api"."svc_identity_desired_state_upsert"("p_keycloak_sub" "text", "p_status" "text", "p_role_code" "text", "p_role_operation" "text", "p_metadata" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_desired_state_upsert"("p_keycloak_sub" "text", "p_status" "text", "p_role_code" "text", "p_role_operation" "text", "p_metadata" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_desired_state_upsert"("p_keycloak_sub" "text", "p_status" "text", "p_role_code" "text", "p_role_operation" "text", "p_metadata" "jsonb") TO "service_role";
