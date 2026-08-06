CREATE OR REPLACE FUNCTION "api"."svc_identity_desired_state_read"("p_keycloak_sub" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_state private.identity_center_users%rowtype;
begin
  if nullif(pg_catalog.btrim(p_keycloak_sub), '') is null then
    raise exception using errcode = '22023', message = 'KEYCLOAK_SUB_REQUIRED';
  end if;
  select identity_state.* into v_state
  from private.identity_center_users as identity_state
  where identity_state.keycloak_sub = p_keycloak_sub;
  if not found then return null; end if;
  return pg_catalog.to_jsonb(v_state);
end
$$;

ALTER FUNCTION "api"."svc_identity_desired_state_read"("p_keycloak_sub" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."svc_identity_desired_state_read"("p_keycloak_sub" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."svc_identity_desired_state_read"("p_keycloak_sub" "text") TO "service_role";
