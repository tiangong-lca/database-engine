CREATE OR REPLACE FUNCTION "util"."is_service_request"() RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_role text;
  v_claims jsonb;
  v_headers jsonb;
  v_api_key text;
  v_authorization text;
  v_bearer_token text;
  v_project_secret_key text;
begin
  v_role := nullif(current_setting('request.jwt.claim.role', true), '');

  if v_role = 'service_role' then
    return true;
  end if;

  begin
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  exception when others then
    v_claims := null;
  end;

  if v_claims->>'role' = 'service_role' then
    return true;
  end if;

  begin
    v_headers := nullif(current_setting('request.headers', true), '')::jsonb;
  exception when others then
    v_headers := null;
  end;

  if v_headers is null then
    return false;
  end if;

  select h.value #>> '{}'
  into v_api_key
  from jsonb_each(v_headers) as h(key, value)
  where lower(h.key) = 'apikey'
  limit 1;

  select h.value #>> '{}'
  into v_authorization
  from jsonb_each(v_headers) as h(key, value)
  where lower(h.key) = 'authorization'
  limit 1;

  if v_authorization ~* '^bearer[[:space:]]+' then
    v_bearer_token := regexp_replace(v_authorization, '^bearer[[:space:]]+', '', 'i');
  end if;

  if nullif(v_api_key, '') is null and nullif(v_bearer_token, '') is null then
    return false;
  end if;

  begin
    v_project_secret_key := util.project_secret_key();
  exception when others then
    return false;
  end;

  return nullif(v_project_secret_key, '') is not null
    and (
      coalesce(v_api_key = v_project_secret_key, false)
      or coalesce(v_bearer_token = v_project_secret_key, false)
    );
end;
$$;

ALTER FUNCTION "util"."is_service_request"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."is_service_request"() FROM PUBLIC;
