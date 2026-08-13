CREATE OR REPLACE FUNCTION "util"."project_url"() RETURNS "text"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  secret_value text;
begin
  -- Retrieve the project URL from Vault
  select ds.decrypted_secret
    into secret_value
  from vault.decrypted_secrets ds
  where ds.name = 'project_url';

  if secret_value is null or secret_value = '' then
    raise exception 'Missing vault secret: project_url';
  end if;

  return secret_value;
end;
$$;

ALTER FUNCTION "util"."project_url"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."project_url"() FROM PUBLIC;
