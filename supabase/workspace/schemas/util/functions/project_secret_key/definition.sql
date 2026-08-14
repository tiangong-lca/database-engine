CREATE OR REPLACE FUNCTION "util"."project_secret_key"() RETURNS "text"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  secret_value text;
begin
  select ds.decrypted_secret
    into secret_value
  from vault.decrypted_secrets ds
  where ds.name = 'project_secret_key';

  if secret_value is null or secret_value = '' then
    raise exception 'Missing vault secret: project_secret_key';
  end if;

  return secret_value;
end;
$$;

ALTER FUNCTION "util"."project_secret_key"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."project_secret_key"() FROM PUBLIC;
