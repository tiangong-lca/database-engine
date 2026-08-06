CREATE OR REPLACE FUNCTION "util"."dataset_alias_execution_server_context"() RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_project_url text;
  v_host text;
  v_project_ref text;
  v_environment text;
begin
  v_project_url := btrim(util.project_url());
  v_host := lower(
    pg_catalog.regexp_replace(
      v_project_url,
      '^https?://([^/:]+).*$'::text,
      '\1'::text
    )
  );

  if nullif(v_host, '') is null or v_host = lower(v_project_url) then
    raise exception using
      errcode = '22023',
      message = 'Branch-local project_url is not a valid HTTP(S) project URL';
  end if;

  if v_host in ('127.0.0.1', 'localhost', 'kong', 'host.docker.internal') then
    v_project_ref := 'local';
    v_environment := 'local';
  elsif v_host ~ '^[a-z0-9-]+\.supabase\.co$' then
    v_project_ref := pg_catalog.split_part(v_host, '.', 1);
    v_environment := case
      when v_project_ref = 'qgzvkongdjqiiamzbbts' then 'production'
      else 'preview'
    end;
  else
    v_project_ref := v_host;
    v_environment := 'preview';
  end if;

  return jsonb_build_object(
    'environment', v_environment,
    'project_ref', v_project_ref,
    'project_url_sha256', util.dataset_alias_execution_sha256(v_project_url)
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_alias_execution_server_context"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_alias_execution_server_context"() FROM PUBLIC;
