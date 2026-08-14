CREATE OR REPLACE FUNCTION "util"."invoke_edge_function"("name" "text", "body" "jsonb", "timeout_milliseconds" integer DEFAULT ((5 * 60) * 1000)) RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  service_key text;
begin
  service_key := util.project_secret_key();

  perform net.http_post(
    url => util.project_url() || '/functions/v1/' || name,
    headers => jsonb_build_object(
      'Content-Type', 'application/json',
      'Authorization', 'Bearer ' || service_key,
      'apikey', service_key,
      'x_region', 'us-east-1'
    ),
    body => body,
    timeout_milliseconds => timeout_milliseconds
  );
end;
$$;

ALTER FUNCTION "util"."invoke_edge_function"("name" "text", "body" "jsonb", "timeout_milliseconds" integer) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."invoke_edge_function"("name" "text", "body" "jsonb", "timeout_milliseconds" integer) FROM PUBLIC;
