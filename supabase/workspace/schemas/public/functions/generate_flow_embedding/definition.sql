CREATE OR REPLACE FUNCTION "public"."generate_flow_embedding"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
DECLARE
  request_url text;
  legacy_x_key text;
BEGIN
  request_url := util.project_url();
  legacy_x_key := util.project_x_key();

  SELECT embedding, extracted_text INTO NEW.embedding, NEW.extracted_text
  FROM supabase_functions.http_request(
    request_url || '/functions/v1/flow_embedding',
    'POST',
    jsonb_build_object(
      'Content-Type', 'application/json',
      'x_key', legacy_x_key,
      'x_region', 'us-east-1'
    )::text,
    to_json(NEW.json_ordered)::text,
    '1000'
  );
  RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."generate_flow_embedding"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "anon";

GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."generate_flow_embedding"() TO "service_role";
