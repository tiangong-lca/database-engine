CREATE OR REPLACE FUNCTION "util"."invoke_edge_webhook"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  edge_function text := TG_ARGV[0];
  timeout_milliseconds integer := coalesce(nullif(TG_ARGV[1], '')::integer, 1000);
  payload jsonb;
begin
  if edge_function is null or edge_function = '' then
    raise exception 'Missing webhook edge function name';
  end if;

  payload := jsonb_build_object(
    'type', TG_OP,
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'record', case when TG_OP = 'DELETE' then to_jsonb(OLD) else to_jsonb(NEW) end,
    'old_record', case when TG_OP = 'INSERT' then null else to_jsonb(OLD) end
  );

  perform util.invoke_edge_function(
    name => edge_function,
    body => payload,
    timeout_milliseconds => timeout_milliseconds
  );

  if TG_OP = 'DELETE' then
    return OLD;
  end if;

  return NEW;
end;
$$;

ALTER FUNCTION "util"."invoke_edge_webhook"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."invoke_edge_webhook"() FROM PUBLIC;
