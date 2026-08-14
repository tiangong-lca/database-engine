CREATE OR REPLACE FUNCTION "private"."update_modified_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
begin
  new.modified_at = now();
  return new;
end;
$$;

ALTER FUNCTION "private"."update_modified_at"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."update_modified_at"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."update_modified_at"() TO "service_role";

GRANT ALL ON FUNCTION "private"."update_modified_at"() TO "api_internal_executor";
