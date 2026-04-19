CREATE OR REPLACE FUNCTION "public"."update_modified_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
begin
  new.modified_at = now();
  return new;
end;
$$;

ALTER FUNCTION "public"."update_modified_at"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "anon";

GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "authenticated";

GRANT ALL ON FUNCTION "public"."update_modified_at"() TO "service_role";
