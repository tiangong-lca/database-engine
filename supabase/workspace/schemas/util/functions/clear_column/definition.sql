CREATE OR REPLACE FUNCTION "util"."clear_column"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
declare
    clear_column text := TG_ARGV[0];
begin
    NEW := NEW #= extensions.hstore(clear_column, NULL);
    return NEW;
end;
$$;

ALTER FUNCTION "util"."clear_column"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."clear_column"() FROM PUBLIC;
