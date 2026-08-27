CREATE OR REPLACE FUNCTION "private"."portal_lcia_projection_row_guard_v1"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
begin
  raise exception 'portal_lcia_projection_row_immutable'
    using errcode = '55000';
end
$$;

ALTER FUNCTION "private"."portal_lcia_projection_row_guard_v1"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."portal_lcia_projection_row_guard_v1"() FROM PUBLIC;
