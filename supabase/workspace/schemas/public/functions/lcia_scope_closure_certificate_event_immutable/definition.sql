CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_certificate_event_immutable"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
begin
  raise exception 'lcia_scope_closure_certificate_event_immutable'
    using errcode = '23514';
end;
$$;

ALTER FUNCTION "public"."lcia_scope_closure_certificate_event_immutable"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_certificate_event_immutable"() FROM PUBLIC;
