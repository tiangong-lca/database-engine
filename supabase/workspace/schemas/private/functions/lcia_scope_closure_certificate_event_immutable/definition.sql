CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_certificate_event_immutable"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
begin
  raise exception 'lcia_scope_closure_certificate_event_immutable'
    using errcode = '23514';
end;
$$;

ALTER FUNCTION "private"."lcia_scope_closure_certificate_event_immutable"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_certificate_event_immutable"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_certificate_event_immutable"() TO "api_internal_executor";
