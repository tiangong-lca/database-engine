CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_sha256_text"("p_value" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select encode(extensions.digest(coalesce(p_value, ''), 'sha256'), 'hex')
$$;

ALTER FUNCTION "public"."lcia_scope_closure_sha256_text"("p_value" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_sha256_text"("p_value" "text") FROM PUBLIC;
