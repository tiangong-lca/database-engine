CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_sha256_text"("p_value" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select encode(extensions.digest(coalesce(p_value, ''), 'sha256'), 'hex')
$$;

ALTER FUNCTION "private"."lcia_scope_closure_sha256_text"("p_value" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_sha256_text"("p_value" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_sha256_text"("p_value" "text") TO "api_internal_executor";
