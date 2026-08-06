CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_sha256"("p_document" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'pg_temp'
    AS $$
  select encode(extensions.digest(coalesce(p_document, '{}'::jsonb)::text, 'sha256'), 'hex')
$$;

ALTER FUNCTION "private"."lcia_scope_closure_sha256"("p_document" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_sha256"("p_document" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_sha256"("p_document" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_sha256"("p_document" "jsonb") TO "api_internal_executor";
