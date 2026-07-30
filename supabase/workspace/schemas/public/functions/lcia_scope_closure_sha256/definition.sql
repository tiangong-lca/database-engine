CREATE OR REPLACE FUNCTION "public"."lcia_scope_closure_sha256"("p_document" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select encode(extensions.digest(coalesce(p_document, '{}'::jsonb)::text, 'sha256'), 'hex')
$$;

ALTER FUNCTION "public"."lcia_scope_closure_sha256"("p_document" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."lcia_scope_closure_sha256"("p_document" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."lcia_scope_closure_sha256"("p_document" "jsonb") TO "service_role";
