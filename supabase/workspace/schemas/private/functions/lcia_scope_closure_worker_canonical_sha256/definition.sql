CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_worker_canonical_sha256"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select encode(
    extensions.digest(
      convert_to(
        private.lcia_scope_closure_worker_canonical_json_text(p_value),
        'UTF8'
      ),
      'sha256'
    ),
    'hex'
  )
$$;

ALTER FUNCTION "private"."lcia_scope_closure_worker_canonical_sha256"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_worker_canonical_sha256"("p_value" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_worker_canonical_sha256"("p_value" "jsonb") TO "api_internal_executor";
