CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_artifact_v2_lease_sha256"("p_lease_token" "uuid") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE STRICT PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select encode(
    extensions.digest(convert_to(p_lease_token::text, 'UTF8'), 'sha256'),
    'hex'
  )
$$;

ALTER FUNCTION "private"."lcia_scope_closure_artifact_v2_lease_sha256"("p_lease_token" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_lease_sha256"("p_lease_token" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_artifact_v2_lease_sha256"("p_lease_token" "uuid") TO "api_internal_executor";
