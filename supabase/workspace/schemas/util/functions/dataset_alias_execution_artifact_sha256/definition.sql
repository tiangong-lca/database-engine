CREATE OR REPLACE FUNCTION "util"."dataset_alias_execution_artifact_sha256"("p_value" "jsonb") RETURNS "text"
    LANGUAGE "sql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
  select util.dataset_alias_execution_sha256(
    private.dataset_alias_canonical_jsonb_v1(p_value)
  )
$$;

ALTER FUNCTION "util"."dataset_alias_execution_artifact_sha256"("p_value" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_alias_execution_artifact_sha256"("p_value" "jsonb") FROM PUBLIC;
