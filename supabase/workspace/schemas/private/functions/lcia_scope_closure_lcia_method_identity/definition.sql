CREATE OR REPLACE FUNCTION "private"."lcia_scope_closure_lcia_method_identity"("p_locator_id" "uuid", "p_version" "text", "p_document" "jsonb") RETURNS "uuid"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select case
    -- The reviewed LCIA bundle contains one canonical-method/artifact-locator
    -- alias.  Keep this mapping identical to Worker RELEASE_METHOD_IDENTITIES.
    when p_locator_id = '9ec743ea-6b00-400d-a53b-61547a3fc03c'::uuid
      and btrim(p_version) = '01.01.000'
      and p_document #>> '{LCIAMethodDataSet,LCIAMethodInformation,dataSetInformation,common:UUID}'
        = '503699e0-eca9-4089-8bf8-e0f49c93e578'
    then '503699e0-eca9-4089-8bf8-e0f49c93e578'::uuid
    else p_locator_id
  end
$$;

ALTER FUNCTION "private"."lcia_scope_closure_lcia_method_identity"("p_locator_id" "uuid", "p_version" "text", "p_document" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."lcia_scope_closure_lcia_method_identity"("p_locator_id" "uuid", "p_version" "text", "p_document" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."lcia_scope_closure_lcia_method_identity"("p_locator_id" "uuid", "p_version" "text", "p_document" "jsonb") TO "api_internal_executor";
