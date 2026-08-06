CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_reference"("p_exchange" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
  select jsonb_build_object(
    '@refObjectId', p_exchange #> '{referenceToFlowDataSet,@refObjectId}',
    '@type', p_exchange #> '{referenceToFlowDataSet,@type}',
    '@uri', p_exchange #> '{referenceToFlowDataSet,@uri}',
    '@version', p_exchange #> '{referenceToFlowDataSet,@version}',
    'common:shortDescription',
      p_exchange #> '{referenceToFlowDataSet,common:shortDescription}'
  )
$$;

ALTER FUNCTION "private"."dataset_flow_identity_reference"("p_exchange" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_reference"("p_exchange" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_reference"("p_exchange" "jsonb") TO "api_internal_executor";
