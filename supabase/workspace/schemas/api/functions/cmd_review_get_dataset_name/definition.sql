CREATE OR REPLACE FUNCTION "api"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO ''
    AS $$
  select case p_table
    when 'contacts' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'sources' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,sourceDataSet,sourceInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,sourceDataSet,sourceInformation,dataSetInformation,common:shortName}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'unitgroups' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'flowproperties' then pg_catalog.jsonb_build_object(
      'baseName', coalesce(
        nullif(nullif(p_row#>'{json,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        nullif(nullif(p_row#>'{json_ordered,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}', '{}'::jsonb), '[]'::jsonb),
        '[]'::jsonb
      )
    )
    when 'flows' then coalesce(
      p_row#>'{json,flowDataSet,flowInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,flowDataSet,flowInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'processes' then coalesce(
      p_row#>'{json,processDataSet,processInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,processDataSet,processInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'lifecyclemodels' then coalesce(
      p_row#>'{json,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    else '{}'::jsonb
  end
$$;

ALTER FUNCTION "api"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "api"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "api_internal_executor";
