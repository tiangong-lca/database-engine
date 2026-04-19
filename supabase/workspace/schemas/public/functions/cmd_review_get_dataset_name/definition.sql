CREATE OR REPLACE FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") RETURNS "jsonb"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
  select case p_table
    when 'contacts' then coalesce(
      p_row#>'{json,contactDataSet,contactInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,contactDataSet,contactInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'sources' then coalesce(
      p_row#>'{json,sourceDataSet,sourceInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,sourceDataSet,sourceInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'unitgroups' then coalesce(
      p_row#>'{json,unitGroupDataSet,unitGroupInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,unitGroupDataSet,unitGroupInformation,dataSetInformation,name}',
      '{}'::jsonb
    )
    when 'flowproperties' then coalesce(
      p_row#>'{json,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,name}',
      p_row#>'{json_ordered,flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,name}',
      '{}'::jsonb
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

ALTER FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "anon";

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "authenticated";

GRANT ALL ON FUNCTION "public"."cmd_review_get_dataset_name"("p_table" "text", "p_row" "jsonb") TO "service_role";
