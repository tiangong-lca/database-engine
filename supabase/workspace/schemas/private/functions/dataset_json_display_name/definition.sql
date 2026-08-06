CREATE OR REPLACE FUNCTION "private"."dataset_json_display_name"("p_entity_kind" "text", "p_json" "jsonb") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    AS $$
  select coalesce(
    case p_entity_kind
      when 'flow' then coalesce(
        private.dataset_json_first_text(p_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{flowDataSet,flowInformation,dataSetInformation,common:name}')
      )
      when 'process' then coalesce(
        private.dataset_json_first_text(p_json #> '{processDataSet,processInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{processDataSet,processInformation,dataSetInformation,common:name}')
      )
      when 'lifecyclemodel' then coalesce(
        private.dataset_json_first_text(p_json #> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,common:name}')
      )
      when 'source' then coalesce(
        private.dataset_json_first_text(p_json #> '{sourceDataSet,sourceInformation,dataSetInformation,common:shortName}'),
        private.dataset_json_first_text(p_json #> '{sourceDataSet,sourceInformation,dataSetInformation,common:name}')
      )
      when 'contact' then coalesce(
        private.dataset_json_first_text(p_json #> '{contactDataSet,contactInformation,dataSetInformation,common:shortName}'),
        private.dataset_json_first_text(p_json #> '{contactDataSet,contactInformation,dataSetInformation,common:name}')
      )
      when 'unitgroup' then private.dataset_json_first_text(
        p_json #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      )
      when 'flowproperty' then private.dataset_json_first_text(
        p_json #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      )
      else null
    end,
    nullif(btrim(p_json ->> 'name'), ''),
    nullif(btrim(p_json ->> 'title'), '')
  );
$$;

ALTER FUNCTION "private"."dataset_json_display_name"("p_entity_kind" "text", "p_json" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_json_display_name"("p_entity_kind" "text", "p_json" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_json_display_name"("p_entity_kind" "text", "p_json" "jsonb") TO "api_internal_executor";
