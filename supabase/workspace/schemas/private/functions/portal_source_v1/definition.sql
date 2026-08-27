CREATE OR REPLACE FUNCTION "private"."portal_source_v1"("p_kind" "text", "p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $_$
declare
  v_publication jsonb := private.portal_publication_root_v1(p_kind, p_json);
  v_database jsonb := v_publication -> 'common:referenceToUnchangedRepublication';
  v_source jsonb;
begin
  v_source := case p_kind
    when 'process' then p_json #> '{processDataSet,modellingAndValidation,dataSourcesTreatmentAndRepresentativeness,referenceToDataSource}'
    when 'flow' then p_json #> '{flowDataSet,modellingAndValidation,dataSourcesTreatmentAndRepresentativeness,referenceToDataSource}'
    else null
  end;
  if jsonb_typeof(v_source) = 'array' and jsonb_array_length(v_source) = 1 then
    v_source := v_source -> 0;
  elsif jsonb_typeof(v_source) <> 'object' then
    v_source := null;
  end if;
  return jsonb_build_object(
    'databaseId', case
      when lower(coalesce(v_database ->> '@refObjectId', '')) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then lower(v_database ->> '@refObjectId')
      else null
    end,
    'databaseVersion', case
      when coalesce(v_database ->> '@version', '') ~ '^\d{2}\.\d{2}\.\d{3}$'
        then v_database ->> '@version'
      else null
    end,
    'sourceRecordId', case
      when lower(coalesce(v_source ->> '@refObjectId', '')) ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        then lower(v_source ->> '@refObjectId')
      else null
    end,
    'providerName', private.portal_localized_text_v1(
      v_publication #> '{common:referenceToOwnershipOfDataSet,common:shortDescription}'
    ),
    'licenseId', nullif(private.portal_scalar_text_v1(v_publication -> 'common:licenseType'), ''),
    'licenseUrl', null
  );
end
$_$;

ALTER FUNCTION "private"."portal_source_v1"("p_kind" "text", "p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_source_v1"("p_kind" "text", "p_json" "jsonb") FROM PUBLIC;
