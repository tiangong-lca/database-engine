CREATE OR REPLACE FUNCTION "private"."portal_process_reference_product_v1"("p_json" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE PARALLEL RESTRICTED
    SET "search_path" TO ''
    AS $_$
declare
  v_reference_internal text := p_json #>> '{processDataSet,processInformation,quantitativeReference,referenceToReferenceFlow}';
  v_exchange jsonb;
  v_reference jsonb;
  v_id_text text;
  v_version text;
  v_id uuid;
  v_flow_json jsonb;
  v_match_count integer;
begin
  select count(*), jsonb_agg(item) -> 0
  into v_match_count, v_exchange
  from private.portal_json_items_v1(p_json #> '{processDataSet,exchanges,exchange}') as item
  where item ->> '@dataSetInternalID' = v_reference_internal;
  if v_match_count <> 1 then
    return '[]'::jsonb;
  end if;
  v_reference := v_exchange -> 'referenceToFlowDataSet';
  v_id_text := lower(btrim(coalesce(v_reference ->> '@refObjectId', '')));
  v_version := btrim(coalesce(v_reference ->> '@version', ''));
  if v_id_text ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
     and v_version ~ '^\d{2}\.\d{2}\.\d{3}$' then
    v_id := v_id_text::uuid;
    select row.json
    into v_flow_json
    from public.flows as row
    where row.id = v_id
      and row.version::text = v_version
      and row.state_code in (100, 200)
      and jsonb_typeof(row.json) = 'object'
      and jsonb_typeof(row.json -> 'flowDataSet') = 'object'
    limit 1;
  end if;
  return coalesce(
    private.portal_localized_text_v1(
      v_flow_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'
    ),
    '[]'::jsonb
  );
end
$_$;

ALTER FUNCTION "private"."portal_process_reference_product_v1"("p_json" "jsonb") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_process_reference_product_v1"("p_json" "jsonb") FROM PUBLIC;
