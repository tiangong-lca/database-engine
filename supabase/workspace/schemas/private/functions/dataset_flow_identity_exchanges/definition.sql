CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_exchanges"("p_payload" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE STRICT
    SET "search_path" TO ''
    AS $$
declare
  v_exchange jsonb := p_payload #> '{processDataSet,exchanges,exchange}';
begin
  if jsonb_typeof(v_exchange) = 'array' then
    return v_exchange;
  elsif jsonb_typeof(v_exchange) = 'object' then
    return jsonb_build_array(v_exchange);
  end if;
  return null;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_exchanges"("p_payload" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_exchanges"("p_payload" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_exchanges"("p_payload" "jsonb") TO "api_internal_executor";
