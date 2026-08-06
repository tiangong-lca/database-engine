CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_replace_exchanges"("p_payload" "jsonb", "p_exchanges" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" IMMUTABLE STRICT
    SET "search_path" TO ''
    AS $$
declare
  v_original jsonb := p_payload #> '{processDataSet,exchanges,exchange}';
begin
  if jsonb_typeof(v_original) = 'array' then
    return jsonb_set(
      p_payload,
      '{processDataSet,exchanges,exchange}',
      p_exchanges,
      false
    );
  elsif jsonb_typeof(v_original) = 'object'
    and jsonb_array_length(p_exchanges) = 1 then
    return jsonb_set(
      p_payload,
      '{processDataSet,exchanges,exchange}',
      p_exchanges->0,
      false
    );
  end if;
  return null;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_replace_exchanges"("p_payload" "jsonb", "p_exchanges" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_replace_exchanges"("p_payload" "jsonb", "p_exchanges" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_replace_exchanges"("p_payload" "jsonb", "p_exchanges" "jsonb") TO "api_internal_executor";
