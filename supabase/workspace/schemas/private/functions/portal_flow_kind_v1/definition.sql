CREATE OR REPLACE FUNCTION "private"."portal_flow_kind_v1"("p_type" "text") RETURNS "text"
    LANGUAGE "sql" IMMUTABLE PARALLEL SAFE
    SET "search_path" TO ''
    AS $$
  select case lower(btrim(coalesce(p_type, '')))
    when 'elementary flow' then 'elementary'
    when 'waste flow' then 'waste'
    when 'product flow' then 'product'
    when 'other flow' then 'other'
    else 'unknown'
  end
$$;

ALTER FUNCTION "private"."portal_flow_kind_v1"("p_type" "text") OWNER TO "portal_public_executor";

REVOKE ALL ON FUNCTION "private"."portal_flow_kind_v1"("p_type" "text") FROM PUBLIC;
