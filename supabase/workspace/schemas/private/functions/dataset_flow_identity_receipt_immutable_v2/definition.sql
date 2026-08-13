CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_receipt_immutable_v2"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
begin
  raise exception using errcode = '55000',
    message = 'FLOW_IDENTITY_CAPTURE_RECEIPT_IMMUTABLE';
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_receipt_immutable_v2"() OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_receipt_immutable_v2"() FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_receipt_immutable_v2"() TO "api_internal_executor";
