CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_invalidate_wrapper_permit_v1"("p_invocation_id" "uuid") RETURNS "void"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
declare
  v_token text := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
begin
  update util.dataset_flow_identity_wrapper_invocations
  set generation = generation + 1,
    token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    status = 'superseded',
    updated_at = clock_timestamp(),
    closed_at = clock_timestamp()
  where id = p_invocation_id and status = 'active';
  if not found then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_WRAPPER_PERMIT_INVALIDATE_FAILED';
  end if;
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_invalidate_wrapper_permit_v1"("p_invocation_id" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_invalidate_wrapper_permit_v1"("p_invocation_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_invalidate_wrapper_permit_v1"("p_invocation_id" "uuid") TO "api_internal_executor";
