CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_rotate_wrapper_permit_v1"("p_invocation_id" "uuid", "p_post_kind" "text", "p_terminal" boolean DEFAULT false) RETURNS "jsonb"
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
declare
  v_invocation util.dataset_flow_identity_wrapper_invocations%rowtype;
  v_token text;
begin
  select invocation.* into v_invocation
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = p_invocation_id and invocation.status = 'active'
  for update;
  if v_invocation.id is null
    or p_post_kind not in ('process', 'finalize') then
    raise exception using errcode = 'P0001',
      message = 'FLOW_IDENTITY_WRAPPER_PERMIT_ROTATE_INVALID';
  end if;
  v_token := pg_catalog.encode(extensions.gen_random_bytes(32), 'hex');
  update util.dataset_flow_identity_wrapper_invocations
  set generation = generation + 1,
    token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(v_token),
    successful_process_posts = successful_process_posts
      + case when p_post_kind = 'process' then 1 else 0 end,
    successful_finalize_posts = successful_finalize_posts
      + case when p_post_kind = 'finalize' then 1 else 0 end,
    status = case when p_terminal then 'completed' else status end,
    updated_at = clock_timestamp(),
    closed_at = case when p_terminal then clock_timestamp() else closed_at end
  where id = v_invocation.id
  returning * into v_invocation;
  if p_terminal then
    return null;
  end if;
  return jsonb_build_object(
    'schema_version', 'dataset-flow-identity-execution-permit.v1',
    'invocation_id', v_invocation.id,
    'generation', v_invocation.generation,
    'token', v_token
  );
end;
$$;

ALTER FUNCTION "private"."dataset_flow_identity_rotate_wrapper_permit_v1"("p_invocation_id" "uuid", "p_post_kind" "text", "p_terminal" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_rotate_wrapper_permit_v1"("p_invocation_id" "uuid", "p_post_kind" "text", "p_terminal" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_rotate_wrapper_permit_v1"("p_invocation_id" "uuid", "p_post_kind" "text", "p_terminal" boolean) TO "api_internal_executor";
