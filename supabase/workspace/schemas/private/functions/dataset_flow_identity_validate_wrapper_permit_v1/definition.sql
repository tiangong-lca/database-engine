CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_validate_wrapper_permit_v1"("p_actor" "uuid", "p_scope_id" "uuid", "p_authorization" "jsonb", "p_post_kind" "text") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $_$
declare
  v_invocation_id uuid;
  v_generation numeric;
begin
  if p_actor is null or p_scope_id is null
    or p_post_kind not in ('process', 'finalize')
    or not coalesce(private.dataset_flow_identity_exact_keys(
      p_authorization,
      array['schema_version', 'invocation_id', 'generation', 'token']
    ), false)
    or p_authorization->>'schema_version'
      <> 'dataset-flow-identity-execution-permit.v1'
    or p_authorization->>'invocation_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or jsonb_typeof(p_authorization->'generation') <> 'number'
    or p_authorization->>'token' !~ '^[a-f0-9]{64}$' then
    return null;
  end if;
  v_generation := (p_authorization->>'generation')::numeric;
  if v_generation < 0 or v_generation > 2147483647
    or v_generation <> trunc(v_generation) then
    return null;
  end if;
  select invocation.id into v_invocation_id
  from util.dataset_flow_identity_wrapper_invocations as invocation
  where invocation.id = (p_authorization->>'invocation_id')::uuid
    and invocation.scope_id = p_scope_id
    and invocation.actor_user_id = p_actor
    and invocation.status = 'active'
    and invocation.generation = v_generation::integer
    and invocation.token_sha256 =
      private.dataset_flow_identity_permit_token_sha256_v1(
        p_authorization->>'token'
      )
    and case p_post_kind
      when 'process' then invocation.successful_process_posts
        < invocation.maximum_process_posts
      when 'finalize' then invocation.successful_finalize_posts
        < invocation.maximum_finalize_posts
      else false
    end;
  return v_invocation_id;
exception when invalid_text_representation or numeric_value_out_of_range then
  return null;
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_validate_wrapper_permit_v1"("p_actor" "uuid", "p_scope_id" "uuid", "p_authorization" "jsonb", "p_post_kind" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_validate_wrapper_permit_v1"("p_actor" "uuid", "p_scope_id" "uuid", "p_authorization" "jsonb", "p_post_kind" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_validate_wrapper_permit_v1"("p_actor" "uuid", "p_scope_id" "uuid", "p_authorization" "jsonb", "p_post_kind" "text") TO "api_internal_executor";
