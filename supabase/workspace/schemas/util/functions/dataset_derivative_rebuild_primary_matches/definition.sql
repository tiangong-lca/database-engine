CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests") RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $$
declare
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
begin
  if p_request.target_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_request.target_id
      and btrim(flow.version::text) = p_request.target_version;
    return util.dataset_derivative_rebuild_primary_matches(
      p_request,
      v_flow
    );
  elsif p_request.target_table = 'processes' then
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_request.target_id
      and btrim(process.version::text) = p_request.target_version;
    return util.dataset_derivative_rebuild_primary_matches(
      p_request,
      v_process
    );
  end if;

  return false;
end;
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_flow" "public"."flows") RETURNS boolean
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    p_request.target_table = 'flows'
    and p_flow.id is not null
    and p_flow.user_id is not null
    and p_flow.state_code is not null
    and p_flow.modified_at is not null
    and p_flow.id = p_request.target_id
    and btrim(p_flow.version::text) = p_request.target_version
    and p_flow.user_id = p_request.actor_user_id
    and p_flow.state_code = 0
    and p_flow.modified_at = p_request.expected_modified_at
    and p_flow.json is not null
    and p_flow.json_ordered is not null
    and util.dataset_derivative_rebuild_sha256(
      p_flow.json::jsonb::text
    ) = p_request.expected_json_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_flow.json_ordered::jsonb::text
    ) = p_request.expected_json_ordered_sha256,
    false
  )
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_flow" "public"."flows") OWNER TO "postgres";

CREATE OR REPLACE FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_process" "public"."processes") RETURNS boolean
    LANGUAGE "sql" STABLE
    SET "search_path" TO ''
    AS $$
  select coalesce(
    p_request.target_table = 'processes'
    and p_process.id is not null
    and p_process.user_id is not null
    and p_process.state_code is not null
    and p_process.modified_at is not null
    and p_process.id = p_request.target_id
    and btrim(p_process.version::text) = p_request.target_version
    and p_process.user_id = p_request.actor_user_id
    and p_process.state_code = 0
    and p_process.modified_at = p_request.expected_modified_at
    and p_process.json is not null
    and p_process.json_ordered is not null
    and util.dataset_derivative_rebuild_sha256(
      p_process.json::jsonb::text
    ) = p_request.expected_json_sha256
    and util.dataset_derivative_rebuild_sha256(
      p_process.json_ordered::jsonb::text
    ) = p_request.expected_json_ordered_sha256,
    false
  )
$$;

ALTER FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_process" "public"."processes") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_flow" "public"."flows") FROM PUBLIC;

REVOKE ALL ON FUNCTION "util"."dataset_derivative_rebuild_primary_matches"("p_request" "util"."dataset_derivative_rebuild_requests", "p_process" "public"."processes") FROM PUBLIC;
