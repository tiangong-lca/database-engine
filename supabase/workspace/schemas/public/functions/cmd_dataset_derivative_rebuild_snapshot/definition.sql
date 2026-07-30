CREATE OR REPLACE FUNCTION "public"."cmd_dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_actor uuid := auth.uid();
  v_flow public.flows%rowtype;
  v_process public.processes%rowtype;
  v_snapshot jsonb;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table is null
    or p_table not in ('flows', 'processes')
    or p_id is null
    or p_version is null
    or btrim(p_version) !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_INVALID_REQUEST',
      'status', 400,
      'message', 'A flows or processes id and canonical version are required'
    );
  end if;

  if p_table = 'flows' then
    select flow.*
    into v_flow
    from public.flows as flow
    where flow.id = p_id
      and btrim(flow.version::text) = btrim(p_version)
      and flow.user_id = v_actor
      and flow.state_code = 0;
    if v_flow.id is not null then
      begin
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_flow);
      exception
        when others then
          v_snapshot := null;
      end;
    end if;
  else
    select process.*
    into v_process
    from public.processes as process
    where process.id = p_id
      and btrim(process.version::text) = btrim(p_version)
      and process.user_id = v_actor
      and process.state_code = 0;
    if v_process.id is not null then
      begin
        v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
      exception
        when others then
          v_snapshot := null;
      end;
    end if;
  end if;

  if (p_table = 'flows' and v_flow.id is null)
    or (p_table = 'processes' and v_process.id is null) then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_NOT_AVAILABLE',
      'status', 404,
      'message', 'Owner-draft dataset snapshot is not available'
    );
  end if;

  if v_snapshot is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_NOT_READY',
      'status', 409,
      'message', 'Owner-draft dataset primary snapshot is incomplete'
    );
  end if;

  if v_snapshot->>'json_sha256'
    is distinct from v_snapshot->>'json_ordered_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'DERIVATIVE_SNAPSHOT_PRIMARY_MISMATCH',
      'status', 409,
      'message', 'json and json_ordered are not synchronized'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'command', 'cmd_dataset_derivative_rebuild_snapshot'
  ) || v_snapshot;
end;
$_$;

ALTER FUNCTION "public"."cmd_dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "public"."cmd_dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "public"."cmd_dataset_derivative_rebuild_snapshot"("p_table" "text", "p_id" "uuid", "p_version" "text") TO "authenticated";
