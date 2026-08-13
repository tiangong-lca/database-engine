CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_validate_support_set"("p_actor" "uuid", "p_snapshots" "jsonb", "p_expected_sha256" "text") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_snapshot jsonb;
  v_validation jsonb;
  v_count integer;
begin
  if p_actor is null
    or jsonb_typeof(p_snapshots) <> 'array'
    or jsonb_array_length(p_snapshots) not between 2 and 100
    or p_expected_sha256 !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(p_snapshots)
      is distinct from p_expected_sha256 then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_SCHEMA_MISMATCH'
    );
  end if;
  v_count := jsonb_array_length(p_snapshots);
  if (
    select count(distinct (
      (item.value->>'table') || ':' || (item.value->>'id') || '@'
        || (item.value->>'version')
    )) = v_count
      and min((item.value->>'ordinal')::integer) = 1
      and max((item.value->>'ordinal')::integer) = v_count
      and bool_and(
        (item.value->>'ordinal')::integer = item.ordinality::integer
      )
    from jsonb_array_elements(p_snapshots)
      with ordinality as item(value, ordinality)
  ) is not true then
    return jsonb_build_object(
      'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_IDENTITY_MISMATCH'
    );
  end if;
  for v_snapshot in
    select item.value
    from jsonb_array_elements(p_snapshots)
      with ordinality as item(value, ordinality)
    order by item.ordinality
  loop
    v_validation := util.dataset_flow_identity_validate_support_snapshot(
      p_actor, v_snapshot
    );
    if coalesce((v_validation->>'ok')::boolean, false) is false then
      return jsonb_build_object(
        'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_LIVE_MISMATCH',
        'details', v_validation
      );
    end if;
  end loop;
  return jsonb_build_object(
    'ok', true, 'support_count', v_count,
    'support_snapshot_set_sha256', p_expected_sha256
  );
exception when others then
  return jsonb_build_object(
    'ok', false, 'code', 'FLOW_IDENTITY_SUPPORT_SET_INVALID',
    'sqlstate', sqlstate, 'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_validate_support_set"("p_actor" "uuid", "p_snapshots" "jsonb", "p_expected_sha256" "text") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_validate_support_set"("p_actor" "uuid", "p_snapshots" "jsonb", "p_expected_sha256" "text") FROM PUBLIC;
