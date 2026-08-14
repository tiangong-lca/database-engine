CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_validate_process_guard"("p_actor" "uuid", "p_manifest" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_keys constant text[] := array[
    'ordinal', 'id', 'version', 'user_id', 'state_code', 'modified_at',
    'model_id', 'rule_verification',
    'before_row_sha256', 'before_payload_sha256',
    'before_exchange_set_sha256', 'before_exchange_count',
    'desired_payload_sha256', 'desired_exchange_set_sha256',
    'rewrite_count', 'process_template_sha256', 'rewrite_set_sha256',
    'rewrites', 'collision_ledger', 'collision_ledger_sha256',
    'derivative_baseline_snapshot_sha256',
    'process_schema', 'pending_blocker_closure_sha256'
  ];
  v_process public.processes%rowtype;
  v_payload jsonb;
  v_exchanges jsonb;
  v_payload_sha256 text;
  v_exchange_sha256 text;
  v_row_sha256 text;
  v_snapshot jsonb;
  v_template_sha256 text;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_manifest, v_keys)
    or jsonb_typeof(p_manifest->'ordinal') <> 'number'
    or (p_manifest->>'ordinal')::integer <= 0
    or p_manifest->>'id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or p_manifest->>'user_id' is distinct from p_actor::text
    or p_manifest->>'version' !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(p_manifest->'state_code') <> 'number'
    or (p_manifest->>'state_code')::integer <> 0
    or jsonb_typeof(p_manifest->'before_exchange_count') <> 'number'
    or jsonb_typeof(p_manifest->'rewrite_count') <> 'number'
    or (p_manifest->>'rewrite_count')::integer <= 0
    or jsonb_typeof(p_manifest->'rewrites') <> 'array'
    or jsonb_array_length(p_manifest->'rewrites')
      <> (p_manifest->>'rewrite_count')::integer
    or jsonb_typeof(p_manifest->'collision_ledger') <> 'object'
    or util.dataset_flow_identity_restricted_sha256_v2(p_manifest->'rewrites')
      is distinct from p_manifest->>'rewrite_set_sha256'
    or util.dataset_flow_identity_restricted_sha256_v2(
      p_manifest->'collision_ledger'
    )
      is distinct from p_manifest->>'collision_ledger_sha256'
    or not private.dataset_flow_identity_exact_keys(
      p_manifest->'process_schema', array['status', 'evidence_sha256']
    )
    or p_manifest #>> '{process_schema,status}' <> 'pass'
    or exists (
      select 1
      from unnest(array[
        'before_row_sha256', 'before_payload_sha256',
        'before_exchange_set_sha256', 'desired_payload_sha256',
        'desired_exchange_set_sha256', 'process_template_sha256',
        'rewrite_set_sha256', 'collision_ledger_sha256',
        'derivative_baseline_snapshot_sha256',
        'pending_blocker_closure_sha256'
      ]) as hash_field(name)
      where p_manifest->>hash_field.name !~ '^[a-f0-9]{64}$'
    )
    or p_manifest #>> '{process_schema,evidence_sha256}'
      !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_SCHEMA_MISMATCH'
    );
  end if;

  if jsonb_typeof(p_manifest->'model_id') not in ('string', 'null')
    or (
      jsonb_typeof(p_manifest->'model_id') = 'string'
      and p_manifest->>'model_id'
        !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    )
    or jsonb_typeof(p_manifest->'rule_verification')
      not in ('boolean', 'null') then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_METADATA_SCHEMA_MISMATCH'
    );
  end if;

  begin
    perform (p_manifest->>'modified_at')::timestamp with time zone;
  exception when others then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_VALUE_MISMATCH'
    );
  end;

  v_template_sha256 := util.dataset_flow_identity_restricted_sha256_v2(
    p_manifest - 'process_template_sha256'
  );
  if v_template_sha256 is distinct from
    p_manifest->>'process_template_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_TEMPLATE_HASH_MISMATCH'
    );
  end if;

  select process.*
  into v_process
  from public.processes as process
  where process.id = (p_manifest->>'id')::uuid
    and btrim(process.version::text) = p_manifest->>'version'
    and process.user_id = p_actor
    and process.state_code = 0;

  if v_process.id is null
    or v_process.json is null
    or v_process.json_ordered is null
    or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb
    or v_process.modified_at is distinct from
      (p_manifest->>'modified_at')::timestamp with time zone
    or coalesce(to_jsonb(v_process.model_id), 'null'::jsonb)
      is distinct from p_manifest->'model_id'
    or coalesce(to_jsonb(v_process.rule_verification), 'null'::jsonb)
      is distinct from p_manifest->'rule_verification' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_SCOPE_MISMATCH'
    );
  end if;

  v_payload := v_process.json_ordered::jsonb;
  v_exchanges := private.dataset_flow_identity_exchanges(v_payload);
  if v_exchanges is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_EXCHANGES_INVALID'
    );
  end if;

  v_payload_sha256 := util.dataset_flow_identity_sha256(v_payload);
  v_exchange_sha256 := util.dataset_flow_identity_sha256(v_exchanges);
  v_row_sha256 := util.dataset_flow_identity_sha256(jsonb_build_object(
    'id', v_process.id,
    'version', btrim(v_process.version::text),
    'user_id', v_process.user_id,
    'state_code', v_process.state_code,
    'modified_at', v_process.modified_at,
    'model_id', v_process.model_id,
    'rule_verification', v_process.rule_verification,
    'payload_sha256', v_payload_sha256
  ));
  begin
    v_snapshot := util.dataset_derivative_rebuild_snapshot(v_process);
  exception when others then
    v_snapshot := null;
  end;

  if v_payload_sha256 is distinct from
      p_manifest->>'before_payload_sha256'
    or v_exchange_sha256 is distinct from
      p_manifest->>'before_exchange_set_sha256'
    or v_row_sha256 is distinct from p_manifest->>'before_row_sha256'
    or jsonb_array_length(v_exchanges) is distinct from
      (p_manifest->>'before_exchange_count')::integer
    or v_snapshot is null
    or v_snapshot->>'snapshot_sha256' is distinct from
      p_manifest->>'derivative_baseline_snapshot_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROCESS_BASELINE_DRIFT'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'payload_sha256', v_payload_sha256,
    'exchange_set_sha256', v_exchange_sha256,
    'row_sha256', v_row_sha256,
    'derivative_snapshot_sha256', v_snapshot->>'snapshot_sha256'
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_PROCESS_MANIFEST_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_validate_process_guard"("p_actor" "uuid", "p_manifest" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_validate_process_guard"("p_actor" "uuid", "p_manifest" "jsonb") FROM PUBLIC;
