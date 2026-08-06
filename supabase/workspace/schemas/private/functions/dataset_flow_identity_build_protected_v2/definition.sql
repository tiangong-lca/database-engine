CREATE OR REPLACE FUNCTION "private"."dataset_flow_identity_build_protected_v2"("p_actor" "uuid", "p_intent" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_partition text;
  v_entry jsonb;
  v_occurrence jsonb;
  v_process public.processes%rowtype;
  v_exchanges jsonb;
  v_exchange jsonb;
  v_reference jsonb;
  v_occurrences jsonb;
  v_entries jsonb;
  v_pending jsonb := '[]'::jsonb;
  v_blockers jsonb := '[]'::jsonb;
  v_orphans jsonb := '[]'::jsonb;
  v_result jsonb;
  v_validation jsonb;
begin
  if not private.dataset_flow_identity_exact_keys(
      p_intent, array['schema_version', 'pending', 'blockers', 'orphans']
    )
    or p_intent->>'schema_version'
      <> 'dataset-flow-identity-protected-intent.v2'
    or jsonb_typeof(p_intent->'pending') <> 'array'
    or jsonb_typeof(p_intent->'blockers') <> 'array'
    or jsonb_typeof(p_intent->'orphans') <> 'array' then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_SCHEMA_MISMATCH';
  end if;

  foreach v_partition in array array['pending', 'blockers'] loop
    v_entries := '[]'::jsonb;
    for v_entry in
      select item.value
      from jsonb_array_elements(p_intent->v_partition)
        with ordinality as item(value, ordinality)
      order by item.ordinality
    loop
      if not private.dataset_flow_identity_exact_keys(v_entry, array[
          'source_id', 'source_version', 'expected_reference_count',
          'occurrences', 'evidence_sha256'
        ])
        or v_entry->>'source_id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or v_entry->>'source_version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or v_entry->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
        or jsonb_typeof(v_entry->'expected_reference_count') <> 'number'
        or (v_entry->>'expected_reference_count')::integer < 0
        or jsonb_typeof(v_entry->'occurrences') <> 'array'
        or jsonb_array_length(v_entry->'occurrences')
          <> (v_entry->>'expected_reference_count')::integer then
        raise exception using errcode = '22023',
          message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_ENTRY_MISMATCH';
      end if;
      v_occurrences := '[]'::jsonb;
      for v_occurrence in
        select item.value
        from jsonb_array_elements(v_entry->'occurrences') as item(value)
        order by item.value->>'process_id', item.value->>'process_version',
          (item.value->>'exchange_index')::integer
      loop
        if not private.dataset_flow_identity_exact_keys(v_occurrence, array[
            'process_id', 'process_version', 'exchange_index', 'internal_id',
            'direction'
          ])
          or v_occurrence->>'process_id'
            !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          or v_occurrence->>'process_version'
            !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
          or jsonb_typeof(v_occurrence->'exchange_index') <> 'number'
          or (v_occurrence->>'exchange_index')::integer < 0
          or nullif(v_occurrence->>'internal_id', '') is null
          or v_occurrence->>'direction' not in ('Input', 'Output') then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_OCCURRENCE_MISMATCH';
        end if;
        select process.* into v_process
        from public.processes as process
        where process.id = (v_occurrence->>'process_id')::uuid
          and btrim(process.version::text) = v_occurrence->>'process_version'
          and process.user_id = p_actor and process.state_code = 0;
        v_exchanges := private.dataset_flow_identity_exchanges(
          v_process.json_ordered::jsonb
        );
        if v_process.id is null
          or v_process.json is null or v_process.json_ordered is null
          or v_process.json::jsonb is distinct from v_process.json_ordered::jsonb
          or (v_occurrence->>'exchange_index')::integer
            >= coalesce(jsonb_array_length(v_exchanges), 0) then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_PROCESS_MISMATCH';
        end if;
        v_exchange := v_exchanges->(v_occurrence->>'exchange_index')::integer;
        v_reference := private.dataset_flow_identity_reference(v_exchange);
        if v_exchange->>'@dataSetInternalID'
            is distinct from v_occurrence->>'internal_id'
          or v_exchange->>'exchangeDirection'
            is distinct from v_occurrence->>'direction'
          or v_reference->>'@refObjectId'
            is distinct from v_entry->>'source_id'
          or v_reference->>'@version'
            is distinct from v_entry->>'source_version'
          or not private.dataset_flow_identity_short_description_v2(
            v_reference->'common:shortDescription'
          ) then
          raise exception using errcode = '22023',
            message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_LOCATOR_DRIFT';
        end if;
        v_occurrences := v_occurrences || jsonb_build_array(
          v_occurrence || jsonb_build_object(
            'reference_sha256', util.dataset_flow_identity_sha256(v_reference)
          )
        );
      end loop;
      v_entries := v_entries || jsonb_build_array(jsonb_build_object(
        'source_id', v_entry->>'source_id',
        'source_version', v_entry->>'source_version',
        'expected_reference_count', jsonb_array_length(v_occurrences),
        'occurrences', v_occurrences,
        'occurrence_set_sha256',
          util.dataset_flow_identity_restricted_sha256_v2(v_occurrences),
        'evidence_sha256', v_entry->>'evidence_sha256'
      ));
    end loop;
    if v_partition = 'pending' then
      v_pending := v_entries;
    else
      v_blockers := v_entries;
    end if;
  end loop;

  select coalesce(jsonb_agg(jsonb_build_object(
    'source_id', item.value->>'source_id',
    'source_version', item.value->>'source_version',
    'evidence_sha256', item.value->>'evidence_sha256'
  ) order by item.ordinality), '[]'::jsonb)
  into v_orphans
  from jsonb_array_elements(p_intent->'orphans')
    with ordinality as item(value, ordinality)
  where private.dataset_flow_identity_exact_keys(
      item.value, array['source_id', 'source_version', 'evidence_sha256']
    )
    and item.value->>'source_id'
      ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    and item.value->>'source_version'
      ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    and item.value->>'evidence_sha256' ~ '^[a-f0-9]{64}$';
  if jsonb_array_length(v_orphans)
      <> jsonb_array_length(p_intent->'orphans') then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_ORPHAN_SCHEMA_MISMATCH';
  end if;
  v_result := jsonb_build_object(
    'schema_version', 'dataset-flow-identity-protected-closure.v1',
    'pending', v_pending,
    'blockers', v_blockers,
    'orphans', v_orphans,
    'pending_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_pending),
    'blocker_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_blockers),
    'orphan_set_sha256',
      util.dataset_flow_identity_restricted_sha256_v2(v_orphans),
    'total_expected_reference_count',
      coalesce((select sum((item.value->>'expected_reference_count')::integer)
        from jsonb_array_elements(v_pending || v_blockers) as item(value)), 0)
  );
  v_validation := util.dataset_flow_identity_protected_closure(p_actor, v_result);
  if coalesce((v_validation->>'ok')::boolean, false) is false then
    raise exception using errcode = '22023',
      message = 'FLOW_IDENTITY_CAPTURE_PROTECTED_LIVE_MISMATCH';
  end if;
  return v_result;
end;
$_$;

ALTER FUNCTION "private"."dataset_flow_identity_build_protected_v2"("p_actor" "uuid", "p_intent" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."dataset_flow_identity_build_protected_v2"("p_actor" "uuid", "p_intent" "jsonb") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."dataset_flow_identity_build_protected_v2"("p_actor" "uuid", "p_intent" "jsonb") TO "api_internal_executor";
