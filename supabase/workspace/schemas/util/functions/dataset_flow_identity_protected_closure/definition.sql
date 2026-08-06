CREATE OR REPLACE FUNCTION "util"."dataset_flow_identity_protected_closure"("p_actor" "uuid", "p_closure" "jsonb") RETURNS "jsonb"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO ''
    AS $_$
declare
  v_keys constant text[] := array[
    'schema_version', 'pending', 'blockers', 'orphans',
    'pending_set_sha256', 'blocker_set_sha256', 'orphan_set_sha256',
    'total_expected_reference_count'
  ];
  v_entries jsonb;
  v_observed jsonb;
  v_invalid integer;
  v_expected_total bigint;
  v_observed_total bigint;
begin
  if p_actor is null
    or not private.dataset_flow_identity_exact_keys(p_closure, v_keys)
    or p_closure->>'schema_version'
      <> 'dataset-flow-identity-protected-closure.v1'
    or jsonb_typeof(p_closure->'pending') <> 'array'
    or jsonb_typeof(p_closure->'blockers') <> 'array'
    or jsonb_typeof(p_closure->'orphans') <> 'array'
    or p_closure->>'pending_set_sha256' !~ '^[a-f0-9]{64}$'
    or p_closure->>'blocker_set_sha256' !~ '^[a-f0-9]{64}$'
    or p_closure->>'orphan_set_sha256' !~ '^[a-f0-9]{64}$'
    or jsonb_typeof(p_closure->'total_expected_reference_count')
      <> 'number'
    or util.dataset_flow_identity_sha256(p_closure->'pending')
      is distinct from p_closure->>'pending_set_sha256'
    or util.dataset_flow_identity_sha256(p_closure->'blockers')
      is distinct from p_closure->>'blocker_set_sha256'
    or util.dataset_flow_identity_sha256(p_closure->'orphans')
      is distinct from p_closure->>'orphan_set_sha256' then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_SCHEMA_MISMATCH'
    );
  end if;

  select coalesce(jsonb_agg(entry order by partition_ordinal, ordinal), '[]'::jsonb)
  into v_entries
  from (
    select
      1 as partition_ordinal,
      item.ordinality::integer as ordinal,
      item.value || jsonb_build_object('partition', 'pending') as entry
    from jsonb_array_elements(p_closure->'pending')
      with ordinality as item(value, ordinality)
    union all
    select
      2,
      item.ordinality::integer,
      item.value || jsonb_build_object('partition', 'blockers')
    from jsonb_array_elements(p_closure->'blockers')
      with ordinality as item(value, ordinality)
    union all
    select
      3,
      item.ordinality::integer,
      item.value || jsonb_build_object(
        'partition', 'orphans',
        'expected_reference_count', 0,
        'occurrences', '[]'::jsonb,
        'occurrence_set_sha256',
          util.dataset_flow_identity_sha256('[]'::jsonb)
      )
    from jsonb_array_elements(p_closure->'orphans')
      with ordinality as item(value, ordinality)
  ) as combined;

  select count(*)::integer
  into v_invalid
  from jsonb_array_elements(v_entries) as entry(value)
  where not private.dataset_flow_identity_exact_keys(entry.value, array[
      'source_id', 'source_version', 'expected_reference_count',
      'occurrences', 'occurrence_set_sha256', 'evidence_sha256', 'partition'
    ])
    or entry.value->>'source_id'
      !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    or entry.value->>'source_version'
      !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
    or jsonb_typeof(entry.value->'expected_reference_count') <> 'number'
    or (entry.value->>'expected_reference_count')::integer < 0
    or jsonb_typeof(entry.value->'occurrences') <> 'array'
    or jsonb_array_length(entry.value->'occurrences')
      <> (entry.value->>'expected_reference_count')::integer
    or entry.value->>'occurrence_set_sha256' !~ '^[a-f0-9]{64}$'
    or util.dataset_flow_identity_sha256(entry.value->'occurrences')
      is distinct from entry.value->>'occurrence_set_sha256'
    or entry.value->>'evidence_sha256' !~ '^[a-f0-9]{64}$'
    or entry.value->>'partition' not in ('pending', 'blockers', 'orphans')
    or exists (
      select 1
      from jsonb_array_elements(entry.value->'occurrences')
        with ordinality as occurrence(value, ordinality)
      where not private.dataset_flow_identity_exact_keys(
          occurrence.value,
          array[
            'process_id', 'process_version', 'exchange_index', 'internal_id',
            'direction', 'reference_sha256'
          ]
        )
        or occurrence.value->>'process_id'
          !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or occurrence.value->>'process_version'
          !~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{3}$'
        or jsonb_typeof(occurrence.value->'exchange_index') <> 'number'
        or (occurrence.value->>'exchange_index')::integer < 0
        or nullif(occurrence.value->>'internal_id', '') is null
        or occurrence.value->>'direction' not in ('Input', 'Output')
        or occurrence.value->>'reference_sha256' !~ '^[a-f0-9]{64}$'
    );

  if v_invalid > 0 or (
    select count(*)
    from (
      select distinct
        entry.value->>'source_id' as id,
        entry.value->>'source_version' as version
      from jsonb_array_elements(v_entries) as entry(value)
    ) as unique_entry
  ) <> jsonb_array_length(v_entries) then
    return jsonb_build_object(
      'ok', false,
      'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_ENTRY_MISMATCH'
    );
  end if;

  with wanted as (
    select
      entry.ordinality::integer as ordinal,
      (entry.value->>'source_id')::uuid as source_id,
      entry.value->>'source_version' as source_version,
      (entry.value->>'expected_reference_count')::integer
        as expected_reference_count,
      entry.value->'occurrences' as expected_occurrences,
      entry.value->>'occurrence_set_sha256' as occurrence_set_sha256,
      entry.value->>'evidence_sha256' as evidence_sha256,
      entry.value->>'partition' as partition
    from jsonb_array_elements(v_entries)
      with ordinality as entry(value, ordinality)
  ), live_occurrence as (
    select
      wanted.source_id,
      wanted.source_version,
      process.id as process_id,
      btrim(process.version::text) as process_version,
      (exchange.ordinality - 1)::integer as exchange_index,
      exchange.value->>'@dataSetInternalID' as internal_id,
      exchange.value->>'exchangeDirection' as direction,
      util.dataset_flow_identity_sha256(
        private.dataset_flow_identity_reference(exchange.value)
      ) as reference_sha256
    from public.processes as process
    cross join lateral jsonb_array_elements(
      private.dataset_flow_identity_exchanges(process.json_ordered::jsonb)
    ) with ordinality as exchange(value, ordinality)
    join wanted
      on wanted.source_id::text = exchange.value #>>
          '{referenceToFlowDataSet,@refObjectId}'
      and wanted.source_version = exchange.value #>>
          '{referenceToFlowDataSet,@version}'
    where process.user_id = p_actor
      and process.state_code = 0
  ), live as (
    select
      wanted.source_id,
      wanted.source_version,
      coalesce(jsonb_agg(jsonb_build_object(
        'process_id', live_occurrence.process_id,
        'process_version', live_occurrence.process_version,
        'exchange_index', live_occurrence.exchange_index,
        'internal_id', live_occurrence.internal_id,
        'direction', live_occurrence.direction,
        'reference_sha256', live_occurrence.reference_sha256
      ) order by
        live_occurrence.process_id,
        live_occurrence.process_version,
        live_occurrence.exchange_index
      ) filter (where live_occurrence.process_id is not null), '[]'::jsonb)
        as observed_occurrences
    from wanted
    left join live_occurrence using (source_id, source_version)
    group by wanted.source_id, wanted.source_version
  )
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'ordinal', wanted.ordinal,
      'partition', wanted.partition,
      'source_id', wanted.source_id,
      'source_version', wanted.source_version,
      'expected_reference_count', wanted.expected_reference_count,
      'observed_reference_count',
        jsonb_array_length(live.observed_occurrences),
      'evidence_sha256', wanted.evidence_sha256,
      'expected_occurrence_set_sha256', wanted.occurrence_set_sha256,
      'observed_occurrence_set_sha256',
        util.dataset_flow_identity_sha256(live.observed_occurrences),
      'matches', wanted.expected_occurrences = live.observed_occurrences
    ) order by wanted.ordinal), '[]'::jsonb),
    coalesce(sum(wanted.expected_reference_count), 0)::bigint,
    coalesce(sum(jsonb_array_length(live.observed_occurrences)), 0)::bigint
  into v_observed, v_expected_total, v_observed_total
  from wanted
  left join live using (source_id, source_version);

  return jsonb_build_object(
    'ok', v_expected_total = v_observed_total
      and not exists (
        select 1
        from jsonb_array_elements(v_observed) as observed(value)
        where coalesce((observed.value->>'matches')::boolean, false) is false
      )
      and v_expected_total =
        (p_closure->>'total_expected_reference_count')::bigint,
    'schema_version', 'dataset-flow-identity-protected-closure-proof.v1',
    'expected_total_reference_count', v_expected_total,
    'observed_total_reference_count', v_observed_total,
    'entries', v_observed,
    'observed_sha256', util.dataset_flow_identity_sha256(v_observed)
  );
exception when others then
  return jsonb_build_object(
    'ok', false,
    'code', 'FLOW_IDENTITY_PROTECTED_CLOSURE_INVALID',
    'sqlstate', sqlstate,
    'message', sqlerrm
  );
end;
$_$;

ALTER FUNCTION "util"."dataset_flow_identity_protected_closure"("p_actor" "uuid", "p_closure" "jsonb") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."dataset_flow_identity_protected_closure"("p_actor" "uuid", "p_closure" "jsonb") FROM PUBLIC;
