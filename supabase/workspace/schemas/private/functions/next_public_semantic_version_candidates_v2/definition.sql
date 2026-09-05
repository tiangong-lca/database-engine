CREATE OR REPLACE FUNCTION "private"."next_public_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision, "semantic_route" "text", "candidate_population" integer)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_exact_cutover constant integer := 2000;
  v_state_code integer;
  v_table_name text;
  v_filter_sql text;
  v_exact_filter_sql text;
  v_projection_filter_sql text;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer := 0;
  v_indexed_probe boolean;
  v_route_population integer;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('tg', 'co')
     or p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024
     or pg_catalog.jsonb_typeof(v_residual) is distinct from 'object' then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;

  v_state_code := case p_data_source when 'tg' then 100 else 200 end;
  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  -- Typed public filters are already projected transactionally into the
  -- candidate sidecar.  Rechecking those JSON paths here made every exact
  -- Flow request pay the large dynamic-planning cost again.  Keep only the
  -- source-of-truth boundary and filters that are intentionally not projected.
  v_exact_filter_sql := $sql$
    source.embedding_ft is not null
    and source.state_code = $1
    and ($2::uuid is null or source.team_id = $2)
    and ($3::jsonb = '{}'::jsonb or source.json @> $3)
  $sql$;

  if p_kind = 'process' then
    v_filter_sql := $sql$
      source.embedding_ft is not null
      and source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        $4::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $4
      )
    $sql$;
    v_projection_filter_sql := $sql$
      candidate.dataset_kind = 'process'
      and candidate.state_code = $1
      and ($2::uuid is null or candidate.team_id = $2)
      and ($4::text is null or candidate.dataset_type = $4)
    $sql$;
    v_indexed_probe := p_team_id is not null or p_process_type is not null;
  else
    v_filter_sql := $sql$
      source.embedding_ft is not null
      and source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        pg_catalog.cardinality($5::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($5)
      )
      and (
        not coalesce($6::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($7::text[]) = 0
          and pg_catalog.cardinality($8::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $7
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $8
      )
    $sql$;
    v_projection_filter_sql := $sql$
      candidate.dataset_kind = 'flow'
      and candidate.state_code = $1
      and ($2::uuid is null or candidate.team_id = $2)
      and (
        pg_catalog.cardinality($5::text[]) = 0
        or candidate.dataset_type = any($5)
      )
      and (not coalesce($6::boolean, false) or not candidate.is_emission)
      and (
        (
          pg_catalog.cardinality($7::text[]) = 0
          and pg_catalog.cardinality($8::text[]) = 0
        )
        or candidate.classification_codes && $7
        or candidate.elementary_codes && $8
      )
    $sql$;
    v_indexed_probe := p_team_id is not null
      or pg_catalog.cardinality(v_flow_types) > 0
      or coalesce(p_as_input, false)
      or pg_catalog.cardinality(v_classification_codes) > 0
      or pg_catalog.cardinality(v_elementary_codes) > 0;
  end if;

  if v_indexed_probe then
    v_sql := pg_catalog.format($query$
      select
        pg_catalog.array_agg(candidate.id),
        pg_catalog.array_agg(candidate.version)
      from (
        select candidate.id, candidate.version
        from private.next_hybrid_public_candidates_v2 as candidate
        where %s
        limit $9
      ) as candidate
    $query$, v_projection_filter_sql);

    execute v_sql
      into v_candidate_ids, v_candidate_versions
      using v_state_code, p_team_id, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1;
    v_candidate_count := coalesce(pg_catalog.cardinality(v_candidate_ids), 0);
  end if;

  if v_indexed_probe and v_candidate_count <= v_exact_cutover then
    if v_candidate_count = 0 then
      return;
    end if;

    v_sql := pg_catalog.format($query$
      with candidate_keys as materialized (
        select
          ($10::uuid[])[ordinal] as id,
          ($11::text[])[ordinal] as version
        from pg_catalog.generate_subscripts($10::uuid[], 1) as key(ordinal)
      ), nearest as materialized (
        select
          source.id,
          source.version::text as version,
          source.embedding_ft operator(extensions.<=>) $12::extensions.vector as distance
        from candidate_keys as candidate
        join public.%I as source
          on source.id = candidate.id
         and source.version::text = candidate.version
        where %s
        order by
          (source.embedding_ft operator(extensions.<=>) $12::extensions.vector)
            + 0::double precision,
          source.id,
          source.version::text desc
        limit 200
      )
      select
        pg_catalog.row_number() over (
          order by nearest.distance + 0::double precision,
            nearest.id, nearest.version desc
        )::bigint,
        nearest.id,
        nearest.version,
        nearest.distance,
        'exact'::text,
        $13::integer
      from nearest
      order by nearest.distance + 0::double precision,
        nearest.id, nearest.version desc
    $query$, v_table_name, v_exact_filter_sql);

    return query execute v_sql
      using v_state_code, p_team_id, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1,
        v_candidate_ids, v_candidate_versions, p_query_embedding,
        v_candidate_count;
    return;
  end if;

  v_route_population := case when v_indexed_probe then v_candidate_count else null end;
  v_sql := pg_catalog.format($query$
    with nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $10::extensions.vector as distance
      from public.%I as source
      where %s
      order by source.embedding_ft operator(extensions.<=>) $10::extensions.vector
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by nearest.distance + 0::double precision,
          nearest.id, nearest.version desc
      )::bigint,
      nearest.id,
      nearest.version,
      nearest.distance,
      'hnsw'::text,
      $11::integer
    from nearest
    order by nearest.distance + 0::double precision,
      nearest.id, nearest.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_state_code, p_team_id, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1,
      p_query_embedding, v_route_population;
end;
$_$;

ALTER FUNCTION "private"."next_public_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") OWNER TO "next_public_search_executor";

REVOKE ALL ON FUNCTION "private"."next_public_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."next_public_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") TO "api_internal_executor";
