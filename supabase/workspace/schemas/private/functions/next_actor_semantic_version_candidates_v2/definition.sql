CREATE OR REPLACE FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision, "semantic_route" "text", "candidate_population" integer)
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
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_candidate_ids uuid[];
  v_candidate_versions text[];
  v_candidate_count integer := 0;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te')
     or p_query_embedding is null
     or extensions.vector_dims(p_query_embedding) <> 1024
     or pg_catalog.jsonb_typeof(v_residual) is distinct from 'object'
     or p_state_code < 0 then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  if v_actor is null then return; end if;
  if p_data_source = 'te' and (
    p_team_id is null
    or not private.dataset_search_can_read_team_filter(p_team_id, v_actor)
  ) then
    return;
  end if;

  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
  v_scope_sql := case p_data_source
    when 'my' then 'source.user_id = $1 and ($3::integer is null or source.state_code = $3)'
    else 'source.team_id = $2 and ($3::integer is null or source.state_code = $3)'
  end;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
      and source.embedding_ft is not null
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        pg_catalog.cardinality($6::text[]) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($6)
      )
      and (
        not coalesce($7::boolean, false)
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality($8::text[]) = 0
          and pg_catalog.cardinality($9::text[]) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && $8
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && $9
      )
    $sql$;
  end if;

  v_sql := pg_catalog.format($query$
    select
      pg_catalog.array_agg(candidate.id),
      pg_catalog.array_agg(candidate.version)
    from (
      select source.id, source.version::text as version
      from public.%I as source
      where %s
      limit $10
    ) as candidate
  $query$, v_table_name, v_filter_sql);

  execute v_sql
    into v_candidate_ids, v_candidate_versions
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1;
  v_candidate_count := coalesce(pg_catalog.cardinality(v_candidate_ids), 0);

  if v_candidate_count <= v_exact_cutover then
    v_sql := pg_catalog.format($query$
      with candidate_keys as materialized (
        select
          ($11::uuid[])[ordinal] as id,
          ($12::text[])[ordinal] as version
        from pg_catalog.generate_subscripts($11::uuid[], 1) as key(ordinal)
      ), nearest as materialized (
        select
          source.id,
          source.version::text as version,
          source.embedding_ft operator(extensions.<=>) $13::extensions.vector as distance
        from candidate_keys as candidate
        join public.%I as source
          on source.id = candidate.id
         and source.version::text = candidate.version
        where %s
        order by
          (source.embedding_ft operator(extensions.<=>) $13::extensions.vector)
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
        $14::integer
      from nearest
      order by nearest.distance + 0::double precision,
        nearest.id, nearest.version desc
    $query$, v_table_name, v_filter_sql);

    return query execute v_sql
      using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
        v_flow_types, p_as_input, v_classification_codes,
        v_elementary_codes, v_exact_cutover + 1,
        v_candidate_ids, v_candidate_versions, p_query_embedding,
        v_candidate_count;
    return;
  end if;

  v_sql := pg_catalog.format($query$
    with nearest as materialized (
      select
        source.id,
        source.version::text as version,
        source.embedding_ft operator(extensions.<=>) $11::extensions.vector as distance
      from public.%I as source
      where %s
      order by source.embedding_ft operator(extensions.<=>) $11::extensions.vector
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
      $12::integer
    from nearest
    order by nearest.distance + 0::double precision,
      nearest.id, nearest.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_exact_cutover + 1,
      p_query_embedding, v_candidate_count;
end;
$_$;

ALTER FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_actor_semantic_version_candidates_v2"("p_kind" "text", "p_query_embedding" "extensions"."vector", "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;
