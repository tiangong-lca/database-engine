CREATE OR REPLACE FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "hnsw.iterative_scan" TO 'strict_order'
    SET "hnsw.ef_search" TO '200'
    SET "hnsw.max_scan_tuples" TO '20000'
    SET "hnsw.scan_mem_multiplier" TO '2'
    AS $$
declare
  query_embedding_vector extensions.vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
begin
  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 200), 1), 200);
  candidate_size := normalized_match_count;
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  flow_type := nullif(btrim(filter_condition_jsonb->>'flowType'), '');
  if flow_type is not null then
    flow_type_array := string_to_array(flow_type, ',');
  else
    flow_type_array := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'flowType';

  if filter_condition_jsonb ? 'asInput' then
    as_input := nullif(btrim(filter_condition_jsonb->>'asInput'), '')::boolean;
  else
    as_input := null;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'asInput';

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          f.version::text as candidate_version,
          (f.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or f.json @> filter_condition_jsonb)
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft operator(extensions.<=>) query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        row_number() over (order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc)::bigint,
        filtered.candidate_id,
        filtered.candidate_version,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance,filtered.candidate_id,filtered.candidate_version desc
      limit normalized_match_count;
    return;
  end if;
end;
$$;

ALTER FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC;
