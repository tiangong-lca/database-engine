CREATE OR REPLACE FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint, "semantic_route" "text", "semantic_candidate_population" integer, "semantic_fallback_used" boolean)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)), ''), 'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_filter jsonb := coalesce(filter_condition, '{}'::jsonb);
  v_residual jsonb;
  v_flow_types text[] := '{}'::text[];
  v_as_input boolean := false;
  v_classification jsonb := '[]'::jsonb;
  v_classification_codes text[] := '{}'::text[];
  v_elementary_codes text[] := '{}'::text[];
  v_query_embedding extensions.vector(1024);
begin
  if v_source not in ('tg', 'co', 'my', 'te')
     or query_text is null or pg_catalog.btrim(query_text) = ''
     or match_count is distinct from 200
     or page_size is null or page_size not between 1 and 100
     or page_current is null or page_current not between 1 and 400
     or match_threshold is null or match_threshold not between 0 and 1
     or lexical_weight is null or lexical_weight not between 0 and 1
     or semantic_weight is null or semantic_weight not between 0 and 1
     or lexical_weight + semantic_weight <= 0
     or rrf_k is null or rrf_k not between 1 and 1000
     or pg_catalog.jsonb_typeof(v_filter) is distinct from 'object'
     or state_code_filter < 0 then
    raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
  end if;

  if v_filter ? 'flowType' then
    if pg_catalog.jsonb_typeof(v_filter -> 'flowType') <> 'string' then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    select coalesce(pg_catalog.array_agg(value order by value), '{}'::text[])
    into v_flow_types
    from (
      select distinct nullif(pg_catalog.btrim(item), '') as value
      from pg_catalog.regexp_split_to_table(v_filter ->> 'flowType', ',') as item
    ) as values
    where value is not null;
    if pg_catalog.cardinality(v_flow_types) = 0 or exists (
      select 1 from pg_catalog.unnest(v_flow_types) as value
      where value not in ('Elementary flow', 'Product flow', 'Waste flow', 'Other flow')
    ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'asInput' then
    if pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'boolean'
       or (
         pg_catalog.jsonb_typeof(v_filter -> 'asInput') = 'string'
         and pg_catalog.lower(v_filter ->> 'asInput') in ('true', 'false')
       ) then
      v_as_input := (v_filter ->> 'asInput')::boolean;
    else
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
  end if;

  if v_filter ? 'classification' then
    if pg_catalog.jsonb_typeof(v_filter -> 'classification') <> 'array'
       or pg_catalog.jsonb_array_length(v_filter -> 'classification') > 50
       or exists (
         select 1
         from pg_catalog.jsonb_array_elements(v_filter -> 'classification') as selected(item)
         where pg_catalog.jsonb_typeof(selected.item) <> 'object'
           or selected.item ->> 'scope' not in ('classification', 'elementary')
           or nullif(pg_catalog.btrim(selected.item ->> 'code'), '') is null
           or pg_catalog.length(selected.item ->> 'code') > 200
       ) then
      raise exception using errcode = '22023', message = 'invalid Next Flow Hybrid V2 request';
    end if;
    v_classification := v_filter -> 'classification';
    select
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'classification'), '{}'::text[]),
      coalesce(pg_catalog.array_agg(distinct pg_catalog.btrim(selected.item ->> 'code'))
        filter (where selected.item ->> 'scope' = 'elementary'), '{}'::text[])
    into v_classification_codes, v_elementary_codes
    from pg_catalog.jsonb_array_elements(v_classification) as selected(item);
  end if;

  v_residual := v_filter - 'flowType' - 'asInput' - 'classification';

  if v_source in ('my', 'te') and v_actor is null then return; end if;
  if v_source = 'te' and (
    team_id_filter is null
    or not private.dataset_search_can_read_team_filter(team_id_filter, v_actor)
  ) then
    return;
  end if;
  if (v_source = 'tg' and state_code_filter is not null and state_code_filter <> 100)
     or (v_source = 'co' and state_code_filter is not null and state_code_filter <> 200) then
    return;
  end if;

  v_query_embedding := query_embedding::extensions.vector(1024);

  return query
  with fused as materialized (
    select candidate.*
    from private.next_hybrid_version_keys_v2(
      'flow', query_text, query_terms, v_query_embedding,
      v_residual, null, v_flow_types, v_as_input,
      v_classification_codes, v_elementary_codes, match_threshold,
      lexical_weight, semantic_weight, rrf_k, v_source,
      state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.flows as source
      on source.id = fused.id
     and source.version::text = fused.version
    where (
        (v_source = 'tg' and source.state_code = 100
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'co' and source.state_code = 200
          and (team_id_filter is null or source.team_id = team_id_filter))
        or (v_source = 'my' and source.user_id = v_actor
          and (state_code_filter is null or source.state_code = state_code_filter))
        or (v_source = 'te' and source.team_id = team_id_filter
          and (state_code_filter is null or source.state_code = state_code_filter)
          and private.dataset_search_can_read_team_filter(team_id_filter, v_actor))
      )
      and (v_residual = '{}'::jsonb or source.json @> v_residual)
      and (
        pg_catalog.cardinality(v_flow_types) = 0
        or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any(v_flow_types)
      )
      and (
        not v_as_input
        or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        )
      )
      and (
        (
          pg_catalog.cardinality(v_classification_codes) = 0
          and pg_catalog.cardinality(v_elementary_codes) = 0
        )
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}',
          '@classId'
        ) && v_classification_codes
        or private.next_hybrid_json_codes_v2(
          source.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}',
          '@catId'
        ) && v_elementary_codes
      )
  ), counted as (
    select hydrated.*, pg_catalog.count(*) over()::bigint as total_count
    from hydrated
  )
  select
    rows.id,
    rows.json,
    rows.version,
    rows.modified_at,
    rows.team_id,
    rows.total_count,
    rows.semantic_route,
    rows.semantic_candidate_population,
    rows.semantic_fallback_used
  from counted as rows
  order by rows.score desc, rows.id, rows.version desc
  limit page_size offset (page_current - 1) * page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_flow_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid") TO "authenticated";
