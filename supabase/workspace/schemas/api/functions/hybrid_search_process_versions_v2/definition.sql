CREATE OR REPLACE FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[], "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid", "type_of_data_set_filter" "text" DEFAULT NULL::"text") RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "model_version" character, "team_id" "uuid", "total_count" bigint, "semantic_route" "text", "semantic_candidate_population" integer, "semantic_fallback_used" boolean)
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
  v_process_type text := nullif(pg_catalog.btrim(type_of_data_set_filter), '');
  v_query_embedding extensions.vector(1024);
begin
  if v_process_type = 'all' then v_process_type := null; end if;
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
     or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object'
     or state_code_filter < 0
     or (
       v_process_type is not null
       and v_process_type not in (
         'Unit process, single operation',
         'Unit process, black box',
         'LCI result',
         'Partly terminated system',
         'Avoided product system'
       )
     ) then
    raise exception using errcode = '22023', message = 'invalid Next Process Hybrid V2 request';
  end if;
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
      'process', query_text, query_terms, v_query_embedding,
      filter_condition, v_process_type, '{}'::text[], false,
      '{}'::text[], '{}'::text[], match_threshold, lexical_weight,
      semantic_weight, rrf_k, v_source, state_code_filter, team_id_filter
    ) as candidate
  ), hydrated as materialized (
    select
      source.id,
      source.json,
      source.version,
      source.modified_at,
      source.model_id,
      source.model_version,
      source.team_id,
      fused.score,
      fused.semantic_route,
      fused.semantic_candidate_population,
      fused.semantic_fallback_used
    from fused
    join public.processes as source
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
      and (filter_condition = '{}'::jsonb or source.json @> filter_condition)
      and (
        v_process_type is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = v_process_type
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
    rows.model_id,
    rows.model_version,
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

ALTER FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v2"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[], "state_code_filter" integer, "team_id_filter" "uuid", "type_of_data_set_filter" "text") TO "authenticated";
