CREATE OR REPLACE FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "lexical_weight" double precision DEFAULT 0.5, "semantic_weight" double precision DEFAULT 0.5, "rrf_k" integer DEFAULT 10, "data_source" "text" DEFAULT 'tg'::"text", "page_size" integer DEFAULT 10, "page_current" integer DEFAULT 1, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "model_id" "uuid", "model_version" character, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $$
declare
  v_source text := coalesce(nullif(pg_catalog.lower(pg_catalog.btrim(data_source)),''),'tg');
  v_actor uuid := private.dataset_search_effective_user_id('');
begin
  if v_source not in ('tg','co','my','te')
    or match_count is distinct from 200
    or page_size is null or page_size not between 1 and 100
    or page_current is null or page_current not between 1 and 400
    or match_threshold is null or match_threshold not between 0 and 1
    or lexical_weight is null or lexical_weight not between 0 and 1
    or semantic_weight is null or semantic_weight not between 0 and 1
    or lexical_weight + semantic_weight <= 0
    or rrf_k is null or rrf_k not between 1 and 1000
    or pg_catalog.jsonb_typeof(filter_condition) is distinct from 'object' then
    raise exception using errcode='22023',message='invalid version search request';
  end if;
  if v_source in ('my','te') and v_actor is null then return; end if;
  return query
  with lexical as materialized (
    select candidate.*
    from private.lexical_version_candidates_v1('process',query_text,query_terms,filter_condition,v_source) as candidate
    where lexical_weight > 0
  ), semantic as materialized (
    select candidate.*
    from private.semantic_process_version_candidates_v1(
      query_embedding,filter_condition::text,match_threshold,200,v_source) as candidate
    where semantic_weight > 0
  ), fused as materialized (
    select coalesce(lexical.id,semantic.id) as id,
      coalesce(lexical.version,semantic.version) as version,
      coalesce(lexical_weight/(rrf_k+lexical.rank),0::double precision)
        + coalesce(semantic_weight/(rrf_k+semantic.rank),0::double precision) as score
    from lexical full outer join semantic
      on semantic.id=lexical.id and semantic.version=lexical.version
  ), hydrated as materialized (
    select source.id,source.json,source.version,source.modified_at,source.model_id,source.model_version,source.team_id,fused.score
    from fused join public.processes as source
      on source.id=fused.id and source.version::text=fused.version
    where (v_source='tg' and source.state_code=100)
      or (v_source='co' and source.state_code=200)
      or (v_source='my' and source.user_id=v_actor)
      or (v_source='te' and exists(
        select 1 from private.roles as membership
        where membership.user_id=v_actor and membership.team_id=source.team_id
          and membership.role::text in ('admin','member','owner')
      ))
  ), counted as (
    select hydrated.*,count(*) over()::bigint as total_count from hydrated
  )
  select rows.id,rows.json,rows.version,rows.modified_at,rows.model_id,rows.model_version,rows.team_id,rows.total_count
  from counted as rows
  order by rows.score desc,rows.id,rows.version desc
  limit page_size offset (page_current-1)*page_size;
end;
$$;

ALTER FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "anon";

GRANT ALL ON FUNCTION "api"."hybrid_search_process_versions_v1"("query_text" "text", "query_embedding" "text", "filter_condition" "jsonb", "match_threshold" double precision, "match_count" integer, "lexical_weight" double precision, "semantic_weight" double precision, "rrf_k" integer, "data_source" "text", "page_size" integer, "page_current" integer, "query_terms" "text"[]) TO "authenticated";
