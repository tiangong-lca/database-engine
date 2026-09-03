-- Database #600: opt-in Next Process/Flow Hybrid APIs preserve exact versions.
-- Existing API/semantic/latest wrappers and their caller semantics remain unchanged.
begin;
select extensions.vector_dims('[1]'::extensions.vector);
grant api_internal_executor to postgres;
grant create on schema private,api to api_internal_executor;


CREATE OR REPLACE FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 200, "data_source" "text" DEFAULT 'tg'::"text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "distance" double precision)
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
begin
  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 200), 1), 200);
  candidate_size := normalized_match_count;
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 100
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
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
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 200
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
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
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.user_id = effective_user_id
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
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
          p.id as candidate_id,
          p.version::text as candidate_version,
          (p.embedding_ft operator(extensions.<=>) query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and exists (
            select 1
            from private.roles r
            where r.user_id = effective_user_id
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and (filter_condition_jsonb = '{}'::jsonb or p.json @> filter_condition_jsonb)
        order by p.embedding_ft operator(extensions.<=>) query_embedding_vector
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

ALTER FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC, anon, authenticated, service_role;


GRANT ALL ON FUNCTION "private"."semantic_process_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";


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

REVOKE ALL ON FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") FROM PUBLIC, anon, authenticated, service_role;


GRANT ALL ON FUNCTION "private"."semantic_flow_version_candidates_v1"("query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text") TO "api_internal_executor";



create function private.lexical_version_candidates_v1(
  p_kind text,p_query text,p_terms text[],p_filters jsonb,p_source text
) returns table(rank bigint,id uuid,version text,score double precision)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
as $$
declare
  v_table text;
  v_scope text;
  v_filters jsonb := coalesce(p_filters,'{}'::jsonb);
  v_terms text[];
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_exact uuid;
  v_flow_types text[];
  v_as_input boolean;
begin
  if p_kind = 'process' then v_table := 'processes';
  elsif p_kind = 'flow' then v_table := 'flows';
  else raise exception using errcode='22023',message='invalid version search request';
  end if;
  -- Preserve the existing lexical scope: Team Hybrid's semantic branch has
  -- actor-checked membership; its old lexical caller has no explicit team selector.
  -- Do not broaden that lexical scope as a side effect of retaining versions.
  if p_source = 'tg' then v_scope := 'source.state_code = 100';
  elsif p_source = 'co' then v_scope := 'source.state_code = 200';
  elsif p_source = 'my' and v_actor is not null then v_scope := 'source.user_id = $3';
  else return;
  end if;
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms),0)=0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms),0)=0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query,'')) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;
  if p_kind = 'flow' then
    v_flow_types := pg_catalog.string_to_array(nullif(pg_catalog.btrim(v_filters ->> 'flowType'),''),',');
    v_as_input := nullif(pg_catalog.btrim(v_filters ->> 'asInput'),'')::boolean;
    v_filters := v_filters - 'flowType' - 'asInput';
  end if;
  return query execute pg_catalog.format($query$
    with matched as materialized (
      select source.id,source.version::text as version,source.modified_at,
        case when $4 is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid,source.ctid)::double precision end as score
      from public.%I as source
      where %s
        and (($4 is not null and source.id=$4)
          or ($4 is null and source.search_text operator(extensions.&@~|) $1))
        and ($2='{}'::jsonb or source.json @> $2)
        and ($5 is null or source.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}' = any($5))
        and (not coalesce($6,false) or not (
          source.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'::jsonb
        ))
      order by score desc,source.modified_at desc,source.id,source.version desc
      limit 200
    )
    select row_number() over(order by matched.score desc,matched.modified_at desc,matched.id,matched.version desc),
      matched.id,matched.version,matched.score
    from matched order by matched.score desc,matched.modified_at desc,matched.id,matched.version desc
  $query$,v_table,v_scope) using v_terms,v_filters,v_actor,v_exact,v_flow_types,v_as_input;
end;
$$;
alter function private.lexical_version_candidates_v1(text,text,text[],jsonb,text) owner to api_internal_executor;
revoke all on function private.lexical_version_candidates_v1(text,text,text[],jsonb,text) from public,anon,authenticated,service_role;



create function api.hybrid_search_process_versions_v1(
  query_text text,query_embedding text,filter_condition jsonb default '{}'::jsonb,
  match_threshold double precision default 0.5,match_count integer default 200,
  lexical_weight double precision default 0.5,semantic_weight double precision default 0.5,
  rrf_k integer default 10,data_source text default 'tg',
  page_size integer default 10,page_current integer default 1,query_terms text[] default null
) returns table(id uuid,"json" jsonb,version character,modified_at timestamptz,model_id uuid,model_version character,team_id uuid,total_count bigint)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
as $$
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
alter function api.hybrid_search_process_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) owner to api_internal_executor;
revoke all on function api.hybrid_search_process_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) from public,anon,authenticated,service_role;
grant execute on function api.hybrid_search_process_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) to anon,authenticated;



create function api.hybrid_search_flow_versions_v1(
  query_text text,query_embedding text,filter_condition jsonb default '{}'::jsonb,
  match_threshold double precision default 0.5,match_count integer default 200,
  lexical_weight double precision default 0.5,semantic_weight double precision default 0.5,
  rrf_k integer default 10,data_source text default 'tg',
  page_size integer default 10,page_current integer default 1,query_terms text[] default null
) returns table(id uuid,"json" jsonb,version character,modified_at timestamptz,team_id uuid,total_count bigint)
language plpgsql stable security definer parallel restricted
set search_path = ''
set statement_timeout = '60s'
set plan_cache_mode = 'force_custom_plan'
as $$
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
    from private.lexical_version_candidates_v1('flow',query_text,query_terms,filter_condition,v_source) as candidate
    where lexical_weight > 0
  ), semantic as materialized (
    select candidate.*
    from private.semantic_flow_version_candidates_v1(
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
    select source.id,source.json,source.version,source.modified_at,source.team_id,fused.score
    from fused join public.flows as source
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
  select rows.id,rows.json,rows.version,rows.modified_at,rows.team_id,rows.total_count
  from counted as rows
  order by rows.score desc,rows.id,rows.version desc
  limit page_size offset (page_current-1)*page_size;
end;
$$;
alter function api.hybrid_search_flow_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) owner to api_internal_executor;
revoke all on function api.hybrid_search_flow_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) from public,anon,authenticated,service_role;
grant execute on function api.hybrid_search_flow_versions_v1(text,text,jsonb,double precision,integer,double precision,double precision,integer,text,integer,integer,text[]) to anon,authenticated;



insert into private.api_capability_grants(
  routine_identity,capability_id,allow_anon,allow_authenticated,allow_service_role
) values
  ('api.hybrid_search_process_versions_v1(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])','NX-CORE-02',true,true,false),
  ('api.hybrid_search_flow_versions_v1(text, text, jsonb, double precision, integer, double precision, double precision, integer, text, integer, integer, text[])','NX-CORE-02',true,true,false);
revoke create on schema private,api from api_internal_executor;
revoke api_internal_executor from postgres;
notify pgrst,'reload schema';
commit;
