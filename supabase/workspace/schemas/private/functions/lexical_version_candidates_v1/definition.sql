CREATE OR REPLACE FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    AS $_$
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
$_$;

ALTER FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."lexical_version_candidates_v1"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_filters" "jsonb", "p_source" "text") FROM PUBLIC;
