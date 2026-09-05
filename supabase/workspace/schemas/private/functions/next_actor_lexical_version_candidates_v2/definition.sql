CREATE OR REPLACE FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_actor uuid := private.dataset_search_effective_user_id('');
  v_table_name text;
  v_scope_sql text;
  v_filter_sql text;
  v_terms text[];
  v_exact uuid;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow')
     or p_data_source not in ('my', 'te')
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
  v_terms := private.pgroonga_escape_query_terms(p_terms);
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then
    v_terms := private.pgroonga_escape_query_terms(array[p_query]);
  end if;
  if coalesce(pg_catalog.cardinality(v_terms), 0) = 0 then return; end if;
  if pg_catalog.btrim(coalesce(p_query, '')) ~*
     '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' then
    v_exact := pg_catalog.btrim(p_query)::uuid;
  end if;

  if p_kind = 'process' then
    v_filter_sql := v_scope_sql || $sql$
      and ($4::jsonb = '{}'::jsonb or source.json @> $4)
      and (
        $5::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $5
      )
    $sql$;
  else
    v_filter_sql := v_scope_sql || $sql$
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
    with matched as materialized (
      select
        source.id,
        source.version::text as version,
        source.modified_at,
        case when $11::uuid is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid, source.ctid)::double precision
        end as search_score
      from public.%I as source
      where %s
        and (
          ($11::uuid is not null and source.id = $11)
          or ($11::uuid is null and source.search_text operator(extensions.&@~|) $10::text[])
        )
      order by search_score desc, source.modified_at desc,
        source.id, source.version desc
      limit 200
    )
    select
      pg_catalog.row_number() over (
        order by matched.search_score desc, matched.modified_at desc,
          matched.id, matched.version desc
      )::bigint,
      matched.id,
      matched.version,
      matched.search_score
    from matched
    order by matched.search_score desc, matched.modified_at desc,
      matched.id, matched.version desc
  $query$, v_table_name, v_filter_sql);

  return query execute v_sql
    using v_actor, p_team_id, p_state_code, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_terms, v_exact;
end;
$_$;

ALTER FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") OWNER TO "api_internal_executor";

REVOKE ALL ON FUNCTION "private"."next_actor_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_state_code" integer, "p_team_id" "uuid") FROM PUBLIC;
