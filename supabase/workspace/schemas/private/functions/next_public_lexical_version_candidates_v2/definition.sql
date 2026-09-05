CREATE OR REPLACE FUNCTION "private"."next_public_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "version" "text", "score" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER PARALLEL RESTRICTED
    SET "search_path" TO ''
    SET "statement_timeout" TO '60s'
    SET "plan_cache_mode" TO 'force_custom_plan'
    SET "jit" TO 'off'
    SET "row_security" TO 'on'
    AS $_$
declare
  v_state_code integer;
  v_table_name text;
  v_filter_sql text;
  v_terms text[];
  v_exact uuid;
  v_residual jsonb := coalesce(p_residual_filter, '{}'::jsonb);
  v_flow_types text[] := coalesce(p_flow_types, '{}'::text[]);
  v_classification_codes text[] := coalesce(p_classification_codes, '{}'::text[]);
  v_elementary_codes text[] := coalesce(p_elementary_codes, '{}'::text[]);
  v_sql text;
begin
  if p_kind not in ('process', 'flow') or p_data_source not in ('tg', 'co') then
    raise exception using errcode = '22023', message = 'invalid Next Hybrid V2 request';
  end if;
  v_state_code := case p_data_source when 'tg' then 100 else 200 end;
  v_table_name := case p_kind when 'process' then 'processes' else 'flows' end;
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
    v_filter_sql := $sql$
      source.state_code = $1
      and ($2::uuid is null or source.team_id = $2)
      and ($3::jsonb = '{}'::jsonb or source.json @> $3)
      and (
        $4::text is null
        or source.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $4
      )
    $sql$;
  else
    v_filter_sql := $sql$
      source.state_code = $1
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
  end if;

  v_sql := pg_catalog.format($query$
    with matched as materialized (
      select
        source.id,
        source.version::text as version,
        source.modified_at,
        case when $10::uuid is not null then 1::double precision
          else extensions.pgroonga_score(source.tableoid, source.ctid)::double precision
        end as search_score
      from public.%I as source
      where %s
        and (
          ($10::uuid is not null and source.id = $10)
          or ($10::uuid is null and source.search_text operator(extensions.&@~|) $9::text[])
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
    using v_state_code, p_team_id, v_residual, p_process_type,
      v_flow_types, p_as_input, v_classification_codes,
      v_elementary_codes, v_terms, v_exact;
end;
$_$;

ALTER FUNCTION "private"."next_public_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") OWNER TO "next_public_search_executor";

REVOKE ALL ON FUNCTION "private"."next_public_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."next_public_lexical_version_candidates_v2"("p_kind" "text", "p_query" "text", "p_terms" "text"[], "p_residual_filter" "jsonb", "p_process_type" "text", "p_flow_types" "text"[], "p_as_input" boolean, "p_classification_codes" "text"[], "p_elementary_codes" "text"[], "p_data_source" "text", "p_team_id" "uuid") TO "api_internal_executor";
