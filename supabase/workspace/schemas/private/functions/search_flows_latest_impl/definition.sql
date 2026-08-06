CREATE OR REPLACE FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
  classification_filter jsonb;
  json_filter_clause text;
  v_sql text;
  escaped_query_terms text[];
  text_match_clause text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(team_id_filter, effective_user_id);
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  text_match_clause := 'where f.extracted_md &@~| $14';

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

  if jsonb_typeof(filter_condition_jsonb->'classification') = 'array' then
    classification_filter := filter_condition_jsonb->'classification';
  else
    classification_filter := '[]'::jsonb;
  end if;
  filter_condition_jsonb := filter_condition_jsonb - 'classification';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select f.id, 1.0::double precision as search_score
        from public.flows f
        where f.id = exact_query_id
          and f.json @> filter_condition_jsonb
          and (
            (normalized_data_source = 'tg' and f.state_code = 100 and (team_id_filter is null or f.team_id = team_id_filter))
            or (normalized_data_source = 'co' and f.state_code = 200 and (team_id_filter is null or f.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and f.user_id = effective_user_id and (state_code_filter is null or f.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and f.team_id = team_id_filter and (state_code_filter is null or f.state_code = state_code_filter))
          )
          and (
            flow_type is null
            or (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
          and (
            jsonb_array_length(classification_filter) = 0
            or exists (
              select 1
              from jsonb_array_elements(classification_filter) as selected_class(item)
              where
                (
                  selected_class.item->>'scope' = 'elementary'
                  and exists (
                    select 1
                    from jsonb_array_elements(
                      case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                        when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                        else '[]'::jsonb
                      end
                    ) as category(item)
                    where category.item->>'@catId' = selected_class.item->>'code'
                  )
                )
                or (
                  selected_class.item->>'scope' = 'classification'
                  and exists (
                    select 1
                    from jsonb_array_elements(
                      case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                        when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                        else '[]'::jsonb
                      end
                    ) as class_item(item)
                    where class_item.item->>'@classId' = selected_class.item->>'code'
                  )
                )
            )
          )
        group by f.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select f2.json, f2.version, f2.modified_at, f2.team_id
          from public.flows f2
          where f2.id = matched_ids.id
            and (
              (normalized_data_source = 'tg' and f2.state_code = 100 and (team_id_filter is null or f2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and f2.state_code = 200 and (team_id_filter is null or f2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and f2.user_id = effective_user_id and (state_code_filter is null or f2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and f2.team_id = team_id_filter and (state_code_filter is null or f2.state_code = state_code_filter))
            )
          order by f2.version desc, f2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and f.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select f.id,
             f.json,
             f.state_code,
             f.team_id,
             f.user_id,
             pgroonga_score(f.tableoid, f.ctid) as search_score
      from public.flows f
      %s
    ),
    matched_ids as (
      select f.id, max(f.search_score) as search_score
      from text_matches f
      where (
          ($5 = 'tg' and f.state_code = 100 and ($7 is null or f.team_id = $7))
          or ($5 = 'co' and f.state_code = 200 and ($7 is null or f.team_id = $7))
          or ($5 = 'my' and $6 is not null and f.user_id = $6 and ($8 is null or f.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and f.team_id = $7 and ($8 is null or f.state_code = $8))
        )
        %s
        and (
          $10 is null
          or (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = any($11)
        )
        and (
          $12 is null
          or $12 = false
          or not (
            f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
          )
        )
        and (
          jsonb_array_length($13) = 0
          or exists (
            select 1
            from jsonb_array_elements($13) as selected_class(item)
            where
              (
                selected_class.item->>'scope' = 'elementary'
                and exists (
                  select 1
                  from jsonb_array_elements(
                    case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}'
                      when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:elementaryFlowCategorization,common:category}')
                      else '[]'::jsonb
                    end
                  ) as category(item)
                  where category.item->>'@catId' = selected_class.item->>'code'
                )
              )
              or (
                selected_class.item->>'scope' = 'classification'
                and exists (
                  select 1
                  from jsonb_array_elements(
                    case jsonb_typeof(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      when 'array' then f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}'
                      when 'object' then jsonb_build_array(f.json #> '{flowDataSet,flowInformation,dataSetInformation,classificationInformation,common:classification,common:class}')
                      else '[]'::jsonb
                    end
                  ) as class_item(item)
                  where class_item.item->>'@classId' = selected_class.item->>'code'
                )
              )
          )
        )
      group by f.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select f2.json, f2.version, f2.modified_at, f2.team_id
        from public.flows f2
        where f2.id = matched_ids.id
          and (
            ($5 = 'tg' and f2.state_code = 100 and ($7 is null or f2.team_id = $7))
            or ($5 = 'co' and f2.state_code = 200 and ($7 is null or f2.team_id = $7))
            or ($5 = 'my' and $6 is not null and f2.user_id = $6 and ($8 is null or f2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and f2.team_id = $7 and ($8 is null or f2.state_code = $8))
          )
        order by f2.version desc, f2.modified_at desc
        limit 1
      ) latest_row on true
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    ),
    ranked_rows as (
      select rank() over (order by counted_rows.search_score desc, counted_rows.modified_at desc, counted_rows.id)::bigint as rank,
             counted_rows.*
      from counted_rows
    )
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, flow_type, flow_type_array, as_input, classification_filter,
          escaped_query_terms;
end;
$_$;

ALTER FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_flows_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "query_terms" "text"[]) TO "api_internal_executor";
