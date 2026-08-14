CREATE OR REPLACE FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer, "type_of_data_set_filter" "text" DEFAULT 'all'::"text", "query_terms" "text"[] DEFAULT NULL::"text"[]) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "model_id" "uuid", "total_count" bigint)
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
  text_match_clause := 'where p.extracted_md &@~| $11';

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select p.id, 1.0::double precision as search_score
        from public.processes p
        where p.id = exact_query_id
          and p.json @> filter_condition_jsonb
          and (
            (normalized_data_source = 'tg' and p.state_code = 100 and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'co' and p.state_code = 200 and (team_id_filter is null or p.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and p.user_id = effective_user_id and (state_code_filter is null or p.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p.team_id = team_id_filter and (state_code_filter is null or p.state_code = state_code_filter))
          )
          and (
            coalesce(type_of_data_set_filter, 'all') = 'all'
            or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
          )
        group by p.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
          from public.processes p2
          where p2.id = matched_ids.id
            and (
              (normalized_data_source = 'tg' and p2.state_code = 100 and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and p2.state_code = 200 and (team_id_filter is null or p2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and p2.user_id = effective_user_id and (state_code_filter is null or p2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and p2.team_id = team_id_filter and (state_code_filter is null or p2.state_code = state_code_filter))
            )
          order by p2.version desc, p2.modified_at desc
          limit 1
        ) latest_row on true
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select 1::bigint as rank, counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.model_id, counted_rows.total_count
      from counted_rows
      order by rank, counted_rows.id
      limit normalized_page_size
      offset (normalized_page_current - 1) * normalized_page_size;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and p.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select p.id,
             p.json,
             p.state_code,
             p.team_id,
             p.user_id,
             p.model_id,
             pgroonga_score(p.tableoid, p.ctid) as search_score
      from public.processes p
      %s
    ),
    matched_ids as (
      select p.id, max(p.search_score) as search_score
      from text_matches p
      where (
          ($5 = 'tg' and p.state_code = 100 and ($7 is null or p.team_id = $7))
          or ($5 = 'co' and p.state_code = 200 and ($7 is null or p.team_id = $7))
          or ($5 = 'my' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and p.team_id = $7 and ($8 is null or p.state_code = $8))
        )
        %s
        and (
          coalesce($10, 'all') = 'all'
          or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $10
        )
      group by p.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, latest_row.model_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select p2.json, p2.version, p2.modified_at, p2.team_id, p2.model_id
        from public.processes p2
        where p2.id = matched_ids.id
          and (
            ($5 = 'tg' and p2.state_code = 100 and ($7 is null or p2.team_id = $7))
            or ($5 = 'co' and p2.state_code = 200 and ($7 is null or p2.team_id = $7))
            or ($5 = 'my' and $6 is not null and p2.user_id = $6 and ($8 is null or p2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and p2.team_id = $7 and ($8 is null or p2.state_code = $8))
          )
        order by p2.version desc, p2.modified_at desc
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
    select ranked_rows.rank, ranked_rows.id, ranked_rows.json, ranked_rows.version, ranked_rows.modified_at, ranked_rows.team_id, ranked_rows.model_id, ranked_rows.total_count
    from ranked_rows
    order by ranked_rows.rank, ranked_rows.id
    limit $3
    offset ($4 - 1) * $3
  $sql$, text_match_clause, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, type_of_data_set_filter, escaped_query_terms;
end;
$_$;

ALTER FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "service_role";

GRANT ALL ON FUNCTION "private"."search_processes_latest_impl"("query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer, "type_of_data_set_filter" "text", "query_terms" "text"[]) TO "api_internal_executor";
