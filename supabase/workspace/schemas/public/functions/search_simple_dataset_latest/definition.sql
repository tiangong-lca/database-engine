CREATE OR REPLACE FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb" DEFAULT '{}'::"jsonb", "page_size" bigint DEFAULT 10, "page_current" bigint DEFAULT 1, "data_source" "text" DEFAULT 'tg'::"text", "this_user_id" "text" DEFAULT ''::"text", "team_id_filter" "uuid" DEFAULT NULL::"uuid", "state_code_filter" integer DEFAULT NULL::integer) RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "team_id" "uuid", "total_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO 'public', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
  exact_query_id uuid;
  filter_condition_jsonb jsonb;
  json_filter_clause text;
  v_sql text;
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := case
    when coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(this_user_id)::uuid
    else null::uuid
  end;
  exact_query_id := case
    when coalesce(btrim(query_text) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(query_text)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $8'
  end;

  if exact_query_id is not null then
    v_sql := format($sql$
      with matched_ids as (
        select d.id, 1.0::double precision as search_score
        from %1$s d
        where d.id = $1
          and (
            ($4 = 'tg' and d.state_code = 100 and ($6 is null or d.team_id = $6))
            or ($4 = 'co' and d.state_code = 200 and ($6 is null or d.team_id = $6))
            or ($4 = 'my' and $5 is not null and d.user_id = $5 and ($7 is null or d.state_code = $7))
            or ($4 = 'te' and $6 is not null and d.team_id = $6 and ($7 is null or d.state_code = $7))
          )
          %2$s
        group by d.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select d2.json, d2.version, d2.modified_at, d2.team_id
          from %1$s d2
          where d2.id = matched_ids.id
            and (
              ($4 = 'tg' and d2.state_code = 100 and ($6 is null or d2.team_id = $6))
              or ($4 = 'co' and d2.state_code = 200 and ($6 is null or d2.team_id = $6))
              or ($4 = 'my' and $5 is not null and d2.user_id = $5 and ($7 is null or d2.state_code = $7))
              or ($4 = 'te' and $6 is not null and d2.team_id = $6 and ($7 is null or d2.state_code = $7))
            )
          order by d2.version desc, d2.modified_at desc
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
      limit $2
      offset ($3 - 1) * $2
    $sql$, p_table, json_filter_clause);

    return query execute v_sql
      using exact_query_id, normalized_page_size, normalized_page_current,
            data_source, normalized_this_user_id, team_id_filter, state_code_filter,
            filter_condition_jsonb;
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select d.id,
             d.json,
             d.state_code,
             d.team_id,
             d.user_id,
             pgroonga_score(d.tableoid, d.ctid) as search_score
      from %1$s d
      where d.extracted_text &@~ $1
    ),
    matched_ids as (
      select d.id, max(d.search_score) as search_score
      from text_matches d
      where (
          ($5 = 'tg' and d.state_code = 100 and ($7 is null or d.team_id = $7))
          or ($5 = 'co' and d.state_code = 200 and ($7 is null or d.team_id = $7))
          or ($5 = 'my' and $6 is not null and d.user_id = $6 and ($8 is null or d.state_code = $8))
          or ($5 = 'te' and $7 is not null and d.team_id = $7 and ($8 is null or d.state_code = $8))
        )
        %2$s
      group by d.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select d2.json, d2.version, d2.modified_at, d2.team_id
        from %1$s d2
        where d2.id = matched_ids.id
          and (
            ($5 = 'tg' and d2.state_code = 100 and ($7 is null or d2.team_id = $7))
            or ($5 = 'co' and d2.state_code = 200 and ($7 is null or d2.team_id = $7))
            or ($5 = 'my' and $6 is not null and d2.user_id = $6 and ($8 is null or d2.state_code = $8))
            or ($5 = 'te' and $7 is not null and d2.team_id = $7 and ($8 is null or d2.state_code = $8))
          )
        order by d2.version desc, d2.modified_at desc
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
  $sql$, p_table, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          data_source, normalized_this_user_id, team_id_filter, state_code_filter;
end;
$_$;

ALTER FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "anon";

GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "authenticated";

GRANT ALL ON FUNCTION "public"."_search_simple_dataset_latest"("p_table" "regclass", "query_text" "text", "filter_condition" "jsonb", "page_size" bigint, "page_current" bigint, "data_source" "text", "this_user_id" "text", "team_id_filter" "uuid", "state_code_filter" integer) TO "service_role";
