CREATE OR REPLACE FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text" DEFAULT ''::"text", "match_threshold" double precision DEFAULT 0.5, "match_count" integer DEFAULT 20, "data_source" "text" DEFAULT 'tg'::"text", "state_code_filter" integer DEFAULT NULL::integer, "team_id_filter" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("rank" bigint, "id" "uuid", "json" "jsonb", "version" character, "modified_at" timestamp with time zone, "total_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'private', 'api', 'public', 'util', 'extensions', 'extensions', 'pg_temp'
    SET "statement_timeout" TO '60s'
    AS $_$
declare
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  visibility_clause text;
  search_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported semantic dataset table: %', p_table;
  end if;

  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id('');
  can_read_team_filter := private.dataset_search_can_read_team_filter(
    team_id_filter,
    effective_user_id
  );

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100 and ($9::uuid is null or d.team_id = $9)';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200 and ($9::uuid is null or d.team_id = $9)';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $7 and ($8::integer is null or d.state_code = $8)';
  elsif normalized_data_source = 'te' then
    if team_id_filter is null or not can_read_team_filter then
      return;
    end if;
    visibility_clause := 'd.team_id = $9 and ($8::integer is null or d.state_code = $8)';
  else
    return;
  end if;

  search_sql := format(
    $sql$
      with semantic as materialized (
        select candidate.rank, candidate.id, candidate.distance
        from private.semantic_simple_dataset_candidates(
          $1, $2, $3, $4, $5, $6, $8, $9
        ) candidate
      ),
      visible_rows as (
        select
          d.id,
          d.json,
          d.version,
          d.modified_at,
          semantic.rank as semantic_rank,
          semantic.distance
        from %1$s d
        join semantic on semantic.id = d.id
        where %2$s
      ),
      latest_rows as (
        select distinct on (visible_rows.id)
          visible_rows.id,
          visible_rows.json,
          visible_rows.version,
          visible_rows.modified_at,
          visible_rows.semantic_rank,
          visible_rows.distance
        from visible_rows
        order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
      ),
      counted_rows as (
        select latest_rows.*, count(*) over()::bigint as total_count
        from latest_rows
      )
      select
        counted_rows.semantic_rank,
        counted_rows.id,
        counted_rows.json,
        counted_rows.version,
        counted_rows.modified_at,
        counted_rows.total_count
      from counted_rows
      order by counted_rows.semantic_rank, counted_rows.distance, counted_rows.id
    $sql$,
    p_table,
    visibility_clause
  );

  return query execute search_sql
    using p_table, query_embedding, filter_condition, match_threshold,
          match_count, normalized_data_source, effective_user_id,
          state_code_filter, team_id_filter;
end;
$_$;

ALTER FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") OWNER TO "postgres";

REVOKE ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") FROM PUBLIC;

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "private"."semantic_simple_dataset_search"("p_table" "regclass", "query_embedding" "text", "filter_condition" "text", "match_threshold" double precision, "match_count" integer, "data_source" "text", "state_code_filter" integer, "team_id_filter" "uuid") TO "api_internal_executor";
