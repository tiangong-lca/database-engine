-- Restore exact UUID lookup for dataset search without putting UUID metadata
-- back into extracted_text. UUID queries are identifiers, so they should use
-- indexed id lookups and then apply the same visibility/latest-version rules
-- as text search.

create or replace function public._search_simple_dataset_latest(
  p_table regclass,
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
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
$$;

create or replace function private.search_lifecyclemodels_latest_impl(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
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

  if exact_query_id is not null then
    return query
      with matched_ids as (
        select l.id, 1.0::double precision as search_score
        from public.lifecyclemodels l
        where l.id = exact_query_id
          and l.json @> filter_condition_jsonb
          and (
            (normalized_data_source = 'tg' and l.state_code = 100 and (team_id_filter is null or l.team_id = team_id_filter))
            or (normalized_data_source = 'co' and l.state_code = 200 and (team_id_filter is null or l.team_id = team_id_filter))
            or (normalized_data_source = 'my' and effective_user_id is not null and l.user_id = effective_user_id and (state_code_filter is null or l.state_code = state_code_filter))
            or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and l.team_id = team_id_filter and (state_code_filter is null or l.state_code = state_code_filter))
          )
        group by l.id
      ),
      latest_rows as (
        select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
        from matched_ids
        join lateral (
          select l2.json, l2.version, l2.modified_at, l2.team_id
          from public.lifecyclemodels l2
          where l2.id = matched_ids.id
            and (
              (normalized_data_source = 'tg' and l2.state_code = 100 and (team_id_filter is null or l2.team_id = team_id_filter))
              or (normalized_data_source = 'co' and l2.state_code = 200 and (team_id_filter is null or l2.team_id = team_id_filter))
              or (normalized_data_source = 'my' and effective_user_id is not null and l2.user_id = effective_user_id and (state_code_filter is null or l2.state_code = state_code_filter))
              or (normalized_data_source = 'te' and team_id_filter is not null and can_read_team_filter and l2.team_id = team_id_filter and (state_code_filter is null or l2.state_code = state_code_filter))
            )
          order by l2.version desc, l2.modified_at desc
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
    else 'and l.json @> $2'
  end;

  v_sql := format($sql$
    with text_matches as materialized (
      select l.id,
             l.json,
             l.state_code,
             l.team_id,
             l.user_id,
             pgroonga_score(l.tableoid, l.ctid) as search_score
      from public.lifecyclemodels l
      where l.extracted_text &@~ $1
    ),
    matched_ids as (
      select l.id, max(l.search_score) as search_score
      from text_matches l
      where (
          ($5 = 'tg' and l.state_code = 100 and ($7 is null or l.team_id = $7))
          or ($5 = 'co' and l.state_code = 200 and ($7 is null or l.team_id = $7))
          or ($5 = 'my' and $6 is not null and l.user_id = $6 and ($8 is null or l.state_code = $8))
          or ($5 = 'te' and $7 is not null and $9 and l.team_id = $7 and ($8 is null or l.state_code = $8))
        )
        %s
      group by l.id
    ),
    latest_rows as (
      select matched_ids.id, latest_row.json, latest_row.version, latest_row.modified_at, latest_row.team_id, matched_ids.search_score
      from matched_ids
      join lateral (
        select l2.json, l2.version, l2.modified_at, l2.team_id
        from public.lifecyclemodels l2
        where l2.id = matched_ids.id
          and (
            ($5 = 'tg' and l2.state_code = 100 and ($7 is null or l2.team_id = $7))
            or ($5 = 'co' and l2.state_code = 200 and ($7 is null or l2.team_id = $7))
            or ($5 = 'my' and $6 is not null and l2.user_id = $6 and ($8 is null or l2.state_code = $8))
            or ($5 = 'te' and $7 is not null and $9 and l2.team_id = $7 and ($8 is null or l2.state_code = $8))
          )
        order by l2.version desc, l2.modified_at desc
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
  $sql$, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter, can_read_team_filter;
end;
$$;

create or replace function private.search_processes_latest_impl(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer,
  type_of_data_set_filter text default 'all'::text
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  model_id uuid,
  total_count bigint
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
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
      where p.extracted_text &@~ $1
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
  $sql$, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, type_of_data_set_filter;
end;
$$;

create or replace function private.search_flows_latest_impl(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
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
      where f.extracted_text &@~ $1
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
  $sql$, json_filter_clause);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          normalized_data_source, effective_user_id, team_id_filter, state_code_filter,
          can_read_team_filter, flow_type, flow_type_array, as_input, classification_filter;
end;
$$;

alter function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text) owner to postgres;
alter function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;

revoke all on function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) from public;
revoke all on function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text) from public;
revoke all on function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) from public;

grant execute on function private.search_lifecyclemodels_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant execute on function private.search_processes_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer, text) to anon, authenticated, service_role;
grant execute on function private.search_flows_latest_impl(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
