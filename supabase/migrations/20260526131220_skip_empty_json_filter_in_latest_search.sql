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
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  v_sql := format($sql$
    with matched_ids as (
      select d.id, max(pgroonga_score(d.tableoid, d.ctid)) as search_score
      from %1$s d
      where d.extracted_text &@~ $1
        %2$s
        and (
          ($5 = 'tg' and d.state_code = 100 and ($7 is null or d.team_id = $7))
          or ($5 = 'co' and d.state_code = 200 and ($7 is null or d.team_id = $7))
          or ($5 = 'my' and $6 is not null and d.user_id = $6 and ($8 is null or d.state_code = $8))
          or ($5 = 'te' and $7 is not null and d.team_id = $7 and ($8 is null or d.state_code = $8))
        )
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

create or replace function public.search_processes_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
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
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
declare
  normalized_page_size bigint;
  normalized_page_current bigint;
  normalized_this_user_id uuid;
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
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and p.json @> $2'
  end;

  v_sql := format($sql$
    with matched_ids as (
      select p.id, max(pgroonga_score(p.tableoid, p.ctid)) as search_score
      from public.processes p
      where p.extracted_text &@~ $1
        %s
        and (
          coalesce($9, 'all') = 'all'
          or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = $9
        )
        and (
          ($5 = 'tg' and p.state_code = 100 and ($7 is null or p.team_id = $7))
          or ($5 = 'co' and p.state_code = 200 and ($7 is null or p.team_id = $7))
          or ($5 = 'my' and $6 is not null and p.user_id = $6 and ($8 is null or p.state_code = $8))
          or ($5 = 'te' and $7 is not null and p.team_id = $7 and ($8 is null or p.state_code = $8))
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
            or ($5 = 'te' and $7 is not null and p2.team_id = $7 and ($8 is null or p2.state_code = $8))
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
          data_source, normalized_this_user_id, team_id_filter, state_code_filter, type_of_data_set_filter;
end;
$$;

create or replace function public.search_flows_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
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
  normalized_this_user_id := case
    when coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(this_user_id)::uuid
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
  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and f.json @> $2'
  end;

  v_sql := format($sql$
    with matched_ids as (
      select f.id, max(pgroonga_score(f.tableoid, f.ctid)) as search_score
      from public.flows f
      where f.extracted_text &@~ $1
        %s
        and (
          ($5 = 'tg' and f.state_code = 100 and ($7 is null or f.team_id = $7))
          or ($5 = 'co' and f.state_code = 200 and ($7 is null or f.team_id = $7))
          or ($5 = 'my' and $6 is not null and f.user_id = $6 and ($8 is null or f.state_code = $8))
          or ($5 = 'te' and $7 is not null and f.team_id = $7 and ($8 is null or f.state_code = $8))
        )
        and (
          $9 is null
          or (f.json #>> '{flowDataSet,modellingAndValidation,LCIMethod,typeOfDataSet}') = any($10)
        )
        and (
          $11 is null
          or $11 = false
          or not (
            f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
          )
        )
        and (
          jsonb_array_length($12) = 0
          or exists (
            select 1
            from jsonb_array_elements($12) as selected_class(item)
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
            or ($5 = 'te' and $7 is not null and f2.team_id = $7 and ($8 is null or f2.state_code = $8))
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
          data_source, normalized_this_user_id, team_id_filter, state_code_filter,
          flow_type, flow_type_array, as_input, classification_filter;
end;
$$;

alter function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) owner to postgres;
alter function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;

grant all on function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) to anon, authenticated, service_role;
grant all on function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
