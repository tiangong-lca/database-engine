create schema if not exists util;

create or replace function util.dataset_json_search_text(p_json jsonb)
returns text
language sql
immutable
parallel safe
as $$
  with recursive walk(value) as (
    select p_json
    where p_json is not null

    union all

    select child.value
    from walk w
    cross join lateral (
      select elem.value
      from jsonb_array_elements(
        case when jsonb_typeof(w.value) = 'array' then w.value else '[]'::jsonb end
      ) as elem(value)

      union all

      select obj.value
      from jsonb_each(
        case when jsonb_typeof(w.value) = 'object' then w.value else '{}'::jsonb end
      ) as obj(key, value)
    ) child
  )
  select coalesce(string_agg(value #>> '{}', ' ' order by value #>> '{}'), '')
  from walk
  where jsonb_typeof(value) in ('string', 'number', 'boolean');
$$;

alter function util.dataset_json_search_text(jsonb) owner to postgres;

create or replace function util.set_dataset_extracted_text_from_json()
returns trigger
language plpgsql
set search_path to 'public', 'util', 'pg_temp'
as $$
begin
  new.extracted_text := util.dataset_json_search_text(new.json);
  return new;
end;
$$;

alter function util.set_dataset_extracted_text_from_json() owner to postgres;
revoke all on function util.dataset_json_search_text(jsonb) from public;
revoke all on function util.set_dataset_extracted_text_from_json() from public;
grant usage on schema util to service_role;
grant execute on function util.dataset_json_search_text(jsonb) to service_role;
grant execute on function util.set_dataset_extracted_text_from_json() to service_role;

alter table public.contacts add column if not exists extracted_text text;
alter table public.sources add column if not exists extracted_text text;
alter table public.unitgroups add column if not exists extracted_text text;
alter table public.flowproperties add column if not exists extracted_text text;

drop trigger if exists zz_contacts_extracted_text_sync_trigger on public.contacts;
create trigger zz_contacts_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.contacts
  for each row execute function util.set_dataset_extracted_text_from_json();

drop trigger if exists zz_sources_extracted_text_sync_trigger on public.sources;
create trigger zz_sources_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.sources
  for each row execute function util.set_dataset_extracted_text_from_json();

drop trigger if exists zz_unitgroups_extracted_text_sync_trigger on public.unitgroups;
create trigger zz_unitgroups_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.unitgroups
  for each row execute function util.set_dataset_extracted_text_from_json();

drop trigger if exists zz_flowproperties_extracted_text_sync_trigger on public.flowproperties;
create trigger zz_flowproperties_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.flowproperties
  for each row execute function util.set_dataset_extracted_text_from_json();

update public.contacts
   set extracted_text = util.dataset_json_search_text(json)
 where extracted_text is null
   and json is not null;

update public.sources
   set extracted_text = util.dataset_json_search_text(json)
 where extracted_text is null
   and json is not null;

update public.unitgroups
   set extracted_text = util.dataset_json_search_text(json)
 where extracted_text is null
   and json is not null;

update public.flowproperties
   set extracted_text = util.dataset_json_search_text(json)
 where extracted_text is null
   and json is not null;

create index if not exists contacts_text_pgroonga
  on public.contacts using pgroonga (extracted_text);

create index if not exists sources_text_pgroonga
  on public.sources using pgroonga (extracted_text);

create index if not exists unitgroups_text_pgroonga
  on public.unitgroups using pgroonga (extracted_text);

create index if not exists flowproperties_text_pgroonga
  on public.flowproperties using pgroonga (extracted_text);

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

  v_sql := format($sql$
    with matched_ids as (
      select d.id, max(pgroonga_score(d.tableoid, d.ctid)) as search_score
      from %1$s d
      where d.extracted_text &@~ $1
        and d.json @> $2
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
  $sql$, p_table);

  return query execute v_sql
    using query_text, filter_condition_jsonb, normalized_page_size, normalized_page_current,
          data_source, normalized_this_user_id, team_id_filter, state_code_filter;
end;
$$;

alter function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
grant all on function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) to anon;
grant all on function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) to authenticated;
grant all on function public._search_simple_dataset_latest(regclass, text, jsonb, bigint, bigint, text, text, uuid, integer) to service_role;

create or replace function public.search_sources_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.sources'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;

create or replace function public.search_contacts_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.contacts'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;

create or replace function public.search_unitgroups_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.unitgroups'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;

create or replace function public.search_flowproperties_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.flowproperties'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
end;
$$;

create or replace function public.search_lifecyclemodels_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query
    select *
    from public._search_simple_dataset_latest(
      'public.lifecyclemodels'::regclass,
      query_text,
      filter_condition,
      page_size,
      page_current,
      data_source,
      this_user_id,
      team_id_filter,
      state_code_filter
    );
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
begin
  normalized_page_size := greatest(coalesce(page_size, 10), 1);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  normalized_this_user_id := case
    when coalesce(btrim(this_user_id) ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', false)
      then btrim(this_user_id)::uuid
    else null::uuid
  end;
  filter_condition_jsonb := coalesce(filter_condition, '{}'::jsonb);

  return query
    with matched_ids as (
      select p.id, max(pgroonga_score(p.tableoid, p.ctid)) as search_score
      from public.processes p
      where p.extracted_text &@~ query_text
        and p.json @> filter_condition_jsonb
        and (
          coalesce(type_of_data_set_filter, 'all') = 'all'
          or p.json #>> '{processDataSet,modellingAndValidation,LCIMethodAndAllocation,typeOfDataSet}' = type_of_data_set_filter
        )
        and (
          (data_source = 'tg' and p.state_code = 100 and (team_id_filter is null or p.team_id = team_id_filter))
          or (data_source = 'co' and p.state_code = 200 and (team_id_filter is null or p.team_id = team_id_filter))
          or (data_source = 'my' and normalized_this_user_id is not null and p.user_id = normalized_this_user_id and (state_code_filter is null or p.state_code = state_code_filter))
          or (data_source = 'te' and team_id_filter is not null and p.team_id = team_id_filter and (state_code_filter is null or p.state_code = state_code_filter))
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
            (data_source = 'tg' and p2.state_code = 100 and (team_id_filter is null or p2.team_id = team_id_filter))
            or (data_source = 'co' and p2.state_code = 200 and (team_id_filter is null or p2.team_id = team_id_filter))
            or (data_source = 'my' and normalized_this_user_id is not null and p2.user_id = normalized_this_user_id and (state_code_filter is null or p2.state_code = state_code_filter))
            or (data_source = 'te' and team_id_filter is not null and p2.team_id = team_id_filter and (state_code_filter is null or p2.state_code = state_code_filter))
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
    limit normalized_page_size
    offset (normalized_page_current - 1) * normalized_page_size;
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

  return query
    with matched_ids as (
      select f.id, max(pgroonga_score(f.tableoid, f.ctid)) as search_score
      from public.flows f
      where f.extracted_text &@~ query_text
        and f.json @> filter_condition_jsonb
        and (
          (data_source = 'tg' and f.state_code = 100 and (team_id_filter is null or f.team_id = team_id_filter))
          or (data_source = 'co' and f.state_code = 200 and (team_id_filter is null or f.team_id = team_id_filter))
          or (data_source = 'my' and normalized_this_user_id is not null and f.user_id = normalized_this_user_id and (state_code_filter is null or f.state_code = state_code_filter))
          or (data_source = 'te' and team_id_filter is not null and f.team_id = team_id_filter and (state_code_filter is null or f.state_code = state_code_filter))
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
            (data_source = 'tg' and f2.state_code = 100 and (team_id_filter is null or f2.team_id = team_id_filter))
            or (data_source = 'co' and f2.state_code = 200 and (team_id_filter is null or f2.team_id = team_id_filter))
            or (data_source = 'my' and normalized_this_user_id is not null and f2.user_id = normalized_this_user_id and (state_code_filter is null or f2.state_code = state_code_filter))
            or (data_source = 'te' and team_id_filter is not null and f2.team_id = team_id_filter and (state_code_filter is null or f2.state_code = state_code_filter))
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
    limit normalized_page_size
    offset (normalized_page_current - 1) * normalized_page_size;
end;
$$;

alter function public.search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) owner to postgres;
alter function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;

grant all on function public.search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) to anon, authenticated, service_role;
grant all on function public.search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;

create or replace function public.pgroonga_search_sources_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_sources_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

create or replace function public.pgroonga_search_contacts_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_contacts_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

create or replace function public.pgroonga_search_unitgroups_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_unitgroups_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

create or replace function public.pgroonga_search_flowproperties_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_flowproperties_latest(query_text, filter_condition, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

create or replace function public.pgroonga_search_lifecyclemodels_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_lifecyclemodels_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

create or replace function public.pgroonga_search_processes_latest(
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
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, model_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_processes_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter, type_of_data_set_filter);
end;
$$;

create or replace function public.pgroonga_search_flows_latest(
  query_text text,
  filter_condition jsonb default '{}'::jsonb,
  order_by jsonb default '{}'::jsonb,
  page_size bigint default 10,
  page_current bigint default 1,
  data_source text default 'tg'::text,
  this_user_id text default ''::text,
  team_id_filter uuid default null::uuid,
  state_code_filter integer default null::integer
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
begin
  return query select * from public.search_flows_latest(query_text, filter_condition, order_by, page_size, page_current, data_source, this_user_id, team_id_filter, state_code_filter);
end;
$$;

alter function public.pgroonga_search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.pgroonga_search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.pgroonga_search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.pgroonga_search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.pgroonga_search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;
alter function public.pgroonga_search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) owner to postgres;
alter function public.pgroonga_search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) owner to postgres;

grant all on function public.pgroonga_search_sources_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_contacts_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_unitgroups_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_flowproperties_latest(text, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_lifecyclemodels_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_processes_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer, text) to anon, authenticated, service_role;
grant all on function public.pgroonga_search_flows_latest(text, jsonb, jsonb, bigint, bigint, text, text, uuid, integer) to anon, authenticated, service_role;

create or replace function public.semantic_search_flows(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
as $$
  select * from public.semantic_search_flows_v1($1, $2, $3, $4, $5);
$$;

create or replace function public.semantic_search_processes(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
as $$
  select * from public.semantic_search_processes_v1($1, $2, $3, $4, $5);
$$;

create or replace function public.semantic_search_lifecyclemodels(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
as $$
  select * from public.semantic_search_lifecyclemodels_v1($1, $2, $3, $4, $5);
$$;

alter function public.semantic_search_flows(text, text, double precision, integer, text) owner to postgres;
alter function public.semantic_search_processes(text, text, double precision, integer, text) owner to postgres;
alter function public.semantic_search_lifecyclemodels(text, text, double precision, integer, text) owner to postgres;
grant all on function public.semantic_search_flows(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant all on function public.semantic_search_processes(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant all on function public.semantic_search_lifecyclemodels(text, text, double precision, integer, text) to anon, authenticated, service_role;

create or replace function public.hybrid_search_flows(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  full_text_weight double precision default 0.3,
  extracted_text_weight double precision default 0.2,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_text text;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_text := coalesce(nullif(btrim(filter_condition), ''), '{}');
  filter_condition_jsonb := filter_condition_text::jsonb;
  text_weight := coalesce(full_text_weight, 0.0) + coalesce(extracted_text_weight, 0.0);

  return query
    with text_match as (
      select txt.rank as text_rank, txt.id as text_id
      from public.search_flows_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        coalesce(auth.uid()::text, ''),
        null::uuid,
        null::integer
      ) txt
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from public.semantic_search_flows(
        query_embedding,
        filter_condition_text,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_match.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_match.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_match
      full outer join semantic on text_match.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select f.*
      from public.flows f
      join fused on fused.id = f.id
      where
        (
          (data_source = 'tg' and f.state_code = 100)
          or (data_source = 'co' and f.state_code = 200)
          or (data_source = 'my' and f.user_id = auth.uid())
          or (
            data_source = 'te'
            and exists (
              select 1
              from public.roles r
              where r.user_id = auth.uid()
                and r.team_id = f.team_id
                and r.role::text in ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

create or replace function public.hybrid_search_processes(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  full_text_weight double precision default 0.3,
  extracted_text_weight double precision default 0.2,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, model_id uuid, team_id uuid, total_count bigint)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_text text;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_text := coalesce(nullif(btrim(filter_condition), ''), '{}');
  filter_condition_jsonb := filter_condition_text::jsonb;
  text_weight := coalesce(full_text_weight, 0.0) + coalesce(extracted_text_weight, 0.0);

  return query
    with text_match as (
      select txt.rank as text_rank, txt.id as text_id
      from public.search_processes_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        coalesce(auth.uid()::text, ''),
        null::uuid,
        null::integer,
        'all'
      ) txt
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from public.semantic_search_processes(
        query_embedding,
        filter_condition_text,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_match.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_match.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_match
      full outer join semantic on text_match.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select p.*
      from public.processes p
      join fused on fused.id = p.id
      where
        (
          (data_source = 'tg' and p.state_code = 100)
          or (data_source = 'co' and p.state_code = 200)
          or (data_source = 'my' and p.user_id = auth.uid())
          or (
            data_source = 'te'
            and exists (
              select 1
              from public.roles r
              where r.user_id = auth.uid()
                and r.team_id = p.team_id
                and r.role::text in ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.model_id,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.model_id, counted_rows.team_id, counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

create or replace function public.hybrid_search_lifecyclemodels(
  query_text text,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  full_text_weight double precision default 0.3,
  extracted_text_weight double precision default 0.2,
  semantic_weight double precision default 0.5,
  rrf_k integer default 10,
  data_source text default 'tg'::text,
  page_size integer default 10,
  page_current integer default 1
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_text text;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_text := coalesce(nullif(btrim(filter_condition), ''), '{}');
  filter_condition_jsonb := filter_condition_text::jsonb;
  text_weight := coalesce(full_text_weight, 0.0) + coalesce(extracted_text_weight, 0.0);

  return query
    with text_match as (
      select txt.rank as text_rank, txt.id as text_id
      from public.search_lifecyclemodels_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        coalesce(auth.uid()::text, ''),
        null::uuid,
        null::integer
      ) txt
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from public.semantic_search_lifecyclemodels(
        query_embedding,
        filter_condition_text,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_match.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_match.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_match
      full outer join semantic on text_match.text_id = semantic.ss_id
    ),
    fused as (
      select fused_raw.id, sum(fused_raw.score) as score
      from fused_raw
      where fused_raw.id is not null
      group by fused_raw.id
    ),
    visible_rows as (
      select l.*
      from public.lifecyclemodels l
      join fused on fused.id = l.id
      where
        (
          (data_source = 'tg' and l.state_code = 100)
          or (data_source = 'co' and l.state_code = 200)
          or (data_source = 'my' and l.user_id = auth.uid())
          or (
            data_source = 'te'
            and exists (
              select 1
              from public.roles r
              where r.user_id = auth.uid()
                and r.team_id = l.team_id
                and r.role::text in ('admin', 'member', 'owner')
            )
          )
        )
    ),
    latest_rows as (
      select distinct on (visible_rows.id)
        visible_rows.id,
        visible_rows.json,
        visible_rows.version,
        visible_rows.modified_at,
        visible_rows.team_id,
        fused.score
      from visible_rows
      join fused on fused.id = visible_rows.id
      order by visible_rows.id, visible_rows.version desc, visible_rows.modified_at desc
    ),
    counted_rows as (
      select latest_rows.*, count(*) over()::bigint as total_count
      from latest_rows
    )
    select counted_rows.id, counted_rows.json, counted_rows.version, counted_rows.modified_at, counted_rows.team_id, counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

alter function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
grant all on function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;

drop index if exists public.flows_json_pgroonga;
drop index if exists public.flows_public_json_pgroonga_idx;
drop index if exists public.flows_co_json_pgroonga_idx;
drop index if exists public.processes_json_pgroonga;
drop index if exists public.processes_public_json_pgroonga_idx;
drop index if exists public.processes_co_json_pgroonga_idx;
drop index if exists public.lifecyclemodels_json_pgroonga;
drop index if exists public.lifecyclemodels_public_json_pgroonga_idx;
drop index if exists public.lifecyclemodels_co_json_pgroonga_idx;
drop index if exists public.contacts_json_pgroonga;
drop index if exists public.contacts_public_json_pgroonga_idx;
drop index if exists public.contacts_co_json_pgroonga_idx;
drop index if exists public.sources_json_pgroonga;
drop index if exists public.sources_public_json_pgroonga_idx;
drop index if exists public.sources_co_json_pgroonga_idx;
drop index if exists public.unitgroups_json_pgroonga;
drop index if exists public.unitgroups_public_json_pgroonga_idx;
drop index if exists public.unitgroups_co_json_pgroonga_idx;
drop index if exists public.flowproperties_json_pgroonga;
drop index if exists public.flowproperties_public_json_pgroonga_idx;
drop index if exists public.flowproperties_co_json_pgroonga_idx;
