-- Add a deliberately bounded, low-frequency UUID deep search path. This is
-- separate from search_*_latest: exact UUID search still returns the object
-- itself, while this RPC finds visible latest dataset rows whose JSON contains
-- the UUID text.

create or replace function private.dataset_json_first_text(p_value jsonb)
returns text
language sql
immutable
set search_path to 'public', 'extensions', 'pg_temp'
as $$
  select nullif(
    btrim(
      case
        when p_value is null or p_value = 'null'::jsonb then null
        when jsonb_typeof(p_value) = 'string' then p_value #>> '{}'
        when jsonb_typeof(p_value) = 'object' then p_value ->> '#text'
        when jsonb_typeof(p_value) = 'array' then (
          select coalesce(item ->> '#text', item #>> '{}')
          from jsonb_array_elements(p_value) as value(item)
          where nullif(btrim(coalesce(item ->> '#text', item #>> '{}')), '') is not null
          limit 1
        )
        else null
      end
    ),
    ''
  );
$$;

create or replace function private.dataset_json_display_name(
  p_entity_kind text,
  p_json jsonb
) returns text
language sql
immutable
set search_path to 'public', 'extensions', 'pg_temp'
as $$
  select coalesce(
    case p_entity_kind
      when 'flow' then coalesce(
        private.dataset_json_first_text(p_json #> '{flowDataSet,flowInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{flowDataSet,flowInformation,dataSetInformation,common:name}')
      )
      when 'process' then coalesce(
        private.dataset_json_first_text(p_json #> '{processDataSet,processInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{processDataSet,processInformation,dataSetInformation,common:name}')
      )
      when 'lifecyclemodel' then coalesce(
        private.dataset_json_first_text(p_json #> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,name,baseName}'),
        private.dataset_json_first_text(p_json #> '{lifeCycleModelDataSet,lifeCycleModelInformation,dataSetInformation,common:name}')
      )
      when 'source' then coalesce(
        private.dataset_json_first_text(p_json #> '{sourceDataSet,sourceInformation,dataSetInformation,common:shortName}'),
        private.dataset_json_first_text(p_json #> '{sourceDataSet,sourceInformation,dataSetInformation,common:name}')
      )
      when 'contact' then coalesce(
        private.dataset_json_first_text(p_json #> '{contactDataSet,contactInformation,dataSetInformation,common:shortName}'),
        private.dataset_json_first_text(p_json #> '{contactDataSet,contactInformation,dataSetInformation,common:name}')
      )
      when 'unitgroup' then private.dataset_json_first_text(
        p_json #> '{unitGroupDataSet,unitGroupInformation,dataSetInformation,common:name}'
      )
      when 'flowproperty' then private.dataset_json_first_text(
        p_json #> '{flowPropertyDataSet,flowPropertiesInformation,dataSetInformation,common:name}'
      )
      else null
    end,
    nullif(btrim(p_json ->> 'name'), ''),
    nullif(btrim(p_json ->> 'title'), '')
  );
$$;

create or replace function private.search_dataset_json_uuid_mentions_impl(
  p_uuid uuid,
  p_source_entity_kinds text[] default null::text[],
  p_data_source text default 'tg'::text,
  p_this_user_id text default ''::text,
  p_team_id_filter uuid default null::uuid,
  p_state_code_filter integer default null::integer,
  p_limit integer default 20
) returns table(
  rank bigint,
  source_entity_kind text,
  source_id uuid,
  source_version character(9),
  source_name text,
  source_modified_at timestamp with time zone,
  source_team_id uuid,
  source_json jsonb,
  matched_by text,
  matched_entity_table text
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '8s'
as $$
declare
  normalized_data_source text;
  effective_user_id uuid;
  can_read_team_filter boolean;
  normalized_limit integer;
  per_entity_limit integer;
  uuid_pattern text;
  normalized_source_entity_kinds text[];
  branches text[] := array[]::text[];
  v_sql text;
begin
  normalized_data_source := coalesce(nullif(lower(btrim(p_data_source)), ''), 'tg');
  effective_user_id := private.dataset_search_effective_user_id(p_this_user_id);
  can_read_team_filter := private.dataset_search_can_read_team_filter(p_team_id_filter, effective_user_id);
  normalized_limit := least(greatest(coalesce(p_limit, 20), 1), 50);
  per_entity_limit := normalized_limit;
  uuid_pattern := '%' || p_uuid::text || '%';

  if p_source_entity_kinds is not null then
    select array_agg(distinct normalized_kind order by normalized_kind)
    into normalized_source_entity_kinds
    from (
      select case lower(btrim(kind))
        when 'flow' then 'flow'
        when 'flows' then 'flow'
        when 'process' then 'process'
        when 'processes' then 'process'
        when 'lifecyclemodel' then 'lifecyclemodel'
        when 'lifecyclemodels' then 'lifecyclemodel'
        when 'model' then 'lifecyclemodel'
        when 'models' then 'lifecyclemodel'
        when 'source' then 'source'
        when 'sources' then 'source'
        when 'contact' then 'contact'
        when 'contacts' then 'contact'
        when 'unitgroup' then 'unitgroup'
        when 'unitgroups' then 'unitgroup'
        when 'flowproperty' then 'flowproperty'
        when 'flowproperties' then 'flowproperty'
        else null
      end as normalized_kind
      from unnest(p_source_entity_kinds) as requested(kind)
    ) normalized
    where normalized_kind is not null;

    if coalesce(array_length(normalized_source_entity_kinds, 1), 0) = 0 then
      return;
    end if;
  end if;

  if normalized_source_entity_kinds is null or 'process' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          10::integer as entity_rank,
          'process'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('process', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.processes'::text as matched_entity_table
        from public.processes d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flow' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          20::integer as entity_rank,
          'flow'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flow', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flows'::text as matched_entity_table
        from public.flows d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'lifecyclemodel' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          30::integer as entity_rank,
          'lifecyclemodel'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('lifecyclemodel', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.lifecyclemodels'::text as matched_entity_table
        from public.lifecyclemodels d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'source' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          40::integer as entity_rank,
          'source'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('source', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.sources'::text as matched_entity_table
        from public.sources d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'contact' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          50::integer as entity_rank,
          'contact'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('contact', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.contacts'::text as matched_entity_table
        from public.contacts d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'unitgroup' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          60::integer as entity_rank,
          'unitgroup'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('unitgroup', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.unitgroups'::text as matched_entity_table
        from public.unitgroups d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if normalized_source_entity_kinds is null or 'flowproperty' = any(normalized_source_entity_kinds) then
    branches := branches || array[$branch$
      (select *
      from (
        select distinct on (d.id)
          70::integer as entity_rank,
          'flowproperty'::text as source_entity_kind,
          d.id as source_id,
          d.version as source_version,
          private.dataset_json_display_name('flowproperty', d.json) as source_name,
          d.modified_at as source_modified_at,
          d.team_id as source_team_id,
          d.json as source_json,
          'json_uuid_scan'::text as matched_by,
          'public.flowproperties'::text as matched_entity_table
        from public.flowproperties d
        where (
            ($1 = 'tg' and d.state_code = 100 and ($3 is null or d.team_id = $3))
            or ($1 = 'co' and d.state_code = 200 and ($3 is null or d.team_id = $3))
            or ($1 = 'my' and $2 is not null and d.user_id = $2 and ($4 is null or d.state_code = $4))
            or ($1 = 'te' and $3 is not null and $5 and d.team_id = $3 and ($4 is null or d.state_code = $4))
          )
        order by d.id, d.version desc, d.modified_at desc
      ) latest
      where latest.source_json::text like $6
      order by latest.source_modified_at desc nulls last, latest.source_id
      limit $8
      )
    $branch$];
  end if;

  if coalesce(array_length(branches, 1), 0) = 0 then
    return;
  end if;

  v_sql := format($sql$
    with matched_rows as (
      %s
    )
    select
      row_number() over (
        order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
      )::bigint as rank,
      source_entity_kind,
      source_id,
      source_version,
      source_name,
      source_modified_at,
      source_team_id,
      source_json,
      matched_by,
      matched_entity_table
    from matched_rows
    order by entity_rank, source_modified_at desc nulls last, source_entity_kind, source_id
    limit $7
  $sql$, array_to_string(branches, E'\nunion all\n'));

  return query execute v_sql
    using normalized_data_source, effective_user_id, p_team_id_filter, p_state_code_filter,
          can_read_team_filter, uuid_pattern, normalized_limit, per_entity_limit;
end;
$$;

create or replace function public.search_dataset_json_uuid_mentions(
  p_uuid uuid,
  p_source_entity_kinds text[] default null::text[],
  p_data_source text default 'tg'::text,
  p_this_user_id text default ''::text,
  p_team_id_filter uuid default null::uuid,
  p_state_code_filter integer default null::integer,
  p_limit integer default 20
) returns table(
  rank bigint,
  source_entity_kind text,
  source_id uuid,
  source_version character(9),
  source_name text,
  source_modified_at timestamp with time zone,
  source_team_id uuid,
  source_json jsonb,
  matched_by text,
  matched_entity_table text
)
language plpgsql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '8s'
as $$
begin
  return query
    select *
    from private.search_dataset_json_uuid_mentions_impl(
      p_uuid,
      p_source_entity_kinds,
      p_data_source,
      p_this_user_id,
      p_team_id_filter,
      p_state_code_filter,
      p_limit
    );
end;
$$;

alter function private.dataset_json_first_text(jsonb) owner to postgres;
alter function private.dataset_json_display_name(text, jsonb) owner to postgres;
alter function private.search_dataset_json_uuid_mentions_impl(uuid, text[], text, text, uuid, integer, integer) owner to postgres;
alter function public.search_dataset_json_uuid_mentions(uuid, text[], text, text, uuid, integer, integer) owner to postgres;

revoke all on function private.dataset_json_first_text(jsonb) from public;
revoke all on function private.dataset_json_display_name(text, jsonb) from public;
revoke all on function private.search_dataset_json_uuid_mentions_impl(uuid, text[], text, text, uuid, integer, integer) from public;

grant execute on function private.search_dataset_json_uuid_mentions_impl(uuid, text[], text, text, uuid, integer, integer) to anon, authenticated, service_role;
grant all on function public.search_dataset_json_uuid_mentions(uuid, text[], text, text, uuid, integer, integer) to anon, authenticated, service_role;

grant execute on function util.dataset_json_search_text_allowed_prefixes(text) to service_role;
grant execute on function util.dataset_json_search_text_is_noise(text, text) to service_role;
