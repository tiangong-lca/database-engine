-- Optimize hybrid search's semantic side by making vector candidate
-- generation table-specific, visibility-aware, and lightweight.

create schema if not exists private;

revoke all on schema private from public;
grant usage on schema private to anon, authenticated, service_role;

create or replace function private.semantic_flow_candidates(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(
  rank bigint,
  id uuid,
  distance double precision
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  flow_type text;
  flow_type_array text[];
  as_input boolean;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

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

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 100
          and f.json @> filter_condition_jsonb
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.state_code = 200
          and f.json @> filter_condition_jsonb
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and f.user_id = effective_user_id
          and f.json @> filter_condition_jsonb
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          f.id as candidate_id,
          (f.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.flows f
        where f.embedding_ft is not null
          and exists (
            select 1
            from public.roles r
            where r.user_id = effective_user_id
              and r.team_id = f.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and f.json @> filter_condition_jsonb
          and (
            flow_type is null
            or flow_type = ''
            or (f.json->'flowDataSet'->'modellingAndValidation'->'LCIMethod'->>'typeOfDataSet') = any(flow_type_array)
          )
          and (
            as_input is null
            or as_input = false
            or not (
              f.json @> '{"flowDataSet":{"flowInformation":{"dataSetInformation":{"classificationInformation":{"common:elementaryFlowCategorization":{"common:category":[{"#text":"Emissions","@level":"0"}]}}}}}}'
            )
          )
        order by f.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

create or replace function private.semantic_process_candidates(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(
  rank bigint,
  id uuid,
  distance double precision
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 100
          and p.json @> filter_condition_jsonb
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.state_code = 200
          and p.json @> filter_condition_jsonb
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and p.user_id = effective_user_id
          and p.json @> filter_condition_jsonb
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          p.id as candidate_id,
          (p.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.processes p
        where p.embedding_ft is not null
          and exists (
            select 1
            from public.roles r
            where r.user_id = effective_user_id
              and r.team_id = p.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and p.json @> filter_condition_jsonb
        order by p.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

create or replace function private.semantic_lifecyclemodel_candidates(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(
  rank bigint,
  id uuid,
  distance double precision
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
declare
  query_embedding_vector vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
begin
  query_embedding_vector := query_embedding::vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := greatest(coalesce(match_count, 20), 1);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - coalesce(match_threshold, 0.5);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.state_code = 100
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'co' then
    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.state_code = 200
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and l.user_id = effective_user_id
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;

  if normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;

    return query
      with candidates as materialized (
        select
          l.id as candidate_id,
          (l.embedding_ft <=> query_embedding_vector) as candidate_distance
        from public.lifecyclemodels l
        where l.embedding_ft is not null
          and exists (
            select 1
            from public.roles r
            where r.user_id = effective_user_id
              and r.team_id = l.team_id
              and r.role::text in ('admin', 'member', 'owner')
          )
          and l.json @> filter_condition_jsonb
        order by l.embedding_ft <=> query_embedding_vector
        limit candidate_size
      ),
      filtered as (
        select candidates.*
        from candidates
        where candidates.candidate_distance < threshold_distance
      )
      select
        rank() over (order by filtered.candidate_distance)::bigint,
        filtered.candidate_id,
        filtered.candidate_distance
      from filtered
      order by filtered.candidate_distance
      limit normalized_match_count;
    return;
  end if;
end;
$$;

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
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_flows_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_flow_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
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
      where (
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
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
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
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  model_id uuid,
  team_id uuid,
  total_count bigint
)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_processes_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer,
        'all'
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_process_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
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
      where (
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
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.model_id,
      counted_rows.team_id,
      counted_rows.total_count
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
) returns table(
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  team_id uuid,
  total_count bigint
)
language plpgsql
set statement_timeout to '60s'
set search_path to 'public', 'extensions', 'pg_temp'
as $$
declare
  candidate_limit integer;
  filter_condition_jsonb jsonb;
  text_weight double precision;
begin
  candidate_limit := greatest(coalesce(match_count, 20), coalesce(page_size, 10)) * 10;
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  return query
    with text_matches as (
      select ts.rank as text_rank, ts.id as text_id
      from public.search_lifecyclemodels_latest(
        query_text,
        filter_condition_jsonb,
        '{}'::jsonb,
        candidate_limit,
        1,
        data_source,
        '',
        null::uuid,
        null::integer
      ) ts
    ),
    semantic as (
      select ss.rank as ss_rank, ss.id as ss_id
      from private.semantic_lifecyclemodel_candidates(
        query_embedding,
        filter_condition,
        match_threshold,
        candidate_limit,
        data_source
      ) ss
    ),
    fused_raw as (
      select
        coalesce(text_matches.text_id, semantic.ss_id) as id,
        coalesce(1.0 / (rrf_k + text_matches.text_rank), 0.0) * text_weight
          + coalesce(1.0 / (rrf_k + semantic.ss_rank), 0.0) * semantic_weight as score
      from text_matches
      full outer join semantic on text_matches.text_id = semantic.ss_id
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
      where (
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
    select
      counted_rows.id,
      counted_rows.json,
      counted_rows.version,
      counted_rows.modified_at,
      counted_rows.team_id,
      counted_rows.total_count
    from counted_rows
    order by counted_rows.score desc, counted_rows.modified_at desc, counted_rows.id
    limit greatest(coalesce(page_size, 10), 1)
    offset (greatest(coalesce(page_current, 1), 1) - 1) * greatest(coalesce(page_size, 10), 1);
end;
$$;

alter function private.semantic_flow_candidates(text, text, double precision, integer, text) owner to postgres;
alter function private.semantic_process_candidates(text, text, double precision, integer, text) owner to postgres;
alter function private.semantic_lifecyclemodel_candidates(text, text, double precision, integer, text) owner to postgres;
alter function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;

revoke all on function private.semantic_flow_candidates(text, text, double precision, integer, text) from public;
revoke all on function private.semantic_process_candidates(text, text, double precision, integer, text) from public;
revoke all on function private.semantic_lifecyclemodel_candidates(text, text, double precision, integer, text) from public;

grant execute on function private.semantic_flow_candidates(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant execute on function private.semantic_process_candidates(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant execute on function private.semantic_lifecyclemodel_candidates(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant all on function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
