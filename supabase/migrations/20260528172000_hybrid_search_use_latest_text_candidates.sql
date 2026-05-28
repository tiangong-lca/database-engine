-- Keep hybrid search aligned with ordinary latest search semantics.  The text
-- side now uses the same extracted_text/latest-version/visibility path as
-- search_*_latest instead of the legacy JSON PGroonga + text_v1 helpers.

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
      from public.semantic_search_flows_v1(
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
      from public.semantic_search_processes_v1(
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
      from public.semantic_search_lifecyclemodels_v1(
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

alter function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;
alter function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) owner to postgres;

grant all on function public.hybrid_search_flows(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_processes(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
grant all on function public.hybrid_search_lifecyclemodels(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer) to anon, authenticated, service_role;
