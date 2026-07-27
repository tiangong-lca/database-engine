-- Give contacts, flow properties, sources, and unit groups the same current
-- full-text + 1024-dimensional semantic search contract as the three larger
-- dataset families. Historical rows are intentionally not rewritten here;
-- operators enqueue bounded backfill pages through the service-only RPC below.

alter table public.contacts
  add column if not exists extracted_md text,
  add column if not exists embedding_ft_at timestamp with time zone,
  add column if not exists embedding_ft extensions.vector(1024);

alter table public.flowproperties
  add column if not exists extracted_md text,
  add column if not exists embedding_ft_at timestamp with time zone,
  add column if not exists embedding_ft extensions.vector(1024);

alter table public.sources
  add column if not exists extracted_md text,
  add column if not exists embedding_ft_at timestamp with time zone,
  add column if not exists embedding_ft extensions.vector(1024);

alter table public.unitgroups
  add column if not exists extracted_md text,
  add column if not exists embedding_ft_at timestamp with time zone,
  add column if not exists embedding_ft extensions.vector(1024);

-- These four 1536-dimensional indexes never had a producer or consumer. Drop
-- them before removing their permanently-null legacy columns.
drop index if exists public.contacts_json_ordered_vector;
drop index if exists public.flowproperties_json_ordered_vector;
drop index if exists public.sources_json_ordered_vector;
drop index if exists public.unitgroups_json_ordered_vector;

alter table public.contacts drop column if exists embedding;
alter table public.flowproperties drop column if exists embedding;
alter table public.sources drop column if exists embedding;
alter table public.unitgroups drop column if exists embedding;

-- Derived Markdown/vector writes must not look like authored dataset changes.
-- Scope the legacy all-column triggers the same way as flows/processes/models.
drop trigger if exists contacts_json_sync_trigger on public.contacts;
create trigger contacts_json_sync_trigger
before insert or update of json_ordered on public.contacts
for each row execute function public.contacts_sync_jsonb_version();

drop trigger if exists contacts_set_modified_at_trigger on public.contacts;
create trigger contacts_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.contacts
for each row execute function public.update_modified_at();

drop trigger if exists flowproperties_json_sync_trigger on public.flowproperties;
create trigger flowproperties_json_sync_trigger
before insert or update of json_ordered on public.flowproperties
for each row execute function public.flowproperties_sync_jsonb_version();

drop trigger if exists flowproperties_set_modified_at_trigger on public.flowproperties;
create trigger flowproperties_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.flowproperties
for each row execute function public.update_modified_at();

drop trigger if exists sources_json_sync_trigger on public.sources;
create trigger sources_json_sync_trigger
before insert or update of json_ordered on public.sources
for each row execute function public.sources_sync_jsonb_version();

drop trigger if exists sources_set_modified_at_trigger on public.sources;
create trigger sources_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.sources
for each row execute function public.update_modified_at();

drop trigger if exists unitgroups_json_sync_trigger on public.unitgroups;
create trigger unitgroups_json_sync_trigger
before insert or update of json_ordered on public.unitgroups
for each row execute function public.unitgroups_sync_jsonb_version();

drop trigger if exists unitgroups_set_modified_at_trigger on public.unitgroups;
create trigger unitgroups_set_modified_at_trigger
before update of
  json,
  json_ordered,
  user_id,
  state_code,
  version,
  team_id,
  review_id,
  rule_verification,
  reviews
on public.unitgroups
for each row execute function public.update_modified_at();

create or replace function public.contacts_embedding_ft_input(proc public.contacts)
returns text
language sql
immutable
strict
set search_path to ''
as $$
  select proc.extracted_md;
$$;

create or replace function public.flowproperties_embedding_ft_input(proc public.flowproperties)
returns text
language sql
immutable
strict
set search_path to ''
as $$
  select proc.extracted_md;
$$;

create or replace function public.sources_embedding_ft_input(proc public.sources)
returns text
language sql
immutable
strict
set search_path to ''
as $$
  select proc.extracted_md;
$$;

create or replace function public.unitgroups_embedding_ft_input(proc public.unitgroups)
returns text
language sql
immutable
strict
set search_path to ''
as $$
  select proc.extracted_md;
$$;

alter function public.contacts_embedding_ft_input(public.contacts) owner to postgres;
alter function public.flowproperties_embedding_ft_input(public.flowproperties) owner to postgres;
alter function public.sources_embedding_ft_input(public.sources) owner to postgres;
alter function public.unitgroups_embedding_ft_input(public.unitgroups) owner to postgres;

revoke all on function public.contacts_embedding_ft_input(public.contacts) from public;
revoke all on function public.flowproperties_embedding_ft_input(public.flowproperties) from public;
revoke all on function public.sources_embedding_ft_input(public.sources) from public;
revoke all on function public.unitgroups_embedding_ft_input(public.unitgroups) from public;

grant execute on function public.contacts_embedding_ft_input(public.contacts) to service_role;
grant execute on function public.flowproperties_embedding_ft_input(public.flowproperties) to service_role;
grant execute on function public.sources_embedding_ft_input(public.sources) to service_role;
grant execute on function public.unitgroups_embedding_ft_input(public.unitgroups) to service_role;

insert into util.embedding_queue_policy (
  scope_schema,
  scope_table,
  scope_edge_function,
  scope_embedding_column,
  mode,
  max_in_flight,
  max_read_count,
  retry_backoff_seconds
)
values
  ('public', 'contacts', 'embedding_ft', 'embedding_ft', 'normal', 2, 20, 300),
  ('public', 'flowproperties', 'embedding_ft', 'embedding_ft', 'normal', 2, 20, 300),
  ('public', 'sources', 'embedding_ft', 'embedding_ft', 'normal', 2, 20, 300),
  ('public', 'unitgroups', 'embedding_ft', 'embedding_ft', 'normal', 2, 20, 300)
on conflict (
  scope_schema,
  scope_table,
  scope_edge_function,
  scope_embedding_column
) do update
set
  mode = excluded.mode,
  max_in_flight = excluded.max_in_flight,
  max_read_count = excluded.max_read_count,
  retry_backoff_seconds = excluded.retry_backoff_seconds,
  updated_at = now();

-- Reuse the compact, durable dataset-extraction queue. New rows and authored
-- JSON changes enqueue only identity metadata; the Edge worker reads the
-- current row and rejects stale id/version messages.
create or replace function util.queue_dataset_extraction_jobs()
returns trigger
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_entity_kind text;
  v_message jsonb;
begin
  if TG_TABLE_SCHEMA <> 'public' then
    raise exception 'dataset extraction jobs only support public schema, got %', TG_TABLE_SCHEMA;
  end if;

  v_entity_kind := case TG_TABLE_NAME
    when 'flows' then 'flow'
    when 'processes' then 'process'
    when 'contacts' then 'contact'
    when 'flowproperties' then 'flowproperty'
    when 'sources' then 'source'
    when 'unitgroups' then 'unitgroup'
    else null
  end;

  if v_entity_kind is null then
    raise exception 'unsupported dataset extraction table %', TG_TABLE_NAME;
  end if;

  v_message := jsonb_build_object(
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'version', btrim(NEW.version::text),
    'entity_kind', v_entity_kind,
    'extraction_kind', 'extracted_md',
    'created_at', clock_timestamp()
  );

  perform pgmq.send(
    queue_name => 'dataset_extraction_jobs',
    msg => v_message
  );

  return NEW;
end;
$$;

alter function util.queue_dataset_extraction_jobs() owner to postgres;
revoke all on function util.queue_dataset_extraction_jobs() from public;

drop trigger if exists contact_dataset_extraction_trigger_insert on public.contacts;
create trigger contact_dataset_extraction_trigger_insert
after insert on public.contacts
for each row execute function util.queue_dataset_extraction_jobs();

drop trigger if exists contact_dataset_extraction_trigger_update on public.contacts;
create trigger contact_dataset_extraction_trigger_update
after update of json, json_ordered on public.contacts
for each row
when (NEW.json is distinct from OLD.json)
execute function util.queue_dataset_extraction_jobs();

drop trigger if exists flowproperty_dataset_extraction_trigger_insert on public.flowproperties;
create trigger flowproperty_dataset_extraction_trigger_insert
after insert on public.flowproperties
for each row execute function util.queue_dataset_extraction_jobs();

drop trigger if exists flowproperty_dataset_extraction_trigger_update on public.flowproperties;
create trigger flowproperty_dataset_extraction_trigger_update
after update of json, json_ordered on public.flowproperties
for each row
when (NEW.json is distinct from OLD.json)
execute function util.queue_dataset_extraction_jobs();

drop trigger if exists source_dataset_extraction_trigger_insert on public.sources;
create trigger source_dataset_extraction_trigger_insert
after insert on public.sources
for each row execute function util.queue_dataset_extraction_jobs();

drop trigger if exists source_dataset_extraction_trigger_update on public.sources;
create trigger source_dataset_extraction_trigger_update
after update of json, json_ordered on public.sources
for each row
when (NEW.json is distinct from OLD.json)
execute function util.queue_dataset_extraction_jobs();

drop trigger if exists unitgroup_dataset_extraction_trigger_insert on public.unitgroups;
create trigger unitgroup_dataset_extraction_trigger_insert
after insert on public.unitgroups
for each row execute function util.queue_dataset_extraction_jobs();

drop trigger if exists unitgroup_dataset_extraction_trigger_update on public.unitgroups;
create trigger unitgroup_dataset_extraction_trigger_update
after update of json, json_ordered on public.unitgroups
for each row
when (NEW.json is distinct from OLD.json)
execute function util.queue_dataset_extraction_jobs();

drop trigger if exists contact_embedding_ft_on_extract_md_update on public.contacts;
create trigger contact_embedding_ft_on_extract_md_update
after update of extracted_md on public.contacts
for each row
when (OLD.extracted_md is distinct from NEW.extracted_md)
execute function util.queue_embeddings(
  'contacts_embedding_ft_input',
  'embedding_ft',
  'embedding_ft'
);

drop trigger if exists flowproperty_embedding_ft_on_extract_md_update on public.flowproperties;
create trigger flowproperty_embedding_ft_on_extract_md_update
after update of extracted_md on public.flowproperties
for each row
when (OLD.extracted_md is distinct from NEW.extracted_md)
execute function util.queue_embeddings(
  'flowproperties_embedding_ft_input',
  'embedding_ft',
  'embedding_ft'
);

drop trigger if exists source_embedding_ft_on_extract_md_update on public.sources;
create trigger source_embedding_ft_on_extract_md_update
after update of extracted_md on public.sources
for each row
when (OLD.extracted_md is distinct from NEW.extracted_md)
execute function util.queue_embeddings(
  'sources_embedding_ft_input',
  'embedding_ft',
  'embedding_ft'
);

drop trigger if exists unitgroup_embedding_ft_on_extract_md_update on public.unitgroups;
create trigger unitgroup_embedding_ft_on_extract_md_update
after update of extracted_md on public.unitgroups
for each row
when (OLD.extracted_md is distinct from NEW.extracted_md)
execute function util.queue_embeddings(
  'unitgroups_embedding_ft_input',
  'embedding_ft',
  'embedding_ft'
);

-- Enqueue historical work in bounded, cursor-based pages. The RPC is
-- idempotent with respect to live extraction/embedding queues so an operator
-- can safely retry a page after a network interruption.
create or replace function public.cmd_dataset_semantic_backfill(
  p_table text,
  p_batch_size integer default 100,
  p_after_id uuid default null,
  p_after_version text default null,
  p_force_extraction boolean default false
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_entity_kind text;
  v_content_function text;
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 100), 1), 500);
  v_row_id uuid;
  v_row_version text;
  v_needs_extraction boolean;
  v_needs_embedding boolean;
  v_scanned_count integer := 0;
  v_extraction_enqueued integer := 0;
  v_embedding_enqueued integer := 0;
  v_already_queued integer := 0;
  v_last_id uuid;
  v_last_version text;
  v_job_exists boolean;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required'
    );
  end if;

  v_entity_kind := case v_table
    when 'contacts' then 'contact'
    when 'flowproperties' then 'flowproperty'
    when 'sources' then 'source'
    when 'unitgroups' then 'unitgroup'
    else null
  end;

  if v_entity_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'status', 400,
      'message', format('Unsupported semantic backfill table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  v_content_function := v_table || '_embedding_ft_input';

  for v_row_id, v_row_version, v_needs_extraction, v_needs_embedding in execute format(
    $sql$
      select
        dataset.id,
        btrim(dataset.version::text) as version,
        dataset.extracted_md is null or $4 as needs_extraction,
        dataset.embedding_ft is null as needs_embedding
      from public.%I dataset
      where dataset.json is not null
        and ($4 or dataset.extracted_md is null or dataset.embedding_ft is null)
        and (
          $1 is null
          or (dataset.id, btrim(dataset.version::text)) > ($1, coalesce($2, ''))
        )
      order by dataset.id, btrim(dataset.version::text)
      limit $3
    $sql$,
    v_table
  ) using p_after_id, p_after_version, v_batch_size, coalesce(p_force_extraction, false)
  loop
    v_scanned_count := v_scanned_count + 1;
    v_last_id := v_row_id;
    v_last_version := v_row_version;

    if v_needs_extraction then
      select exists (
        select 1
        from pgmq.q_dataset_extraction_jobs queued
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = v_table
          and queued.message->>'id' = v_row_id::text
          and queued.message->>'version' = v_row_version
          and queued.message->>'extraction_kind' = 'extracted_md'
      ) into v_job_exists;

      if v_job_exists then
        v_already_queued := v_already_queued + 1;
      else
        perform pgmq.send(
          queue_name => 'dataset_extraction_jobs',
          msg => jsonb_build_object(
            'schema', 'public',
            'table', v_table,
            'id', v_row_id,
            'version', v_row_version,
            'entity_kind', v_entity_kind,
            'extraction_kind', 'extracted_md',
            'created_at', clock_timestamp(),
            'backfill', true
          )
        );
        v_extraction_enqueued := v_extraction_enqueued + 1;
      end if;
    elsif v_needs_embedding then
      select exists (
        select 1
        from pgmq.q_embedding_jobs queued
        where queued.message->>'schema' = 'public'
          and queued.message->>'table' = v_table
          and queued.message->>'id' = v_row_id::text
          and queued.message->>'version' = v_row_version
          and queued.message->>'embeddingColumn' = 'embedding_ft'
          and queued.message->>'edgeFunction' = 'embedding_ft'
      ) or exists (
        select 1
        from util.pending_embedding_jobs pending
        where pending.schema_name = 'public'
          and pending.table_name = v_table
          and pending.record_id = v_row_id::text
          and pending.record_version = v_row_version
          and pending.embedding_column = 'embedding_ft'
          and pending.edge_function = 'embedding_ft'
          and pending.status in ('pending', 'enqueued')
      ) into v_job_exists;

      if v_job_exists then
        v_already_queued := v_already_queued + 1;
      else
        perform pgmq.send(
          queue_name => 'embedding_jobs',
          msg => jsonb_build_object(
            'id', v_row_id,
            'version', v_row_version,
            'schema', 'public',
            'table', v_table,
            'contentFunction', v_content_function,
            'embeddingColumn', 'embedding_ft',
            'edgeFunction', 'embedding_ft',
            'backfill', true
          )
        );
        v_embedding_enqueued := v_embedding_enqueued + 1;
      end if;
    end if;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'scanned_count', v_scanned_count,
    'extraction_enqueued_count', v_extraction_enqueued,
    'embedding_enqueued_count', v_embedding_enqueued,
    'already_queued_count', v_already_queued,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$$;

alter function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) owner to postgres;
revoke all on function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) from public;
revoke all on function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) from anon;
revoke all on function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) from authenticated;
grant execute on function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) to service_role;

-- A single audited implementation keeps the four simple dataset families on
-- identical visibility, threshold, HNSW, and RRF behavior. regclass input is
-- accepted only after an exact four-table allow-list check.
create or replace function private.semantic_simple_dataset_candidates(
  p_table regclass,
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
set plan_cache_mode to 'force_custom_plan'
set hnsw.iterative_scan to 'strict_order'
as $$
declare
  query_embedding_vector extensions.vector(1024);
  filter_condition_jsonb jsonb;
  normalized_data_source text;
  normalized_match_count integer;
  candidate_size integer;
  threshold_distance double precision;
  effective_user_id uuid;
  visibility_clause text;
  json_filter_clause text;
  candidate_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported semantic dataset table: %', p_table;
  end if;

  query_embedding_vector := query_embedding::extensions.vector(1024);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 20), 1), 200);
  candidate_size := greatest(normalized_match_count * 10, 200);
  threshold_distance := 1 - least(greatest(coalesce(match_threshold, 0.5), -1), 1);
  effective_user_id := private.dataset_search_effective_user_id('');

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $5';
  elsif normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := $visibility$
      exists (
        select 1
        from public.roles role_row
        where role_row.user_id = $5
          and role_row.team_id = d.team_id
          and role_row.role::text in ('admin', 'member', 'owner')
      )
    $visibility$;
  else
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;

  candidate_sql := format(
    $sql$
      with candidates as materialized (
        select
          d.id as candidate_id,
          d.embedding_ft <=> $1 as candidate_distance
        from %1$s d
        where d.embedding_ft is not null
          and %2$s
          %3$s
        order by d.embedding_ft <=> $1
        limit $3
      ),
      deduplicated as (
        select
          candidates.candidate_id,
          min(candidates.candidate_distance) as candidate_distance
        from candidates
        where candidates.candidate_distance < $4
        group by candidates.candidate_id
      )
      select
        rank() over (
          order by deduplicated.candidate_distance, deduplicated.candidate_id
        )::bigint,
        deduplicated.candidate_id,
        deduplicated.candidate_distance
      from deduplicated
      order by deduplicated.candidate_distance, deduplicated.candidate_id
      limit $6
    $sql$,
    p_table,
    visibility_clause,
    json_filter_clause
  );

  return query execute candidate_sql
    using query_embedding_vector, filter_condition_jsonb, candidate_size,
          threshold_distance, effective_user_id, normalized_match_count;
end;
$$;

alter function private.semantic_simple_dataset_candidates(regclass, text, text, double precision, integer, text) owner to postgres;
revoke all on function private.semantic_simple_dataset_candidates(regclass, text, text, double precision, integer, text) from public;
grant execute on function private.semantic_simple_dataset_candidates(regclass, text, text, double precision, integer, text) to anon, authenticated, service_role;

create or replace function private.semantic_simple_dataset_search(
  p_table regclass,
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(
  rank bigint,
  id uuid,
  "json" jsonb,
  version character(9),
  modified_at timestamp with time zone,
  total_count bigint
)
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
declare
  normalized_data_source text;
  effective_user_id uuid;
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

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $7';
  elsif normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := $visibility$
      exists (
        select 1
        from public.roles role_row
        where role_row.user_id = $7
          and role_row.team_id = d.team_id
          and role_row.role::text in ('admin', 'member', 'owner')
      )
    $visibility$;
  else
    return;
  end if;

  search_sql := format(
    $sql$
      with semantic as materialized (
        select candidate.rank, candidate.id, candidate.distance
        from private.semantic_simple_dataset_candidates(
          $1, $2, $3, $4, $5, $6
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
          match_count, normalized_data_source, effective_user_id;
end;
$$;

alter function private.semantic_simple_dataset_search(regclass, text, text, double precision, integer, text) owner to postgres;
revoke all on function private.semantic_simple_dataset_search(regclass, text, text, double precision, integer, text) from public;
grant execute on function private.semantic_simple_dataset_search(regclass, text, text, double precision, integer, text) to anon, authenticated, service_role;

create or replace function private.hybrid_search_simple_dataset(
  p_table regclass,
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
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(
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
set plan_cache_mode to 'force_custom_plan'
as $$
declare
  normalized_data_source text;
  normalized_match_count integer;
  semantic_match_count integer;
  normalized_page_size integer;
  normalized_page_current integer;
  candidate_limit integer;
  normalized_rrf_k integer;
  filter_condition_jsonb jsonb;
  escaped_query_terms text[];
  effective_user_id uuid;
  visibility_clause text;
  json_filter_clause text;
  text_match_clause text;
  text_weight double precision;
  hybrid_sql text;
begin
  if p_table not in (
    'public.contacts'::regclass,
    'public.flowproperties'::regclass,
    'public.sources'::regclass,
    'public.unitgroups'::regclass
  ) then
    raise exception 'unsupported hybrid dataset table: %', p_table;
  end if;

  normalized_data_source := coalesce(nullif(lower(btrim(data_source)), ''), 'tg');
  normalized_match_count := least(greatest(coalesce(match_count, 20), 1), 200);
  normalized_page_size := least(greatest(coalesce(page_size, 10), 1), 200);
  normalized_page_current := greatest(coalesce(page_current, 1), 1);
  semantic_match_count := greatest(normalized_match_count, normalized_page_size);
  candidate_limit := least(greatest(normalized_match_count, normalized_page_size) * 10, 5000);
  normalized_rrf_k := greatest(coalesce(rrf_k, 10), 1);
  filter_condition_jsonb := coalesce(nullif(btrim(filter_condition), ''), '{}')::jsonb;
  escaped_query_terms := private.pgroonga_escape_query_terms(query_terms);
  if cardinality(escaped_query_terms) = 0 then
    escaped_query_terms := private.pgroonga_escape_query_terms(array[query_text]);
  end if;
  effective_user_id := private.dataset_search_effective_user_id('');
  text_weight := coalesce(full_text_weight, 0) + coalesce(extracted_text_weight, 0);

  if normalized_data_source = 'tg' then
    visibility_clause := 'd.state_code = 100';
  elsif normalized_data_source = 'co' then
    visibility_clause := 'd.state_code = 200';
  elsif normalized_data_source = 'my' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := 'd.user_id = $4';
  elsif normalized_data_source = 'te' then
    if effective_user_id is null then
      return;
    end if;
    visibility_clause := $visibility$
      exists (
        select 1
        from public.roles role_row
        where role_row.user_id = $4
          and role_row.team_id = d.team_id
          and role_row.role::text in ('admin', 'member', 'owner')
      )
    $visibility$;
  else
    return;
  end if;

  json_filter_clause := case
    when filter_condition_jsonb = '{}'::jsonb then ''
    else 'and d.json @> $2'
  end;
  text_match_clause := case
    when cardinality(escaped_query_terms) = 0 then 'false'
    else 'd.extracted_text &@~| $1'
  end;

  hybrid_sql := format(
    $sql$
      with text_rows as materialized (
        select
          d.id,
          pgroonga_score(d.tableoid, d.ctid) as search_score
        from %1$s d
        where %2$s
          and %3$s
          %4$s
      ),
      text_scores as (
        select text_rows.id, max(text_rows.search_score) as search_score
        from text_rows
        group by text_rows.id
      ),
      text_matches as materialized (
        select
          rank() over (
            order by text_scores.search_score desc, text_scores.id
          )::bigint as text_rank,
          text_scores.id as text_id
        from text_scores
        order by text_scores.search_score desc, text_scores.id
        limit $5
      ),
      semantic as materialized (
        select
          candidate.rank as semantic_rank,
          candidate.id as semantic_id
        from private.semantic_simple_dataset_candidates(
          $6, $7, $8, $9, $10, $3
        ) candidate
      ),
      fused_raw as (
        select
          coalesce(text_matches.text_id, semantic.semantic_id) as id,
          coalesce(
            1.0 / ($11 + text_matches.text_rank),
            0.0
          ) * $12
          + coalesce(
            1.0 / ($11 + semantic.semantic_rank),
            0.0
          ) * $13 as score
        from text_matches
        full outer join semantic
          on text_matches.text_id = semantic.semantic_id
      ),
      fused as (
        select fused_raw.id, sum(fused_raw.score) as score
        from fused_raw
        where fused_raw.id is not null
        group by fused_raw.id
      ),
      visible_rows as (
        select d.*, fused.score
        from %1$s d
        join fused on fused.id = d.id
        where %3$s
      ),
      latest_rows as (
        select distinct on (visible_rows.id)
          visible_rows.id,
          visible_rows.json,
          visible_rows.version,
          visible_rows.modified_at,
          visible_rows.team_id,
          visible_rows.score
        from visible_rows
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
      limit $14
      offset ($15 - 1) * $14
    $sql$,
    p_table,
    text_match_clause,
    visibility_clause,
    json_filter_clause
  );

  return query execute hybrid_sql
    using escaped_query_terms, filter_condition_jsonb, normalized_data_source,
          effective_user_id, candidate_limit, p_table, query_embedding,
          filter_condition, match_threshold, semantic_match_count,
          normalized_rrf_k, text_weight, coalesce(semantic_weight, 0),
          normalized_page_size, normalized_page_current;
end;
$$;

alter function private.hybrid_search_simple_dataset(regclass, text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
revoke all on function private.hybrid_search_simple_dataset(regclass, text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) from public;
grant execute on function private.hybrid_search_simple_dataset(regclass, text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;

create or replace function public.semantic_search_contacts_v1(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.semantic_simple_dataset_search(
    'public.contacts'::regclass,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    data_source
  );
$$;

create or replace function public.semantic_search_flowproperties_v1(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.semantic_simple_dataset_search(
    'public.flowproperties'::regclass,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    data_source
  );
$$;

create or replace function public.semantic_search_sources_v1(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.semantic_simple_dataset_search(
    'public.sources'::regclass,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    data_source
  );
$$;

create or replace function public.semantic_search_unitgroups_v1(
  query_embedding text,
  filter_condition text default ''::text,
  match_threshold double precision default 0.5,
  match_count integer default 20,
  data_source text default 'tg'::text
) returns table(rank bigint, id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.semantic_simple_dataset_search(
    'public.unitgroups'::regclass,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    data_source
  );
$$;

create or replace function public.hybrid_search_contacts(
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
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.hybrid_search_simple_dataset(
    'public.contacts'::regclass,
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    full_text_weight,
    extracted_text_weight,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_flowproperties(
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
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.hybrid_search_simple_dataset(
    'public.flowproperties'::regclass,
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    full_text_weight,
    extracted_text_weight,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_sources(
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
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.hybrid_search_simple_dataset(
    'public.sources'::regclass,
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    full_text_weight,
    extracted_text_weight,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

create or replace function public.hybrid_search_unitgroups(
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
  page_current integer default 1,
  query_terms text[] default null::text[]
) returns table(id uuid, "json" jsonb, version character(9), modified_at timestamp with time zone, team_id uuid, total_count bigint)
language sql
set search_path to 'public', 'extensions', 'pg_temp'
set statement_timeout to '60s'
as $$
  select * from private.hybrid_search_simple_dataset(
    'public.unitgroups'::regclass,
    query_text,
    query_embedding,
    filter_condition,
    match_threshold,
    match_count,
    full_text_weight,
    extracted_text_weight,
    semantic_weight,
    rrf_k,
    data_source,
    page_size,
    page_current,
    query_terms
  );
$$;

alter function public.semantic_search_contacts_v1(text, text, double precision, integer, text) owner to postgres;
alter function public.semantic_search_flowproperties_v1(text, text, double precision, integer, text) owner to postgres;
alter function public.semantic_search_sources_v1(text, text, double precision, integer, text) owner to postgres;
alter function public.semantic_search_unitgroups_v1(text, text, double precision, integer, text) owner to postgres;

alter function public.hybrid_search_contacts(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_flowproperties(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_sources(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;
alter function public.hybrid_search_unitgroups(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) owner to postgres;

grant execute on function public.semantic_search_contacts_v1(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant execute on function public.semantic_search_flowproperties_v1(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant execute on function public.semantic_search_sources_v1(text, text, double precision, integer, text) to anon, authenticated, service_role;
grant execute on function public.semantic_search_unitgroups_v1(text, text, double precision, integer, text) to anon, authenticated, service_role;

grant execute on function public.hybrid_search_contacts(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_flowproperties(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_sources(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;
grant execute on function public.hybrid_search_unitgroups(text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) to anon, authenticated, service_role;

comment on function public.cmd_dataset_semantic_backfill(text, integer, uuid, text, boolean) is
  'Service-only bounded cursor RPC that enqueues missing foundation-dataset Markdown or embedding work without a synchronous table rewrite.';

comment on function private.semantic_simple_dataset_candidates(regclass, text, text, double precision, integer, text) is
  'Allow-listed, visibility-first HNSW candidate generator shared by contacts, flow properties, sources, and unit groups.';

comment on function private.hybrid_search_simple_dataset(regclass, text, text, text, double precision, integer, double precision, double precision, double precision, integer, text, integer, integer, text[]) is
  'Allow-listed PGroonga plus HNSW reciprocal-rank-fusion implementation for the four foundation dataset families.';
