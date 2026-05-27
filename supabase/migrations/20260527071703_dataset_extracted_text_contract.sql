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
  ),
  scalar_text as (
    select distinct nullif(btrim(value #>> '{}'), '') as text_value
    from walk
    where jsonb_typeof(value) in ('string', 'number', 'boolean')
  )
  select coalesce(
    string_agg(text_value, ' ' order by lower(text_value), text_value),
    ''
  )
  from scalar_text
  where text_value is not null;
$$;

alter function util.dataset_json_search_text(jsonb) owner to postgres;
revoke all on function util.dataset_json_search_text(jsonb) from public;
grant execute on function util.dataset_json_search_text(jsonb) to service_role;

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
revoke all on function util.set_dataset_extracted_text_from_json() from public;
grant execute on function util.set_dataset_extracted_text_from_json() to service_role;

drop trigger if exists flow_extract_text_trigger_insert on public.flows;
drop trigger if exists flow_extract_text_trigger_update on public.flows;
drop trigger if exists process_extract_text_trigger_insert on public.processes;
drop trigger if exists process_extract_text_trigger_update on public.processes;
drop trigger if exists lifecyclemodels_extract_text_trigger_insert on public.lifecyclemodels;
drop trigger if exists lifecyclemodels_extract_text_trigger_update on public.lifecyclemodels;

drop trigger if exists zz_flows_extracted_text_sync_trigger on public.flows;
create trigger zz_flows_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.flows
  for each row execute function util.set_dataset_extracted_text_from_json();

drop trigger if exists zz_processes_extracted_text_sync_trigger on public.processes;
create trigger zz_processes_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.processes
  for each row execute function util.set_dataset_extracted_text_from_json();

drop trigger if exists zz_lifecyclemodels_extracted_text_sync_trigger on public.lifecyclemodels;
create trigger zz_lifecyclemodels_extracted_text_sync_trigger
  before insert or update of json, json_ordered on public.lifecyclemodels
  for each row execute function util.set_dataset_extracted_text_from_json();

create or replace function util.queue_dataset_extraction_jobs() returns trigger
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_entity_kind text;
  v_message_base jsonb;
begin
  if TG_TABLE_SCHEMA <> 'public' then
    raise exception 'dataset extraction jobs only support public schema, got %', TG_TABLE_SCHEMA;
  end if;

  v_entity_kind := case TG_TABLE_NAME
    when 'flows' then 'flow'
    when 'processes' then 'process'
    else null
  end;

  if v_entity_kind is null then
    raise exception 'unsupported dataset extraction table %', TG_TABLE_NAME;
  end if;

  v_message_base := jsonb_build_object(
    'schema', TG_TABLE_SCHEMA,
    'table', TG_TABLE_NAME,
    'id', NEW.id,
    'version', NEW.version,
    'entity_kind', v_entity_kind,
    'created_at', now()
  );

  perform pgmq.send(
    queue_name => 'dataset_extraction_jobs',
    msg => v_message_base || jsonb_build_object('extraction_kind', 'extracted_md')
  );

  return NEW;
end;
$$;

alter function util.queue_dataset_extraction_jobs() owner to postgres;

create or replace function public.cmd_dataset_extracted_text_backfill(
  p_table text,
  p_batch_size integer default 1000,
  p_after_id uuid default null,
  p_after_version text default null
) returns jsonb
language plpgsql
security definer
set search_path to ''
as $$
declare
  v_table text := lower(btrim(coalesce(p_table, '')));
  v_batch_size integer := least(greatest(coalesce(p_batch_size, 1000), 1), 5000);
  v_scanned_count integer := 0;
  v_updated_count integer := 0;
  v_last_id uuid;
  v_last_version text;
begin
  if v_table not in (
    'flows',
    'processes',
    'lifecyclemodels',
    'contacts',
    'sources',
    'unitgroups',
    'flowproperties'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'UNSUPPORTED_DATASET_TABLE',
      'message', format('Unsupported dataset table: %s', coalesce(p_table, '<null>'))
    );
  end if;

  execute format(
    $sql$
      with page as (
        select id, version
          from public.%I
         where json is not null
           and ($1 is null or (id, version) > ($1, coalesce($2, '')::character(9)))
         order by id, version
         limit $3
         for update skip locked
      ),
      updated as (
        update public.%I as dataset
           set extracted_text = util.dataset_json_search_text(dataset.json)
          from page
         where dataset.id = page.id
           and dataset.version = page.version
           and dataset.extracted_text is distinct from util.dataset_json_search_text(dataset.json)
         returning 1
      )
      select
        (select count(*)::integer from page),
        (select count(*)::integer from updated),
        (select id from page order by id desc, version desc limit 1),
        (select version::text from page order by id desc, version desc limit 1)
    $sql$,
    v_table,
    v_table
  )
  using p_after_id, p_after_version, v_batch_size
  into v_scanned_count, v_updated_count, v_last_id, v_last_version;

  return jsonb_build_object(
    'ok', true,
    'table', v_table,
    'scanned_count', v_scanned_count,
    'updated_count', v_updated_count,
    'last_id', v_last_id,
    'last_version', v_last_version,
    'has_more', v_scanned_count = v_batch_size
  );
end;
$$;

alter function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) owner to postgres;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) from public;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) from anon;
revoke all on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) from authenticated;
grant execute on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) to service_role;

comment on function util.dataset_json_search_text(jsonb) is
  'Builds deterministic dataset full-text search input from every scalar JSON value, preserving all authored languages without LLM summarization.';

comment on function util.set_dataset_extracted_text_from_json() is
  'Maintains extracted_text as deterministic searchable text from dataset JSON during INSERT/UPDATE.';

comment on function util.queue_dataset_extraction_jobs() is
  'Queues compact dataset extraction jobs for asynchronous extracted_md generation without carrying json/json_ordered in the transaction payload.';

comment on function public.cmd_dataset_extracted_text_backfill(text, integer, uuid, text) is
  'Service-role RPC for bounded historical extracted_text backfill. Call repeatedly with the returned cursor; migrations must not run full-table backfills synchronously.';
