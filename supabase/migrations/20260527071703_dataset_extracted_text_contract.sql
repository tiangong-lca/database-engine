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

update public.flows as f
   set extracted_text = util.dataset_json_search_text(f.json)
 where f.json is not null
   and f.extracted_text is distinct from util.dataset_json_search_text(f.json);

update public.processes as p
   set extracted_text = util.dataset_json_search_text(p.json)
 where p.json is not null
   and p.extracted_text is distinct from util.dataset_json_search_text(p.json);

update public.lifecyclemodels as lm
   set extracted_text = util.dataset_json_search_text(lm.json)
 where lm.json is not null
   and lm.extracted_text is distinct from util.dataset_json_search_text(lm.json);

update public.contacts as c
   set extracted_text = util.dataset_json_search_text(c.json)
 where c.json is not null
   and c.extracted_text is distinct from util.dataset_json_search_text(c.json);

update public.sources as s
   set extracted_text = util.dataset_json_search_text(s.json)
 where s.json is not null
   and s.extracted_text is distinct from util.dataset_json_search_text(s.json);

update public.unitgroups as u
   set extracted_text = util.dataset_json_search_text(u.json)
 where u.json is not null
   and u.extracted_text is distinct from util.dataset_json_search_text(u.json);

update public.flowproperties as fp
   set extracted_text = util.dataset_json_search_text(fp.json)
 where fp.json is not null
   and fp.extracted_text is distinct from util.dataset_json_search_text(fp.json);

comment on function util.dataset_json_search_text(jsonb) is
  'Builds deterministic dataset full-text search input from every scalar JSON value, preserving all authored languages without LLM summarization.';

comment on function util.set_dataset_extracted_text_from_json() is
  'Maintains extracted_text as deterministic searchable text from dataset JSON during INSERT/UPDATE.';

comment on function util.queue_dataset_extraction_jobs() is
  'Queues compact dataset extraction jobs for asynchronous extracted_md generation without carrying json/json_ordered in the transaction payload.';
