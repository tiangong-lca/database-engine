begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create temporary table derivative_webhook_calls (
  edge_function text not null,
  body jsonb not null,
  timeout_milliseconds integer not null
) on commit drop;

-- Keep the real trigger function and payload construction in the test path,
-- but replace the final pg_net boundary transactionally with a deterministic
-- call ledger. ROLLBACK restores the production implementation.
create or replace function util.invoke_edge_function(
  name text,
  body jsonb,
  timeout_milliseconds integer default ((5 * 60) * 1000)
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into pg_temp.derivative_webhook_calls (
    edge_function,
    body,
    timeout_milliseconds
  )
  values (name, body, timeout_milliseconds);
end;
$$;

create or replace function pg_temp.trigger_update_columns(
  p_table regclass,
  p_trigger_name text
) returns text
language sql
stable
as $$
  select string_agg(att.attname, ', ' order by att.attnum)
  from pg_trigger trg
  cross join lateral unnest(trg.tgattr::smallint[]) a(attnum)
  join pg_attribute att
    on att.attrelid = trg.tgrelid
   and att.attnum = a.attnum
  where trg.tgrelid = p_table
    and trg.tgname = p_trigger_name
    and not trg.tgisinternal
  group by trg.oid;
$$;

select plan(18);

select is(
  pg_temp.trigger_update_columns(
    'public.flows'::regclass,
    'flow_extract_md_trigger_update'
  ),
  'json, json_ordered',
  'flow markdown extraction covers direct and ordered JSON updates'
);

select is(
  pg_temp.trigger_update_columns(
    'public.processes'::regclass,
    'process_extract_md_trigger_update'
  ),
  'json, json_ordered',
  'process markdown extraction covers direct and ordered JSON updates'
);

select is(
  pg_temp.trigger_update_columns(
    'public.lifecyclemodels'::regclass,
    'lifecyclemodel_extract_md_trigger_update'
  ),
  'json, json_ordered',
  'lifecycle model markdown extraction covers direct and ordered JSON updates'
);

select is(
  (
    with expected(table_name, trigger_name, edge_function_name) as (
      values
        ('flows', 'flow_extract_md_trigger_update', 'webhook_flow_embedding_ft'),
        ('processes', 'process_extract_md_trigger_update', 'webhook_process_embedding_ft'),
        ('lifecyclemodels', 'lifecyclemodel_extract_md_trigger_update', 'webhook_model_embedding_ft')
    )
    select count(*)::integer
    from expected e
    join pg_class c on c.relname = e.table_name
    join pg_namespace cn on cn.oid = c.relnamespace and cn.nspname = 'public'
    join pg_trigger t on t.tgrelid = c.oid and t.tgname = e.trigger_name
    join pg_proc p on p.oid = t.tgfoid
    join pg_namespace pn on pn.oid = p.pronamespace
    where not t.tgisinternal
      and pn.nspname = 'util'
      and p.proname = 'invoke_edge_webhook'
      and lower(replace(pg_get_triggerdef(t.oid, true), '"', ''))
        like '%after update of json, json_ordered%'
      and lower(replace(pg_get_triggerdef(t.oid, true), '"', ''))
        like '%new.json is distinct from old.json%'
      and pg_get_triggerdef(t.oid, true) like '%' || e.edge_function_name || '%'
      and pg_get_triggerdef(t.oid, true) like '%''1000''%'
  ),
  3,
  'all three update hooks preserve timing, no-op guard, function, target, and timeout'
);

alter table public.flows disable trigger flow_dataset_extraction_trigger_insert;
alter table public.processes disable trigger process_extract_md_trigger_insert;
alter table public.lifecyclemodels disable trigger lifecyclemodel_extract_md_trigger_insert;

insert into public.flows (id, version, json_ordered)
values (
  'e7100000-0000-0000-0000-000000000001',
  '01.00.000',
  '{
    "flowDataSet": {
      "flowInformation": {
        "dataSetInformation": {
          "common:UUID": "e7100000-0000-0000-0000-000000000001"
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json
);

insert into public.processes (id, version, json_ordered)
values (
  'e7200000-0000-0000-0000-000000000001',
  '01.00.000',
  '{
    "processDataSet": {
      "processInformation": {
        "dataSetInformation": {
          "common:UUID": "e7200000-0000-0000-0000-000000000001"
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json
);

insert into public.lifecyclemodels (id, version, json_ordered)
values (
  'e7300000-0000-0000-0000-000000000001',
  '01.00.000',
  '{
    "lifeCycleModelDataSet": {
      "lifeCycleModelInformation": {
        "dataSetInformation": {
          "common:UUID": "e7300000-0000-0000-0000-000000000001"
        }
      },
      "administrativeInformation": {
        "publicationAndOwnership": {
          "common:dataSetVersion": "01.00.000"
        }
      }
    }
  }'::json
);

alter table public.flows enable trigger flow_dataset_extraction_trigger_insert;
alter table public.processes enable trigger process_extract_md_trigger_insert;
alter table public.lifecyclemodels enable trigger lifecyclemodel_extract_md_trigger_insert;

truncate pg_temp.derivative_webhook_calls;

update public.flows
set json = jsonb_set(json, '{derivativeProbe}', '"direct-json"')
where id = 'e7100000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.processes
set json = jsonb_set(json, '{derivativeProbe}', '"direct-json"')
where id = 'e7200000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.lifecyclemodels
set json = jsonb_set(json, '{derivativeProbe}', '"direct-json"')
where id = 'e7300000-0000-0000-0000-000000000001'
  and version = '01.00.000';

select is(
  (select count(*)::integer from pg_temp.derivative_webhook_calls),
  3,
  'direct json changes invoke one webhook per dataset table'
);

select is(
  (
    select count(distinct body->>'table')::integer
    from pg_temp.derivative_webhook_calls
    where body->>'type' = 'UPDATE'
  ),
  3,
  'direct json webhook payloads identify all three updated tables'
);

select is(
  (
    select count(distinct edge_function)::integer
    from pg_temp.derivative_webhook_calls
    where timeout_milliseconds = 1000
  ),
  3,
  'direct json webhook calls retain the three exact Edge targets and timeout'
);

truncate pg_temp.derivative_webhook_calls;

update public.flows
set json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"ordered-json"')::json
where id = 'e7100000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.processes
set json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"ordered-json"')::json
where id = 'e7200000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.lifecyclemodels
set json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"ordered-json"')::json
where id = 'e7300000-0000-0000-0000-000000000001'
  and version = '01.00.000';

select is(
  (select count(*)::integer from pg_temp.derivative_webhook_calls),
  3,
  'json_ordered changes invoke one webhook per dataset table'
);

select is(
  (
    select count(distinct body->>'table')::integer
    from pg_temp.derivative_webhook_calls
    where body->>'type' = 'UPDATE'
  ),
  3,
  'json_ordered webhook payloads identify all three updated tables'
);

select is(
  (
    select count(*)::integer
    from public.flows
    where id = 'e7100000-0000-0000-0000-000000000001'
      and json = json_ordered::jsonb
      and extracted_text is not distinct from
        util.dataset_json_search_text('flows', json)
  ),
  1,
  'flow ordered JSON keeps json and extracted_text synchronized'
);

select is(
  (
    select count(*)::integer
    from public.processes
    where id = 'e7200000-0000-0000-0000-000000000001'
      and json = json_ordered::jsonb
      and extracted_text is not distinct from
        util.dataset_json_search_text('processes', json)
  ),
  1,
  'process ordered JSON keeps json and extracted_text synchronized'
);

select is(
  (
    select count(*)::integer
    from public.lifecyclemodels
    where id = 'e7300000-0000-0000-0000-000000000001'
      and json = json_ordered::jsonb
      and extracted_text is not distinct from
        util.dataset_json_search_text('lifecyclemodels', json)
  ),
  1,
  'lifecycle model ordered JSON keeps json and extracted_text synchronized'
);

truncate pg_temp.derivative_webhook_calls;

update public.flows
set json = jsonb_set(json, '{derivativeProbe}', '"both-columns"'),
    json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"both-columns"')::json
where id = 'e7100000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.processes
set json = jsonb_set(json, '{derivativeProbe}', '"both-columns"'),
    json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"both-columns"')::json
where id = 'e7200000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.lifecyclemodels
set json = jsonb_set(json, '{derivativeProbe}', '"both-columns"'),
    json_ordered = jsonb_set(json_ordered::jsonb, '{derivativeProbe}', '"both-columns"')::json
where id = 'e7300000-0000-0000-0000-000000000001'
  and version = '01.00.000';

select is(
  (select count(*)::integer from pg_temp.derivative_webhook_calls),
  3,
  'one UPDATE naming both JSON columns invokes only once per row'
);

select is(
  (
    select count(*)::integer
    from (
      select body->>'table'
      from pg_temp.derivative_webhook_calls
      group by body->>'table'
      having count(*) = 1
    ) exactly_once
  ),
  3,
  'both-column updates produce one call for each dataset table'
);

truncate pg_temp.derivative_webhook_calls;

update public.flows
set json_ordered = json_ordered
where id = 'e7100000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.processes
set json_ordered = json_ordered
where id = 'e7200000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.lifecyclemodels
set json_ordered = json_ordered
where id = 'e7300000-0000-0000-0000-000000000001'
  and version = '01.00.000';

select is(
  (select count(*)::integer from pg_temp.derivative_webhook_calls),
  0,
  'same-value ordered JSON updates do not invoke markdown extraction'
);

update public.flows
set rule_verification = true
where id = 'e7100000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.processes
set rule_verification = true
where id = 'e7200000-0000-0000-0000-000000000001'
  and version = '01.00.000';

update public.lifecyclemodels
set rule_verification = true
where id = 'e7300000-0000-0000-0000-000000000001'
  and version = '01.00.000';

select is(
  (select count(*)::integer from pg_temp.derivative_webhook_calls),
  0,
  'unrelated changed columns do not invoke markdown extraction'
);

select is(
  (
    with expected(table_name, trigger_name, input_function) as (
      values
        ('flows', 'flow_embedding_ft_on_extract_md_update', 'flows_embedding_ft_input'),
        ('processes', 'process_embedding_ft_on_extract_md_update', 'processes_embedding_ft_input'),
        ('lifecyclemodels', 'lifecyclemodel_embedding_ft_on_extract_md_update', 'lifecyclemodels_embedding_ft_input')
    )
    select count(*)::integer
    from expected e
    join pg_class c on c.relname = e.table_name
    join pg_namespace cn on cn.oid = c.relnamespace and cn.nspname = 'public'
    join pg_trigger t on t.tgrelid = c.oid and t.tgname = e.trigger_name
    join pg_proc p on p.oid = t.tgfoid
    join pg_namespace pn on pn.oid = p.pronamespace
    where not t.tgisinternal
      and pn.nspname = 'util'
      and p.proname = 'queue_embeddings'
      and lower(replace(pg_get_triggerdef(t.oid, true), '"', ''))
        like '%after update of extracted_md%'
      and lower(replace(pg_get_triggerdef(t.oid, true), '"', ''))
        like '%old.extracted_md is distinct from new.extracted_md%'
      and pg_get_triggerdef(t.oid, true) like '%' || e.input_function || '%'
  ),
  3,
  'downstream extracted_md-to-embedding triggers remain unchanged'
);

select is(
  (
    select count(*)::integer
    from pg_trigger t
    join pg_proc p on p.oid = t.tgfoid
    join pg_namespace pn on pn.oid = p.pronamespace
    where t.tgname in (
      'flow_extract_md_trigger_update',
      'process_extract_md_trigger_update',
      'lifecyclemodel_extract_md_trigger_update'
    )
      and not t.tgisinternal
      and pn.nspname = 'util'
      and p.proname = 'invoke_edge_webhook'
  ),
  3,
  'all update triggers remain attached to the governed webhook function'
);

select * from finish();
rollback;
