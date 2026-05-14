begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(15);

create or replace function pg_temp.trigger_update_columns(
  p_table regclass,
  p_trigger_name text
) returns text
language sql
stable
as $$
  select coalesce(
    string_agg(att.attname, ', ' order by att.attnum),
    '<all update columns>'
  )
  from pg_trigger trg
  left join lateral unnest(trg.tgattr::smallint[]) a(attnum) on true
  left join pg_attribute att
    on att.attrelid = trg.tgrelid
   and att.attnum = a.attnum
  where trg.tgrelid = p_table
    and trg.tgname = p_trigger_name
    and not trg.tgisinternal
  group by trg.oid;
$$;

select is(
  pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_json_sync_trigger'),
  'json_ordered',
  'flows json sync only runs when json_ordered is updated'
);

select is(
  pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_json_sync_trigger'),
  'json_ordered',
  'lifecyclemodels json sync only runs when json_ordered is updated'
);

select is(
  pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_json_sync_trigger'),
  'json_ordered',
  'processes json sync only runs when json_ordered is updated'
);

select isnt(
  pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger'),
  '<all update columns>',
  'flows modified_at trigger is column-scoped'
);

select isnt(
  pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger'),
  '<all update columns>',
  'lifecyclemodels modified_at trigger is column-scoped'
);

select isnt(
  pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger'),
  '<all update columns>',
  'processes modified_at trigger is column-scoped'
);

select ok(
  pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') like '%json%'
  and pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') like '%state_code%',
  'flows modified_at still covers business fields'
);

select ok(
  pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') like '%json%'
  and pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') like '%state_code%',
  'lifecyclemodels modified_at still covers business fields'
);

select ok(
  pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') like '%json%'
  and pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') like '%state_code%',
  'processes modified_at still covers business fields'
);

select ok(
  pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') not like '%embedding_ft%'
  and pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') not like '%embedding_at%'
  and pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') not like '%extracted_text%'
  and pg_temp.trigger_update_columns('public.flows'::regclass, 'flows_set_modified_at_trigger') not like '%extracted_md%',
  'flows modified_at skips derived embedding and extraction columns'
);

select ok(
  pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') not like '%embedding_ft%'
  and pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') not like '%embedding_at%'
  and pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') not like '%extracted_text%'
  and pg_temp.trigger_update_columns('public.lifecyclemodels'::regclass, 'lifecyclemodels_set_modified_at_trigger') not like '%extracted_md%',
  'lifecyclemodels modified_at skips derived embedding and extraction columns'
);

select ok(
  pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') not like '%embedding_ft%'
  and pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') not like '%embedding_at%'
  and pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') not like '%extracted_text%'
  and pg_temp.trigger_update_columns('public.processes'::regclass, 'processes_set_modified_at_trigger') not like '%extracted_md%',
  'processes modified_at skips derived embedding and extraction columns'
);

select is(
  (select (tgtype & 4) <> 0 from pg_trigger where tgrelid = 'public.flows'::regclass and tgname = 'flows_json_sync_trigger'),
  true,
  'flows json sync still runs on insert'
);

select is(
  (select (tgtype & 4) <> 0 from pg_trigger where tgrelid = 'public.lifecyclemodels'::regclass and tgname = 'lifecyclemodels_json_sync_trigger'),
  true,
  'lifecyclemodels json sync still runs on insert'
);

select is(
  (select (tgtype & 4) <> 0 from pg_trigger where tgrelid = 'public.processes'::regclass and tgname = 'processes_json_sync_trigger'),
  true,
  'processes json sync still runs on insert'
);

select * from finish();

rollback;
