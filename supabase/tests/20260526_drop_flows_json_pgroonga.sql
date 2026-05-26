begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(6);

select ok(
  to_regclass('public.flows_json_pgroonga') is null,
  'flow full-table JSON PGroonga index is removed from the create write path'
);

select ok(
  to_regclass('public.flows_public_json_pgroonga_idx') is not null,
  'flow open-data latest search keeps the state_code=100 partial JSON PGroonga index'
);

select ok(
  to_regclass('public.flows_co_json_pgroonga_idx') is not null,
  'flow collaborative latest search keeps the state_code=200 partial JSON PGroonga index'
);

select ok(
  exists (
    select 1
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname = 'flow_dataset_extraction_trigger_insert'
      and not tgisinternal
  ),
  'flow insert still queues compact dataset extraction jobs'
);

select ok(
  not exists (
    select 1
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname in ('flow_extract_md_trigger_insert', 'flow_extract_text_trigger_insert')
      and not tgisinternal
  ),
  'legacy full-row flow insert webhook triggers remain removed'
);

select is(
  (
    select count(*)
    from pg_trigger
    where tgrelid = 'public.flows'::regclass
      and tgname in ('flow_extract_md_trigger_update', 'flow_extract_text_trigger_update')
      and not tgisinternal
  ),
  2::bigint,
  'flow json update extraction triggers remain unchanged for the non-insert path'
);

select * from finish();

rollback;
