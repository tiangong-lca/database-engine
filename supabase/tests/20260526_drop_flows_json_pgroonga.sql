begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(6);

select ok(
  to_regclass('public.flows_json_pgroonga') is null,
  'flow full-table JSON PGroonga index is removed from the create write path'
);

select ok(
  to_regclass('public.flows_text_pgroonga') is not null,
  'flow latest search keeps the extracted_text PGroonga index'
);

select ok(
  to_regclass('public.flows_public_json_pgroonga_idx') is null
    and to_regclass('public.flows_co_json_pgroonga_idx') is null,
  'flow latest search no longer keeps partial JSON PGroonga indexes'
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
      and tgname = 'flow_extract_md_trigger_update'
      and not tgisinternal
  ),
  1::bigint,
  'flow json update keeps only the markdown extraction webhook trigger'
);

select * from finish();

rollback;
