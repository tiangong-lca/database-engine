begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public;

select plan(9);

select ok(
  strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'with text_matches as materialized') > 0,
  'flow latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'where f.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'from text_matches f') > 0,
  'flow latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'join lateral') > 0,
  'flow latest search keeps lateral latest-version lookup after candidate matching'
);

select ok(
  strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure), 'with text_matches as materialized') > 0,
  'process latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure), 'where p.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure), 'from text_matches p') > 0,
  'process latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text)'::regprocedure), 'join lateral') > 0,
  'process latest search keeps lateral latest-version lookup after candidate matching'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'with text_matches as materialized') > 0,
  'simple dataset latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'where d.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'from text_matches d') > 0,
  'simple dataset latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure), 'join lateral') > 0,
  'simple dataset latest search keeps lateral latest-version lookup after candidate matching'
);

select * from finish();

rollback;
