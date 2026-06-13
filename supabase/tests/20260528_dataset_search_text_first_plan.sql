begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public;

select plan(16);

select ok(
  strpos(pg_get_functiondef('public.search_flows_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'private.search_flows_latest_impl') > 0,
  'flow public latest search delegates scanning to the private helper'
);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'with text_matches as materialized') > 0,
  'flow latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'where f.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'from text_matches f') > 0,
  'flow latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'join lateral') > 0,
  'flow latest search keeps lateral latest-version lookup after candidate matching'
);

select ok(
  strpos(pg_get_functiondef('public.search_processes_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'private.search_processes_latest_impl') > 0,
  'process public latest search delegates scanning to the private helper'
);

select ok(
  strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'with text_matches as materialized') > 0,
  'process latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'where p.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'from text_matches p') > 0,
  'process latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure), 'join lateral') > 0,
  'process latest search keeps lateral latest-version lookup after candidate matching'
);

select ok(
  strpos(pg_get_functiondef('public.search_lifecyclemodels_latest(text,jsonb,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'private.search_lifecyclemodels_latest_impl') > 0,
  'lifecyclemodel public latest search delegates scanning to the private helper'
);

select ok(
  strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'with text_matches as materialized') > 0,
  'lifecyclemodel latest search materializes text matches before non-text filters'
);

select ok(
  strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'where l.extracted_text &@~ $1') > 0
    and strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'from text_matches l') > 0,
  'lifecyclemodel latest search separates extracted_text scan from source/filter predicates'
);

select ok(
  strpos(pg_get_functiondef('private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure), 'join lateral') > 0,
  'lifecyclemodel latest search keeps lateral latest-version lookup after candidate matching'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_flows_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure),
  'flow private helper is security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_processes_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text,text[])'::regprocedure),
  'process private helper is security definer'
);

select ok(
  (select prosecdef from pg_proc where oid = 'private.search_lifecyclemodels_latest_impl(text,jsonb,bigint,bigint,text,text,uuid,integer,text[])'::regprocedure),
  'lifecyclemodel private helper is security definer'
);

select ok(
  not (select prosecdef from pg_proc where oid = 'public._search_simple_dataset_latest(regclass,text,jsonb,bigint,bigint,text,text,uuid,integer)'::regprocedure),
  'generic regclass search helper remains security invoker'
);

select * from finish();

rollback;
