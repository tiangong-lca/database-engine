begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

select plan(9);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'semantic_match_count integer') > 0,
  'flow hybrid declares a separate semantic match count'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        semantic_match_count,
        data_source') > 0,
  'flow hybrid passes the unexpanded semantic match count to semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_flows(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        candidate_limit,
        data_source') = 0,
  'flow hybrid no longer passes the 10x text candidate limit to semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'semantic_match_count integer') > 0,
  'process hybrid declares a separate semantic match count'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        semantic_match_count,
        data_source') > 0,
  'process hybrid passes the unexpanded semantic match count to semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_processes(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        candidate_limit,
        data_source') = 0,
  'process hybrid no longer passes the 10x text candidate limit to semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'semantic_match_count integer') > 0,
  'lifecyclemodel hybrid declares a separate semantic match count'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        semantic_match_count,
        data_source') > 0,
  'lifecyclemodel hybrid passes the unexpanded semantic match count to semantic candidates'
);

select ok(
  strpos(pg_get_functiondef('public.hybrid_search_lifecyclemodels(text,text,text,double precision,integer,double precision,double precision,double precision,integer,text,integer,integer)'::regprocedure), 'match_threshold,
        candidate_limit,
        data_source') = 0,
  'lifecyclemodel hybrid no longer passes the 10x text candidate limit to semantic candidates'
);

select * from finish();

rollback;
