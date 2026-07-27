begin;

create extension if not exists pgtap with schema extensions;
set local search_path = extensions, public, auth;

create temporary table pg_temp.embedding_selector_function as
select
  lower(pg_get_functiondef(routine.oid)) as definition,
  pg_get_userbyid(routine.proowner) as owner_name,
  obj_description(routine.oid, 'pg_proc') as description
from pg_proc routine
where routine.oid = 'util.process_embeddings(integer,integer,integer)'::regprocedure;

select plan(8);

select is(
  (select count(*)::integer from pg_temp.embedding_selector_function),
  1,
  'the governed embedding dispatcher signature exists exactly once'
);

select is(
  (select owner_name from pg_temp.embedding_selector_function),
  'postgres',
  'the embedding dispatcher remains owned by postgres'
);

select ok(
  (select definition like '%visible_jobs as materialized%'
    from pg_temp.embedding_selector_function),
  'visible embedding jobs are materialized once per selector snapshot'
);

select ok(
  (select definition like '%visible_scopes as materialized%'
    from pg_temp.embedding_selector_function),
  'queue policy lookup is driven by distinct visible scopes'
);

select ok(
  (select definition like '%active_counts as materialized%'
    from pg_temp.embedding_selector_function),
  'active embedding jobs are aggregated once per scope'
);

select ok(
  (select definition ~ 'active_counts as materialized[[:space:]]*\([\s\S]*group by 1, 2, 3, 4'
    from pg_temp.embedding_selector_function),
  'active counts group by the complete embedding policy scope'
);

select ok(
  (select definition !~ 'cross join lateral[[:space:]]*\([[:space:]]*select[[:space:]]+count\(\*\)::integer[[:space:]]+as[[:space:]]+active_count'
    from pg_temp.embedding_selector_function),
  'the selector does not rescan the full active queue for every visible job'
);

select ok(
  (select
      definition like '%pg_try_advisory_xact_lock(hashtext(''util.process_embeddings''))%'
      and definition like '%scope_position <= (max_in_flight - active_count)%'
    from pg_temp.embedding_selector_function),
  'the dispatcher retains its singleton lock and per-scope admission bound'
);

select * from finish();
rollback;
